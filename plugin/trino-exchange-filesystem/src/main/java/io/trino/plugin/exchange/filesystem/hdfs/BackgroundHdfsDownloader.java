/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exchange.filesystem.hdfs;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.trino.plugin.exchange.filesystem.hdfs.HdfsFileSystemExchangeStorage.DownloadTaskEnvironment;
import io.trino.plugin.exchange.filesystem.hdfs.util.ListenableLinkedListBlockingQueue;
import io.trino.plugin.exchange.filesystem.hdfs.util.ListenableLinkedListBlockingQueue.DequeueStatus;
import io.trino.plugin.exchange.filesystem.hdfs.util.ListenableTask;
import io.trino.plugin.exchange.filesystem.hdfs.util.ResumableTask;
import io.trino.plugin.exchange.filesystem.hdfs.util.ResumableTasks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class BackgroundHdfsDownloader
        implements ResumableTask
{
    private static final Logger log = Logger.get(BackgroundHdfsDownloader.class);

    private final Configuration conf;
    private final int blockSize;
    private volatile int curBlockOffset;

    private final ListenableLinkedListBlockingQueue<ListenableTask> queue;
    private final Executor executor;
    private boolean stopped;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public BackgroundHdfsDownloader(
            ExchangeHdfsEnvironment hdfsEnvironment,
            int blockSize,
            Executor executor)
    {
        requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.conf = hdfsEnvironment.getHdfsConfiguration();
        this.blockSize = blockSize;
        this.executor = requireNonNull(executor, "executor is null");
        this.queue = new ListenableLinkedListBlockingQueue<>(executor);
    }

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            ResumableTasks.submit(executor, this);
        }
    }

    public ListenableFuture<Void> submit(DownloadTaskEnvironment taskEnvironment)
    {
        if (stopped) {
            return immediateVoidFuture();
        }
        ListenableTask task = new DownloadTask(taskEnvironment);
        SettableFuture<Void> completionFuture = task.getCompletionFuture();
        boolean added = queue.offer(task);
        if (!added) {
            log.error("Failed to add upload task to queue");
            completionFuture.setException(new RuntimeException("Failed to add upload task to queue"));
        }
        return completionFuture;
    }

    @Override
    public TaskStatus process()
    {
        DequeueStatus<ListenableTask> status;
        ListenableTask task;
        while (true) {
            if (stopped) {
                return TaskStatus.finished();
            }
            try {
                status = queue.pollListenable(10, TimeUnit.MILLISECONDS);
                task = status.getElement();
                if (task == null) {
                    // make sure task can receive stop signal
                    if (stopped) {
                        return TaskStatus.finished();
                    }
                    return TaskStatus.continueOn(status.getListenableFuture());
                }
                task.process();
            }
            catch (InterruptedException ignore) {
            }
        }
    }

    public void stop()
    {
        if (stopped) {
            return;
        }
        stopped = true;
        queue.signalWaiting();
    }

    private class DownloadTask
            extends ListenableTask
    {
        private final DownloadTaskEnvironment taskEnvironment;

        DownloadTask(DownloadTaskEnvironment taskEnvironment)
        {
            this.taskEnvironment = taskEnvironment;
        }

        @Override
        protected void internalProcess()
                throws IOException
        {
            ExchangeHdfsEnvironment hdfsEnvironment = taskEnvironment.hdfsEnvironment;
            long fileOffset = taskEnvironment.fileOffset;
            Path file = taskEnvironment.file;
            Optional<SecretKey> secretKey = taskEnvironment.secretKey;
            byte[] buffer = taskEnvironment.buffer;
            int offset = taskEnvironment.offset;
            int length = taskEnvironment.length;
            try (FSDataInputStream in = CryptoUtils.wrapIfNecessary(conf, secretKey, hdfsEnvironment.getFileSystem(file).open(file), fileOffset)) {
                in.readFully(buffer, offset, length);
            }
        }
    }
}
