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
import io.airlift.slice.Slice;
import io.trino.plugin.exchange.filesystem.hdfs.util.CryptoUtils;
import io.trino.plugin.exchange.filesystem.hdfs.util.ListenableLinkedListBlockingQueue;
import io.trino.plugin.exchange.filesystem.hdfs.util.ListenableLinkedListBlockingQueue.DequeueStatus;
import io.trino.plugin.exchange.filesystem.hdfs.util.ListenableTask;
import io.trino.plugin.exchange.filesystem.hdfs.util.ResumableTask;
import io.trino.plugin.exchange.filesystem.hdfs.util.ResumableTasks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class BackgroundHdfsWriter
{
    private static final Logger log = Logger.get(BackgroundHdfsWriter.class);

    private final Configuration conf;
    private final FSDataOutputStream out;

    private final ListenableLinkedListBlockingQueue<ListenableTask> queue;
    private final Executor executor;
    private boolean stopped;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public BackgroundHdfsWriter(
            ExchangeHdfsEnvironment hdfsEnvironment,
            Path file,
            Optional<SecretKey> secretKey,
            Executor executor)
    {
        requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.conf = hdfsEnvironment.getHdfsConfiguration();
        requireNonNull(secretKey, "secretKey is null");
        requireNonNull(file, "file is null");
        try {
            this.out = CryptoUtils.wrapIfNecessary(conf, secretKey, hdfsEnvironment.getFileSystem(file).create(file), true);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.executor = requireNonNull(executor, "executor is null");
        this.queue = new ListenableLinkedListBlockingQueue<>(executor);
    }

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            ResumableTasks.submit(executor, new ResumableWriteTask());
        }
    }

    public ListenableFuture<Void> submit(Slice slice)
    {
        if (stopped) {
            return immediateVoidFuture();
        }
        ListenableTask task = new WriteTask(slice);
        SettableFuture<Void> completionFuture = task.getCompletionFuture();
        boolean added = queue.offer(task);
        if (!added) {
            log.error("Failed to add upload task to queue");
            completionFuture.setException(new RuntimeException("Failed to add upload task to queue"));
        }
        return completionFuture;
    }

    public void stop()
    {
        if (stopped) {
            return;
        }
        stopped = true;
        queue.signalWaiting();
    }

    public void close()
            throws IOException
    {
        out.close();
    }

    private class ResumableWriteTask
            implements ResumableTask
    {
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
    }

    private class WriteTask
            extends ListenableTask
    {
        final Slice slice;

        WriteTask(Slice slice)
        {
            this.slice = slice;
        }

        @Override
        protected void internalProcess()
                throws IOException
        {
            out.write(slice.getBytes());
        }
    }
}
