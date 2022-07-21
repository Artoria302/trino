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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class BackgroundHdfsUploader
        implements Runnable
{
    private static final Logger log = Logger.get(BackgroundHdfsUploader.class);

    private final Configuration conf;
    private final Optional<SecretKey> secretKey;
    private final FSDataOutputStream outputStream;
    private FSDataOutputStream wrappedOutputStream;
    private final int blockSize;
    private final int cryptoHeaderSize;
    private int curBlockOffset;

    private volatile Thread currentThread;
    private volatile boolean stopped;
    private final BlockingQueue<BackgroundTask> queue = new LinkedBlockingQueue<>();

    public BackgroundHdfsUploader(
            ExchangeHdfsEnvironment hdfsEnvironment,
            FSDataOutputStream outputStream,
            int blockSize,
            Optional<SecretKey> secretKey)
    {
        requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.conf = hdfsEnvironment.getHdfsConfiguration();
        this.blockSize = blockSize;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.cryptoHeaderSize = CryptoUtils.getCryptoHeaderSize(secretKey);
        this.outputStream = requireNonNull(outputStream, "outputStream is null");
    }

    public ListenableFuture<Void> submit(Slice slice)
    {
        if (stopped) {
            return immediateVoidFuture();
        }
        SettableFuture<Void> completionFuture = SettableFuture.create();
        BackgroundTask task = new BackgroundTask(slice, completionFuture);
        boolean added = queue.add(task);
        if (!added) {
            log.error("Failed to add upload task to queue");
            completionFuture.setException(new RuntimeException("Failed to add upload task to queue"));
        }
        return completionFuture;
    }

    @Override
    public void run()
    {
        currentThread = Thread.currentThread();
        BackgroundTask task;
        while (!stopped) {
            try {
                task = queue.take();
                if (task.completionFuture.isCancelled()) {
                    continue;
                }
                try {
                    writeSlice(task);
                    task.completionFuture.set(null);
                }
                catch (Throwable t) {
                    task.completionFuture.setException(t);
                }
            }
            catch (InterruptedException ignore) {
            }
        }
    }

    private void writeSlice(BackgroundTask task)
            throws IOException
    {
        Slice slice = task.slice;
        byte[] buffer = slice.getBytes();
        int offset = 0;
        int remain = slice.length();
        int length;
        while (remain > 0) {
            if (curBlockOffset == 0) {
                wrappedOutputStream = CryptoUtils.wrapIfNecessary(conf, secretKey, outputStream, false);
                curBlockOffset = cryptoHeaderSize;
            }
            length = min(blockSize - curBlockOffset, remain);
            checkState(length > 0, "Try to write non positive length buffer, length: %d", length);
            remain -= length;
            curBlockOffset = (curBlockOffset + length) % blockSize;
            wrappedOutputStream.write(buffer, offset, length);
            offset += length;
        }
        checkState(remain == 0, "Remain non zero length buffer to write, length: %d", remain);
    }

    public void stop()
    {
        if (stopped) {
            return;
        }
        stopped = true;
        if (currentThread != null) {
            currentThread.interrupt();
        }
    }

    private static class BackgroundTask
    {
        final Slice slice;
        final SettableFuture<Void> completionFuture;

        BackgroundTask(Slice slice, SettableFuture<Void> completionFuture)
        {
            this.slice = slice;
            this.completionFuture = completionFuture;
        }
    }
}
