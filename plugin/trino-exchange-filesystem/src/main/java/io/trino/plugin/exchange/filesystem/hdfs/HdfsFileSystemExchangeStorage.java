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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeFutures.translateFailures;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HdfsFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private final ExchangeHdfsEnvironment hdfsEnvironment;
    private final int blockSize;
    private final Executor executor;

    @Inject
    public HdfsFileSystemExchangeStorage(
            ExchangeHdfsEnvironment hdfsEnvironment,
            ExchangeHdfsConfig config)
    {
        requireNonNull(config, "config is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.blockSize = toIntExact(config.getHdfsStorageBlockSize().toBytes());
        this.executor = new BoundedExecutor(newCachedThreadPool(daemonThreadsNamed("hdfs-exchange-storage-%s")), config.getMaxBackgroundThreads());
    }

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        WriteUtils.createDirectory(hdfsEnvironment, new Path(dir));
    }

    @Override
    public ExchangeStorageReader createExchangeStorageReader(Queue<ExchangeSourceFile> sourceFiles, int maxPageStorageSize)
    {
        return new HdfsExchangeStorageReader(hdfsEnvironment, sourceFiles, blockSize, maxPageStorageSize, executor);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file, Optional<SecretKey> secretKey)
    {
        return new HdfsExchangeStorageWriter(hdfsEnvironment, file, blockSize, secretKey, executor);
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        return Futures.submit(() -> WriteUtils.createEmptyFile(hdfsEnvironment, new Path(file)), executor);
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        return Futures.submit(() ->
                directories.forEach(dir -> {
                    try {
                        WriteUtils.checkedDelete(hdfsEnvironment, new Path(dir), true);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }), executor);
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        return Futures.submit(() -> {
            ImmutableList.Builder<FileStatus> builder = ImmutableList.builder();
            Path p = new Path(dir);
            RemoteIterator<LocatedFileStatus> iterator = hdfsEnvironment.getFileSystem(p).listFiles(p, true);
            LocatedFileStatus status;
            while (iterator.hasNext()) {
                status = iterator.next();
                if (status.isFile()) {
                    builder.add(new FileStatus(status.getPath().toString(), status.getLen()));
                }
            }
            return builder.build();
        }, executor);
    }

    @Override
    public int getWriteBufferSize()
    {
        return blockSize;
    }

    @PreDestroy
    @Override
    public void close()
            throws IOException
    {
    }

    @ThreadSafe
    private static class HdfsExchangeStorageReader
            implements ExchangeStorageReader
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(HdfsFileSystemExchangeStorage.HdfsExchangeStorageReader.class).instanceSize();
        private final ExchangeHdfsEnvironment hdfsEnvironment;
        private final Queue<ExchangeSourceFile> sourceFiles;
        private final int blockSize;
        private final int bufferSize;

        @GuardedBy("this")
        private ExchangeSourceFile currentFile;
        @GuardedBy("this")
        private long fileOffset;
        @GuardedBy("this")
        private SliceInput sliceInput;
        @GuardedBy("this")
        private int sliceSize = -1;
        private volatile boolean closed;
        private volatile long bufferRetainedSize;
        private volatile ListenableFuture<Void> inProgressReadFuture = immediateVoidFuture();
        private final BackgroundHdfsDownloader backgroundHdfsDownloader;

        public HdfsExchangeStorageReader(
                ExchangeHdfsEnvironment hdfsEnvironment,
                Queue<ExchangeSourceFile> sourceFiles,
                int blockSize,
                int maxPageStorageSize,
                Executor executor)
        {
            this.hdfsEnvironment = hdfsEnvironment;
            this.sourceFiles = requireNonNull(sourceFiles, "sourceFiles is null");
            this.blockSize = blockSize;
            // Make sure buffer can accommodate at least one complete Slice, and keep reads aligned to part boundaries
            this.bufferSize = maxPageStorageSize + blockSize;
            this.backgroundHdfsDownloader = new BackgroundHdfsDownloader(hdfsEnvironment, blockSize, executor);

            this.backgroundHdfsDownloader.start();
            fillBuffer();
        }

        @Override
        public synchronized Slice read()
                throws IOException
        {
            if (closed || !inProgressReadFuture.isDone()) {
                return null;
            }

            try {
                getFutureValue(inProgressReadFuture);
            }
            catch (RuntimeException e) {
                throw new IOException(e);
            }

            if (sliceSize < 0) {
                sliceSize = sliceInput.readInt();
            }
            Slice data = sliceInput.readSlice(sliceSize);

            if (sliceInput.available() > Integer.BYTES) {
                sliceSize = sliceInput.readInt();
                if (sliceInput.available() < sliceSize) {
                    fillBuffer();
                }
            }
            else {
                sliceSize = -1;
                fillBuffer();
            }

            return data;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            // rely on FileSystemExchangeSource implementation to wrap with nonCancellationPropagating
            return inProgressReadFuture;
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE + bufferRetainedSize;
        }

        @Override
        public boolean isFinished()
        {
            return closed;
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;

            currentFile = null;
            sliceInput = null;
            bufferRetainedSize = 0;
            inProgressReadFuture.cancel(true);
            inProgressReadFuture = immediateVoidFuture(); // such that we don't retain reference to the buffer
            backgroundHdfsDownloader.stop();
        }

        @GuardedBy("this")
        private void fillBuffer()
        {
            if (currentFile == null || fileOffset == currentFile.getFileSize()) {
                currentFile = sourceFiles.poll();
                if (currentFile == null) {
                    close();
                    return;
                }
                fileOffset = 0;
            }

            byte[] buffer = new byte[bufferSize];
            int bufferFill = 0;
            if (sliceInput != null) {
                int length = sliceInput.available();
                sliceInput.readBytes(buffer, 0, length);
                bufferFill += length;
            }

            Configuration conf = hdfsEnvironment.getHdfsConfiguration();
            ImmutableList.Builder<ListenableFuture<Void>> readFutures = ImmutableList.builder();
            while (true) {
                long fileSize = currentFile.getFileSize();
                // Make sure hdfs read request byte ranges align with block sizes for best performance
                int readableBlocks = (buffer.length - bufferFill) / blockSize;
                if (readableBlocks == 0) {
                    if (buffer.length - bufferFill >= fileSize - fileOffset) {
                        readableBlocks = 1;
                    }
                    else {
                        break;
                    }
                }

                Path f = new Path(currentFile.getFileUri());
                Optional<SecretKey> secretKey = currentFile.getSecretKey();
                int cryptoHeaderSize = CryptoUtils.getCryptoHeaderSize(secretKey);
                // Blocks start with iv data if crypto is enabled
                for (int i = 0; i < readableBlocks && fileOffset < fileSize; ++i) {
                    int length = (int) min(blockSize, fileSize - fileOffset);
                    int dataLength = length - cryptoHeaderSize;
                    DownloadTaskEnvironment taskEnvironment = new DownloadTaskEnvironment(hdfsEnvironment, secretKey, f, fileOffset, buffer, bufferFill, dataLength);
                    ListenableFuture<Void> future = backgroundHdfsDownloader.submit(taskEnvironment);
                    readFutures.add(future);
                    bufferFill += dataLength;
                    fileOffset += length;
                }

                if (fileOffset == fileSize) {
                    currentFile = sourceFiles.poll();
                    if (currentFile == null) {
                        break;
                    }
                    fileOffset = 0;
                }
            }

            inProgressReadFuture = asVoid(Futures.allAsList(readFutures.build()));
            sliceInput = Slices.wrappedBuffer(buffer, 0, bufferFill).getInput();
            bufferRetainedSize = sliceInput.getRetainedSize();
        }
    }

    static class DownloadTaskEnvironment
    {
        final ExchangeHdfsEnvironment hdfsEnvironment;
        final Optional<SecretKey> secretKey;
        final Path file;
        final long fileOffset;

        final byte[] buffer;
        final int offset;
        final int length;

        public DownloadTaskEnvironment(
                ExchangeHdfsEnvironment hdfsEnvironment,
                Optional<SecretKey> secretKey,
                Path file,
                long fileOffset,
                byte[] buffer,
                int offset,
                int length)
        {
            this.hdfsEnvironment = hdfsEnvironment;
            this.secretKey = secretKey;
            this.file = file;
            this.fileOffset = fileOffset;
            this.buffer = buffer;
            this.offset = offset;
            this.length = length;
        }
    }

    @NotThreadSafe
    private static class HdfsExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final Logger log = Logger.get(HdfsExchangeStorageWriter.class);
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(HdfsFileSystemExchangeStorage.HdfsExchangeStorageWriter.class).instanceSize();

        private final FSDataOutputStream outputStream;
        private final BackgroundHdfsUploader backgroundHdfsUploader;
        private final List<ListenableFuture<Void>> uploadFutures = new ArrayList<>();
        private volatile boolean closed;

        public HdfsExchangeStorageWriter(
                ExchangeHdfsEnvironment hdfsEnvironment,
                URI file,
                int blockSize,
                Optional<SecretKey> secretKey,
                Executor executor)
        {
            Path f = new Path(requireNonNull(file, "file is null"));
            try {
                this.outputStream = hdfsEnvironment.getFileSystem(f).create(f, true);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.backgroundHdfsUploader = new BackgroundHdfsUploader(hdfsEnvironment, outputStream, blockSize, secretKey, executor);
            this.backgroundHdfsUploader.start();
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
        {
            if (closed) {
                // Ignore writes after writer is closed
                return immediateVoidFuture();
            }
            ListenableFuture<Void> future = backgroundHdfsUploader.submit(slice);
            uploadFutures.add(future);
            return translateFailures(future);
        }

        @Override
        public ListenableFuture<Void> finish()
        {
            if (closed) {
                return immediateVoidFuture();
            }

            ListenableFuture<Void> finishFuture = translateFailures(Futures.transformAsync(
                    Futures.allAsList(uploadFutures),
                    ignore -> Futures.submit(() -> {
                        try {
                            outputStream.close();
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }, directExecutor()), directExecutor()));

            Futures.addCallback(finishFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(Void result)
                {
                    closed = true;
                    backgroundHdfsUploader.stop();
                }

                @Override
                public void onFailure(Throwable ignored)
                {
                    // Rely on caller to abort in case of exceptions during finish
                }
            }, directExecutor());

            return finishFuture;
        }

        @Override
        public ListenableFuture<Void> abort()
        {
            if (closed) {
                return immediateVoidFuture();
            }
            closed = true;
            backgroundHdfsUploader.stop();
            uploadFutures.forEach(future -> future.cancel(true));

            try {
                outputStream.close();
            }
            catch (IOException e) {
                log.warn(e, "Failed to close outputStream %s", outputStream);
            }

            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE;
        }
    }
}
