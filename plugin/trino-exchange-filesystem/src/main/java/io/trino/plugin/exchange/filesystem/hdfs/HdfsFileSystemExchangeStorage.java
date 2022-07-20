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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;
import io.trino.spi.VersionEmbedder;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.crypto.CryptoFSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HdfsFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private final ExchangeHdfsEnvironment hdfsEnvironment;
    private final int blockSize;
    private final Executor executor;

    public static final byte[] IV;

    static {
        int len = CipherSuite.AES_CTR_NOPADDING.getAlgorithmBlockSize();
        IV = new byte[len];
        for (int i = 0; i < len; i++) {
            IV[i] = (byte) i;
        }
    }


    @Inject
    public HdfsFileSystemExchangeStorage(
            ExchangeHdfsEnvironment hdfsEnvironment,
            ExchangeHdfsConfig config,
            ExecutorService executorService,
            VersionEmbedder versionEmbedder)
    {
        requireNonNull(config, "config is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.blockSize = toIntExact(config.getHdfsStorageBlockSize().toBytes());
        this.executor = versionEmbedder.embedVersion(new BoundedExecutor(executorService, config.getMaxBackgroundThreads()));
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
        return null;
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
            LocatedFileStatus fileStatus;
            while (iterator.hasNext()) {
                fileStatus = iterator.next();
                if (fileStatus.isFile()) {
                    builder.add(new FileStatus(fileStatus.getPath().toString(), fileStatus.getLen()));
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
        private final Executor executor;

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
            this.executor = executor;

            // Safe publication of S3ExchangeStorageReader is required as it's a mutable class
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
                for (int i = 0; i < readableBlocks && fileOffset < fileSize; ++i) {
                    int length = (int) min(blockSize, fileSize - fileOffset);
                    int bufferOffset = bufferFill;
                    Futures.submit(() -> {
                        CryptoCodec codec = null;
                        try (FSDataInputStream in = hdfsEnvironment.getFileSystem(f).open(f)) {
                            FSDataInputStream in2 = in;
                            if (secretKey.isPresent()) {
                                byte[] iv = HdfsFileSystemExchangeStorage.IV.clone();
                                codec = CryptoCodec.getInstance(hdfsEnvironment.getHdfsConfiguration(), CipherSuite.AES_CTR_NOPADDING);
                                in2 = new CryptoFSDataInputStream(in, codec, secretKey.get().getEncoded(), iv);
                            }
                            in2.seek(fileSize);
                            IOUtils.readFully(in2, buffer, bufferOffset, length);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        finally {
                            if (codec != null) {
                                try {
                                    codec.close();
                                }
                                catch (IOException ignore) {
                                }
                            }
                        }
                    }, executor);

                    bufferFill += length;
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
}
