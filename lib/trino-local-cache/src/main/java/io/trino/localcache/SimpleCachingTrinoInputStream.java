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
package io.trino.localcache;

import com.google.common.primitives.Longs;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.localcache.CacheResult;
import io.trino.spi.localcache.FileIdentifier;
import io.trino.spi.localcache.FileReadRequest;
import jakarta.annotation.Nonnull;

import java.io.EOFException;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FSExceptionMessages.CANNOT_SEEK_PAST_EOF;
import static org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_SEEK;
import static org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED;

public class SimpleCachingTrinoInputStream
        extends TrinoInputStream
{
    private final TrinoInputFile inputFile;
    private final FileIdentifier fileIdentifier;
    private final CacheManager cacheManager;
    private final long fileLength;
    private long position;
    private boolean closed;
    private TrinoInputStream delegate;
    private byte[] oneByteBuf; // used for 'int read()'

    public SimpleCachingTrinoInputStream(TrinoInputFile inputFile, FileIdentifier fileIdentifier, long fileLength, CacheManager cacheManager)
    {
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
        this.fileIdentifier = requireNonNull(fileIdentifier, "fileIdentifier is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.fileLength = fileLength;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException(STREAM_IS_CLOSED);
        }
    }

    private void openStream()
            throws IOException
    {
        if (delegate == null) {
            delegate = inputFile.newStream();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            if (delegate != null) {
                delegate.close();
                delegate = null;
            }
        }
    }

    @Override
    public long getPosition()
            throws IOException
    {
        return position;
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        ensureOpen();
        if (pos < 0) {
            throw new EOFException(NEGATIVE_SEEK);
        }
        if (pos > fileLength) {
            throw new EOFException(CANNOT_SEEK_PAST_EOF);
        }
        if (delegate != null) {
            delegate.seek(pos);
        }
        this.position = pos;
    }

    @Override
    public int read()
            throws IOException
    {
        if (oneByteBuf == null) {
            oneByteBuf = new byte[1];
        }
        int ret = read(oneByteBuf, 0, 1);
        return (ret <= 0) ? -1 : (oneByteBuf[0] & 0xff);
    }

    @Override
    public int read(@Nonnull byte[] buf)
            throws IOException
    {
        return read(buf, 0, buf.length);
    }

    @Override
    public int read(@Nonnull byte[] buf, int off, int len)
            throws IOException
    {
        long position = getPosition();
        validatePositionedReadArgs(position, buf, off, len);
        if (len == 0) {
            return 0;
        }
        if (position >= fileLength) {
            return -1;
        }
        FileReadRequest key = new FileReadRequest(fileIdentifier, position, len);
        CacheResult result = cacheManager.read(key, buf, off);
        int n = 0;
        int bytesRead = 0;
        try {
            switch (result.getResult()) {
                case HIT:
                    n = result.getLength();
                    bytesRead = n;
                    break;
                case MISS:
                    openStream();
                    seek(position);
                    while (bytesRead < len) {
                        n = delegate.read(buf, off + bytesRead, len - bytesRead);
                        if (n < 0) {
                            break;
                        }
                        bytesRead += n;
                    }
                    if (bytesRead > 0) {
                        // be careful here
                        n = bytesRead;
                        cacheManager.write(new FileReadRequest(fileIdentifier, position, bytesRead), wrappedBuffer(buf, off, bytesRead));
                    }
                    break;
                default:
                    throw new RuntimeException("Should never reach here");
            }
        }
        finally {
            if (bytesRead > 0) {
                this.position += bytesRead;
            }
        }
        return n;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();
        n = Longs.constrainToRange(n, 0, fileLength - position);
        position += n;
        return n;
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        ensureOpen();

        if (n <= 0) {
            return;
        }

        long position;
        try {
            position = addExact(this.position, n);
        }
        catch (ArithmeticException e) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, this.position, fileLength, inputFile.location()));
        }
        if (position > fileLength) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, this.position, fileLength, inputFile.location()));
        }
        this.position = position;
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();

        final long remaining = fileLength - getPosition();
        return remaining <= Integer.MAX_VALUE ? (int) remaining : Integer.MAX_VALUE;
    }

    private void validatePositionedReadArgs(long position, byte[] buffer, int offset, int length)
            throws EOFException
    {
        checkArgument(length >= 0, "length is negative");
        if (position < 0L) {
            throw new EOFException("position is negative");
        }
        else {
            checkArgument(buffer != null, "Null buffer");
            if (buffer.length - offset < length) {
                throw new IndexOutOfBoundsException("Requested more bytes than destination buffer size: request length=" + length + ", with offset =" + offset + "; buffer capacity =" + (buffer.length - offset));
            }
        }
    }
}
