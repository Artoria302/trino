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

import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.localcache.FileIdentifier;
import io.trino.spi.localcache.FileReadRequest;

import java.io.EOFException;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FSExceptionMessages.EOF_IN_READ_FULLY;
import static org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED;

public class SimpleCachingTrinoInput
        implements TrinoInput
{
    private final TrinoInputFile inputFile;
    private final FileIdentifier fileIdentifier;
    private final CacheManager cacheManager;
    private final long fileLength;
    private boolean closed;
    private TrinoInput delegate;

    public SimpleCachingTrinoInput(TrinoInputFile inputFile, FileIdentifier fileIdentifier, long fileLength, CacheManager cacheManager)
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

    private TrinoInput getInput()
            throws IOException
    {
        if (delegate == null) {
            delegate = inputFile.newInput();
        }
        return delegate;
    }

    @Override
    public void readFully(long position, byte[] buf, int off, int len)
            throws IOException
    {
        ensureOpen();
        validatePositionedReadArgs(position, buf, off, len);
        if (len == 0) {
            return;
        }
        if (position + len > fileLength) {
            throw new EOFException(EOF_IN_READ_FULLY);
        }
        FileReadRequest key = new FileReadRequest(fileIdentifier, position, len);
        switch (cacheManager.readFully(key, buf, off).getResult()) {
            case HIT:
                break;
            case MISS:
                getInput().readFully(position, buf, off, len);
                cacheManager.write(key, wrappedBuffer(buf, off, len));
                return;
            default:
                throw new RuntimeException("Should never reach here");
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);

        int readSize = (int) min(fileLength, bufferLength);
        readFully(fileLength - readSize, buffer, bufferOffset, readSize);
        return readSize;
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
