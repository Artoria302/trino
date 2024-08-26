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

import io.trino.filesystem.TrinoInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

public class BufferedTrinoInputStream
        extends TrinoInputStream
{
    public static final int SOFT_MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private final int initialSize;

    private TrinoInputStream in;
    private byte[] buf;
    private int count;
    private int pos;
    private int markPos = -1;
    private int markLimit;

    public BufferedTrinoInputStream(TrinoInputStream trinoInputStream, int size)
    {
        this.in = requireNonNull(trinoInputStream, "trinoInputStream is null");
        this.initialSize = size;
        this.buf = new byte[size];
    }

    public BufferedTrinoInputStream(TrinoInputStream trinoInputStream)
    {
        this(trinoInputStream, DEFAULT_BUFFER_SIZE);
    }

    private void ensureOpen()
            throws IOException
    {
        if (buf == null || in == null) {
            throw new IOException("Stream closed");
        }
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        int n = count - pos;
        int avail = in.available();
        return n > (Integer.MAX_VALUE - avail)
                ? Integer.MAX_VALUE
                : n + avail;
    }

    @Override
    public void close()
            throws IOException
    {
        if (in != null) {
            InputStream input = in;
            buf = null;
            in = null;
            input.close();
        }
    }

    @Override
    public void mark(int readLimit)
    {
        markLimit = readLimit;
        markPos = pos;
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

    public void reset()
            throws IOException
    {
        ensureOpen();
        if (markPos < 0) {
            throw new IOException("Resetting to invalid mark");
        }
        pos = markPos;
    }

    @Override
    public long getPosition()
            throws IOException
    {
        ensureOpen();
        return in.getPosition() - (count - pos);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();
        if (n <= 0) {
            return 0;
        }
        seek(getPosition() + n);
        return n;
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        if (in == null) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (this.pos != this.count) {
            // optimize: check if the pos is in the buffer
            // This optimization only works if pos != count -- if they are
            // equal, it's possible that the previous reads were just
            // longer than the total buffer size, and hence skipped the buffer.
            long end = in.getPosition();
            long start = end - count;
            if (pos >= start && pos < end) {
                this.pos = (int) (pos - start);
                return;
            }
        }

        // invalidate buffer
        this.pos = 0;
        this.count = 0;

        in.seek(pos);
    }

    private void fill()
            throws IOException
    {
        ensureOpen();
        byte[] buffer = buf;
        if (markPos == -1) {
            pos = 0;            /* no mark: throw away the buffer */
        }
        else if (pos >= buffer.length) { /* no room left in buffer */
            if (markPos > 0) {  /* can throw away early part of the buffer */
                int sz = pos - markPos;
                System.arraycopy(buffer, markPos, buffer, 0, sz);
                pos = sz;
                markPos = 0;
            }
            else if (buffer.length >= markLimit) {
                markPos = -1;   /* buffer got too big, invalidate mark */
                pos = 0;        /* drop buffer contents */
            }
            else {            /* grow buffer */
                int nsz = newLength(pos,
                        1,  /* minimum growth */
                        pos /* preferred growth */);
                if (nsz > markLimit) {
                    nsz = markLimit;
                }
                byte[] nbuf = new byte[nsz];
                System.arraycopy(buffer, 0, nbuf, 0, pos);
                buf = nbuf;
                buffer = nbuf;
            }
        }
        count = pos;
        int n = in.read(buffer, pos, buffer.length - pos);
        if (n > 0) {
            count = n + pos;
        }
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        if (pos >= count) {
            fill();
            if (pos >= count) {
                return -1;
            }
        }
        return buf[pos++] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        ensureOpen();
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }
        else if (len == 0) {
            return 0;
        }

        int n = 0;
        for (; ; ) {
            int nread = read1(b, off + n, len - n);
            if (nread <= 0) {
                return (n == 0) ? nread : n;
            }
            n += nread;
            if (n >= len) {
                return n;
            }
            // if not closed but no bytes available, return
            InputStream input = in;
            if (input != null && input.available() <= 0) {
                return n;
            }
        }
    }

    private int read1(byte[] b, int off, int len)
            throws IOException
    {
        int avail = count - pos;
        if (avail <= 0) {
            /* If the requested length is at least as large as the buffer, and
               if there is no mark/reset activity, do not bother to copy the
               bytes into the local buffer.  In this way buffered streams will
               cascade harmlessly. */
            int size = Math.max(buf.length, initialSize);
            if (len >= size && markPos == -1) {
                return in.read(b, off, len);
            }
            fill();
            avail = count - pos;
            if (avail <= 0) {
                return -1;
            }
        }
        int cnt = Math.min(avail, len);
        System.arraycopy(buf, pos, b, off, cnt);
        pos += cnt;
        return cnt;
    }

    public static int newLength(int oldLength, int minGrowth, int prefGrowth)
    {
        // preconditions not checked because of inlining
        // assert oldLength >= 0
        // assert minGrowth > 0

        int prefLength = oldLength + Math.max(minGrowth, prefGrowth); // might overflow
        if (0 < prefLength && prefLength <= SOFT_MAX_ARRAY_LENGTH) {
            return prefLength;
        }
        else {
            // put code cold in a separate method
            return hugeLength(oldLength, minGrowth);
        }
    }

    private static int hugeLength(int oldLength, int minGrowth)
    {
        int minLength = oldLength + minGrowth;
        if (minLength < 0) { // overflow
            throw new OutOfMemoryError(
                    "Required array length " + oldLength + " + " + minGrowth + " is too large");
        }
        else {
            return Math.max(minLength, SOFT_MAX_ARRAY_LENGTH);
        }
    }
}
