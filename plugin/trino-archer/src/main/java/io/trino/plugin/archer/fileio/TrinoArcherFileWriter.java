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
package io.trino.plugin.archer.fileio;

import net.qihoo.archer.arrow.filesystem.ArcherFileWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class TrinoArcherFileWriter
        implements ArcherFileWriter
{
    private final OutputStream output;

    public TrinoArcherFileWriter(OutputStream output)
    {
        this.output = requireNonNull(output, "input is null");
    }

    @Override
    public void write(ByteBuffer buffer)
            throws IOException
    {
        int length = buffer.limit();
        byte[] buf = new byte[length];
        buffer.get(buf);
        output.write(buf);
    }

    @Override
    public void flush()
            throws IOException
    {
        output.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        output.close();
    }
}
