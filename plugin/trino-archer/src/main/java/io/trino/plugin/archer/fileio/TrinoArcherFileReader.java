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

import io.trino.filesystem.TrinoInput;
import net.qihoo.archer.arrow.filesystem.ArcherFileReader;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class TrinoArcherFileReader
        implements ArcherFileReader
{
    private final TrinoInput input;

    public TrinoArcherFileReader(TrinoInput input)
    {
        this.input = requireNonNull(input, "input is null");
    }

    @Override
    public void close()
            throws IOException
    {
        this.input.close();
    }

    @Override
    public void getBytes(long position, int length, ByteBuffer buffer)
            throws IOException
    {
        byte[] buf = new byte[length];
        this.input.readFully(position, buf, 0, length);
        buffer.put(buf);
    }
}
