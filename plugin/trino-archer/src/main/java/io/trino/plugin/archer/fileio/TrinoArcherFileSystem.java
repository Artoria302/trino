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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import net.qihoo.archer.FileSummary;
import net.qihoo.archer.arrow.filesystem.ArcherFileReader;
import net.qihoo.archer.arrow.filesystem.ArcherFileSystem;
import net.qihoo.archer.arrow.filesystem.ArcherFileWriter;
import net.qihoo.archer.arrow.filesystem.Metadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoArcherFileSystem
        extends ArcherFileSystem
{
    private final TrinoFileSystem fileSystem;
    private final Map<String, FileSummary> metadataCache;

    public TrinoArcherFileSystem(TrinoFileSystem fileSystem, Map<String, FileSummary> metadataCache)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.metadataCache = metadataCache;
    }

    @Override
    public ArcherFileReader openRead(String path)
            throws IOException
    {
        return new TrinoArcherFileReader(this.fileSystem.newInputFile(Location.of(path)).newInput());
    }

    @Override
    public ArcherFileReader openRead(String path, long length)
            throws IOException
    {
        return new TrinoArcherFileReader(this.fileSystem.newInputFile(Location.of(path), length).newInput());
    }

    @Override
    public ArcherFileWriter openWrite(String path)
            throws IOException
    {
        return new TrinoArcherFileWriter(this.fileSystem.newOutputFile(Location.of(path)).create());
    }

    @Override
    public void delete(String path, boolean recursive)
            throws IOException
    {
        if (recursive) {
            this.fileSystem.deleteDirectory(Location.of(path));
        }
        else {
            this.fileSystem.deleteFile(Location.of(path));
        }
    }

    @Override
    public Metadata metadata(String path)
            throws IOException
    {
        try {
            if (this.metadataCache != null) {
                FileSummary metadata = this.metadataCache.get(path);
                if (metadata != null) {
                    return new Metadata(path, metadata.fileSize(), metadata.lastModifiedTime());
                }
            }
            TrinoInputFile inputFile = this.fileSystem.newInputFile(Location.of(path));
            return new Metadata(inputFile.location().toString(), inputFile.length(), inputFile.lastModified().toEpochMilli());
        }
        catch (FileNotFoundException ignore) {
            return null;
        }
    }

    @Override
    public void getBytes(String path, long position, int length, ByteBuffer buffer)
            throws IOException
    {
        try (TrinoInput input = this.fileSystem.newInputFile(Location.of(path)).newInput()) {
            byte[] buf = new byte[length];
            input.readFully(position, buf, 0, length);
            buffer.put(buf);
        }
    }

    @Override
    public ByteBuffer read(String path)
            throws IOException
    {
        try (TrinoInputStream input = this.fileSystem.newInputFile(Location.of(path)).newStream()) {
            byte[] data = input.readAllBytes();
            ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
            buffer.put(data);
            return buffer;
        }
    }

    @Override
    public ByteBuffer read(String path, long length)
            throws IOException
    {
        try (TrinoInputStream input = this.fileSystem.newInputFile(Location.of(path), length).newStream()) {
            byte[] data = input.readAllBytes();
            ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
            buffer.put(data);
            return buffer;
        }
    }

    @Override
    public void write(String path, ByteBuffer data, boolean overwrite)
            throws IOException
    {
        TrinoOutputFile outputFile = fileSystem.newOutputFile(Location.of(path));
        int length = data.limit();
        byte[] buf = new byte[length];
        data.get(buf);
        if (overwrite) {
            outputFile.createOrOverwrite(buf);
        }
        else {
            try (OutputStream output = outputFile.create()) {
                output.write(buf);
            }
        }
    }
}
