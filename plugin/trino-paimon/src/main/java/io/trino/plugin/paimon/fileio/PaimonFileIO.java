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
package io.trino.plugin.paimon.fileio;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Trino file io for paimon.
 */
public class PaimonFileIO
        implements FileIO
{
    private final TrinoFileSystem trinoFileSystem;
    private final boolean objectStore;

    public PaimonFileIO(TrinoFileSystem trinoFileSystem, boolean isObjectStore)
    {
        this.trinoFileSystem = requireNonNull(trinoFileSystem, "trinoFileSystem is null");
        this.objectStore = isObjectStore;
    }

    @Override
    public boolean isObjectStore()
    {
        return objectStore;
    }

    @Override
    public void configure(CatalogContext catalogContext) {}

    @Override
    public SeekableInputStream newInputStream(Path path)
            throws IOException
    {
        return new PaimonInputStreamWrapper(trinoFileSystem.newInputFile(Location.of(path.toString())).newStream());
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean b)
            throws IOException
    {
        return new PositionOutputStreamWrapper(trinoFileSystem.newOutputFile(Location.of(path.toString())).create());
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return status(path);
    }

    private FileStatus status(Path path)
            throws IOException
    {
        if (trinoFileSystem.directoryExists(Location.of(path.toString())).orElse(false)) {
            return new PaimonDirectoryFileStatus(path);
        }
        else {
            TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(Location.of(path.toString()));
            return new PaimonFileStatus(trinoInputFile.length(), path, trinoInputFile.lastModified().getEpochSecond());
        }
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<FileStatus> fileStatusList = new ArrayList<>();
        Location location = Location.of(path.toString());
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            FileIterator fileIterator = trinoFileSystem.listFiles(location);
            while (fileIterator.hasNext()) {
                FileEntry fileEntry = fileIterator.next();
                fileStatusList.add(
                        new PaimonFileStatus(
                                fileEntry.length(),
                                new Path(fileEntry.location().toString()),
                                fileEntry.lastModified().getEpochSecond()));
            }
            trinoFileSystem
                    .listDirectories(Location.of(path.toString()))
                    .forEach(l -> fileStatusList.add(new PaimonDirectoryFileStatus(new Path(l.toString()))));
        }
        return fileStatusList.toArray(new FileStatus[0]);
    }

    @Override
    public FileStatus[] listDirectories(Path path)
            throws IOException
    {
        return trinoFileSystem.listDirectories(Location.of(path.toString())).stream()
                .map(l -> new PaimonDirectoryFileStatus(new Path(l.toString())))
                .toArray(FileStatus[]::new);
    }

    @Override
    public boolean exists(Path path)
            throws IOException
    {
        return trinoFileSystem.directoryExists(Location.of(path.toString())).orElse(false)
                || existFile(Location.of(path.toString()));
    }

    private boolean existFile(Location location)
            throws IOException
    {
        try {
            return trinoFileSystem.newInputFile(location).exists();
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean b)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            trinoFileSystem.deleteDirectory(location);
            return true;
        }
        else if (existFile(location)) {
            trinoFileSystem.deleteFile(location);
            return true;
        }

        return false;
    }

    @Override
    public boolean mkdirs(Path path)
            throws IOException
    {
        trinoFileSystem.createDirectory(Location.of(path.toString()));
        return true;
    }

    @Override
    public boolean rename(Path source, Path target)
            throws IOException
    {
        Location sourceLocation = Location.of(source.toString());
        Location targetLocation = Location.of(target.toString());
        if (trinoFileSystem.directoryExists(sourceLocation).orElse(false)) {
            trinoFileSystem.renameDirectory(sourceLocation, targetLocation);
        }
        else {
            trinoFileSystem.renameFile(sourceLocation, targetLocation);
        }
        return true;
    }
}
