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

import io.airlift.units.DataSize;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.spi.localcache.CacheManager;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SimpleCachingTrinoFileSystem
        implements TrinoFileSystem
{
    private static final DataSize EMPTY = DataSize.ofBytes(0);
    private final TrinoFileSystem delegate;
    private final CacheKeyProvider cacheKeyProvider;
    private final CacheManager cacheManager;
    private final DataSize ioBufferSize;

    public SimpleCachingTrinoFileSystem(TrinoFileSystem delegate, CacheKeyProvider cacheKeyProvider, CacheManager cacheManager, DataSize ioBufferSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cacheKeyProvider = requireNonNull(cacheKeyProvider, "cacheKeyProvider is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.ioBufferSize = requireNonNull(ioBufferSize, "ioBufferSize is null");
    }

    public SimpleCachingTrinoFileSystem(TrinoFileSystem delegate, CacheKeyProvider cacheKeyProvider, CacheManager cacheManager)
    {
        this(delegate, cacheKeyProvider, cacheManager, EMPTY);
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new SimpleCachingTrinoInputFile(delegate.newInputFile(location), cacheKeyProvider, cacheManager, ioBufferSize);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new SimpleCachingTrinoInputFile(delegate.newInputFile(location, length), cacheKeyProvider, cacheManager, ioBufferSize);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return delegate.newOutputFile(location);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        delegate.deleteFile(location);
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        delegate.deleteFiles(locations);
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        delegate.deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        delegate.renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return delegate.listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return delegate.directoryExists(location);
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        delegate.createDirectory(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        delegate.renameDirectory(source, target);
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return delegate.listDirectories(location);
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        return delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
    }
}
