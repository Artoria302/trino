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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.localcache.FileIdentifier;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SimpleCachingTrinoInputFile
        implements TrinoInputFile
{
    private final TrinoInputFile delegate;
    private final CacheKeyProvider cacheKeyProvider;
    private final CacheManager cacheManager;
    private final DataSize ioBufferSize;

    public SimpleCachingTrinoInputFile(TrinoInputFile delegate, CacheKeyProvider cacheKeyProvider, CacheManager cacheManager, DataSize ioBufferSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cacheKeyProvider = requireNonNull(cacheKeyProvider, "cacheKeyProvider is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.ioBufferSize = requireNonNull(ioBufferSize, "ioBufferSize is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        Optional<FileIdentifier> fileIdentifier = cacheKeyProvider.getCacheIdentifier(delegate);
        if (fileIdentifier.isEmpty()) {
            return delegate.newInput();
        }
        return new SimpleCachingTrinoInput(delegate, fileIdentifier.get(), delegate.length(), cacheManager);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        Optional<FileIdentifier> fileIdentifier = cacheKeyProvider.getCacheIdentifier(delegate);
        if (fileIdentifier.isEmpty()) {
            return delegate.newStream();
        }
        int bufferSize = toIntExact(this.ioBufferSize.toBytes());
        TrinoInputStream in = new SimpleCachingTrinoInputStream(delegate, fileIdentifier.get(), delegate.length(), cacheManager);
        if (bufferSize > 0) {
            return new BufferedTrinoInputStream(in, bufferSize);
        }
        else {
            return in;
        }
    }

    @Override
    public long length()
            throws IOException
    {
        return delegate.length();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return delegate.lastModified();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return delegate.exists();
    }

    @Override
    public Location location()
    {
        return delegate.location();
    }
}
