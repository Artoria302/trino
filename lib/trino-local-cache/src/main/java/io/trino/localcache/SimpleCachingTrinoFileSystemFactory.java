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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public class SimpleCachingTrinoFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final TrinoFileSystemFactory delegate;
    private final CacheKeyProvider cacheKeyProvider;

    public SimpleCachingTrinoFileSystemFactory(TrinoFileSystemFactory delegate, CacheKeyProvider cacheKeyProvider)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cacheKeyProvider = requireNonNull(cacheKeyProvider, "cacheKeyProvider is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return delegate.create(identity);
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, CacheManager cacheManager)
    {
        return new SimpleCachingTrinoFileSystem(delegate.create(identity), cacheKeyProvider, cacheManager);
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, CacheManager cacheManager)
    {
        return new SimpleCachingTrinoFileSystem(delegate.create(session), cacheKeyProvider, cacheManager);
    }
}
