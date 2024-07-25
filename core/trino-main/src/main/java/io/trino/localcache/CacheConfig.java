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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.annotation.Nullable;

import java.net.URI;

public class CacheConfig
{
    private boolean cachingEnabled;
    private CacheType cacheType;
    private URI baseDirectory;

    @Nullable
    public URI getBaseDirectory()
    {
        return baseDirectory;
    }

    @Config("cache.base-directory")
    @ConfigDescription("Base URI to cache data")
    public CacheConfig setBaseDirectory(URI dataURI)
    {
        this.baseDirectory = dataURI;
        return this;
    }

    public boolean isCachingEnabled()
    {
        return cachingEnabled;
    }

    @Config("cache.enabled")
    @ConfigDescription("Is cache enabled")
    public CacheConfig setCachingEnabled(boolean cachingEnabled)
    {
        this.cachingEnabled = cachingEnabled;
        return this;
    }

    public CacheType getCacheType()
    {
        return cacheType;
    }

    @Config("cache.type")
    @ConfigDescription("Caching type")
    public CacheConfig setCacheType(CacheType cacheType)
    {
        this.cacheType = cacheType;
        return this;
    }
}
