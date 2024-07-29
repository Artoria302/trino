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
package io.trino.plugin.archer.cache;

import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.spi.localcache.FileIdentifier;

import java.util.Optional;

public class ArcherCacheKeyProvider
        implements CacheKeyProvider
{
    @Override
    public Optional<String> getCacheKey(TrinoInputFile inputFile)
    {
        String path = inputFile.location().path();
        if (path.endsWith(".trinoSchema") || path.contains("/.trinoPermissions/")) {
            // Needed to avoid caching files from FileHiveMetastore on coordinator during tests
            return Optional.empty();
        }
        // Iceberg data and metadata files are immutable
        return Optional.of(path);
    }

    @Override
    public Optional<FileIdentifier> getCacheIdentifier(TrinoInputFile inputFile)
    {
        String path = inputFile.location().path();
        if (path.endsWith(".trinoSchema") || path.contains("/.trinoPermissions/")) {
            // Needed to avoid caching files from FileHiveMetastore on coordinator during tests
            return Optional.empty();
        }
        // Iceberg data and metadata files are immutable
        return Optional.of(new FileIdentifier(path, 0));
    }
}
