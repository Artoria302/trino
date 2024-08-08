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
package io.trino.plugin.paimon.catalog.file;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.catalog.PaimonCatalogFactory;
import io.trino.plugin.paimon.fileio.PaimonFileIO;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.localcache.CacheManager;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import static io.trino.plugin.paimon.PaimonSessionProperties.isLocalCacheEnabled;
import static java.util.Objects.requireNonNull;

public class PaimonFileMetastoreCatalogFactory
        implements PaimonCatalogFactory
{
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final CacheManager cacheManager;
    private final Path warehouse;
    private final boolean isObjectStore;

    @Inject
    public PaimonFileMetastoreCatalogFactory(
            TrinoFileSystemFactory trinoFileSystemFactory,
            CacheManager cacheManager,
            PaimonConfig paimonConfig)
    {
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
        this.cacheManager = requireNonNull(cacheManager, "trinoFileSystemFactory is null");
        this.warehouse = new Path(requireNonNull(paimonConfig.getWarehouse(), "warehouse is null"));
        this.isObjectStore = checkObjectStore(warehouse.toUri().getScheme());
    }

    @Override
    public AbstractCatalog create(ConnectorSession session)
    {
        boolean cacheEnabled = isLocalCacheEnabled(session);
        TrinoFileSystem trinoFileSystem = cacheEnabled && cacheManager.isValid()
                ? trinoFileSystemFactory.create(session, cacheManager)
                : trinoFileSystemFactory.create(session);
        FileIO fileIO = new PaimonFileIO(trinoFileSystem, isObjectStore);
        return new FileSystemCatalog(fileIO, warehouse);
    }
}
