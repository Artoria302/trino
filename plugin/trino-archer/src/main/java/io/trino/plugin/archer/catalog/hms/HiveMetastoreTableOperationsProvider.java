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
package io.trino.plugin.archer.catalog.hms;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.archer.catalog.ArcherTableOperations;
import io.trino.plugin.archer.catalog.ArcherTableOperationsProvider;
import io.trino.plugin.archer.catalog.TrinoCatalog;
import io.trino.plugin.archer.fileio.ForwardingFileIo;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.localcache.CacheManager;

import java.util.Optional;

import static io.trino.plugin.archer.ArcherSessionProperties.isLocalCacheEnabled;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreTableOperationsProvider
        implements ArcherTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ThriftMetastoreFactory thriftMetastoreFactory;
    private final CacheManager cacheManager;

    @Inject
    public HiveMetastoreTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ThriftMetastoreFactory thriftMetastoreFactory,
            CacheManager cacheManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.thriftMetastoreFactory = requireNonNull(thriftMetastoreFactory, "thriftMetastoreFactory is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
    }

    @Override
    public ArcherTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        boolean fileCacheEnabled = isLocalCacheEnabled(session);
        TrinoFileSystem fileSystem = fileCacheEnabled && cacheManager.isValid()
                ? fileSystemFactory.create(session, cacheManager)
                : fileSystemFactory.create(session);
        return new HiveMetastoreTableOperations(
                new ForwardingFileIo(fileSystem),
                ((TrinoHiveCatalog) catalog).getMetastore(),
                thriftMetastoreFactory.createMetastore(Optional.of(session.getIdentity())),
                session,
                database,
                table,
                owner,
                location);
    }
}
