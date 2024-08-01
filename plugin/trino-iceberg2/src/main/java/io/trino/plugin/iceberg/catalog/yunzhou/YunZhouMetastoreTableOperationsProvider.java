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
package io.trino.plugin.iceberg.catalog.yunzhou;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.localcache.CacheManager;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergSessionProperties.isLocalCacheEnabled;
import static java.util.Objects.requireNonNull;

public class YunZhouMetastoreTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ThriftMetastoreFactory thriftMetastoreFactory;
    private final CacheManager cacheManager;

    @Inject
    public YunZhouMetastoreTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ThriftMetastoreFactory thriftMetastoreFactory,
            CacheManager cacheManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.thriftMetastoreFactory = requireNonNull(thriftMetastoreFactory, "thriftMetastoreFactory is null");
        this.cacheManager = cacheManager;
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        boolean localCacheEnabled = isLocalCacheEnabled(session);
        TrinoFileSystem fileSystem = localCacheEnabled && cacheManager.isValid()
                ? fileSystemFactory.create(session, cacheManager)
                : fileSystemFactory.create(session);
        return new YunZhouMetastoreTableOperations(
                new ForwardingFileIo(fileSystem),
                ((TrinoYunZhouCatalog) catalog).getMetastore(),
                thriftMetastoreFactory.createMetastore(Optional.of(session.getIdentity())),
                session,
                database,
                table,
                owner,
                location);
    }
}
