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
package io.trino.plugin.paimon.catalog.hms;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.PaimonSecurityConfig;
import io.trino.plugin.paimon.catalog.PaimonCatalogFactory;
import io.trino.plugin.paimon.fileio.PaimonFileIO;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.localcache.CacheManager;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.fs.FileIO;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.paimon.PaimonSecurityConfig.IcebergSecurity.SYSTEM;
import static io.trino.plugin.paimon.PaimonSessionProperties.isLocalCacheEnabled;
import static java.util.Objects.requireNonNull;

public class PaimonHiveMetastoreCatalogFactory
        implements PaimonCatalogFactory
{
    private final HiveMetastoreFactory metastoreFactory;
    private final ThriftMetastoreFactory thriftMetastoreFactory;
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final CacheManager cacheManager;
    private final boolean catalogLockEnabled;
    private final boolean isUsingSystemSecurity;

    @Inject
    public PaimonHiveMetastoreCatalogFactory(
            HiveMetastoreFactory metastoreFactory,
            ThriftMetastoreFactory thriftMetastoreFactory,
            TrinoFileSystemFactory trinoFileSystemFactory,
            CacheManager cacheManager,
            PaimonConfig paimonConfig,
            PaimonSecurityConfig securityConfig)
    {
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.thriftMetastoreFactory = requireNonNull(thriftMetastoreFactory, "thriftMetastoreFactory is null");
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.catalogLockEnabled = paimonConfig.getCatalogLockEnabled();
        this.isUsingSystemSecurity = securityConfig.getSecuritySystem() == SYSTEM;
    }

    @Override
    public AbstractCatalog create(ConnectorSession session)
    {
        CachingHiveMetastore metastore = createPerTransactionCache(metastoreFactory.createMetastore(Optional.of(session.getIdentity())), 1000);
        boolean cacheEnabled = isLocalCacheEnabled(session);
        TrinoFileSystem trinoFileSystem = cacheEnabled && cacheManager.isValid()
                ? trinoFileSystemFactory.create(session, cacheManager)
                : trinoFileSystemFactory.create(session);
        FileIO fileIO = new PaimonFileIO(trinoFileSystem, true);
        return new TrinoHiveCatalog(
                metastore,
                thriftMetastoreFactory.createMetastore(Optional.of(session.getIdentity())),
                session,
                fileIO,
                catalogLockEnabled,
                isUsingSystemSecurity);
    }
}
