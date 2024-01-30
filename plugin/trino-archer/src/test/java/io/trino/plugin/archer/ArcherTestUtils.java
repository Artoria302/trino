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
package io.trino.plugin.archer;

import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.metastore.HiveMetastore;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.archer.catalog.ArcherTableOperationsProvider;
import io.trino.plugin.archer.catalog.TrinoCatalog;
import io.trino.plugin.archer.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.archer.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.QueryRunner;
import net.qihoo.archer.BaseTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterators.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.archer.ArcherQueryRunner.ARCHER_CATALOG;
import static io.trino.plugin.archer.ArcherUtil.loadArcherTable;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.testing.TestingConnectorSession.SESSION;

public final class ArcherTestUtils
{
    private ArcherTestUtils() {}

    public static Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static boolean checkParquetFileSorting(TrinoInputFile inputFile, String sortColumnName)
    {
        ParquetMetadata parquetMetadata;
        try {
            parquetMetadata = MetadataReader.readFooter(
                    new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats()),
                    Optional.empty());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Comparable previousMax = null;
        verify(parquetMetadata.getBlocks().size() > 1, "Test must produce at least two row groups");
        for (BlockMetadata blockMetaData : parquetMetadata.getBlocks()) {
            ColumnChunkMetadata columnMetadata = blockMetaData.columns().stream()
                    .filter(column -> getOnlyElement(column.getPath().iterator()).equalsIgnoreCase(sortColumnName))
                    .collect(onlyElement());
            if (previousMax != null) {
                if (previousMax.compareTo(columnMetadata.getStatistics().genericGetMin()) > 0) {
                    return false;
                }
            }
            previousMax = columnMetadata.getStatistics().genericGetMax();
        }
        return true;
    }

    public static TrinoFileSystemFactory getFileSystemFactory(QueryRunner queryRunner)
    {
        return ((ArcherConnector) queryRunner.getCoordinator().getConnector(ARCHER_CATALOG))
                .getInjector().getInstance(TrinoFileSystemFactory.class);
    }

    public static BaseTable loadTable(String tableName,
            HiveMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            String catalogName,
            String schemaName)
    {
        ArcherTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName(catalogName),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                tableOperationsProvider,
                false,
                false,
                false,
                new ArcherConfig().isHideMaterializedViewStorageTable());
        return (BaseTable) loadArcherTable(catalog, tableOperationsProvider, SESSION, new SchemaTableName(schemaName, tableName));
    }
}
