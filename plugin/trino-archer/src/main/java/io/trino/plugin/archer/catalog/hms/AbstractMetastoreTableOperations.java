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

import io.trino.annotation.NotThreadSafe;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.plugin.archer.UnknownTableTypeException;
import io.trino.plugin.archer.catalog.AbstractArcherTableOperations;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.TableNotFoundException;
import net.qihoo.archer.TableMetadata;
import net.qihoo.archer.io.FileIO;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_INVALID_METADATA;
import static io.trino.plugin.archer.ArcherTableName.isMaterializedViewStorage;
import static io.trino.plugin.archer.ArcherTableName.tableNameFrom;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.util.HiveUtil.isArcherTable;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.BaseMetastoreTableOperations.ARCHER_TABLE_TYPE_VALUE;
import static net.qihoo.archer.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static net.qihoo.archer.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static net.qihoo.archer.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

@NotThreadSafe
public abstract class AbstractMetastoreTableOperations
        extends AbstractArcherTableOperations
{
    protected final CachingHiveMetastore metastore;

    protected AbstractMetastoreTableOperations(
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    protected final String getRefreshedLocation(boolean invalidateCaches)
    {
        if (invalidateCaches) {
            metastore.invalidateTable(database, tableName);
        }

        boolean isMaterializedViewStorageTable = isMaterializedViewStorage(tableName);

        Table table;
        if (isMaterializedViewStorageTable) {
            table = getTable(database, tableNameFrom(tableName));
        }
        else {
            table = getTable();
        }

        if (!isMaterializedViewStorageTable && (isTrinoView(table) || isTrinoMaterializedView(table))) {
            // this is a Hive view or Trino/Presto view, or Trino materialized view, hence not a table
            // TODO table operations should not be constructed for views (remove exception-driven code path)
            throw new TableNotFoundException(getSchemaTableName());
        }
        if (!isMaterializedViewStorageTable && !isArcherTable(table)) {
            throw new UnknownTableTypeException(getSchemaTableName());
        }

        String metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
            throw new TrinoException(ARCHER_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION_PROP, getSchemaTableName()));
        }

        return metadataLocation;
    }

    @Override
    protected final void commitNewTable(TableMetadata metadata)
    {
        verify(version.isEmpty(), "commitNewTable called on a table which already exists");
        String newMetadataLocation = writeNewMetadata(metadata, 0);

        Table table = Table.builder()
                .setDatabaseName(database)
                .setTableName(tableName)
                .setOwner(owner)
                // Table needs to be EXTERNAL, otherwise table rename in HMS would rename table directory and break table contents.
                .setTableType(EXTERNAL_TABLE.name())
                .withStorage(storage -> storage.setStorageFormat(ARCHER_METASTORE_STORAGE_FORMAT))
                // This is a must-have property for the EXTERNAL_TABLE table type
                .setParameter("EXTERNAL", "TRUE")
                .setParameter(TABLE_TYPE_PROP, ARCHER_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                .apply(builder -> updateMetastoreTable(builder, metadata, newMetadataLocation, Optional.empty()))
                .build();

        PrincipalPrivileges privileges = owner.map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
        try {
            metastore.createTable(table, privileges);
        }
        catch (SchemaNotFoundException | TableAlreadyExistsException e) {
            // clean up metadata files corresponding to the current transaction
            fileIo.deleteFile(newMetadataLocation);
            throw e;
        }
    }

    protected Table.Builder updateMetastoreTable(Table.Builder builder, TableMetadata metadata, String metadataLocation, Optional<String> previousMetadataLocation)
    {
        return builder
                .setDataColumns(toHiveColumns(metadata.schema().columns()))
                .withStorage(storage -> storage.setLocation(metadata.location()))
                .setParameter(METADATA_LOCATION_PROP, metadataLocation)
                .setParameter(PREVIOUS_METADATA_LOCATION_PROP, previousMetadataLocation)
                .setParameter(TABLE_COMMENT, Optional.ofNullable(metadata.properties().get(TABLE_COMMENT)));
    }

    protected Table getTable()
    {
        return getTable(database, tableName);
    }

    protected Table getTable(String database, String tableName)
    {
        return metastore.getTable(database, tableName)
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }
}
