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
package io.trino.plugin.archer.catalog;

import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.archer.ArcherMaterializedViewDefinition;
import io.trino.plugin.archer.ArcherUtil;
import io.trino.plugin.archer.ColumnIdentity;
import io.trino.plugin.archer.fileio.ForwardingFileIo;
import io.trino.plugin.archer.fileio.ForwardingOutputFile;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import net.qihoo.archer.AppendFiles;
import net.qihoo.archer.BaseTable;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.PartitionSpec;
import net.qihoo.archer.Schema;
import net.qihoo.archer.SortOrder;
import net.qihoo.archer.Table;
import net.qihoo.archer.TableMetadata;
import net.qihoo.archer.TableMetadataParser;
import net.qihoo.archer.TableOperations;
import net.qihoo.archer.Transaction;
import net.qihoo.archer.types.Types;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.TableInfo.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_FILESYSTEM_ERROR;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_INVALID_METADATA;
import static io.trino.plugin.archer.ArcherMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.archer.ArcherMaterializedViewProperties.STORAGE_SCHEMA;
import static io.trino.plugin.archer.ArcherMaterializedViewProperties.getStorageSchema;
import static io.trino.plugin.archer.ArcherTableName.tableNameWithType;
import static io.trino.plugin.archer.ArcherTableProperties.getInvertedIndex;
import static io.trino.plugin.archer.ArcherTableProperties.getPartitioning;
import static io.trino.plugin.archer.ArcherTableProperties.getSortOrder;
import static io.trino.plugin.archer.ArcherTableProperties.getTableLocation;
import static io.trino.plugin.archer.ArcherUtil.commit;
import static io.trino.plugin.archer.ArcherUtil.createTableProperties;
import static io.trino.plugin.archer.ArcherUtil.getArcherTableProperties;
import static io.trino.plugin.archer.ArcherUtil.schemaFromMetadata;
import static io.trino.plugin.archer.InvertedIndexFieldUtils.parseInvertedIndexFields;
import static io.trino.plugin.archer.PartitionFields.parsePartitionFields;
import static io.trino.plugin.archer.SortFieldUtils.parseSortFields;
import static io.trino.plugin.archer.TableType.MATERIALIZED_VIEW_STORAGE;
import static io.trino.plugin.archer.catalog.AbstractArcherTableOperations.METADATA_FOLDER_NAME;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.metastore.glue.v1.converter.GlueToTrinoConverter.mappedCopy;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static net.qihoo.archer.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static net.qihoo.archer.TableMetadata.newTableMetadata;
import static net.qihoo.archer.TableMetadataParser.getFileExtension;
import static net.qihoo.archer.TableProperties.METADATA_COMPRESSION_DEFAULT;
import static net.qihoo.archer.Transactions.createOrReplaceTableTransaction;
import static net.qihoo.archer.Transactions.createTableTransaction;

public abstract class AbstractTrinoCatalog
        implements TrinoCatalog
{
    public static final String TRINO_CREATED_BY_VALUE = "Trino Archer connector";

    protected static final String TRINO_CREATED_BY = HiveMetadata.TRINO_CREATED_BY;
    protected static final String TRINO_QUERY_ID_NAME = HiveMetadata.TRINO_QUERY_ID_NAME;

    private final CatalogName catalogName;
    private final TypeManager typeManager;
    protected final ArcherTableOperationsProvider tableOperationsProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean useUniqueTableLocation;

    protected AbstractTrinoCatalog(
            CatalogName catalogName,
            TypeManager typeManager,
            ArcherTableOperationsProvider tableOperationsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            boolean useUniqueTableLocation)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table archerTable = loadTable(session, schemaTableName);
        if (comment.isEmpty()) {
            archerTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            archerTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
        invalidateTableCache(schemaTableName);
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        Table archerTable = loadTable(session, schemaTableName);
        archerTable.updateSchema().updateColumnDoc(columnIdentity.getName(), comment.orElse(null)).commit();
        invalidateTableCache(schemaTableName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (TableInfo tableInfo : listTables(session, namespace)) {
            if (tableInfo.extendedRelationType() != TableInfo.ExtendedRelationType.TRINO_VIEW) {
                continue;
            }
            SchemaTableName name = tableInfo.tableName();
            try {
                getView(session, name).ifPresent(view -> views.put(name, view));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode()) || e instanceof TableNotFoundException || e instanceof ViewNotFoundException) {
                    // Ignore view that was dropped during query execution (race condition)
                }
                else {
                    throw e;
                }
            }
        }
        return views.buildOrThrow();
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        try {
            return Failsafe.with(RetryPolicy.builder()
                            .withMaxAttempts(10)
                            .withBackoff(1, 5_000, ChronoUnit.MILLIS, 4)
                            .withMaxDuration(Duration.ofSeconds(30))
                            .abortOn(failure -> !(failure instanceof MaterializedViewMayBeBeingRemovedException))
                            .build())
                    .get(() -> doGetMaterializedView(session, schemaViewName));
        }
        catch (MaterializedViewMayBeBeingRemovedException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    protected abstract Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName);

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        SchemaTableName storageTableName = definition.getStorageTable()
                .orElseThrow(() -> new TrinoException(ARCHER_INVALID_METADATA, "Materialized view definition is missing a storage table"))
                .getSchemaTableName();

        try {
            Table storageTable = loadTable(session, definition.getStorageTable().orElseThrow().getSchemaTableName());
            return ImmutableMap.<String, Object>builder()
                    .putAll(getArcherTableProperties(storageTable))
                    .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                    .buildOrThrow();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ARCHER_FILESYSTEM_ERROR, "Unable to load storage table metadata for materialized view: " + viewName);
        }
    }

    protected Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            InvertedIndex invertedIndex,
            String location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, sortOrder, invertedIndex, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    protected Transaction newCreateOrReplaceTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            InvertedIndex invertedIndex,
            String location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        BaseTable table;
        Optional<TableMetadata> metadata = Optional.empty();
        try {
            table = (BaseTable) loadTable(session, new SchemaTableName(schemaTableName.getSchemaName(), schemaTableName.getTableName()));
            metadata = Optional.of(table.operations().current());
        }
        catch (TableNotFoundException _) {
            // ignored
        }
        ArcherTableOperations operations = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                Optional.of(location));
        TableMetadata newMetaData;
        if (metadata.isPresent()) {
            operations.initializeFromMetadata(metadata.get());
            newMetaData = operations.current()
                    // don't inherit table properties from earlier snapshots
                    .replaceProperties(properties)
                    .buildReplacement(schema, partitionSpec, sortOrder, invertedIndex, location, properties);
        }
        else {
            newMetaData = newTableMetadata(schema, partitionSpec, sortOrder, invertedIndex, location, properties);
        }
        return createOrReplaceTableTransaction(schemaTableName.toString(), operations, newMetaData);
    }

    protected String createNewTableName(String baseTableName)
    {
        String tableName = baseTableName;
        if (useUniqueTableLocation) {
            tableName += "-" + randomUUID().toString().replace("-", "");
        }
        return tableName;
    }

    protected void deleteTableDirectory(TrinoFileSystem fileSystem, SchemaTableName schemaTableName, String tableLocation)
    {
        try {
            fileSystem.deleteDirectory(Location.of(tableLocation));
        }
        catch (IOException e) {
            throw new TrinoException(ARCHER_FILESYSTEM_ERROR, format("Failed to delete directory %s of the table %s", tableLocation, schemaTableName), e);
        }
    }

    protected Location createMaterializedViewStorage(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        if (getStorageSchema(materializedViewProperties).isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Materialized view property '%s' is not supported when hiding materialized view storage tables is enabled".formatted(STORAGE_SCHEMA));
        }
        SchemaTableName storageTableName = new SchemaTableName(viewName.getSchemaName(), tableNameWithType(viewName.getTableName(), MATERIALIZED_VIEW_STORAGE));
        String tableLocation = getTableLocation(materializedViewProperties)
                .orElseGet(() -> defaultTableLocation(session, viewName));
        List<ColumnMetadata> columns = columnsForMaterializedView(definition, materializedViewProperties);

        Schema schema = schemaFromMetadata(columns);
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(materializedViewProperties));
        SortOrder sortOrder = parseSortFields(schema, getSortOrder(materializedViewProperties));
        InvertedIndex invertedIndex = parseInvertedIndexFields(schema, getInvertedIndex(materializedViewProperties));
        Map<String, String> properties = createTableProperties(new ConnectorTableMetadata(storageTableName, columns, materializedViewProperties, Optional.empty()));

        TableMetadata metadata = newTableMetadata(schema, partitionSpec, sortOrder, invertedIndex, tableLocation, properties);

        String fileName = format("%08d-%s%s", 0, randomUUID(), getFileExtension(METADATA_COMPRESSION_DEFAULT));
        Location metadataFileLocation = Location.of(tableLocation).appendPath(METADATA_FOLDER_NAME).appendPath(fileName);

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TableMetadataParser.write(metadata, new ForwardingOutputFile(fileSystem, metadataFileLocation));

        return metadataFileLocation;
    }

    protected void dropMaterializedViewStorage(TrinoFileSystem fileSystem, String storageMetadataLocation)
            throws IOException
    {
        TableMetadata metadata = TableMetadataParser.read(new ForwardingFileIo(fileSystem), storageMetadataLocation);
        String storageLocation = metadata.location();
        fileSystem.deleteDirectory(Location.of(storageLocation));
    }

    protected SchemaTableName createMaterializedViewStorageTable(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        // Generate a storage table name and create a storage table. The properties in the definition are table properties for the
        // storage table as indicated in the materialized view definition.
        String storageTableName = "st_" + randomUUID().toString().replace("-", "");

        String storageSchema = getStorageSchema(materializedViewProperties).orElse(viewName.getSchemaName());
        SchemaTableName storageTable = new SchemaTableName(storageSchema, storageTableName);
        List<ColumnMetadata> columns = columnsForMaterializedView(definition, materializedViewProperties);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTable, columns, materializedViewProperties, Optional.empty());
        String tableLocation = getTableLocation(tableMetadata.getProperties())
                .orElseGet(() -> defaultTableLocation(session, tableMetadata.getTable()));
        Transaction transaction = ArcherUtil.newCreateTableTransaction(this, tableMetadata, session, false, tableLocation);
        AppendFiles appendFiles = transaction.newAppend();
        commit(appendFiles, session);
        transaction.commitTransaction();
        return storageTable;
    }

    private List<ColumnMetadata> columnsForMaterializedView(ConnectorMaterializedViewDefinition definition, Map<String, Object> materializedViewProperties)
    {
        Schema schemaWithTimestampTzPreserved = schemaFromMetadata(mappedCopy(
                definition.getColumns(),
                column -> {
                    Type type = typeManager.getType(column.getType());
                    if (type instanceof TimestampWithTimeZoneType timestampTzType && timestampTzType.getPrecision() <= 6) {
                        // For now preserve timestamptz columns so that we can parse partitioning
                        type = TIMESTAMP_TZ_MICROS;
                    }
                    else {
                        type = typeForMaterializedViewStorageTable(type);
                    }
                    return new ColumnMetadata(column.getName(), type);
                }));
        PartitionSpec partitionSpec = parsePartitionFields(schemaWithTimestampTzPreserved, getPartitioning(materializedViewProperties));
        Set<String> temporalPartitioningSources = partitionSpec.fields().stream()
                .flatMap(partitionField -> {
                    Types.NestedField sourceField = schemaWithTimestampTzPreserved.findField(partitionField.sourceId());
                    return Stream.of(sourceField.name());
                })
                .collect(toImmutableSet());

        return mappedCopy(
                definition.getColumns(),
                column -> {
                    Type type = typeManager.getType(column.getType());
                    if (type instanceof TimestampWithTimeZoneType timestampTzType && timestampTzType.getPrecision() <= 6 && temporalPartitioningSources.contains(column.getName())) {
                        // Apply point-in-time semantics to maintain partitioning capabilities
                        type = TIMESTAMP_TZ_MICROS;
                    }
                    else {
                        type = typeForMaterializedViewStorageTable(type);
                    }
                    return new ColumnMetadata(column.getName(), type);
                });
    }

    /**
     * Substitutes type not supported by Archer with a type that is supported.
     * Upon reading from a materialized view, the types will be coerced back to the original ones,
     * stored in the materialized view definition.
     */
    private Type typeForMaterializedViewStorageTable(Type type)
    {
        if (type == TINYINT || type == SMALLINT) {
            return INTEGER;
        }
        if (type instanceof CharType) {
            return VARCHAR;
        }
        if (type instanceof TimeType timeType) {
            // Archer supports microsecond precision only
            return timeType.getPrecision() <= 6
                    ? TIME_MICROS
                    : VARCHAR;
        }
        if (type instanceof TimeWithTimeZoneType) {
            return VARCHAR;
        }
        if (type instanceof TimestampType timestampType) {
            // Archer supports microsecond precision only
            return timestampType.getPrecision() <= 6
                    ? TIMESTAMP_MICROS
                    : VARCHAR;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            // Archer does not store the time zone.
            return VARCHAR;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(typeForMaterializedViewStorageTable(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(
                    typeForMaterializedViewStorageTable(mapType.getKeyType()),
                    typeForMaterializedViewStorageTable(mapType.getValueType()),
                    typeManager.getTypeOperators());
        }
        if (type instanceof RowType rowType) {
            return RowType.rowType(
                    rowType.getFields().stream()
                            .map(field -> new RowType.Field(field.getName(), typeForMaterializedViewStorageTable(field.getType())))
                            .toArray(RowType.Field[]::new));
        }

        // Pass through all the types not explicitly handled above. If a type is not accepted by the connector,
        // creation of the storage table will fail anyway.
        return type;
    }

    protected ConnectorMaterializedViewDefinition getMaterializedViewDefinition(
            Optional<String> owner,
            String viewOriginalText,
            SchemaTableName storageTableName)
    {
        ArcherMaterializedViewDefinition definition = decodeMaterializedViewData(viewOriginalText);
        return new ConnectorMaterializedViewDefinition(
                definition.originalSql(),
                Optional.of(new CatalogSchemaTableName(catalogName.toString(), storageTableName)),
                definition.catalog(),
                definition.schema(),
                toSpiMaterializedViewColumns(definition.columns()),
                definition.gracePeriod(),
                definition.comment(),
                owner,
                definition.path());
    }

    protected List<ConnectorMaterializedViewDefinition.Column> toSpiMaterializedViewColumns(List<ArcherMaterializedViewDefinition.Column> columns)
    {
        return columns.stream()
                .map(column -> new ConnectorMaterializedViewDefinition.Column(column.name(), column.type(), column.comment()))
                .collect(toImmutableList());
    }

    protected Map<String, String> createMaterializedViewProperties(ConnectorSession session, SchemaTableName storageTableName)
    {
        return ImmutableMap.<String, String>builder()
                .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                .put(STORAGE_TABLE, storageTableName.getTableName())
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .buildOrThrow();
    }

    protected Map<String, String> createMaterializedViewProperties(ConnectorSession session, Location storageMetadataLocation)
    {
        return ImmutableMap.<String, String>builder()
                .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                .put(METADATA_LOCATION_PROP, storageMetadataLocation.toString())
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .buildOrThrow();
    }

    protected abstract void invalidateTableCache(SchemaTableName schemaTableName);

    protected static class MaterializedViewMayBeBeingRemovedException
            extends RuntimeException
    {
        public MaterializedViewMayBeBeingRemovedException(Throwable cause)
        {
            super(requireNonNull(cause, "cause is null"));
        }
    }
}
