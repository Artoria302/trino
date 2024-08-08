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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.plugin.paimon.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.difference;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_COMMIT_ERROR;
import static io.trino.plugin.paimon.PaimonTableOptionUtils.checkConfigOptions;
import static io.trino.plugin.paimon.PaimonTableOptions.LOCATION_PROPERTY;
import static io.trino.plugin.paimon.PaimonTableOptions.OPTIONS_PROPERTY;
import static io.trino.plugin.paimon.PaimonTableOptions.getTableLocation;
import static io.trino.plugin.paimon.catalog.hms.TrinoHiveCatalog.DB_LOCATION_PROP;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Trino {@link ConnectorMetadata}.
 */
public class PaimonMetadata
        implements ConnectorMetadata
{
    public static final Set<String> UPDATABLE_TABLE_PROPERTIES = ImmutableSet.of(OPTIONS_PROPERTY);

    private final AbstractCatalog catalog;

    public PaimonMetadata(AbstractCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return catalog.databaseExists(schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listDatabases();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        try {
            Map<String, String> dbProperties = new HashMap<>();
            properties.forEach((property, value) -> {
                if (property.equals(LOCATION_PROPERTY)) {
                    String location = (String) value;
                    dbProperties.put(DB_LOCATION_PROP, location);
                }
                else {
                    throw new IllegalArgumentException("Unrecognized property: " + property);
                }
            });
            if (catalog instanceof TrinoCatalog trinoCatalog) {
                trinoCatalog.createDatabase(schemaName, true, dbProperties, owner);
            }
            else {
                catalog.createDatabase(schemaName, true, dbProperties);
            }
        }
        catch (Catalog.DatabaseAlreadyExistException e) {
            throw new TrinoException(SCHEMA_ALREADY_EXISTS, format("schema already existed: '%s'", schemaName), e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(schemaName), "schemaName cannot be null or empty");
        try {
            catalog.dropDatabase(schemaName, false, cascade);
        }
        catch (Catalog.DatabaseNotEmptyException e) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, format("schema is not empty: '%s'", schemaName), e);
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("schema not exists: '%s'", schemaName), e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read paimon table with start version is not supported");
        }

        Map<String, String> dynamicOptions = new HashMap<>();
        if (endVersion.isPresent()) {
            ConnectorTableVersion version = endVersion.get();
            Type versionType = version.getVersionType();
            switch (version.getPointerType()) {
                case TEMPORAL: {
                    long epochMillis = getEpochMillis(versionType, version);
                    dynamicOptions.put(
                            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                            String.valueOf(epochMillis));
                    break;
                }
                case TARGET_ID: {
                    dynamicOptions.put(
                            CoreOptions.SCAN_SNAPSHOT_ID.key(),
                            version.getVersion().toString());
                    break;
                }
            }
        }
        return getTableHandle(session, tableName, dynamicOptions);
    }

    private static long getEpochMillis(Type versionType, ConnectorTableVersion version)
    {
        if (!(versionType instanceof TimestampWithTimeZoneType timeZonedVersionType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
        }
        return timeZonedVersionType.isShort()
                ? unpackMillisUtc((long) version.getVersion())
                : ((LongTimestampWithTimeZone) version.getVersion())
                .getEpochMillis();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    public PaimonTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Map<String, String> dynamicOptions)
    {
        return catalog.tableExists(Identifier.create(tableName.getSchemaName(), tableName.getTableName()))
                ? new PaimonTableHandle(tableName.getSchemaName(), tableName.getTableName(), dynamicOptions)
                : null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return ((PaimonTableHandle) tableHandle).tableMetadata(catalog);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier = new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        Set<String> unsupportedProperties = difference(properties.keySet(), UPDATABLE_TABLE_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }

        List<SchemaChange> changes = new ArrayList<>();

        if (properties.containsKey(OPTIONS_PROPERTY)) {
            Set<String> fieldNames = ((PaimonTableHandle) tableHandle).columnMetadatas(catalog).stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
            @SuppressWarnings("unchecked")
            Map<String, String> options = (Map<String, String>) properties.get(OPTIONS_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The options property cannot be empty"));
            checkConfigOptions(options, fieldNames);
            options.forEach((key, value) -> changes.add(SchemaChange.setOption(key, value)));
        }

        try {
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new TrinoException(PAIMON_COMMIT_ERROR, format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName
                .map(Collections::singletonList)
                .orElseGet(catalog::listDatabases)
                .forEach(schema -> tables.addAll(listTables(schema)));
        return tables;
    }

    private List<SchemaTableName> listTables(String schema)
    {
        try {
            return catalog.listTables(schema).stream()
                    .map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("schema not exists: '%s'", schema), e);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName table = tableMetadata.getTable();
        Identifier identifier = Identifier.create(table.getSchemaName(), table.getTableName());
        try {
            Optional<String> tableLocation = getTableLocation(tableMetadata.getProperties());
            if (catalog instanceof TrinoCatalog trinoCatalog) {
                trinoCatalog.createTable(identifier, prepareSchema(tableMetadata), session.getUser(), tableLocation.map(Path::new), false);
            }
            else {
                if (tableLocation.isPresent()) {
                    throw new TrinoException(PAIMON_COMMIT_ERROR, "Only hive catalog support set location when create table");
                }
                catalog.createTable(identifier, prepareSchema(tableMetadata), false);
            }
        }
        catch (Catalog.DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("schema not exists: '%s'", table.getSchemaName()), e);
        }
        catch (Catalog.TableAlreadyExistException e) {
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("table already existed: '%s'", table.getTableName()), e);
        }
    }

    private Schema prepareSchema(ConnectorTableMetadata tableMetadata)
    {
        ImmutableSet.Builder<String> fieldNames = ImmutableSet.builder();
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        Schema.Builder builder =
                Schema.newBuilder()
                        .primaryKey(PaimonTableOptions.getPrimaryKeys(properties))
                        .partitionKeys(PaimonTableOptions.getPartitionedKeys(properties));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            fieldNames.add(column.getName());
            builder.column(
                    column.getName(),
                    PaimonTypeUtils.toPaimonType(column.getType()),
                    column.getComment());
        }

        if (tableMetadata.getComment().isPresent()) {
            builder.comment(tableMetadata.getComment().get());
        }

        Map<String, String> tableOptions = PaimonTableOptions.getTableOptions(properties);
        checkConfigOptions(tableOptions, fieldNames.build());
        builder.options(tableOptions);

        return builder.build();
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        PaimonTableHandle oldTableHandle = (PaimonTableHandle) tableHandle;
        try {
            catalog.renameTable(
                    new Identifier(oldTableHandle.getSchemaName(), oldTableHandle.getTableName()),
                    new Identifier(newTableName.getSchemaName(), newTableName.getTableName()),
                    false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(TABLE_NOT_FOUND, format("table not exists: '%s'", oldTableHandle.getTableName()), e);
        }
        catch (Catalog.TableAlreadyExistException e) {
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("table already existed: '%s'", newTableName.getTableName()), e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        try {
            catalog.dropTable(new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName()), false);
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(TABLE_NOT_FOUND, format("table not exists: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle table = (PaimonTableHandle) tableHandle;
        Map<String, ColumnHandle> handleMap = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(catalog)) {
            handleMap.put(column.getName(), table.columnHandle(catalog, column.getName()));
        }
        return handleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((PaimonColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames;
        if (prefix.getTable().isPresent()) {
            tableNames = Collections.singletonList(prefix.toSchemaTableName());
        }
        else {
            tableNames = listTables(session, prefix.getSchema());
        }

        return Lists.partition(tableNames, 1000).stream()
                .map(tableBatch -> {
                    ImmutableList.Builder<TableColumnsMetadata> tableMetadatas = ImmutableList.builderWithExpectedSize(tableBatch.size());
                    for (SchemaTableName tableName : tableBatch) {
                        List<ColumnMetadata> columns = getTableHandle(session, tableName, Collections.emptyMap()).columnMetadatas(catalog);
                        tableMetadatas.add(TableColumnsMetadata.forTable(tableName, columns));
                    }
                    return tableMetadatas.build();
                })
                .flatMap(List::stream)
                .iterator();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(
                SchemaChange.addColumn(
                        column.getName(), PaimonTypeUtils.toPaimonType(column.getType())));
        try {
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new TrinoException(PAIMON_COMMIT_ERROR, format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void renameColumn(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle source,
            String target)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) source;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(paimonColumnHandle.getColumnName(), target));
        try {
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new TrinoException(PAIMON_COMMIT_ERROR, format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(paimonTableHandle.getSchemaName(), paimonTableHandle.getTableName());
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) column;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(paimonColumnHandle.getColumnName()));
        try {
            catalog.alterTable(identifier, changes, false);
        }
        catch (Exception e) {
            throw new TrinoException(PAIMON_COMMIT_ERROR, format("failed to alter table: '%s'", paimonTableHandle.getTableName()), e);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) handle;
        Optional<PaimonFilterExtractor.TrinoFilter> extract =
                PaimonFilterExtractor.extract(catalog, paimonTableHandle, constraint);
        if (extract.isPresent()) {
            PaimonFilterExtractor.TrinoFilter trinoFilter = extract.get();
            return Optional.of(
                    new ConstraintApplicationResult<>(
                            paimonTableHandle.copy(trinoFilter.filter()),
                            trinoFilter.remainFilter(),
                            false));
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) handle;
        List<ColumnHandle> newColumns = new ArrayList<>(assignments.values());

        if (paimonTableHandle.getProjectedColumns().isPresent()
                && containSameElements(newColumns, paimonTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        List<Assignment> assignmentList = new ArrayList<>();
        assignments.forEach(
                (name, column) ->
                        assignmentList.add(
                                new Assignment(
                                        name,
                                        column,
                                        ((PaimonColumnHandle) column).getTrinoType())));

        return Optional.of(
                new ProjectionApplicationResult<>(
                        paimonTableHandle.copy(Optional.of(newColumns)),
                        projections,
                        assignmentList,
                        false));
    }

    private static boolean containSameElements(List<? extends ColumnHandle> first, List<? extends ColumnHandle> second)
    {
        return new HashSet<>(first).equals(new HashSet<>(second));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        PaimonTableHandle table = (PaimonTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        if (!table.getFilter().isAll()) {
            Table paimonTable = table.table(catalog);
            LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
            LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
            new PaimonFilterConverter(paimonTable.rowType())
                    .convert(table.getFilter(), acceptedDomains, unsupportedDomains);
            Set<String> acceptedFields =
                    acceptedDomains.keySet().stream()
                            .map(PaimonColumnHandle::getColumnName)
                            .collect(Collectors.toSet());
            if (!unsupportedDomains.isEmpty()
                    || !new HashSet<>(paimonTable.partitionKeys()).containsAll(acceptedFields)) {
                return Optional.empty();
            }
        }

        table = table.copy(OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }
}
