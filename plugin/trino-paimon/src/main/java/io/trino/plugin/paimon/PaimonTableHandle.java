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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static io.trino.plugin.paimon.PaimonSessionProperties.getScanSnapshotId;
import static io.trino.plugin.paimon.PaimonSessionProperties.getScanTimestampMillis;
import static io.trino.plugin.paimon.PaimonTableOptions.LOCATION_PROPERTY;
import static io.trino.plugin.paimon.PaimonTableOptions.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.paimon.PaimonTableOptions.PRIMARY_KEY_IDENTIFIER;

/**
 * Trino {@link ConnectorTableHandle}.
 */
public class PaimonTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<PaimonColumnHandle> filter;
    private final Optional<List<ColumnHandle>> projectedColumns;
    private final OptionalLong limit;
    private final Map<String, String> dynamicOptions;

    private transient String location;
    private transient Table table;

    public PaimonTableHandle(String schemaName, String tableName, Map<String, String> dynamicOptions)
    {
        this(schemaName, tableName, dynamicOptions, TupleDomain.all(), Optional.empty(), OptionalLong.empty());
    }

    @JsonCreator
    public PaimonTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("dynamicOptions") Map<String, String> dynamicOptions,
            @JsonProperty("filter") TupleDomain<PaimonColumnHandle> filter,
            @JsonProperty("projection") Optional<List<ColumnHandle>> projectedColumns,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.dynamicOptions = dynamicOptions;
        this.filter = filter;
        this.projectedColumns = projectedColumns;
        this.limit = limit;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Map<String, String> getDynamicOptions()
    {
        return dynamicOptions;
    }

    @JsonProperty
    public TupleDomain<PaimonColumnHandle> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    public OptionalLong getLimit()
    {
        return limit;
    }

    public Table tableWithDynamicOptions(Catalog catalog, ConnectorSession session)
    {
        Table paimonTable = table(catalog);

        // see TrinoConnector.getSessionProperties
        Map<String, String> dynamicOptions = new HashMap<>();
        Long scanTimestampMills = getScanTimestampMillis(session);
        if (scanTimestampMills != null) {
            dynamicOptions.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), scanTimestampMills.toString());
        }
        Long scanSnapshotId = getScanSnapshotId(session);
        if (scanSnapshotId != null) {
            dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), scanSnapshotId.toString());
        }

        return !dynamicOptions.isEmpty() ? paimonTable.copy(dynamicOptions) : paimonTable;
    }

    public Table table(Catalog catalog)
    {
        if (table != null) {
            return table;
        }
        try {
            table = catalog.getTable(Identifier.create(schemaName, tableName)).copy(dynamicOptions);
        }
        catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    public String location(AbstractCatalog catalog)
    {
        if (location != null) {
            return location;
        }
        location = catalog.getDataTableLocation(Identifier.create(schemaName, tableName)).toString();
        return location;
    }

    public ConnectorTableMetadata tableMetadata(AbstractCatalog catalog)
    {
        return new ConnectorTableMetadata(
                SchemaTableName.schemaTableName(schemaName, tableName),
                columnMetadatas(catalog),
                getPaimonTableProperties(catalog),
                table(catalog).comment());
    }

    public Map<String, Object> getPaimonTableProperties(AbstractCatalog catalog)
    {
        Table table = table(catalog);
        String location = location(catalog);
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (table.partitionKeys() != null && !table.partitionKeys().isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, table.partitionKeys());
        }
        if (table.primaryKeys() != null && !table.primaryKeys().isEmpty()) {
            properties.put(PRIMARY_KEY_IDENTIFIER, table.primaryKeys());
        }
        properties.put(LOCATION_PROPERTY, location);
        return properties.buildOrThrow();
    }

    public List<ColumnMetadata> columnMetadatas(Catalog catalog)
    {
        return table(catalog).rowType().getFields().stream()
                .map(
                        column ->
                                ColumnMetadata.builder()
                                        .setName(column.name())
                                        .setType(PaimonTypeUtils.fromPaimonType(column.type()))
                                        .setNullable(column.type().isNullable())
                                        .setComment(Optional.ofNullable(column.description()))
                                        .build())
                .collect(Collectors.toList());
    }

    public PaimonColumnHandle columnHandle(Catalog catalog, String field)
    {
        Table paimonTable = table(catalog);
        List<String> lowerCaseFieldNames = FieldNameUtils.fieldNames(paimonTable.rowType());
        List<String> originFieldNames = paimonTable.rowType().getFieldNames();
        int index = lowerCaseFieldNames.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format("Cannot find field %s in schema %s", field, lowerCaseFieldNames));
        }
        return PaimonColumnHandle.of(
                originFieldNames.get(index), paimonTable.rowType().getTypeAt(index));
    }

    public PaimonTableHandle copy(TupleDomain<PaimonColumnHandle> filter)
    {
        return new PaimonTableHandle(
                schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
    }

    public PaimonTableHandle copy(Optional<List<ColumnHandle>> projectedColumns)
    {
        return new PaimonTableHandle(
                schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
    }

    public PaimonTableHandle copy(OptionalLong limit)
    {
        return new PaimonTableHandle(
                schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonTableHandle that = (PaimonTableHandle) o;
        return Objects.equals(dynamicOptions, that.dynamicOptions)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(filter, that.filter)
                && Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, filter, projectedColumns, dynamicOptions);
    }
}
