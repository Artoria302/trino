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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.InvertedIndexParser;
import net.qihoo.archer.Schema;
import net.qihoo.archer.SchemaParser;
import net.qihoo.archer.index.InvertedIndexQuery;
import net.qihoo.archer.index.InvertedIndexQueryParser;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ArcherTableHandle
        implements ConnectorTableHandle
{
    private final CatalogHandle catalog;
    private final String schemaName;
    private final String tableName;
    private final TableType tableType;
    private final Optional<Long> snapshotId;
    private final String tableSchemaJson;
    // Empty means the partitioning spec is not known (can be the case for certain time travel queries).
    private final Optional<String> partitionSpecJson;
    private final Optional<String> invertedIndexJson;
    private final int formatVersion;
    private final String tableLocation;
    private final Map<String, String> storageProperties;
    private final RetryMode retryMode;

    // UPDATE only
    private final List<ArcherColumnHandle> updatedColumns;

    // Filter used during split generation and table scan, but not required to be strictly enforced by Archer Connector
    private final TupleDomain<ArcherColumnHandle> unenforcedPredicate;

    // Filter guaranteed to be enforced by Archer connector
    private final TupleDomain<ArcherColumnHandle> enforcedPredicate;
    private final Set<ArcherColumnHandle> constraintColumns;
    private final Optional<String> invertedIndexQueryJson;
    private final OptionalLong limit;
    private final Set<ArcherColumnHandle> projectedColumns;
    private final Optional<String> nameMappingJson;

    // OPTIMIZE only. Coordinator-only
    private final boolean recordScannedFiles;
    private final Optional<DataSize> maxScannedFileSize;

    private transient Schema schema;
    private transient InvertedIndex invertedIndex;
    private transient InvertedIndexQuery invertedIndexQuery;

    @JsonCreator
    public static ArcherTableHandle fromJsonForDeserializationOnly(
            @JsonProperty("catalog") CatalogHandle catalog,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("tableSchemaJson") String tableSchemaJson,
            @JsonProperty("partitionSpecJson") Optional<String> partitionSpecJson,
            @JsonProperty("invertedIndexJson") Optional<String> invertedIndexJson,
            @JsonProperty("formatVersion") int formatVersion,
            @JsonProperty("unenforcedPredicate") TupleDomain<ArcherColumnHandle> unenforcedPredicate,
            @JsonProperty("enforcedPredicate") TupleDomain<ArcherColumnHandle> enforcedPredicate,
            @JsonProperty("invertedIndexQueryJson") Optional<String> invertedIndexQueryJson,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("projectedColumns") Set<ArcherColumnHandle> projectedColumns,
            @JsonProperty("nameMappingJson") Optional<String> nameMappingJson,
            @JsonProperty("tableLocation") String tableLocation,
            @JsonProperty("storageProperties") Map<String, String> storageProperties,
            @JsonProperty("retryMode") RetryMode retryMode,
            @JsonProperty("updatedColumns") List<ArcherColumnHandle> updatedColumns)
    {
        return new ArcherTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                invertedIndexJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                invertedIndexQueryJson,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                null,
                null,
                null,
                retryMode,
                updatedColumns,
                false,
                Optional.empty(),
                ImmutableSet.of());
    }

    public ArcherTableHandle(
            CatalogHandle catalog,
            String schemaName,
            String tableName,
            TableType tableType,
            Optional<Long> snapshotId,
            String tableSchemaJson,
            Optional<String> partitionSpecJson,
            Optional<String> invertedIndexJson,
            int formatVersion,
            TupleDomain<ArcherColumnHandle> unenforcedPredicate,
            TupleDomain<ArcherColumnHandle> enforcedPredicate,
            Optional<String> invertedIndexQueryJson,
            OptionalLong limit,
            Set<ArcherColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            String tableLocation,
            Map<String, String> storageProperties,
            Schema schema,
            InvertedIndex invertedIndex,
            InvertedIndexQuery invertedIndexQuery,
            RetryMode retryMode,
            List<ArcherColumnHandle> updatedColumns,
            boolean recordScannedFiles,
            Optional<DataSize> maxScannedFileSize,
            Set<ArcherColumnHandle> constraintColumns)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.tableSchemaJson = requireNonNull(tableSchemaJson, "schemaJson is null");
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.invertedIndexJson = requireNonNull(invertedIndexJson, "invertedIndexJson is null");
        this.formatVersion = formatVersion;
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        this.enforcedPredicate = requireNonNull(enforcedPredicate, "enforcedPredicate is null");
        this.invertedIndexQueryJson = invertedIndexQueryJson;
        this.limit = requireNonNull(limit, "limit is null");
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.nameMappingJson = requireNonNull(nameMappingJson, "nameMappingJson is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.retryMode = requireNonNull(retryMode, "retryMode is null");
        this.updatedColumns = ImmutableList.copyOf(requireNonNull(updatedColumns, "updatedColumns is null"));
        this.recordScannedFiles = recordScannedFiles;
        this.maxScannedFileSize = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
        this.schema = schema;
        this.invertedIndex = invertedIndex;
        this.invertedIndexQuery = invertedIndexQuery;
        this.constraintColumns = requireNonNull(constraintColumns, "constraintColumns is null");
    }

    @JsonProperty
    public CatalogHandle getCatalog()
    {
        return catalog;
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
    public TableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public String getTableSchemaJson()
    {
        return tableSchemaJson;
    }

    @JsonProperty
    public Optional<String> getPartitionSpecJson()
    {
        return partitionSpecJson;
    }

    @JsonProperty
    public Optional<String> getInvertedIndexJson()
    {
        return invertedIndexJson;
    }

    @JsonProperty
    public int getFormatVersion()
    {
        return formatVersion;
    }

    @JsonProperty
    public TupleDomain<ArcherColumnHandle> getUnenforcedPredicate()
    {
        return unenforcedPredicate;
    }

    @JsonProperty
    public TupleDomain<ArcherColumnHandle> getEnforcedPredicate()
    {
        return enforcedPredicate;
    }

    @JsonProperty
    public Optional<String> getInvertedIndexQueryJson()
    {
        return invertedIndexQueryJson;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Set<ArcherColumnHandle> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public Optional<String> getNameMappingJson()
    {
        return nameMappingJson;
    }

    @JsonProperty
    public String getTableLocation()
    {
        return tableLocation;
    }

    @JsonProperty
    public Map<String, String> getStorageProperties()
    {
        return storageProperties;
    }

    @JsonProperty
    public RetryMode getRetryMode()
    {
        return retryMode;
    }

    @JsonProperty
    public List<ArcherColumnHandle> getUpdatedColumns()
    {
        return updatedColumns;
    }

    @JsonIgnore
    public boolean isRecordScannedFiles()
    {
        return recordScannedFiles;
    }

    @JsonIgnore
    public Optional<DataSize> getMaxScannedFileSize()
    {
        return maxScannedFileSize;
    }

    @JsonIgnore
    public Set<ArcherColumnHandle> getConstraintColumns()
    {
        return constraintColumns;
    }

    public Schema getSchema()
    {
        if (schema == null) {
            schema = SchemaParser.fromJson(tableSchemaJson);
        }
        return schema;
    }

    public Optional<InvertedIndex> getInvertedIndex()
    {
        if (invertedIndexJson.isEmpty()) {
            return Optional.empty();
        }
        if (invertedIndex == null) {
            invertedIndex = InvertedIndexParser.fromJson(getSchema(), invertedIndexJson.get());
        }
        return Optional.of(invertedIndex);
    }

    public Optional<InvertedIndexQuery> getInvertedIndexQuery()
    {
        if (invertedIndexQueryJson.isEmpty()) {
            return Optional.empty();
        }
        if (invertedIndexQuery == null) {
            invertedIndexQuery = InvertedIndexQueryParser.fromJson(invertedIndexQueryJson.get());
        }
        return Optional.of(invertedIndexQuery);
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public SchemaTableName getSchemaTableNameWithType()
    {
        return new SchemaTableName(schemaName, tableName + "$" + tableType.name().toLowerCase(Locale.ROOT));
    }

    public ArcherTableHandle withProjectedColumns(Set<ArcherColumnHandle> projectedColumns)
    {
        return new ArcherTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                invertedIndexJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                invertedIndexQueryJson,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                schema,
                invertedIndex,
                invertedIndexQuery,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize,
                constraintColumns);
    }

    public ArcherTableHandle withRetryMode(RetryMode retryMode)
    {
        return new ArcherTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                invertedIndexJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                invertedIndexQueryJson,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                schema,
                invertedIndex,
                invertedIndexQuery,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize,
                constraintColumns);
    }

    public ArcherTableHandle withUpdatedColumns(List<ArcherColumnHandle> updatedColumns)
    {
        return new ArcherTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                invertedIndexJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                invertedIndexQueryJson,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                schema,
                invertedIndex,
                invertedIndexQuery,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize,
                constraintColumns);
    }

    public ArcherTableHandle forOptimize(boolean recordScannedFiles, DataSize maxScannedFileSize)
    {
        return new ArcherTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                invertedIndexJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                invertedIndexQueryJson,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                schema,
                invertedIndex,
                invertedIndexQuery,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                Optional.of(maxScannedFileSize),
                constraintColumns);
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

        ArcherTableHandle that = (ArcherTableHandle) o;
        return recordScannedFiles == that.recordScannedFiles &&
                Objects.equals(catalog, that.catalog) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                tableType == that.tableType &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(tableSchemaJson, that.tableSchemaJson) &&
                Objects.equals(partitionSpecJson, that.partitionSpecJson) &&
                Objects.equals(invertedIndexJson, that.invertedIndexJson) &&
                formatVersion == that.formatVersion &&
                Objects.equals(unenforcedPredicate, that.unenforcedPredicate) &&
                Objects.equals(enforcedPredicate, that.enforcedPredicate) &&
                Objects.equals(invertedIndexQueryJson, that.invertedIndexQueryJson) &&
                Objects.equals(limit, that.limit) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(nameMappingJson, that.nameMappingJson) &&
                Objects.equals(tableLocation, that.tableLocation) &&
                Objects.equals(retryMode, that.retryMode) &&
                Objects.equals(updatedColumns, that.updatedColumns) &&
                Objects.equals(storageProperties, that.storageProperties) &&
                Objects.equals(maxScannedFileSize, that.maxScannedFileSize) &&
                Objects.equals(constraintColumns, that.constraintColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schemaName, tableName, tableType, snapshotId, tableSchemaJson, partitionSpecJson, invertedIndexJson, formatVersion, unenforcedPredicate, enforcedPredicate,
                invertedIndexQueryJson, limit, projectedColumns, nameMappingJson, tableLocation, storageProperties, retryMode, updatedColumns, recordScannedFiles, maxScannedFileSize, constraintColumns);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(getSchemaTableNameWithType().toString());
        snapshotId.ifPresent(snapshotId -> builder.append("@").append(snapshotId));
        if (enforcedPredicate.isNone()) {
            builder.append(" constraint=FALSE");
        }
        else if (!enforcedPredicate.isAll()) {
            builder.append(" constraint on ");
            builder.append(enforcedPredicate.getDomains().orElseThrow().keySet().stream()
                    .map(ArcherColumnHandle::getQualifiedName)
                    .collect(joining(", ", "[", "]")));
        }
        return builder.toString();
    }
}
