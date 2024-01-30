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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import net.qihoo.archer.DataFile;
import net.qihoo.archer.FileScanTask;
import net.qihoo.archer.Schema;
import net.qihoo.archer.Table;
import net.qihoo.archer.TableScan;
import net.qihoo.archer.io.CloseableGroup;
import net.qihoo.archer.io.CloseableIterable;
import net.qihoo.archer.io.CloseableIterator;
import net.qihoo.archer.transforms.Transforms;
import net.qihoo.archer.types.Conversions;
import net.qihoo.archer.types.Type;
import net.qihoo.archer.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class FilesTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final TypeManager typeManager;
    private final Table archerTable;
    private final Optional<Long> snapshotId;

    public FilesTable(SchemaTableName tableName, TypeManager typeManager, Table archerTable, Optional<Long> snapshotId)
    {
        this.archerTable = requireNonNull(archerTable, "archerTable is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("content", INTEGER))
                        .add(new ColumnMetadata("file_path", VARCHAR))
                        .add(new ColumnMetadata("file_name", VARCHAR))
                        .add(new ColumnMetadata("version", INTEGER))
                        .add(new ColumnMetadata("file_format", VARCHAR))
                        .add(new ColumnMetadata("record_count", BIGINT))
                        .add(new ColumnMetadata("file_size_in_bytes", BIGINT))
                        .add(new ColumnMetadata("last_modified_time", TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata("column_sizes", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("null_value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("nan_value_counts", typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))))
                        .add(new ColumnMetadata("lower_bounds", typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("upper_bounds", typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("key_metadata", VARBINARY))
                        .add(new ColumnMetadata("split_offsets", new ArrayType(BIGINT)))
                        .add(new ColumnMetadata("primary_key_ids", new ArrayType(INTEGER)))
                        .add(new ColumnMetadata("sort_order_id", INTEGER))
                        .add(new ColumnMetadata("inverted_index_id", INTEGER))
                        .add(new ColumnMetadata("delta_record_count", BIGINT))
                        .add(new ColumnMetadata("segment_size_in_bytes", BIGINT))
                        .build());
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        List<io.trino.spi.type.Type> types = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        if (snapshotId.isEmpty()) {
            return InMemoryRecordSet.builder(types).build().cursor();
        }

        Map<Integer, Type> idToTypeMapping = getArcherIdToTypeMapping(archerTable.schema());
        TableScan tableScan = archerTable.newScan()
                .useSnapshot(snapshotId.get())
                .includeColumnStats();

        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        PlanFilesIterable planFilesIterable = new PlanFilesIterable(tableScan.planFiles(), idToTypeMapping, types, typeManager, timeZoneKey);
        return planFilesIterable.cursor();
    }

    private static class PlanFilesIterable
            extends CloseableGroup
            implements Iterable<List<Object>>
    {
        private final CloseableIterable<FileScanTask> planFiles;
        private final Map<Integer, Type> idToTypeMapping;
        private final List<io.trino.spi.type.Type> types;
        private boolean closed;
        private final MapType integerToBigintMapType;
        private final MapType integerToVarcharMapType;
        private final TimeZoneKey timeZoneKey;

        public PlanFilesIterable(CloseableIterable<FileScanTask> planFiles, Map<Integer, Type> idToTypeMapping, List<io.trino.spi.type.Type> types, TypeManager typeManager, TimeZoneKey timeZoneKey)
        {
            this.planFiles = requireNonNull(planFiles, "planFiles is null");
            this.idToTypeMapping = ImmutableMap.copyOf(requireNonNull(idToTypeMapping, "idToTypeMapping is null"));
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.integerToBigintMapType = new MapType(INTEGER, BIGINT, typeManager.getTypeOperators());
            this.integerToVarcharMapType = new MapType(INTEGER, VARCHAR, typeManager.getTypeOperators());
            this.timeZoneKey = timeZoneKey;
            addCloseable(planFiles);
        }

        public RecordCursor cursor()
        {
            CloseableIterator<List<Object>> iterator = this.iterator();
            return new InMemoryRecordSet.InMemoryRecordCursor(types, iterator)
            {
                @Override
                public void close()
                {
                    try (iterator) {
                        super.close();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("Failed to close cursor", e);
                    }
                }
            };
        }

        @Override
        public CloseableIterator<List<Object>> iterator()
        {
            final CloseableIterator<FileScanTask> planFilesIterator = planFiles.iterator();
            addCloseable(planFilesIterator);

            return new CloseableIterator<>()
            {
                @Override
                public boolean hasNext()
                {
                    return !closed && planFilesIterator.hasNext();
                }

                @Override
                public List<Object> next()
                {
                    return getRecord(planFilesIterator.next().file());
                }

                @Override
                public void close()
                        throws IOException
                {
                    PlanFilesIterable.super.close();
                    closed = true;
                }
            };
        }

        private List<Object> getRecord(DataFile dataFile)
        {
            List<Object> columns = new ArrayList<>();
            columns.add(dataFile.content().id());
            columns.add(dataFile.path().toString());
            columns.add(dataFile.fileName().toString());
            columns.add(dataFile.version());
            columns.add(dataFile.format().name());
            columns.add(dataFile.recordCount());
            columns.add(dataFile.fileSizeInBytes());
            columns.add(packDateTimeWithZone(Math.max(dataFile.lastModifiedTime(), 0), timeZoneKey));
            columns.add(getIntegerBigintSqlMap(dataFile.columnSizes()));
            columns.add(getIntegerBigintSqlMap(dataFile.valueCounts()));
            columns.add(getIntegerBigintSqlMap(dataFile.nullValueCounts()));
            columns.add(getIntegerBigintSqlMap(dataFile.nanValueCounts()));
            columns.add(getIntegerVarcharSqlMap(dataFile.lowerBounds()));
            columns.add(getIntegerVarcharSqlMap(dataFile.upperBounds()));
            columns.add(toVarbinarySlice(dataFile.keyMetadata()));
            columns.add(toBigintArrayBlock(dataFile.splitOffsets()));
            columns.add(toIntegerArrayBlock(dataFile.primaryKeyFieldIds()));
            columns.add(dataFile.sortOrderId());
            columns.add(dataFile.invertedIndexId());
            columns.add(dataFile.deltaRecordCount());
            columns.add(dataFile.segmentSizeInBytes());
            checkArgument(columns.size() == types.size(), "Expected %s types in row, but got %s values", types.size(), columns.size());
            return columns;
        }

        private SqlMap getIntegerBigintSqlMap(Map<Integer, Long> value)
        {
            if (value == null) {
                return null;
            }
            return toIntegerBigintSqlMap(value);
        }

        private SqlMap getIntegerVarcharSqlMap(Map<Integer, ByteBuffer> value)
        {
            if (value == null) {
                return null;
            }
            return toIntegerVarcharSqlMap(
                    value.entrySet().stream()
                            .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                            .collect(toImmutableMap(
                                    Map.Entry<Integer, ByteBuffer>::getKey,
                                    entry -> Transforms.identity().toHumanString(
                                            idToTypeMapping.get(entry.getKey()), Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue())))));
        }

        private SqlMap toIntegerBigintSqlMap(Map<Integer, Long> values)
        {
            return buildMapValue(
                    integerToBigintMapType,
                    values.size(),
                    (keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
                        INTEGER.writeLong(keyBuilder, key);
                        BIGINT.writeLong(valueBuilder, value);
                    }));
        }

        private SqlMap toIntegerVarcharSqlMap(Map<Integer, String> values)
        {
            return buildMapValue(
                    integerToVarcharMapType,
                    values.size(),
                    (keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
                        INTEGER.writeLong(keyBuilder, key);
                        VARCHAR.writeString(valueBuilder, value);
                    }));
        }

        @Nullable
        private static Block toIntegerArrayBlock(List<Integer> values)
        {
            if (values == null) {
                return null;
            }
            BlockBuilder builder = INTEGER.createBlockBuilder(null, values.size());
            values.forEach(value -> INTEGER.writeLong(builder, value));
            return builder.build();
        }

        @Nullable
        private static Block toBigintArrayBlock(List<Long> values)
        {
            if (values == null) {
                return null;
            }
            BlockBuilder builder = BIGINT.createBlockBuilder(null, values.size());
            values.forEach(value -> BIGINT.writeLong(builder, value));
            return builder.build();
        }

        @Nullable
        private static Slice toVarbinarySlice(ByteBuffer value)
        {
            if (value == null) {
                return null;
            }
            return Slices.wrappedHeapBuffer(value);
        }
    }

    private static Map<Integer, Type> getArcherIdToTypeMapping(Schema schema)
    {
        ImmutableMap.Builder<Integer, Type> archerIdToTypeMapping = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            populateArcherIdToTypeMapping(field, archerIdToTypeMapping);
        }
        return archerIdToTypeMapping.buildOrThrow();
    }

    private static void populateArcherIdToTypeMapping(Types.NestedField field, ImmutableMap.Builder<Integer, Type> archerIdToTypeMapping)
    {
        Type type = field.type();
        archerIdToTypeMapping.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> populateArcherIdToTypeMapping(child, archerIdToTypeMapping));
        }
    }
}
