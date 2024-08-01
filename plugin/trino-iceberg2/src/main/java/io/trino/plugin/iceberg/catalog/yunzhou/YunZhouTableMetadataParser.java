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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.trino.metastore.YunZhouSnapshot;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UnboundPartitionSpec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.JsonUtil;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class YunZhouTableMetadataParser
{
    private YunZhouTableMetadataParser()
    {
    }

    private static final Logger log = Logger.get(YunZhouTableMetadataParser.class);

    // visible for testing
    static final String FORMAT_VERSION = "format-version";
    static final String TABLE_UUID = "table-uuid";
    static final String LOCATION = "location";
    static final String LAST_SEQUENCE_NUMBER = "last-sequence-number";
    static final String LAST_UPDATED_MILLIS = "last-updated-ms";
    static final String LAST_COLUMN_ID = "last-column-id";
    static final String SCHEMA = "schema";
    static final String SCHEMAS = "schemas";
    static final String CURRENT_SCHEMA_ID = "current-schema-id";
    static final String PARTITION_SPEC = "partition-spec";
    static final String PARTITION_SPECS = "partition-specs";
    static final String DEFAULT_SPEC_ID = "default-spec-id";
    static final String LAST_PARTITION_ID = "last-partition-id";
    static final String DEFAULT_SORT_ORDER_ID = "default-sort-order-id";
    static final String SORT_ORDERS = "sort-orders";
    static final String PROPERTIES = "properties";
    static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
    static final String REFS = "refs";
    static final String SNAPSHOTS = "snapshots";
    static final String SNAPSHOT_ID = "snapshot-id";
    static final String TIMESTAMP_MS = "timestamp-ms";
    static final String SNAPSHOT_LOG = "snapshot-log";
    static final String METADATA_FILE = "metadata-file";
    static final String METADATA_ID = "metadata-id";
    static final String METADATA_LOG = "metadata-log";

    public static void write(
            TableMetadata metadata,
            CachingHiveMetastore metastore,
            String metadataId,
            String database,
            String table)
    {
        String content = toJson(metadata);
        List<Snapshot> snapshots = metadata.snapshots();
        long startTime = System.currentTimeMillis();
        metastore.saveMetadata(metadataId, content);
        log.info("Save %s.%s metadata take time : %d", database, table, System.currentTimeMillis() - startTime);
        // put snapshots to yunzhouMetastore
        if (!snapshots.isEmpty()) {
            long start = System.currentTimeMillis();
            putSnapshots(metastore, metadataId, snapshots);
            log.info("Put %s.%s snapshots take time : %d", database, table, System.currentTimeMillis() - start);
        }
    }

    public static String toJson(TableMetadata metadata)
    {
        try (StringWriter writer = new StringWriter()) {
            JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
            toJson(metadata, generator);
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to write json for: %s", metadata), e);
        }
    }

    private static String toJson(Snapshot snapshot)
    {
        try (StringWriter writer = new StringWriter()) {
            JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
            SnapshotParser.toJson(snapshot, generator);
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to write json for snapshot: %s", snapshot.snapshotId()), e);
        }
    }

    public static void toJson(TableMetadata metadata, JsonGenerator generator)
            throws IOException
    {
        generator.writeStartObject();

        generator.writeNumberField(FORMAT_VERSION, metadata.formatVersion());
        generator.writeStringField(TABLE_UUID, metadata.uuid());
        generator.writeStringField(LOCATION, metadata.location());
        if (metadata.formatVersion() > 1) {
            generator.writeNumberField(LAST_SEQUENCE_NUMBER, metadata.lastSequenceNumber());
        }
        generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
        generator.writeNumberField(LAST_COLUMN_ID, metadata.lastColumnId());

        // for older readers, continue writing the current schema as "schema".
        // this is only needed for v1 because support for schemas and current-schema-id is required in v2 and later.
        if (metadata.formatVersion() == 1) {
            generator.writeFieldName(SCHEMA);
            SchemaParser.toJson(metadata.schema(), generator);
        }

        // write the current schema ID and schema list
        generator.writeNumberField(CURRENT_SCHEMA_ID, metadata.currentSchemaId());
        generator.writeArrayFieldStart(SCHEMAS);
        for (Schema schema : metadata.schemas()) {
            SchemaParser.toJson(schema, generator);
        }
        generator.writeEndArray();

        // for older readers, continue writing the default spec as "partition-spec"
        if (metadata.formatVersion() == 1) {
            generator.writeFieldName(PARTITION_SPEC);
            PartitionSpecParser.toJsonFields(metadata.spec(), generator);
        }

        // write the default spec ID and spec list
        generator.writeNumberField(DEFAULT_SPEC_ID, metadata.defaultSpecId());
        generator.writeArrayFieldStart(PARTITION_SPECS);
        for (PartitionSpec spec : metadata.specs()) {
            PartitionSpecParser.toJson(spec, generator);
        }
        generator.writeEndArray();

        generator.writeNumberField(LAST_PARTITION_ID, metadata.lastAssignedPartitionId());

        // write the default order ID and sort order list
        generator.writeNumberField(DEFAULT_SORT_ORDER_ID, metadata.defaultSortOrderId());
        generator.writeArrayFieldStart(SORT_ORDERS);
        for (SortOrder sortOrder : metadata.sortOrders()) {
            SortOrderParser.toJson(sortOrder, generator);
        }
        generator.writeEndArray();

        // write properties map
        generator.writeObjectFieldStart(PROPERTIES);
        for (Map.Entry<String, String> keyValue : metadata.properties().entrySet()) {
            generator.writeStringField(keyValue.getKey(), keyValue.getValue());
        }
        generator.writeEndObject();

        generator.writeNumberField(CURRENT_SNAPSHOT_ID,
                metadata.currentSnapshot() != null ? metadata.currentSnapshot().snapshotId() : -1);

        toJson(metadata.refs(), generator);

        generator.writeArrayFieldStart(SNAPSHOT_LOG);
        for (HistoryEntry logEntry : metadata.snapshotLog()) {
            generator.writeStartObject();
            generator.writeNumberField(TIMESTAMP_MS, logEntry.timestampMillis());
            generator.writeNumberField(SNAPSHOT_ID, logEntry.snapshotId());
            generator.writeEndObject();
        }
        generator.writeEndArray();

        generator.writeArrayFieldStart(METADATA_LOG);
        for (TableMetadata.MetadataLogEntry logEntry : metadata.previousFiles()) {
            generator.writeStartObject();
            generator.writeNumberField(TIMESTAMP_MS, logEntry.timestampMillis());
            generator.writeStringField(METADATA_FILE, logEntry.file());
            generator.writeStringField(METADATA_ID, logEntry.metadataId());
            generator.writeEndObject();
        }
        generator.writeEndArray();

        generator.writeEndObject();
    }

    private static void toJson(Map<String, SnapshotRef> refs, JsonGenerator generator)
            throws IOException
    {
        generator.writeObjectFieldStart(REFS);
        for (Map.Entry<String, SnapshotRef> refEntry : refs.entrySet()) {
            generator.writeFieldName(refEntry.getKey());
            SnapshotRefParser.toJson(refEntry.getValue(), generator);
        }
        generator.writeEndObject();
    }

    public static TableMetadata read(YunZhouMetastoreTableOperations ops, String metadataId, String database, String tableName)
    {
        return read(ops.metastore(), ops.io(), metadataId, database, tableName);
    }

    public static TableMetadata read(CachingHiveMetastore metastore, FileIO io,
            String metadataId, String database, String tableName)
    {
        long startTime = System.currentTimeMillis();
        String metadata = metastore.getMetadata(metadataId);
        log.info("Get %s.%s metadata take time: %d", database, tableName, System.currentTimeMillis() - startTime);
        StringReader reader = new StringReader(metadata);
        try {
            long start = System.currentTimeMillis();
            JsonNode snapshotArray = getSnapshots(metastore, metadataId);
            log.info("Get %s.%s Snapshots take time: %d", database, tableName, System.currentTimeMillis() - start);
            return fromJson(io, metadataId, JsonUtil.mapper().readValue(reader, JsonNode.class), snapshotArray);
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to read metadata content: %s for table %s.%s", metadataId, database, tableName), e);
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
    static TableMetadata fromJson(FileIO io, String metadataId, JsonNode node, JsonNode snapshotArray)
    {
        checkArgument(node.isObject(),
                "Cannot parse metadata from a non-object: %s", node);

        int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
        checkArgument(formatVersion <= TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION,
                "Cannot read unsupported version %s", formatVersion);

        String uuid = JsonUtil.getStringOrNull(TABLE_UUID, node);
        String location = JsonUtil.getString(LOCATION, node);
        long lastSequenceNumber;
        if (formatVersion > 1) {
            lastSequenceNumber = JsonUtil.getLong(LAST_SEQUENCE_NUMBER, node);
        }
        else {
            lastSequenceNumber = TableMetadata.INITIAL_SEQUENCE_NUMBER;
        }
        int lastAssignedColumnId = JsonUtil.getInt(LAST_COLUMN_ID, node);

        List<Schema> schemas;
        int currentSchemaId;
        Schema schema = null;

        JsonNode schemaArray = node.get(SCHEMAS);
        if (schemaArray != null) {
            checkArgument(schemaArray.isArray(),
                    "Cannot parse schemas from non-array: %s", schemaArray);
            // current schema ID is required when the schema array is present
            currentSchemaId = JsonUtil.getInt(CURRENT_SCHEMA_ID, node);

            // parse the schema array
            ImmutableList.Builder<Schema> builder = ImmutableList.builder();
            for (JsonNode schemaNode : schemaArray) {
                Schema current = SchemaParser.fromJson(schemaNode);
                if (current.schemaId() == currentSchemaId) {
                    schema = current;
                }
                builder.add(current);
            }

            checkArgument(schema != null,
                    "Cannot find schema with %s=%s from %s", CURRENT_SCHEMA_ID, currentSchemaId, SCHEMAS);

            schemas = builder.build();
        }
        else {
            checkArgument(formatVersion == 1,
                    "%s must exist in format v%s", SCHEMAS, formatVersion);

            schema = SchemaParser.fromJson(node.get(SCHEMA));
            currentSchemaId = schema.schemaId();
            schemas = ImmutableList.of(schema);
        }

        JsonNode specArray = node.get(PARTITION_SPECS);
        List<PartitionSpec> specs;
        int defaultSpecId;
        if (specArray != null) {
            checkArgument(specArray.isArray(),
                    "Cannot parse partition specs from non-array: %s", specArray);
            // default spec ID is required when the spec array is present
            defaultSpecId = JsonUtil.getInt(DEFAULT_SPEC_ID, node);

            // parse the spec array
            ImmutableList.Builder<PartitionSpec> builder = ImmutableList.builder();
            for (JsonNode spec : specArray) {
                UnboundPartitionSpec unboundSpec = PartitionSpecParser.fromJson(spec);
                if (unboundSpec.specId() == defaultSpecId) {
                    builder.add(unboundSpec.bind(schema));
                }
                else {
                    builder.add(unboundSpec.bindUnchecked(schema));
                }
            }
            specs = builder.build();
        }
        else {
            checkArgument(formatVersion == 1,
                    "%s must exist in format v%s", PARTITION_SPECS, formatVersion);
            // partition spec is required for older readers, but is always set to the default if the spec
            // array is set. it is only used to default the spec map is missing, indicating that the
            // table metadata was written by an older writer.
            defaultSpecId = TableMetadata.INITIAL_SPEC_ID;
            specs = ImmutableList.of(PartitionSpecParser.fromJsonFields(
                    schema, TableMetadata.INITIAL_SPEC_ID, node.get(PARTITION_SPEC)));
        }

        Integer lastAssignedPartitionId = JsonUtil.getIntOrNull(LAST_PARTITION_ID, node);
        if (lastAssignedPartitionId == null) {
            checkArgument(formatVersion == 1,
                    "%s must exist in format v%s", LAST_PARTITION_ID, formatVersion);
            lastAssignedPartitionId = specs.stream().mapToInt(PartitionSpec::lastAssignedFieldId).max()
                    .orElse(PartitionSpec.unpartitioned().lastAssignedFieldId());
        }

        // parse the sort orders
        JsonNode sortOrderArray = node.get(SORT_ORDERS);
        List<SortOrder> sortOrders;
        int defaultSortOrderId;
        if (sortOrderArray != null) {
            defaultSortOrderId = JsonUtil.getInt(DEFAULT_SORT_ORDER_ID, node);
            ImmutableList.Builder<SortOrder> sortOrdersBuilder = ImmutableList.builder();
            for (JsonNode sortOrder : sortOrderArray) {
                sortOrdersBuilder.add(SortOrderParser.fromJson(schema, sortOrder));
            }
            sortOrders = sortOrdersBuilder.build();
        }
        else {
            checkArgument(formatVersion == 1,
                    "%s must exist in format v%s", SORT_ORDERS, formatVersion);
            SortOrder defaultSortOrder = SortOrder.unsorted();
            sortOrders = ImmutableList.of(defaultSortOrder);
            defaultSortOrderId = defaultSortOrder.orderId();
        }

        // parse properties map
        Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, node);
        long currentSnapshotId = JsonUtil.getLong(CURRENT_SNAPSHOT_ID, node);
        long lastUpdatedMillis = JsonUtil.getLong(LAST_UPDATED_MILLIS, node);

        Map<String, SnapshotRef> refs;
        if (node.has(REFS)) {
            refs = refsFromJson(node.get(REFS));
        }
        else if (currentSnapshotId != -1) {
            // initialize the main branch if there are no refs
            refs = ImmutableMap.of(SnapshotRef.MAIN_BRANCH, SnapshotRef.branchBuilder(currentSnapshotId).build());
        }
        else {
            refs = ImmutableMap.of();
        }

        List<Snapshot> snapshots = Lists.newArrayListWithExpectedSize(snapshotArray.size());
        Iterator<JsonNode> iterator = snapshotArray.elements();
        while (iterator.hasNext()) {
            snapshots.add(SnapshotParser.fromJson(io, iterator.next()));
        }

        ImmutableList.Builder<HistoryEntry> entries = ImmutableList.builder();
        if (node.has(SNAPSHOT_LOG)) {
            Iterator<JsonNode> logIterator = node.get(SNAPSHOT_LOG).elements();
            while (logIterator.hasNext()) {
                JsonNode entryNode = logIterator.next();
                entries.add(new TableMetadata.SnapshotLogEntry(
                        JsonUtil.getLong(TIMESTAMP_MS, entryNode), JsonUtil.getLong(SNAPSHOT_ID, entryNode)));
            }
        }

        ImmutableList.Builder<TableMetadata.MetadataLogEntry> metadataEntries = ImmutableList.builder();
        if (node.has(METADATA_LOG)) {
            Iterator<JsonNode> logIterator = node.get(METADATA_LOG).elements();
            while (logIterator.hasNext()) {
                JsonNode entryNode = logIterator.next();
                metadataEntries.add(new TableMetadata.MetadataLogEntry(
                        JsonUtil.getLong(TIMESTAMP_MS, entryNode), null,
                        JsonUtil.getString(METADATA_ID, entryNode)));
            }
        }

        return new TableMetadata(null, metadataId, formatVersion, uuid, location,
                lastSequenceNumber, lastUpdatedMillis, lastAssignedColumnId, currentSchemaId, schemas, defaultSpecId, specs,
                lastAssignedPartitionId, defaultSortOrderId, sortOrders, properties, currentSnapshotId,
                snapshots, entries.build(), metadataEntries.build(), refs,
                ImmutableList.of()/* no changes from the file */);
    }

    private static Map<String, SnapshotRef> refsFromJson(JsonNode refMap)
    {
        checkArgument(refMap.isObject(), "Cannot parse refs from non-object: %s", refMap);

        ImmutableMap.Builder<String, SnapshotRef> refsBuilder = ImmutableMap.builder();
        Iterator<String> refNames = refMap.fieldNames();
        while (refNames.hasNext()) {
            String refName = refNames.next();
            JsonNode refNode = refMap.get(refName);
            checkArgument(refNode.isObject(), "Cannot parse ref %s from non-object: %s", refName, refMap);
            SnapshotRef ref = SnapshotRefParser.fromJson(refNode);
            refsBuilder.put(refName, ref);
        }

        return refsBuilder.buildOrThrow();
    }

    private static JsonNode getSnapshots(
            CachingHiveMetastore metastore,
            String metadataId)
            throws IOException
    {
        String snapArray = metastore.getAllSnapshots(metadataId);
        StringReader reader = new StringReader(snapArray);
        return JsonUtil.mapper().readValue(reader, JsonNode.class);
    }

    private static void putSnapshots(
            CachingHiveMetastore metastore,
            String metadataId,
            List<Snapshot> snapshots)
    {
        ImmutableList.Builder<YunZhouSnapshot> list = ImmutableList.builder();
        snapshots.forEach(snapshot -> {
            String snapStr = toJson(snapshot);
            long snapshotId = snapshot.snapshotId();
            YunZhouSnapshot yunzhouSnapshot = new YunZhouSnapshot(metadataId, snapshotId, snapStr);
            list.add(yunzhouSnapshot);
        });
        metastore.saveSnapshots(list.build());
    }
}
