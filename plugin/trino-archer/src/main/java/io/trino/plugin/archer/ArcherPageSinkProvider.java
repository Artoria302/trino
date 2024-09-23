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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.archer.procedure.ArcherOptimizeHandle;
import io.trino.plugin.archer.procedure.ArcherTableExecuteHandle;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.InvertedIndexParser;
import net.qihoo.archer.PartitionSpec;
import net.qihoo.archer.PartitionSpecParser;
import net.qihoo.archer.Schema;
import net.qihoo.archer.SchemaParser;
import net.qihoo.archer.io.LocationProvider;

import java.util.Map;

import static com.google.common.collect.Maps.transformValues;
import static io.trino.plugin.archer.ArcherUtil.getLocationProvider;
import static java.util.Objects.requireNonNull;

public class ArcherPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ArcherFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final int maxOpenPartitions;

    @Inject
    public ArcherPageSinkProvider(
            TrinoFileSystemFactory fileSystemFactory,
            JsonCodec<CommitTaskData> jsonCodec,
            ArcherFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            ArcherConfig config)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.maxOpenPartitions = config.getMaxPartitionsPerWriter();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(session, (ArcherWritableTableHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(session, (ArcherWritableTableHandle) insertTableHandle);
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, ArcherWritableTableHandle tableHandle)
    {
        Schema schema = SchemaParser.fromJson(tableHandle.getSchemaAsJson());
        String partitionSpecJson = tableHandle.getPartitionsSpecsAsJson().get(tableHandle.getPartitionSpecId());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, partitionSpecJson);
        InvertedIndex invertedIndex = tableHandle.getInvertedIndexAsJson().map(index -> InvertedIndexParser.fromJson(schema, index)).orElse(InvertedIndex.unindexed());
        LocationProvider locationProvider = getLocationProvider(tableHandle.getName(), tableHandle.getOutputPath(), tableHandle.getStorageProperties());
        return new ArcherPageSink(
                schema,
                partitionSpec,
                invertedIndex,
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                fileSystemFactory.create(session),
                tableHandle.getInputColumns(),
                jsonCodec,
                session,
                tableHandle.getFileFormat(),
                tableHandle.getStorageProperties(),
                maxOpenPartitions);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        ArcherTableExecuteHandle executeHandle = (ArcherTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                ArcherOptimizeHandle optimizeHandle = (ArcherOptimizeHandle) executeHandle.procedureHandle();
                Schema schema = SchemaParser.fromJson(optimizeHandle.schemaAsJson());
                PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, optimizeHandle.partitionSpecAsJson());
                InvertedIndex invertedIndex = optimizeHandle.invertedIndexAsJson().map(index -> InvertedIndexParser.fromJson(schema, index)).orElse(InvertedIndex.unindexed());
                LocationProvider locationProvider = getLocationProvider(executeHandle.schemaTableName(),
                        executeHandle.tableLocation(), optimizeHandle.tableStorageProperties());
                return new ArcherPageSink(
                        schema,
                        partitionSpec,
                        invertedIndex,
                        locationProvider,
                        fileWriterFactory,
                        pageIndexerFactory,
                        fileSystemFactory.create(session),
                        optimizeHandle.tableColumns(),
                        jsonCodec,
                        session,
                        optimizeHandle.fileFormat(),
                        optimizeHandle.tableStorageProperties(),
                        maxOpenPartitions);
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case REMOVE_FILES:
            case REMOVE_MANIFESTS:
            case REMOVE_SNAPSHOTS:
                // handled via ConnectorMetadata.executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure: " + executeHandle.procedureId());
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        ArcherMergeTableHandle merge = (ArcherMergeTableHandle) mergeHandle;
        ArcherWritableTableHandle tableHandle = merge.getInsertTableHandle();
        ConnectorPageSink pageSink = createPageSink(session, tableHandle);
        Schema schema = SchemaParser.fromJson(tableHandle.getSchemaAsJson());
        Map<Integer, PartitionSpec> partitionsSpecs = transformValues(tableHandle.getPartitionsSpecsAsJson(), json -> PartitionSpecParser.fromJson(schema, json));

        return new ArcherMergeSink(
                fileSystemFactory.create(session),
                jsonCodec,
                schema,
                partitionsSpecs,
                pageSink,
                tableHandle.getInputColumns().size());
    }
}
