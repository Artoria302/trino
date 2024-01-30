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

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.archer.util.VersionedPath;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.MergePage;
import net.qihoo.archer.Deletion;
import net.qihoo.archer.DeletionParser;
import net.qihoo.archer.FileContent;
import net.qihoo.archer.Metrics;
import net.qihoo.archer.PartitionSpec;
import net.qihoo.archer.PartitionSpecParser;
import net.qihoo.archer.Schema;
import net.qihoo.archer.types.Type;
import org.roaringbitmap.BitmapDataProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.archer.ArcherUtil.deletionToBitmap;
import static io.trino.plugin.archer.ArcherUtil.fileDeletionToDeletion;
import static io.trino.plugin.archer.ArcherUtil.newDataFileVersion;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ArcherMergeSink
        implements ConnectorMergeSink
{
    private final TrinoFileSystem fileSystem;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final Schema schema;
    private final Map<Integer, PartitionSpec> partitionsSpecs;
    private final ConnectorPageSink insertPageSink;
    private final int columnCount;
    private final Map<VersionedPath, FileDeletion> fileDeletions = new HashMap<>();

    public ArcherMergeSink(
            TrinoFileSystem fileSystem,
            JsonCodec<CommitTaskData> jsonCodec,
            Schema schema,
            Map<Integer, PartitionSpec> partitionsSpecs,
            ConnectorPageSink insertPageSink,
            int columnCount)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.partitionsSpecs = ImmutableMap.copyOf(requireNonNull(partitionsSpecs, "partitionsSpecs is null"));
        this.insertPageSink = requireNonNull(insertPageSink, "insertPageSink is null");
        this.columnCount = columnCount;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);

        mergePage.getInsertionsPage().ifPresent(insertPageSink::appendPage);

        mergePage.getDeletionsPage().ifPresent(deletions -> {
            List<Block> fields = RowBlock.getRowFieldsFromBlock(deletions.getBlock(deletions.getChannelCount() - 1));
            Block segmentPathBlock = fields.get(0);
            Block rowPositionBlock = fields.get(1);
            Block versionBlock = fields.get(2);
            Block fileRecordCountBlock = fields.get(3);
            Block partitionSpecIdBlock = fields.get(4);
            Block partitionDataBlock = fields.get(5);
            Block deletionJsonBlock = fields.get(6);

            for (int position = 0; position < segmentPathBlock.getPositionCount(); position++) {
                String segmentPath = VARCHAR.getSlice(segmentPathBlock, position).toStringUtf8();
                long rowPosition = BIGINT.getLong(rowPositionBlock, position);
                int version = toIntExact(INTEGER.getLong(versionBlock, position));

                int index = position;
                FileDeletion deletion = fileDeletions.computeIfAbsent(new VersionedPath(segmentPath, version), ignored -> {
                    long fileRecordCount = BIGINT.getLong(fileRecordCountBlock, index);
                    int partitionSpecId = toIntExact(INTEGER.getLong(partitionSpecIdBlock, index));
                    String partitionData = VARCHAR.getSlice(partitionDataBlock, index).toStringUtf8();
                    String deletionJson = VARCHAR.getSlice(deletionJsonBlock, index).toStringUtf8();
                    Deletion archerDeletion = DeletionParser.fromJson(deletionJson);

                    int newVersion = newDataFileVersion(version);
                    BitmapDataProvider bitmap = deletionToBitmap(fileSystem, segmentPath, archerDeletion);
                    return new FileDeletion(partitionSpecId, partitionData, newVersion, fileRecordCount, archerDeletion.deletedRecordCount(), bitmap);
                });

                deletion.add(toIntExact(rowPosition));
            }
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>(insertPageSink.finish().join());

        fileDeletions.forEach((versionedPath, deletion) -> {
            BitmapDataProvider bitmap = deletion.rowsToDelete();
            int size = bitmap.serializedSizeInBytes();
            Deletion archerDeletion = fileDeletionToDeletion(fileSystem, versionedPath, deletion);

            PartitionSpec partitionSpec = partitionsSpecs.get(deletion.partitionSpecId());
            Optional<PartitionData> partitionData = Optional.empty();
            if (partitionSpec.isPartitioned()) {
                Type[] columnTypes = partitionSpec.fields().stream()
                        .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                        .toArray(Type[]::new);
                partitionData = Optional.of(PartitionData.fromJson(deletion.partitionDataJson(), columnTypes));
            }

            CommitTaskData task = new CommitTaskData(
                    versionedPath.getPath(),
                    "",
                    ArcherFileFormat.PARQUET,
                    size,
                    0,
                    Optional.empty(),
                    Optional.of(DeletionParser.toJson(archerDeletion)),
                    new MetricsWrapper(new Metrics(deletion.fileRecordCount(), null, null, null, null)),
                    PartitionSpecParser.toJson(partitionSpec),
                    partitionData.map(PartitionData::toJson),
                    FileContent.DATA_DELTA,
                    Optional.of(versionedPath));

            fragments.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
        });

        return completedFuture(fragments);
    }

    @Override
    public void abort()
    {
        insertPageSink.abort();
    }
}
