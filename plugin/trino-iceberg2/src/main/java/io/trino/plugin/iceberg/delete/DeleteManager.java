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
package io.trino.plugin.iceberg.delete;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergPageSourceProvider.ReaderPageSourceWithRowPositions;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Schema;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.delete.EqualityDeleteFilter.readEqualityDeletes;
import static io.trino.plugin.iceberg.delete.PositionDeleteFilter.readPositionDeletes;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

public class DeleteManager
{
    private final TypeManager typeManager;

    public DeleteManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Optional<RowPredicate> getDeletePredicate(
            String dataFilePath,
            List<DeleteFile> deleteFiles,
            List<IcebergColumnHandle> readColumns,
            Schema tableSchema,
            ReaderPageSourceWithRowPositions readerPageSourceWithRowPositions,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        if (deleteFiles.isEmpty()) {
            return Optional.empty();
        }

        List<DeleteFile> positionDeleteFiles = new ArrayList<>();
        List<DeleteFile> equalityDeleteFiles = new ArrayList<>();
        for (DeleteFile deleteFile : deleteFiles) {
            switch (deleteFile.content()) {
                case POSITION_DELETES -> positionDeleteFiles.add(deleteFile);
                case EQUALITY_DELETES -> equalityDeleteFiles.add(deleteFile);
                case DATA -> throw new VerifyException("DATA is not delete file type");
            }
        }

        Optional<RowPredicate> positionDeletes = createPositionDeleteFilter(dataFilePath, positionDeleteFiles, readerPageSourceWithRowPositions, deletePageSourceProvider)
                .map(filter -> filter.createPredicate(readColumns));
        Optional<RowPredicate> equalityDeletes = createEqualityDeleteFilter(equalityDeleteFiles, tableSchema, deletePageSourceProvider).stream()
                .map(filter -> filter.createPredicate(readColumns))
                .reduce(RowPredicate::and);

        if (positionDeletes.isEmpty()) {
            return equalityDeletes;
        }
        return equalityDeletes
                .map(rowPredicate -> positionDeletes.get().and(rowPredicate))
                .or(() -> positionDeletes);
    }

    public interface DeletePageSourceProvider
    {
        ConnectorPageSource openDeletes(
                DeleteFile delete,
                List<IcebergColumnHandle> deleteColumns,
                TupleDomain<IcebergColumnHandle> tupleDomain);
    }

    private Optional<DeleteFilter> createPositionDeleteFilter(
            String dataFilePath,
            List<DeleteFile> positionDeleteFiles,
            ReaderPageSourceWithRowPositions readerPageSourceWithRowPositions,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        if (positionDeleteFiles.isEmpty()) {
            return Optional.empty();
        }

        Slice targetPath = utf8Slice(dataFilePath);

        Optional<Long> startRowPosition = readerPageSourceWithRowPositions.startRowPosition();
        Optional<Long> endRowPosition = readerPageSourceWithRowPositions.endRowPosition();
        verify(startRowPosition.isPresent() == endRowPosition.isPresent(), "startRowPosition and endRowPosition must be specified together");
        IcebergColumnHandle deleteFilePath = getColumnHandle(DELETE_FILE_PATH, typeManager);
        IcebergColumnHandle deleteFilePos = getColumnHandle(DELETE_FILE_POS, typeManager);
        List<IcebergColumnHandle> deleteColumns = ImmutableList.of(deleteFilePath, deleteFilePos);
        TupleDomain<IcebergColumnHandle> deleteDomain = TupleDomain.fromFixedValues(ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, targetPath)));
        if (startRowPosition.isPresent()) {
            Range positionRange = Range.range(deleteFilePos.getType(), startRowPosition.get(), true, endRowPosition.get(), true);
            TupleDomain<IcebergColumnHandle> positionDomain = TupleDomain.withColumnDomains(ImmutableMap.of(deleteFilePos, Domain.create(ValueSet.ofRanges(positionRange), false)));
            deleteDomain = deleteDomain.intersect(positionDomain);
        }

        LongBitmapDataProvider deletedRows = new Roaring64Bitmap();
        for (DeleteFile deleteFile : positionDeleteFiles) {
            if (shouldLoadPositionDeleteFile(deleteFile, startRowPosition, endRowPosition)) {
                try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(deleteFile, deleteColumns, deleteDomain)) {
                    readPositionDeletes(pageSource, targetPath, deletedRows);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        if (deletedRows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new PositionDeleteFilter(deletedRows));
    }

    private static boolean shouldLoadPositionDeleteFile(DeleteFile deleteFile, Optional<Long> startRowPosition, Optional<Long> endRowPosition)
    {
        if (startRowPosition.isEmpty()) {
            return true;
        }

        Optional<Long> positionLowerBound = deleteFile.rowPositionLowerBound();
        Optional<Long> positionUpperBound = deleteFile.rowPositionUpperBound();
        return (positionLowerBound.isEmpty() || positionLowerBound.get() <= endRowPosition.orElseThrow()) &&
                (positionUpperBound.isEmpty() || positionUpperBound.get() >= startRowPosition.get());
    }

    private List<EqualityDeleteFilter> createEqualityDeleteFilter(List<DeleteFile> equalityDeleteFiles, Schema schema, DeletePageSourceProvider deletePageSourceProvider)
    {
        if (equalityDeleteFiles.isEmpty()) {
            return List.of();
        }

        List<EqualityDeleteFilter> filters = new ArrayList<>();

        for (DeleteFile deleteFile : equalityDeleteFiles) {
            List<Integer> fieldIds = deleteFile.equalityFieldIds();
            verify(!fieldIds.isEmpty(), "equality field IDs are missing");
            List<IcebergColumnHandle> deleteColumns = fieldIds.stream()
                    .map(id -> getColumnHandle(schema.findField(id), typeManager))
                    .collect(toImmutableList());

            try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(deleteFile, deleteColumns, TupleDomain.all())) {
                filters.add(readEqualityDeletes(pageSource, deleteColumns, schema));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return filters;
    }
}
