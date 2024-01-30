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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.ParquetWriteValidation;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.archer.fileio.TrinoArcherFileSystem;
import io.trino.plugin.archer.util.PageToArrowConverter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import net.qihoo.archer.Deletion;
import net.qihoo.archer.FileSummary;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.Schema;
import net.qihoo.archer.arrow.DuplicateWriter;
import net.qihoo.archer.arrow.InvertedIndexField;
import net.qihoo.archer.arrow.Segment;
import net.qihoo.archer.arrow.SegmentConfig;
import net.qihoo.archer.arrow.SegmentMetric;
import net.qihoo.archer.arrow.util.ArrowSchemaUtil;
import net.qihoo.archer.types.Types;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_INTERNAL_ERROR;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_WRITER_OPEN_ERROR;
import static io.trino.plugin.archer.ArcherUtil.makeSurePathSchema;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.InvertedIndex.INVERTED_INDEX_DIRECTORY;
import static net.qihoo.archer.arrow.DuplicateWriter.WriterMetric;

public class ArcherParquetWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = instanceSize(ArcherParquetWriter.class);

    private static final Logger log = Logger.get(ArcherParquetWriter.class);
    private static final int CHUNK_MAX_BYTES = toIntExact(DataSize.of(64, MEGABYTE).toBytes());

    private final ParquetWriterOptions writerOption;
    private DuplicateWriter writer;
    private final int chunkMaxLogicalBytes;

    private final Optional<ParquetWriteValidation.ParquetWriteValidationBuilder> validationBuilder;
    private final PageToArrowConverter pageToArrowConverter;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private long memoryUsage;
    private long writtenBytes;
    private long writtenRows;
    private SegmentMetric segmentMetric;

    private final String segmentPath;
    private final TrinoFileSystem fileSystem;
    private final boolean hasInvertedIndex;
    private Optional<List<FileSummary>> invertedIndexFiles;
    private Optional<Deletion> deletion = Optional.empty();

    private final List<ArcherCloseable> closes = new ArrayList<>();

    public ArcherParquetWriter(
            TrinoFileSystem fileSystem,
            String segmentPath,
            String fileName,
            Schema archerSchema,
            InvertedIndex invertedIndex,
            ParquetWriterOptions writerOption,
            List<Type> fileColumnTypes,
            List<String> fileColumnNames,
            Map<String, String> segmentProperties,
            Optional<ParquetWriteValidation.ParquetWriteValidationBuilder> validationBuilder)
    {
        ImmutableList.copyOf(requireNonNull(fileColumnTypes, "fileColumnTypes is null"));
        ImmutableList.copyOf(requireNonNull(fileColumnNames, "fileColumnNames is null"));
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.validationBuilder = requireNonNull(validationBuilder, "validationBuilder is null");
        requireNonNull(segmentPath, "segmentPath is null");
        requireNonNull(fileName, "fileName is null");
        this.writerOption = requireNonNull(writerOption, "writerOption is null");
        this.chunkMaxLogicalBytes = max(1, CHUNK_MAX_BYTES / 2);

        List<InvertedIndexField> invertedIndexFields = new ArrayList<>();
        for (net.qihoo.archer.InvertedIndexField field : invertedIndex.fields()) {
            Types.NestedField archerField = archerSchema.findField(field.sourceId());
            if (archerField != null) {
                Integer pos = archerSchema.findTopLevelFieldIndex(archerField.name());
                if (pos != null) {
                    InvertedIndexField indexField = new InvertedIndexField(pos, field.fieldId(), field.type(), field.properties());
                    invertedIndexFields.add(indexField);
                }
                else {
                    throw new TrinoException(ARCHER_INTERNAL_ERROR, "Not find source field position to create inverted index: " + field);
                }
            }
            else {
                throw new TrinoException(ARCHER_INTERNAL_ERROR, "Not find source field to create inverted index: " + field);
            }
        }

        hasInvertedIndex = invertedIndexFields.size() > 0;

        org.apache.arrow.vector.types.pojo.Schema schema = ArrowSchemaUtil.convert(archerSchema, true);
        try {
            segmentPath = makeSurePathSchema(segmentPath);
            this.segmentPath = segmentPath;

            SegmentConfig config = new SegmentConfig(invertedIndexFields, segmentProperties);

            BufferAllocator allocator = new RootAllocator();
            closes.add(allocator::close);

            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportSchema(allocator, schema, new CDataDictionaryProvider(), arrowSchema);
            closes.add(arrowSchema::close);
            closes.add(arrowSchema::release);

            this.writer = DuplicateWriter.create(
                    allocator,
                    new TrinoArcherFileSystem(fileSystem, null),
                    arrowSchema,
                    segmentPath,
                    fileName,
                    config);
            closes.add(this.writer::close);

            this.pageToArrowConverter = new PageToArrowConverter(allocator, fileColumnNames, fileColumnTypes, null);
        }
        catch (Throwable e) {
            if (writer != null) {
                try {
                    writer.abort();
                    writer = null;
                }
                catch (Exception ex) {
                    log.warn(ex, "Failed to abort writer");
                }
            }
            closeAll();
            throw new TrinoException(ARCHER_WRITER_OPEN_ERROR, format("Failed to open writer: %s", e.getMessage()), e);
        }
    }

    private void closeAll()
    {
        for (int i = this.closes.size() - 1; i >= 0; i--) {
            try {
                this.closes.get(i).close();
            }
            catch (Throwable e) {
                log.warn(e, "Failed to close object");
            }
        }
        this.closes.clear();
    }

    private void createInvertedIndexFiles()
            throws IOException
    {
        if (hasInvertedIndex) {
            ImmutableList.Builder<FileSummary> invertedIndexFiles = ImmutableList.builder();
            String indexPath = segmentPath + "/" + INVERTED_INDEX_DIRECTORY;
            FileIterator iterator = fileSystem.listFiles(Location.of(indexPath));
            while (iterator.hasNext()) {
                FileEntry entry = iterator.next();
                String name = entry.location().fileName();
                invertedIndexFiles.add(new Segment.SegmentFile(name, entry.length(), entry.lastModified().toEpochMilli()));
            }
            this.invertedIndexFiles = Optional.of(invertedIndexFiles.build());
        }
        else {
            this.invertedIndexFiles = Optional.empty();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        try {
            if (writer != null) {
                segmentMetric = writer.finish();
                createInvertedIndexFiles();
            }
        }
        finally {
            memoryUsage = 0;
            writer = null;
            closeAll();
        }
    }

    public void abort()
            throws IOException
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        try {
            if (writer != null) {
                writer.abort();
            }
        }
        finally {
            memoryUsage = 0;
            writer = null;
            closeAll();
        }
    }

    public void write(Page page)
            throws IOException
    {
        requireNonNull(page, "page is null");
        checkState(!closed.get(), "writer is closed");
        if (page.getPositionCount() == 0) {
            return;
        }

        Page validationPage = page;
        recordValidation(validation -> validation.addPage(validationPage));

        while (page != null) {
            int chunkRows = min(page.getPositionCount(), writerOption.getBatchSize());
            Page chunk = page.getRegion(0, chunkRows);

            // avoid chunk with huge logical size
            while (chunkRows > 1 && chunk.getLogicalSizeInBytes() > chunkMaxLogicalBytes) {
                chunkRows /= 2;
                chunk = chunk.getRegion(0, chunkRows);
            }

            // Remove chunk from current page
            if (chunkRows < page.getPositionCount()) {
                page = page.getRegion(chunkRows, page.getPositionCount() - chunkRows);
            }
            else {
                page = null;
            }
            writeChunk(chunk);
        }
    }

    private void writeChunk(Page page)
            throws IOException
    {
        try (VectorSchemaRoot root = pageToArrowConverter.convert(page)) {
            WriterMetric metric = writer.write(root);
            writtenBytes = metric.writtenBytes();
            memoryUsage = metric.memoryUsage();
            writtenRows += page.getPositionCount();
        }
    }

    private void recordValidation(Consumer<ParquetWriteValidation.ParquetWriteValidationBuilder> task)
    {
        validationBuilder.ifPresent(task);
    }

    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + memoryUsage;
    }

    public long getWrittenBytes()
    {
        return writtenBytes;
    }

    public long getWrittenRows()
    {
        return writtenRows;
    }

    public SegmentMetric getSegmentMetric()
    {
        return segmentMetric;
    }

    public Optional<List<FileSummary>> getInvertedIndexFiles()
    {
        return invertedIndexFiles;
    }

    public Optional<Deletion> getDeletion()
    {
        return deletion;
    }
}
