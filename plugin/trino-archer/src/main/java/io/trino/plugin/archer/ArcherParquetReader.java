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
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.plugin.archer.fileio.TrinoArcherFileSystem;
import io.trino.plugin.archer.runtime.ArcherRuntime;
import io.trino.plugin.archer.runtime.ArcherRuntimeManager;
import io.trino.plugin.archer.util.ArrowToPageConverter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import net.qihoo.archer.Deletion;
import net.qihoo.archer.FileSummary;
import net.qihoo.archer.arrow.ReadMetrics;
import net.qihoo.archer.arrow.RecordBatchStream;
import net.qihoo.archer.arrow.Segment;
import net.qihoo.archer.arrow.SegmentReader;
import net.qihoo.archer.arrow.SessionContext;
import net.qihoo.archer.arrow.SessionContexts;
import net.qihoo.archer.arrow.util.ArrowSchemaUtil;
import net.qihoo.archer.expressions.Expression;
import net.qihoo.archer.index.InvertedIndexQuery;
import org.apache.arrow.c.ArrowSchema;
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
import java.util.OptionalLong;

import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_READER_OPEN_ERROR;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_READ_LINE_ERROR;
import static io.trino.plugin.archer.ArcherUtil.makeSurePathSchema;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.InvertedIndex.INVERTED_INDEX_DIRECTORY;

public class ArcherParquetReader
        implements Closeable
{
    private static final Logger log = Logger.get(ArcherParquetReader.class);
    private final ArcherRuntime archerRuntime;
    private final ParquetDataSource dataSource;
    private final RecordBatchStream stream;
    private final ArrowToPageConverter arrowToPageConverter;
    private final List<ArcherCloseable> closes = new ArrayList<>();
    private boolean closed;
    private final String segmentPath;
    private volatile long lastFlushMetricTime;
    private volatile long readBytes;
    private volatile long readTimeNanos;
    private volatile long memoryUsage;

    public ArcherParquetReader(
            TrinoFileSystem fileSystem,
            ArcherRuntimeManager archerRuntimeManager,
            net.qihoo.archer.Schema projectSchema,
            String segmentPath,
            String fileName,
            TrinoInputFile inputFile,
            long start,
            long length,
            OptionalLong limit,
            long fileSize,
            long lastModifiedTime,
            long fileRecordCount,
            Expression expression,
            Optional<InvertedIndexQuery> invertedIndexQuery,
            Optional<List<FileSummary>> invertedIndexFiles,
            Optional<Deletion> deletion,
            ParquetDataSource dataSource)
    {
        requireNonNull(segmentPath, "segmentPath is null");
        requireNonNull(projectSchema, "projectSchema is null");
        requireNonNull(inputFile, "inputFile is null");
        requireNonNull(expression, "expression is null");
        requireNonNull(archerRuntimeManager, "archerRuntimeManager is null");
        this.closed = false;
        this.lastFlushMetricTime = 0;
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        closes.add(dataSource::close);

        try {
            this.segmentPath = makeSurePathSchema(segmentPath);

            FileSummary dataFile = new Segment.SegmentFile(fileName, fileSize, lastModifiedTime);
            Segment.ScanRange scanRange = new Segment.ScanRange(start, start + length);
            List<Segment> segments = List.of(new Segment(segmentPath, dataFile, fileRecordCount, scanRange, deletion.orElse(null)));

            BufferAllocator allocator = new RootAllocator();
            closes.add(allocator::close);

            this.archerRuntime = archerRuntimeManager.get(segmentPath);
            closes.add(() -> archerRuntimeManager.free(archerRuntime));

            SessionContext context = SessionContexts.withRuntimeEnv(archerRuntime.getRuntimeEnv());
            closes.add(context::close);

            org.apache.arrow.vector.types.pojo.Schema schema = ArrowSchemaUtil.convert(projectSchema, false);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            closes.add(arrowSchema::close);
            closes.add(arrowSchema::release);

            Data.exportSchema(allocator, schema, null, arrowSchema);

            Map<String, FileSummary> metadataCache = getMetadataCache(segmentPath, invertedIndexFiles, deletion);

            this.stream = SegmentReader.scanSegments(
                    allocator,
                    new TrinoArcherFileSystem(fileSystem, metadataCache),
                    context,
                    segments,
                    arrowSchema,
                    projectSchema,
                    expression,
                    limit,
                    invertedIndexQuery);
            closes.add(stream::close);

            this.arrowToPageConverter = new ArrowToPageConverter(allocator, ArrowSchemaUtil.convert(projectSchema, true));
        }
        catch (Throwable e) {
            closeAll();
            throw new TrinoException(ARCHER_READER_OPEN_ERROR, format("Failed to open reader: %s", e.getMessage()), e);
        }
    }

    public ParquetDataSource getDataSource()
    {
        return dataSource;
    }

    public synchronized void closeAll()
    {
        if (closed) {
            return;
        }
        closed = true;
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

    @Override
    public void close()
            throws IOException
    {
        memoryUsage = 0;
        closeAll();
    }

    public Page nextPage()
    {
        try {
            if (!stream.loadNextBatch()) {
                return null;
            }
            try (VectorSchemaRoot root = stream.getVectorSchemaRoot()) {
                return arrowToPageConverter.convert(root);
            }
        }
        catch (RuntimeException ex) {
            closeAll();
            throw new TrinoException(ARCHER_READ_LINE_ERROR, "Failed to read segment: " + segmentPath, ex);
        }
    }

    private void flushMetrics()
    {
        if (System.currentTimeMillis() - lastFlushMetricTime < 2000) {
            return;
        }
        synchronized (this) {
            if (closed || stream == null) {
                return;
            }
            ReadMetrics metrics = stream.metrics();
            readBytes = metrics.bytesScanned();
            readTimeNanos = metrics.readTimeNanos();
            memoryUsage = metrics.memoryUsage();
            lastFlushMetricTime = System.currentTimeMillis();
        }
    }

    public long getCompletedBytes()
    {
        flushMetrics();
        return readBytes;
    }

    public long getReadTimeNanos()
    {
        flushMetrics();
        return readTimeNanos;
    }

    public long getMemoryUsage()
    {
        flushMetrics();
        return memoryUsage;
    }

    public static Map<String, FileSummary> getMetadataCache(
            String segmentPath,
            Optional<List<FileSummary>> invertedIndexFiles,
            Optional<Deletion> deletion)
    {
        ImmutableMap.Builder<String, FileSummary> metadataBuilder = ImmutableMap.builder();
        if (invertedIndexFiles.isPresent()) {
            for (FileSummary file : invertedIndexFiles.get()) {
                String key = segmentPath + "/" + INVERTED_INDEX_DIRECTORY + "/" + file.fileName();
                metadataBuilder.put(key, file);
            }
        }
        if (deletion.isPresent() && deletion.get().deletionFile() != null) {
            FileSummary file = deletion.get().deletionFile();
            String key = segmentPath + "/" + file.fileName();
            metadataBuilder.put(key, file);
        }
        return metadataBuilder.buildOrThrow();
    }
}
