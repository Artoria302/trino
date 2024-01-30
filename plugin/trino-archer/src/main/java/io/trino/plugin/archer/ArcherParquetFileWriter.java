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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.archer.fileio.ForwardingFileIo;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import net.qihoo.archer.Deletion;
import net.qihoo.archer.FileSummary;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.Metrics;
import net.qihoo.archer.MetricsConfig;
import net.qihoo.archer.arrow.SegmentMetric;
import net.qihoo.archer.io.InputFile;
import org.apache.parquet.format.CompressionCodec;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_WRITER_CLOSE_ERROR;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_WRITER_DATA_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.parquet.ParquetUtil.fileMetrics;

public class ArcherParquetFileWriter
        implements ArcherFileWriter
{
    private static final Logger log = Logger.get(ArcherParquetFileWriter.class);
    private static final int INSTANCE_SIZE = instanceSize(ArcherParquetFileWriter.class);

    private final MetricsConfig metricsConfig;
    private final String segmentPath;
    private final String fileName;
    private final TrinoFileSystem fileSystem;

    private final ArcherParquetWriter parquetWriter;
    private final Closeable rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;

    public ArcherParquetFileWriter(
            MetricsConfig metricsConfig,
            Closeable rollbackAction,
            List<Type> fileColumnTypes,
            List<String> fileColumnNames,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            Optional<CompressionCodec> compressionCodec,
            String trinoVersion,
            String segmentPath,
            String fileName,
            TrinoFileSystem fileSystem,
            net.qihoo.archer.Schema archerSchema,
            InvertedIndex invertedIndex,
            Map<String, String> segmentProperties)
    {
        requireNonNull(trinoVersion, "trinoVersion is null");
        this.parquetWriter = new ArcherParquetWriter(
                fileSystem,
                segmentPath,
                fileName,
                archerSchema,
                invertedIndex,
                parquetWriterOptions,
                fileColumnTypes,
                fileColumnNames,
                segmentProperties,
                Optional.empty());
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");
        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "fileInputColumnIndexes is null");

        ImmutableList.Builder<Block> nullBlocks = ImmutableList.builder();
        for (Type fileColumnType : fileColumnTypes) {
            BlockBuilder blockBuilder = fileColumnType.createBlockBuilder(null, 1, 0);
            blockBuilder.appendNull();
            nullBlocks.add(blockBuilder.build());
        }
        this.nullBlocks = nullBlocks.build();
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
        this.segmentPath = requireNonNull(segmentPath, "segmentPath is null");
        this.fileName = requireNonNull(fileName, "fileName is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    @Override
    public Metrics getMetrics()
    {
        String file = format("%s/%s", segmentPath, fileName);
        InputFile inputFile = new ForwardingFileIo(fileSystem).newInputFile(file);
        return fileMetrics(inputFile, metricsConfig);
    }

    @Override
    public SegmentMetric getFileMetric()
    {
        return parquetWriter.getSegmentMetric();
    }

    @Override
    public long getWrittenRows()
    {
        return parquetWriter.getWrittenRows();
    }

    @Override
    public Optional<List<FileSummary>> getInvertedIndexFiles()
    {
        return parquetWriter.getInvertedIndexFiles();
    }

    @Override
    public Optional<Deletion> getDeletion()
    {
        return parquetWriter.getDeletion();
    }

    @Override
    public long getWrittenBytes()
    {
        return parquetWriter.getWrittenBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + parquetWriter.getMemoryUsage();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        Block[] blocks = new Block[fileInputColumnIndexes.length];
        for (int i = 0; i < fileInputColumnIndexes.length; i++) {
            int inputColumnIndex = fileInputColumnIndexes[i];
            if (inputColumnIndex < 0) {
                blocks[i] = RunLengthEncodedBlock.create(nullBlocks.get(i), dataPage.getPositionCount());
            }
            else {
                blocks[i] = dataPage.getBlock(inputColumnIndex);
            }
        }
        Page page = new Page(dataPage.getPositionCount(), blocks);
        try {
            parquetWriter.write(page);
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ARCHER_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public Closeable commit()
    {
        try {
            parquetWriter.close();
        }
        catch (IOException | UncheckedIOException e) {
            try {
                rollbackAction.close();
            }
            catch (IOException | RuntimeException ex) {
                if (!e.equals(ex)) {
                    e.addSuppressed(ex);
                }
                log.error(ex, "Exception when committing file");
            }
            throw new TrinoException(ARCHER_WRITER_CLOSE_ERROR, "Error committing write parquet to Archer", e);
        }

        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            parquetWriter.close();
        }
        catch (Exception e) {
            throw new TrinoException(ARCHER_WRITER_CLOSE_ERROR, "Error rolling back write parquet to Archer", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }
}
