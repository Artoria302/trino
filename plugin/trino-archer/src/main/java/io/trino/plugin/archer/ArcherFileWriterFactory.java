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
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.MetricsConfig;
import net.qihoo.archer.Schema;
import net.qihoo.archer.types.Types;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_WRITER_OPEN_ERROR;
import static io.trino.plugin.archer.ArcherSessionProperties.getCompressionCodec;
import static io.trino.plugin.archer.ArcherSessionProperties.getParquetWriterBatchSize;
import static io.trino.plugin.archer.ArcherSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.archer.ArcherSessionProperties.getParquetWriterPageSize;
import static io.trino.plugin.archer.TypeConverter.toTrinoType;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.TableProperties.DEFAULT_WRITE_METRICS_MODE;

public class ArcherFileWriterFactory
{
    private static final MetricsConfig FULL_METRICS_CONFIG = MetricsConfig.fromProperties(ImmutableMap.of(DEFAULT_WRITE_METRICS_MODE, "full"));

    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;

    @Inject
    public ArcherFileWriterFactory(
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    }

    public ArcherFileWriter createDataFileWriter(
            TrinoFileSystem fileSystem,
            String segmentPath,
            String fileName,
            Schema archerSchema,
            ConnectorSession session,
            InvertedIndex invertedIndex,
            ArcherFileFormat fileFormat,
            Map<String, String> segmentProperties)
    {
        return switch (fileFormat) {
            case PARQUET ->
                    // TODO use metricsConfig https://github.com/trinodb/trino/issues/9791
                    createParquetWriter(MetricsConfig.getDefault(), fileSystem, segmentPath, fileName, archerSchema, invertedIndex, session, segmentProperties);
        };
    }

    private ArcherFileWriter createParquetWriter(
            MetricsConfig metricsConfig,
            TrinoFileSystem fileSystem,
            String segmentPath,
            String fileName,
            Schema archerSchema,
            InvertedIndex invertedIndex,
            ConnectorSession session,
            Map<String, String> segmentProperties)
    {
        List<String> fileColumnNames = archerSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(toImmutableList());
        List<Type> fileColumnTypes = archerSchema.columns().stream()
                .map(column -> toTrinoType(column.type(), typeManager))
                .collect(toImmutableList());

        try {
            Closeable rollbackAction = () -> fileSystem.deleteDirectory(Location.of(segmentPath));

            ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                    .setMaxPageSize(getParquetWriterPageSize(session))
                    .setMaxBlockSize(getParquetWriterBlockSize(session))
                    .setBatchSize(getParquetWriterBatchSize(session))
                    .build();

            return new ArcherParquetFileWriter(
                    metricsConfig,
                    rollbackAction,
                    fileColumnTypes,
                    fileColumnNames,
                    parquetWriterOptions,
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    getCompressionCodec(session).getParquetCompressionCodec(),
                    nodeVersion.toString(),
                    segmentPath,
                    fileName,
                    fileSystem,
                    archerSchema,
                    invertedIndex,
                    segmentProperties);
        }
        catch (Exception e) {
            throw new TrinoException(ARCHER_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }
}
