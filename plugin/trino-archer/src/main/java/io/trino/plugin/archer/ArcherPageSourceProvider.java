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
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.archer.runtime.ArcherRuntimeManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import net.qihoo.archer.Deletion;
import net.qihoo.archer.DeletionParser;
import net.qihoo.archer.FileSummary;
import net.qihoo.archer.InvertedIndexFilesParser;
import net.qihoo.archer.MetadataColumns;
import net.qihoo.archer.PartitionSpec;
import net.qihoo.archer.PartitionSpecParser;
import net.qihoo.archer.Schema;
import net.qihoo.archer.SchemaParser;
import net.qihoo.archer.expressions.Expression;
import net.qihoo.archer.index.InvertedIndexQuery;
import net.qihoo.archer.index.InvertedIndexQueryParser;
import net.qihoo.archer.mapping.MappedField;
import net.qihoo.archer.mapping.MappedFields;
import net.qihoo.archer.mapping.NameMapping;
import net.qihoo.archer.mapping.NameMappingParser;
import net.qihoo.archer.parquet.ParquetSchemaUtil;
import net.qihoo.archer.types.Types;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_DYNAMIC_REPARTITIONING_VALUE_ID;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_DELETION;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_FILE_RECORD_COUNT;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_PARTITION_DATA;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_PARTITION_SPEC_ID;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_BAD_DATA;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.archer.ArcherMetadataColumn.FILE_MODIFIED_TIME;
import static io.trino.plugin.archer.ArcherMetadataColumn.FILE_PATH;
import static io.trino.plugin.archer.ArcherMetadataColumn.ROW_POS;
import static io.trino.plugin.archer.ArcherSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.archer.ArcherSessionProperties.isLocalCacheEnabled;
import static io.trino.plugin.archer.ArcherSessionProperties.isUseFileSizeFromMetadata;
import static io.trino.plugin.archer.ArcherSplitManager.ARCHER_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.archer.ArcherUtil.deserializePartitionValue;
import static io.trino.plugin.archer.ArcherUtil.getColumnHandle;
import static io.trino.plugin.archer.ArcherUtil.getPartitionKeys;
import static io.trino.plugin.archer.ExpressionConverter.toArcherExpression;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.Deletion.EMPTY_DELETION_JSON;

public class ArcherPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(ArcherPageSourceProvider.class);

    private static final String AVRO_FIELD_ID = "field-id";

    private final TrinoFileSystemFactory fileSystemFactory;
    private final ArcherRuntimeManager archerRuntimeManager;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;
    private final CacheManager cacheManager;

    public ArcherPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ArcherRuntimeManager archerRuntimeManager,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderOptions parquetReaderOptions,
            TypeManager typeManager,
            CacheManager cacheManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.archerRuntimeManager = requireNonNull(archerRuntimeManager, "archerRuntimeManager is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        ArcherSplit split = (ArcherSplit) connectorSplit;
        ArcherTableHandle table = (ArcherTableHandle) connectorTable;

        List<ArcherColumnHandle> archerColumns = columns.stream()
                .map(ArcherColumnHandle.class::cast)
                .collect(toImmutableList());

        Schema tableSchema = SchemaParser.fromJson(table.getTableSchemaJson());

        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(tableSchema, split.getPartitionSpecJson());
        net.qihoo.archer.types.Type[] partitionColumnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(tableSchema.findType(field.sourceId())))
                .toArray(net.qihoo.archer.types.Type[]::new);
        PartitionData partitionData = PartitionData.fromJson(split.getPartitionDataJson(), partitionColumnTypes);
        Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(partitionData, partitionSpec);

        List<ArcherColumnHandle> requiredColumns = new ArrayList<>(archerColumns);

        // process RowID for merge and update
        archerColumns.stream()
                .filter(column -> column.isUpdateRowIdColumn() || column.isMergeRowIdColumn())
                .findFirst().ifPresent(rowIdColumn -> {
                    Set<Integer> alreadyRequiredColumnIds = requiredColumns.stream()
                            .map(ArcherColumnHandle::getId)
                            .collect(toImmutableSet());
                    for (ColumnIdentity identity : rowIdColumn.getColumnIdentity().getChildren()) {
                        if (alreadyRequiredColumnIds.contains(identity.getId())) {
                            // ignore
                        }
                        else if (identity.getId() == MetadataColumns.FILE_PATH.fieldId()) {
                            requiredColumns.add(new ArcherColumnHandle(identity, VARCHAR, ImmutableList.of(), VARCHAR, false, Optional.empty()));
                        }
                        else if (identity.getId() == MetadataColumns.ROW_POSITION.fieldId()) {
                            requiredColumns.add(new ArcherColumnHandle(identity, BIGINT, ImmutableList.of(), BIGINT, false, Optional.empty()));
                        }
                        else if (identity.getId() == MetadataColumns.VERSION.fieldId()) {
                            requiredColumns.add(new ArcherColumnHandle(identity, INTEGER, ImmutableList.of(), INTEGER, false, Optional.empty()));
                        }
                        else if (identity.getId() == TRINO_MERGE_FILE_RECORD_COUNT) {
                            requiredColumns.add(new ArcherColumnHandle(identity, BIGINT, ImmutableList.of(), BIGINT, false, Optional.empty()));
                        }
                        else if (identity.getId() == TRINO_MERGE_PARTITION_SPEC_ID) {
                            requiredColumns.add(new ArcherColumnHandle(identity, INTEGER, ImmutableList.of(), INTEGER, false, Optional.empty()));
                        }
                        else if (identity.getId() == TRINO_MERGE_PARTITION_DATA) {
                            requiredColumns.add(new ArcherColumnHandle(identity, VARCHAR, ImmutableList.of(), VARCHAR, false, Optional.empty()));
                        }
                        else if (identity.getId() == TRINO_MERGE_DELETION) {
                            requiredColumns.add(new ArcherColumnHandle(identity, VARCHAR, ImmutableList.of(), VARCHAR, false, Optional.empty()));
                        }
                        else {
                            requiredColumns.add(getColumnHandle(tableSchema.findField(identity.getId()), typeManager));
                        }
                    }
                });

        TupleDomain<ArcherColumnHandle> effectivePredicate = table.getUnenforcedPredicate()
                .intersect(dynamicFilter.getCurrentPredicate().transformKeys(ArcherColumnHandle.class::cast))
                .simplify(ARCHER_DOMAIN_COMPACTION_THRESHOLD);
        if (effectivePredicate.isNone()) {
            return new EmptyPageSource();
        }

        boolean localCacheEnabled = isLocalCacheEnabled(session);
        TrinoFileSystem fileSystem = localCacheEnabled && cacheManager.isValid()
                ? fileSystemFactory.create(session.getIdentity(), cacheManager)
                : fileSystemFactory.create(session.getIdentity());
        Location dataFilePath = Location.of(format("%s/%s", split.getSegmentPath(), split.getFileName()));
        TrinoInputFile inputFile = isUseFileSizeFromMetadata(session)
                ? fileSystem.newInputFile(dataFilePath, split.getFileSize())
                : fileSystem.newInputFile(dataFilePath);

        Optional<List<FileSummary>> invertedIndexFiles = split.getInvertedIndexFilesJson().map(InvertedIndexFilesParser::fromJson);
        Optional<Deletion> deletion = split.getDeletionJson().map(DeletionParser::fromJson);

        Optional<InvertedIndexQuery> query = split.getInvertedIndexQueryJson().map(InvertedIndexQueryParser::fromJson);

        ReaderPageSource dataPageSource = createDataPageSource(
                session,
                fileSystem,
                split.getSegmentPath(),
                split.getFileName(),
                split.getVersion(),
                inputFile,
                split.getStart(),
                split.getLength(),
                split.getLimit(),
                split.getFileSize(),
                split.getFileRecordCount(),
                split.getLastModifiedTime(),
                partitionSpec.specId(),
                split.getPartitionDataJson(),
                split.getDynamicRepartitioningBound(),
                split.getFileFormat(),
                query,
                invertedIndexFiles,
                deletion,
                SchemaParser.fromJson(table.getTableSchemaJson()),
                requiredColumns,
                effectivePredicate,
                table.getNameMappingJson().map(NameMappingParser::fromJson),
                partitionKeys);

        Optional<ReaderProjectionsAdapter> projectionsAdapter = dataPageSource.getReaderColumns().map(readerColumns ->
                new ReaderProjectionsAdapter(
                        requiredColumns,
                        readerColumns,
                        column -> ((ArcherColumnHandle) column).getType(),
                        ArcherPageSourceProvider::applyProjection));

        return new ArcherPageSource(
                archerColumns,
                requiredColumns,
                dataPageSource.get(),
                projectionsAdapter);
    }

    public ReaderPageSource createDataPageSource(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            String segmentPath,
            String fileName,
            int version,
            TrinoInputFile inputFile,
            long start,
            long length,
            OptionalLong limit,
            long fileSize,
            long fileRecordCount,
            long lastModifiedTime,
            int partitionSpecId,
            String partitionData,
            OptionalInt dynamicRepartitioningBound,
            ArcherFileFormat fileFormat,
            Optional<InvertedIndexQuery> invertedIndexQuery,
            Optional<List<FileSummary>> invertedIndexFiles,
            Optional<Deletion> deletion,
            Schema fileSchema,
            List<ArcherColumnHandle> dataColumns,
            TupleDomain<ArcherColumnHandle> predicate,
            Optional<NameMapping> nameMapping,
            Map<Integer, Optional<String>> partitionKeys)
    {
        if (requireNonNull(fileFormat) == ArcherFileFormat.PARQUET) {
            return createParquetSegmentPageSource(
                    fileSystem,
                    segmentPath,
                    fileName,
                    version,
                    inputFile,
                    start,
                    length,
                    limit,
                    fileSize,
                    fileRecordCount,
                    lastModifiedTime,
                    partitionSpecId,
                    partitionData,
                    fileSchema,
                    dataColumns,
                    dynamicRepartitioningBound,
                    parquetReaderOptions
                            .withMaxReadBlockSize(getParquetMaxReadBlockSize(session)),
                    predicate,
                    fileFormatDataSourceStats,
                    nameMapping,
                    invertedIndexQuery,
                    invertedIndexFiles,
                    deletion,
                    partitionKeys);
        }
        throw new TrinoException(NOT_SUPPORTED, "File format not supported for Archer: " + fileFormat);
    }

    private ReaderPageSource createParquetSegmentPageSource(
            TrinoFileSystem fileSystem,
            String segmentPath,
            String fileName,
            int version,
            TrinoInputFile inputFile,
            long start,
            long length,
            OptionalLong limit,
            long fileSize,
            long fileRecordCount,
            long lastModifiedTime,
            int partitionSpecId,
            String partitionData,
            Schema archerSchema,
            List<ArcherColumnHandle> dataColumns,
            OptionalInt dynamicRepartitioningBound,
            ParquetReaderOptions options,
            TupleDomain<ArcherColumnHandle> predicate,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            Optional<NameMapping> nameMapping,
            Optional<InvertedIndexQuery> invertedIndexQuery,
            Optional<List<FileSummary>> invertedIndexFiles,
            Optional<Deletion> deletion,
            Map<Integer, Optional<String>> partitionKeys)
    {
        ParquetDataSource dataSource = null;
        try {
            dataSource = new ArcherParquetDataSource(inputFile, fileSize, options, fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();
            if (nameMapping.isPresent() && !ParquetSchemaUtil.hasIds(fileSchema)) {
                // NameMapping conversion is necessary because MetadataReader converts all column names to lowercase and NameMapping is case-sensitive
                fileSchema = ParquetSchemaUtil.applyNameMapping(fileSchema, convertToLowercase(nameMapping.get()));
            }
            // Mapping from Archer field ID to Parquet fields.
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(toImmutableMap(field -> field.getId().intValue(), Function.identity()));

            Optional<ReaderColumns> columnProjections = projectColumns(dataColumns);
            List<ArcherColumnHandle> readColumns = columnProjections
                    .map(readerColumns -> (List<ArcherColumnHandle>) readerColumns.get().stream().map(ArcherColumnHandle.class::cast).collect(toImmutableList()))
                    .orElse(dataColumns);

            List<org.apache.parquet.schema.Type> parquetFields = readColumns.stream()
                    .map(column -> parquetIdToField.get(column.getId()))
                    .toList();
            MessageType requestedSchema = new MessageType(fileSchema.getName(), parquetFields.stream().filter(Objects::nonNull).collect(toImmutableList()));
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);

            TupleDomain<ArcherColumnHandle> tupleDomain = getValidTupleDomain(descriptorsByPath, predicate);

            ArcherParquetPageSource.Builder pageSourceBuilder = ArcherParquetPageSource.builder();
            int parquetSourceChannel = 0;

            ImmutableList.Builder<Types.NestedField> projectFields = ImmutableList.builder();

            for (int columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
                ArcherColumnHandle column = readColumns.get(columnIndex);
                if (column.isIsDeletedColumn()) {
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(BOOLEAN, false));
                }
                else if (partitionKeys.containsKey(column.getId())) {
                    Type trinoType = column.getType();
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(
                            trinoType,
                            deserializePartitionValue(trinoType, partitionKeys.get(column.getId()).orElse(null), column.getName())));
                }
                else if (column.isPathColumn()) {
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(FILE_PATH.getType(), utf8Slice(segmentPath)));
                }
                else if (column.isFileModifiedTimeColumn()) {
                    long lastModified = packDateTimeWithZone(lastModifiedTime <= 0 ? inputFile.lastModified().toEpochMilli() : lastModifiedTime, UTC_KEY);
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(FILE_MODIFIED_TIME.getType(), lastModified));
                }
                else if (column.isRowPositionColumn()) {
                    projectFields.add(Types.NestedField.required(ROW_POS.getId(), ROW_POS.getColumnName(), Types.LongType.get()));
                    pageSourceBuilder.addSourceColumn(parquetSourceChannel);
                    parquetSourceChannel++;
                }
                else if (column.isUpdateRowIdColumn() || column.isMergeRowIdColumn()) {
                    // $row_id is a composite of multiple physical columns, it is assembled by the ArcherPageSource
                    pageSourceBuilder.addNullColumn(column.getType());
                }
                else if (column.getId() == MetadataColumns.VERSION.fieldId()) {
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(column.getType(), (long) version));
                }
                else if (column.getId() == TRINO_MERGE_FILE_RECORD_COUNT) {
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(column.getType(), fileRecordCount));
                }
                else if (column.getId() == TRINO_MERGE_PARTITION_SPEC_ID) {
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(column.getType(), (long) partitionSpecId));
                }
                else if (column.getId() == TRINO_MERGE_PARTITION_DATA) {
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(column.getType(), utf8Slice(partitionData)));
                }
                else if (column.getId() == TRINO_MERGE_DELETION) {
                    String deletionJson = deletion.map(DeletionParser::toJson).orElse(EMPTY_DELETION_JSON);
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(column.getType(), utf8Slice(deletionJson)));
                }
                else if (column.getId() == TRINO_DYNAMIC_REPARTITIONING_VALUE_ID) {
                    if (dynamicRepartitioningBound.isPresent()) {
                        pageSourceBuilder.addDynamicRepartitioningValueColumn(dynamicRepartitioningBound.getAsInt());
                    }
                    else {
                        pageSourceBuilder.addConstantColumn(nativeValueToBlock(column.getType(), 0L));
                    }
                }
                else {
                    org.apache.parquet.schema.Type parquetField = parquetFields.get(columnIndex);
                    Type trinoType = column.getBaseType();

                    if (parquetField == null) {
                        pageSourceBuilder.addNullColumn(trinoType);
                        continue;
                    }

                    // The top level columns are already mapped by name/id appropriately.
                    projectFields.add(archerSchema.findField(column.getId()));
                    pageSourceBuilder.addSourceColumn(parquetSourceChannel);
                    parquetSourceChannel++;
                }
            }

            Schema projectSchema = new Schema(projectFields.build());
            Expression expression = toArcherExpression(tupleDomain);

            ArcherParquetReader parquetReader = new ArcherParquetReader(
                    fileSystem,
                    archerRuntimeManager,
                    projectSchema,
                    segmentPath,
                    fileName,
                    inputFile,
                    start,
                    length,
                    limit,
                    fileSize,
                    lastModifiedTime,
                    fileRecordCount,
                    expression,
                    invertedIndexQuery,
                    invertedIndexFiles,
                    deletion,
                    dataSource);
            return new ReaderPageSource(
                    pageSourceBuilder.build(parquetReader),
                    columnProjections);
        }
        catch (IOException | RuntimeException e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ex) {
                if (!e.equals(ex)) {
                    e.addSuppressed(ex);
                }
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            String message = format("Error opening Archer split %s: %s", inputFile.location(), e.getMessage());

            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(ARCHER_BAD_DATA, message, e);
            }

            if (e instanceof BlockMissingException) {
                throw new TrinoException(ARCHER_BAD_DATA, message, e);
            }
            throw new TrinoException(ARCHER_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    /**
     * Gets the index based dereference chain to get from the readColumnHandle to the expectedColumnHandle
     */
    private static List<Integer> applyProjection(ColumnHandle expectedColumnHandle, ColumnHandle readColumnHandle)
    {
        ArcherColumnHandle expectedColumn = (ArcherColumnHandle) expectedColumnHandle;
        ArcherColumnHandle readColumn = (ArcherColumnHandle) readColumnHandle;
        checkState(readColumn.isBaseColumn(), "Read column path must be a base column");

        ImmutableList.Builder<Integer> dereferenceChain = ImmutableList.builder();
        ColumnIdentity columnIdentity = readColumn.getColumnIdentity();
        for (Integer fieldId : expectedColumn.getPath()) {
            ColumnIdentity nextChild = columnIdentity.getChildByFieldId(fieldId);
            dereferenceChain.add(columnIdentity.getChildIndexByFieldId(fieldId));
            columnIdentity = nextChild;
        }

        return dereferenceChain.build();
    }

    /**
     * Create a new NameMapping with the same names but converted to lowercase.
     *
     * @param nameMapping The original NameMapping, potentially containing non-lowercase characters
     */
    private static NameMapping convertToLowercase(NameMapping nameMapping)
    {
        return NameMapping.of(convertToLowercase(nameMapping.asMappedFields().fields()));
    }

    private static MappedFields convertToLowercase(MappedFields mappedFields)
    {
        if (mappedFields == null) {
            return null;
        }
        return MappedFields.of(convertToLowercase(mappedFields.fields()));
    }

    private static List<MappedField> convertToLowercase(List<MappedField> fields)
    {
        return fields.stream()
                .map(mappedField -> {
                    Set<String> lowercaseNames = mappedField.names().stream().map(name -> name.toLowerCase(ENGLISH)).collect(toImmutableSet());
                    return MappedField.of(mappedField.id(), lowercaseNames, convertToLowercase(mappedField.nestedMapping()));
                })
                .collect(toImmutableList());
    }

    /**
     * Creates a mapping between the input {@code columns} and base columns if required.
     */
    public static Optional<ReaderColumns> projectColumns(List<ArcherColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        // No projection is required if all columns are base columns
        if (columns.stream().allMatch(ArcherColumnHandle::isBaseColumn)) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();
        Map<Integer, Integer> mappedFieldIds = new HashMap<>();
        int projectedColumnCount = 0;

        for (ArcherColumnHandle column : columns) {
            int baseColumnId = column.getBaseColumnIdentity().getId();
            Integer mapped = mappedFieldIds.get(baseColumnId);

            if (mapped == null) {
                projectedColumns.add(column.getBaseColumn());
                mappedFieldIds.put(baseColumnId, projectedColumnCount);
                outputColumnMapping.add(projectedColumnCount);
                projectedColumnCount++;
            }
            else {
                outputColumnMapping.add(mapped);
            }
        }

        return Optional.of(new ReaderColumns(projectedColumns.build(), outputColumnMapping.build()));
    }

    private static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, ColumnDescriptor> descriptorsByPath, TupleDomain<ArcherColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().orElseThrow().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getType().getTypeSignature().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                ColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
                if (descriptor != null) {
                    predicate.put(descriptor, domain);
                }
            }
        });
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    private static TupleDomain<ArcherColumnHandle> getValidTupleDomain(Map<List<String>, ColumnDescriptor> descriptorsByPath, TupleDomain<ArcherColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ArcherColumnHandle, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().orElseThrow().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getType().getTypeSignature().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                ColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
                if (descriptor != null) {
                    predicate.put(columnHandle, domain);
                }
            }
        });
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }
}
