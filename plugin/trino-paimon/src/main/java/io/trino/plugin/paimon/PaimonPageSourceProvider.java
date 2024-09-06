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
package io.trino.plugin.paimon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.TupleDomainOrcPredicate;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.paimon.catalog.PaimonCatalogFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IndexFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.RowType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.paimon.ClassLoaderUtils.runWithContextClassLoader;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_BAD_DATA;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CURSOR_ERROR;
import static io.trino.plugin.paimon.PaimonSessionProperties.isLocalCacheEnabled;
import static io.trino.plugin.paimon.PaimonSessionProperties.isPreferNativeReader;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Trino {@link ConnectorPageSourceProvider}.
 */
public class PaimonPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final PaimonCatalogFactory paimonCatalogFactory;
    private final CacheManager cacheManager;

    @Inject
    public PaimonPageSourceProvider(TrinoFileSystemFactory fileSystemFactory, PaimonCatalogFactory paimonCatalogFactory, CacheManager cacheManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.paimonCatalogFactory = requireNonNull(paimonCatalogFactory, "paimonCatalogFactory is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        PaimonTableHandle paimonTableHandle = (PaimonTableHandle) tableHandle;
        Catalog catalog = paimonCatalogFactory.create(session);
        Table table = paimonTableHandle.tableWithDynamicOptions(catalog, session);
        return runWithContextClassLoader(
                () ->
                        createPageSource(
                                session,
                                table,
                                paimonTableHandle.getFilter(),
                                (PaimonSplit) split,
                                columns,
                                paimonTableHandle.getLimit()),
                PaimonPageSourceProvider.class.getClassLoader());
    }

    private ConnectorPageSource createPageSource(
            ConnectorSession session,
            Table table,
            TupleDomain<PaimonColumnHandle> filter,
            PaimonSplit split,
            List<ColumnHandle> columns,
            OptionalLong limit)
    {
        RowType rowType = table.rowType();
        List<String> fieldNames = rowType.getFieldNames();
        List<String> projectedFields = columns.stream()
                .map(PaimonColumnHandle.class::cast)
                .map(PaimonColumnHandle::getColumnName)
                .toList();

        Optional<Predicate> paimonFilter = new PaimonFilterConverter(rowType).convert(filter);

        try {
            Split paimonSplit = split.decodeSplit();
            Optional<List<RawFile>> optionalRawFiles = paimonSplit.convertToRawFiles();
            if (isPreferNativeReader(session) && checkRawFile(optionalRawFiles)) {
                boolean cacheEnabled = isLocalCacheEnabled(session);
                TrinoFileSystem fileSystem = cacheEnabled && cacheManager.isValid()
                        ? fileSystemFactory.create(session, cacheManager)
                        : fileSystemFactory.create(session);

                FileStoreTable fileStoreTable = (FileStoreTable) table;
                boolean readIndex = fileStoreTable.coreOptions().fileIndexReadEnabled();

                Optional<List<DeletionFile>> deletionFiles = paimonSplit.deletionFiles();
                Optional<List<IndexFile>> indexFiles = readIndex ? paimonSplit.indexFiles() : Optional.empty();
                Map<Long, List<String>> schemaIdToFields = new HashMap<>();
                SchemaManager schemaManager = new SchemaManager(fileStoreTable.fileIO(), fileStoreTable.location());
                List<Type> types = columns.stream()
                        .map(s -> ((PaimonColumnHandle) s).getTrinoType())
                        .collect(Collectors.toList());

                try {
                    List<RawFile> files = optionalRawFiles.orElseThrow();
                    LinkedList<ConnectorPageSource> sources = new LinkedList<>();

                    // if file index exists, do the filter.
                    for (int i = 0; i < files.size(); i++) {
                        RawFile rawFile = files.get(i);
                        if (indexFiles.isPresent()) {
                            IndexFile indexFile = indexFiles.get().get(i);
                            if (indexFile != null && paimonFilter.isPresent()) {
                                try (FileIndexPredicate fileIndexPredicate =
                                        new FileIndexPredicate(
                                                new Path(indexFile.path()),
                                                ((FileStoreTable) table).fileIO(),
                                                rowType)) {
                                    if (!fileIndexPredicate.testPredicate(paimonFilter.get())) {
                                        continue;
                                    }
                                }
                            }
                        }
                        ConnectorPageSource source = createDataPageSource(
                                rawFile.format(),
                                fileSystem.newInputFile(Location.of(rawFile.path()), rawFile.length()),
                                fileStoreTable.coreOptions(),
                                // map table column name to data column
                                // name, if column does not exist in
                                // data columns, set it to null
                                // columns those set to null will generate
                                // a null vector in orc/parquet page
                                fileStoreTable.schema().id() == rawFile.schemaId()
                                        ? projectedFields
                                        : schemaIdToFields.computeIfAbsent(rawFile.schemaId(), id -> schemaEvolutionFieldNames(projectedFields, rowType.getFields(), schemaManager.schema(id).fields())),
                                types,
                                filter);

                        if (deletionFiles.isPresent()) {
                            source = PaimonPageSourceWrapper.wrap(
                                    source,
                                    Optional.ofNullable(deletionFiles.get().get(i))
                                            .map(deletionFile -> {
                                                try {
                                                    return DeletionVector.read(fileStoreTable.fileIO(), deletionFile);
                                                }
                                                catch (IOException e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }));
                        }
                        sources.add(source);
                    }

                    return new DirectPaimonPageSource(sources);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                int[] columnIndex = projectedFields.stream().mapToInt(fieldNames::indexOf).toArray();

                // old read way
                ReadBuilder read = table.newReadBuilder();
                paimonFilter.ifPresent(read::withFilter);

                if (!fieldNames.equals(projectedFields)) {
                    read.withProjection(columnIndex);
                }

                return new PaimonPageSource(read.newRead().executeFilter().createReader(paimonSplit), columns, limit);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // make domains(filters) to be ordered by projected fields' order.
    private List<Domain> orderDomains(List<String> projectedFields, TupleDomain<PaimonColumnHandle> filter)
    {
        Optional<Map<PaimonColumnHandle, Domain>> optionalFilter = filter.getDomains();
        Map<String, Domain> domainMap = new HashMap<>();
        optionalFilter.ifPresent(
                trinoColumnHandleDomainMap ->
                        trinoColumnHandleDomainMap.forEach(
                                (k, v) -> domainMap.put(k.getColumnName(), v)));

        return projectedFields.stream()
                .map(name -> domainMap.getOrDefault(name, null))
                .collect(Collectors.toList());
    }

    private boolean checkRawFile(Optional<List<RawFile>> optionalRawFiles)
    {
        return optionalRawFiles.isPresent() && canUseTrinoPageSource(optionalRawFiles.get());
    }

    // only support orc/parquet yet.
    // TODO: support avro
    private boolean canUseTrinoPageSource(List<RawFile> rawFiles)
    {
        for (RawFile rawFile : rawFiles) {
            if (!rawFile.format().equals("orc") && !rawFile.format().equals("parquet")) {
                return false;
            }
        }
        return true;
    }

    // map the table schema column names to data schema column names
    private List<String> schemaEvolutionFieldNames(List<String> fieldNames, List<DataField> tableFields, List<DataField> dataFields)
    {
        Map<String, Integer> fieldNameToId = new HashMap<>();
        Map<Integer, String> idToFieldName = new HashMap<>();
        List<String> result = new ArrayList<>();

        tableFields.forEach(field -> fieldNameToId.put(field.name(), field.id()));
        dataFields.forEach(field -> idToFieldName.put(field.id(), field.name()));

        for (String fieldName : fieldNames) {
            Integer id = fieldNameToId.get(fieldName);
            result.add(idToFieldName.getOrDefault(id, null));
        }
        return result;
    }

    private ConnectorPageSource createDataPageSource(
            String format,
            TrinoInputFile inputFile,
            // TODO construct read option by core-options
            CoreOptions coreOptions,
            List<String> columns,
            List<Type> types,
            TupleDomain<PaimonColumnHandle> predicate)
    {
        return switch (format) {
            case "orc" -> createOrcDataPageSource(
                    inputFile,
                    // TODO: pass options from catalog configuration
                    new OrcReaderOptions()
                            // Default tiny stripe size 8 M is too big for paimon.
                            // Cache stripe will cause more read (I want to read one column,
                            // but not the whole stripe)
                            .withTinyStripeThreshold(DataSize.of(4, DataSize.Unit.KILOBYTE)),
                    columns,
                    types,
                    orderDomains(columns, predicate));
            case "parquet" -> createParquetDataPageSource(
                    inputFile,
                    new ParquetReaderOptions(),
                    columns,
                    types,
                    predicate);
            // todo
            case "avro" -> throw new RuntimeException("Unsupported file format: " + format);
            default -> throw new RuntimeException("Unsupported file format: " + format);
        };
    }

    private ConnectorPageSource createOrcDataPageSource(
            TrinoInputFile inputFile,
            OrcReaderOptions options,
            List<String> columns,
            List<Type> types,
            List<Domain> domains)
    {
        try {
            OrcDataSource orcDataSource = new PaimonOrcDataSource(inputFile, options);
            OrcReader reader = OrcReader.createOrcReader(orcDataSource, options)
                    .orElseThrow(() -> new RuntimeException("ORC file is zero length"));

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            Map<String, OrcColumn> fieldsMap = new HashMap<>();
            fileColumns.forEach(column -> fieldsMap.put(column.getColumnName(), column));
            TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder predicateBuilder =
                    TupleDomainOrcPredicate.builder();
            List<OrcPageSource.ColumnAdaptation> columnAdaptations = new ArrayList<>();
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.size());
            List<Type> fileReadTypes = new ArrayList<>(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) != null) {
                    // column exists
                    columnAdaptations.add(OrcPageSource.ColumnAdaptation.sourceColumn(fileReadColumns.size()));
                    OrcColumn orcColumn = fieldsMap.get(columns.get(i));
                    if (orcColumn == null) {
                        throw new RuntimeException("Column " + columns.get(i) + " does not exist in orc file.");
                    }
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(types.get(i));
                    if (domains.get(i) != null) {
                        predicateBuilder.addColumn(orcColumn.getColumnId(), domains.get(i));
                    }
                }
                else {
                    columnAdaptations.add(OrcPageSource.ColumnAdaptation.nullColumn(types.get(i)));
                }
            }

            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    predicateBuilder.build(),
                    DateTimeZone.UTC,
                    memoryUsage,
                    INITIAL_BATCH_SIZE,
                    RuntimeException::new);

            return new OrcPageSource(
                    recordReader,
                    columnAdaptations,
                    orcDataSource,
                    Optional.empty(),
                    Optional.empty(),
                    memoryUsage,
                    new FileFormatDataSourceStats(),
                    reader.getCompressionKind());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectorPageSource createParquetDataPageSource(
            TrinoInputFile inputFile,
            ParquetReaderOptions options,
            List<String> columns,
            List<Type> types,
            TupleDomain<PaimonColumnHandle> predicate)
    {
        ParquetDataSource dataSource = null;
        try {
            dataSource = new PaimonParquetDataSource(inputFile, inputFile.length(), options);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Map<String, org.apache.parquet.schema.Type> columnNameToField = createColumnNameToFieldMapping(fileSchema);

            List<org.apache.parquet.schema.Type> parquetFields = columns.stream().map(columnNameToField::get).filter(Objects::nonNull).toList();

            MessageType requestedSchema = getMessageType(parquetFields, fileSchema.getName());
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = options.isIgnoreStatistics() ? TupleDomain.all() : getParquetTupleDomain(descriptorsByPath, predicate);
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, UTC);

            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    0,
                    inputFile.length(),
                    dataSource,
                    parquetMetadata.getBlocks(),
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    1000,
                    options);

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);

            ParquetPageSource.Builder pageSourceBuilder = ParquetPageSource.builder();
            int parquetSourceChannel = 0;

            ImmutableList.Builder<Column> parquetColumnFieldsBuilder = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                String columnName = columns.get(columnIndex);
                Type trinoType = types.get(columnIndex);
                if (columnName != null) {
                    org.apache.parquet.schema.Type parquetField = parquetFields.get(parquetSourceChannel);
                    // The top level columns are already mapped by name/id appropriately.
                    ColumnIO columnIO = messageColumnIO.getChild(parquetField.getName());
                    Field field = constructField(trinoType, columnIO).orElseThrow();
                    parquetColumnFieldsBuilder.add(new Column(parquetField.getName(), field));
                    pageSourceBuilder.addSourceColumn(parquetSourceChannel);
                    parquetSourceChannel++;
                }
                else {
                    pageSourceBuilder.addNullColumn(trinoType);
                }
            }
            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    parquetColumnFieldsBuilder.build(),
                    rowGroups,
                    dataSource,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    options,
                    exception -> handleException(dataSourceId, exception),
                    Optional.empty(),
                    Optional.empty());

            return pageSourceBuilder.build(parquetReader);
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
            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(PAIMON_BAD_DATA, e);
            }
            String message = "Error opening Paimon split %s: %s".formatted(inputFile.location(), e.getMessage());
            throw new TrinoException(PAIMON_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static Map<String, org.apache.parquet.schema.Type> createColumnNameToFieldMapping(MessageType fileSchema)
    {
        ImmutableMap.Builder<String, org.apache.parquet.schema.Type> builder = ImmutableMap.builder();
        addColumnNameToFieldMapping(fileSchema, builder);
        return builder.buildOrThrow();
    }

    private static void addColumnNameToFieldMapping(org.apache.parquet.schema.Type type, ImmutableMap.Builder<String, org.apache.parquet.schema.Type> builder)
    {
        if (type.getId() != null) {
            builder.put(type.getName(), type);
        }
        if (type instanceof PrimitiveType) {
            // Nothing else to do
        }
        else if (type instanceof GroupType groupType) {
            for (org.apache.parquet.schema.Type field : groupType.getFields()) {
                addColumnNameToFieldMapping(field, builder);
            }
        }
        else {
            throw new IllegalStateException("Unsupported field type: " + type);
        }
    }

    private static MessageType getMessageType(List<org.apache.parquet.schema.Type> parquetFields, String fileSchemaName)
    {
        return parquetFields
                .stream()
                .map(type -> new MessageType(fileSchemaName, type))
                .reduce(MessageType::union)
                .orElse(new MessageType(fileSchemaName, ImmutableList.of()));
    }

    @VisibleForTesting
    static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, ColumnDescriptor> descriptorsByPath, TupleDomain<PaimonColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        Map<String, ColumnDescriptor> descriptorsByName = descriptorsByPath.values().stream()
                .filter(descriptor -> descriptor.getPrimitiveType().getName() != null)
                .collect(toImmutableMap(descriptor -> descriptor.getPrimitiveType().getName(), identity()));
        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().orElseThrow().forEach((columnHandle, domain) -> {
            DataType type = columnHandle.getPaimonType();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!type.getTypeRoot().getFamilies().contains(DataTypeFamily.CONSTRUCTED)) {
                ColumnDescriptor descriptor = descriptorsByName.get(columnHandle.getColumnName());
                if (descriptor != null) {
                    predicate.put(descriptor, domain);
                }
            }
        });
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    private static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(PAIMON_BAD_DATA, exception);
        }
        return new TrinoException(PAIMON_CURSOR_ERROR, format("Failed to read Parquet file: %s", dataSourceId), exception);
    }
}
