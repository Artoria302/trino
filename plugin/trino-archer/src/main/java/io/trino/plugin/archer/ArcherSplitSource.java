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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.NonEvictableCache;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.archer.util.ScannedDataFiles;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import net.qihoo.archer.CombinedScanTask;
import net.qihoo.archer.ContentFile;
import net.qihoo.archer.Deletion;
import net.qihoo.archer.DeletionParser;
import net.qihoo.archer.DeltaStoreType;
import net.qihoo.archer.FileScanTask;
import net.qihoo.archer.FileSummary;
import net.qihoo.archer.InvertedIndexFilesParser;
import net.qihoo.archer.InvertedIndexParser;
import net.qihoo.archer.PartitionField;
import net.qihoo.archer.PartitionSpecParser;
import net.qihoo.archer.Scan;
import net.qihoo.archer.Schema;
import net.qihoo.archer.Table;
import net.qihoo.archer.UnboundInvertedIndex;
import net.qihoo.archer.expressions.Expression;
import net.qihoo.archer.index.InvertedIndexQuery;
import net.qihoo.archer.index.InvertedIndexQueryParser;
import net.qihoo.archer.index.InvertedIndexQueryUtil;
import net.qihoo.archer.io.CloseableIterable;
import net.qihoo.archer.io.CloseableIterator;
import net.qihoo.archer.types.Type;
import net.qihoo.archer.util.ByteBuffers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.math.LongMath.saturatedAdd;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.archer.ArcherColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.archer.ArcherColumnHandle.pathColumnHandle;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_FILESYSTEM_ERROR;
import static io.trino.plugin.archer.ArcherMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.archer.ArcherSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.archer.ArcherSessionProperties.getPreferredNodeCount;
import static io.trino.plugin.archer.ArcherSessionProperties.getSplitMode;
import static io.trino.plugin.archer.ArcherSessionProperties.getSplitSize;
import static io.trino.plugin.archer.ArcherSessionProperties.isLocalCacheEnabled;
import static io.trino.plugin.archer.ArcherSplitManager.ARCHER_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.archer.ArcherTypes.convertArcherValueToTrino;
import static io.trino.plugin.archer.ArcherUtil.getColumnHandle;
import static io.trino.plugin.archer.ArcherUtil.getPartitionKeys;
import static io.trino.plugin.archer.ArcherUtil.getPartitionValues;
import static io.trino.plugin.archer.ArcherUtil.primitiveFieldTypes;
import static io.trino.plugin.archer.ExpressionConverter.isConvertableToArcherExpression;
import static io.trino.plugin.archer.ExpressionConverter.toArcherExpression;
import static io.trino.plugin.archer.TypeConverter.toArcherType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.qihoo.archer.types.Conversions.fromByteBuffer;

public class ArcherSplitSource
        implements ConnectorSplitSource
{
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch NO_MORE_SPLITS_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final ConnectorSession session;
    private final ArcherTableHandle tableHandle;
    private final Table archerTable;
    private final Scan<?, FileScanTask, CombinedScanTask> tableScan;
    private final Optional<Long> maxScannedFileSizeInBytes;
    private final Map<Integer, Type.PrimitiveType> fieldIdToType;
    private final Optional<String> invertedIndexJson;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final Constraint constraint;
    private final TypeManager typeManager;
    private final PartitionConstraintMatcher partitionConstraintMatcher;
    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final Set<Integer> projectedBaseColumns;
    private final TupleDomain<ArcherColumnHandle> dataColumnPredicate;
    private final Domain pathDomain;
    private final Domain fileModifiedTimeDomain;
    private final OptionalLong limit;
    private final Set<Integer> predicatedColumnIds;

    private TupleDomain<ArcherColumnHandle> pushedDownDynamicFilterPredicate;
    private CloseableIterable<FileScanTask> fileScanIterable;
    private CloseableIterator<FileScanTask> fileScanIterator;
    private long targetSplitSize;
    private final SplitMode splitMode;
    private Iterator<FileScanTask> fileTasksIterator = emptyIterator();
    private TupleDomain<ArcherColumnHandle> fileStatisticsDomain;

    private final Optional<InvertedIndexQuery> invertedIndexQuery;

    private final boolean recordScannedFiles;
    private final ImmutableSet.Builder<ScannedDataFiles> scannedFiles = ImmutableSet.builder();
    private long outputRowsLowerBound;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    private final boolean localCacheEnabled;
    private final int preferredNodeCount;

    private final Map<Integer, Optional<String>> invertedIndexIdToQuery;

    public ArcherSplitSource(
            TrinoFileSystemFactory fileSystemFactory,
            ConnectorSession session,
            ArcherTableHandle tableHandle,
            Table archerTable,
            Scan<?, FileScanTask, CombinedScanTask> tableScan,
            Optional<DataSize> maxScannedFileSize,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            TypeManager typeManager,
            boolean recordScannedFiles,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.tableScan = requireNonNull(tableScan, "scan is null");
        this.maxScannedFileSizeInBytes = maxScannedFileSize.map(DataSize::toBytes);
        this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
        this.archerTable = requireNonNull(archerTable, "archerTable is null");
        this.invertedIndexJson = archerTable.invertedIndex().isIndexed() ? Optional.of(InvertedIndexParser.toJson(archerTable.invertedIndex())) : Optional.empty();
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.partitionConstraintMatcher = new PartitionConstraintMatcher(constraint);
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.recordScannedFiles = recordScannedFiles;
        this.minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
        this.projectedBaseColumns = tableHandle.getProjectedColumns().stream()
                .map(column -> column.getBaseColumnIdentity().getId())
                .collect(toImmutableSet());
        this.dataColumnPredicate = tableHandle.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        this.invertedIndexQuery = tableHandle.getInvertedIndexQuery();
        this.pathDomain = getPathDomain(tableHandle.getEnforcedPredicate());
        this.limit = tableHandle.getLimit();
        this.predicatedColumnIds = Stream.concat(
                        tableHandle.getUnenforcedPredicate().getDomains().orElse(ImmutableMap.of()).keySet().stream(),
                        dynamicFilter.getColumnsCovered().stream()
                                .map(ArcherColumnHandle.class::cast))
                .map(ArcherColumnHandle::getId)
                .collect(toImmutableSet());
        this.fileModifiedTimeDomain = getFileModifiedTimePathDomain(tableHandle.getEnforcedPredicate());
        this.splitMode = getSplitMode(session);
        this.invertedIndexIdToQuery = new HashMap<>();
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
        this.localCacheEnabled = isLocalCacheEnabled(session);
        this.preferredNodeCount = getPreferredNodeCount(session);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(ignored -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        if (fileScanIterable == null) {
            // Used to avoid duplicating work if the Dynamic Filter was already pushed down to the Archer API
            boolean dynamicFilterIsComplete = dynamicFilter.isComplete();
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                    .transformKeys(ArcherColumnHandle.class::cast)
                    .filter((_, domain) -> isConvertableToArcherExpression(domain));
            TupleDomain<ArcherColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                    .intersect(pushedDownDynamicFilterPredicate);
            // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
            TupleDomain<ArcherColumnHandle> simplifiedPredicate = fullPredicate.simplify(ARCHER_DOMAIN_COMPACTION_THRESHOLD);
            if (!simplifiedPredicate.equals(fullPredicate)) {
                // Pushed down predicate was simplified, always evaluate it against individual splits
                this.pushedDownDynamicFilterPredicate = TupleDomain.all();
            }

            TupleDomain<ArcherColumnHandle> effectivePredicate = dataColumnPredicate
                    .intersect(simplifiedPredicate);

            if (effectivePredicate.isNone()) {
                finish();
                return completedFuture(NO_MORE_SPLITS_BATCH);
            }

            Expression filterExpression = toArcherExpression(effectivePredicate);
            // If the Dynamic Filter will be evaluated against each file, stats are required. Otherwise, skip them.
            Scan scan = (Scan) tableScan.filter(filterExpression);
            if (!predicatedColumnIds.isEmpty()) {
                scan = (Scan) scan.includeColumnStats();
            }

            this.fileScanIterable = closer.register(scan.planFiles());
            this.targetSplitSize = getSplitSize(session)
                    .map(DataSize::toBytes)
                    .orElseGet(tableScan::targetSplitSize);
            this.fileScanIterator = closer.register(fileScanIterable.iterator());
            this.fileTasksIterator = emptyIterator();
        }

        TupleDomain<ArcherColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(ArcherColumnHandle.class::cast);
        if (dynamicFilterPredicate.isNone()) {
            finish();
            return completedFuture(NO_MORE_SPLITS_BATCH);
        }

        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize && (fileTasksIterator.hasNext() || fileScanIterator.hasNext())) {
            if (!fileTasksIterator.hasNext()) {
                if (limit.isPresent() && limit.getAsLong() <= outputRowsLowerBound) {
                    finish();
                    break;
                }
                FileScanTask wholeFileTask = fileScanIterator.next();

                boolean fileHasNoDeletions = !wholeFileTask.file().hasDelta();

                fileStatisticsDomain = createFileStatisticsDomain(wholeFileTask);
                if (pruneFileScanTask(wholeFileTask, fileHasNoDeletions, dynamicFilterPredicate, fileStatisticsDomain)) {
                    continue;
                }

                if (invertedIndexQuery.isPresent()) {
                    Integer indexId = wholeFileTask.file().invertedIndexId();
                    // no inverted index, skip
                    if (indexId == null) {
                        continue;
                    }
                    Optional<String> queryJson = invertedIndexIdToQuery.get(indexId);
                    if (queryJson == null) {
                        if (indexId == archerTable.invertedIndex().indexId()) {
                            invertedIndexIdToQuery.put(indexId, tableHandle.getInvertedIndexQueryJson());
                        }
                        else {
                            UnboundInvertedIndex invertedIndex = archerTable.invertedIndexes().get(indexId).toUnbound();
                            Optional<InvertedIndexQuery> query = InvertedIndexQueryUtil.compatibleWith(invertedIndexQuery.get(), invertedIndex);
                            // skip when this file index is not compatible with the query
                            if (query.isEmpty()) {
                                invertedIndexIdToQuery.put(indexId, Optional.empty());
                                continue;
                            }
                            invertedIndexIdToQuery.put(indexId, query.map(InvertedIndexQueryParser::toJson));
                        }
                    }
                    else if (queryJson.isEmpty()) {
                        continue;
                    }
                }

                switch (splitMode) {
                    case AUTO -> {
                        if (invertedIndexQuery.isPresent() || (wholeFileTask.file().deltaRecordCount() == 0 && noDataColumnsProjected(wholeFileTask))) {
                            fileTasksIterator = List.of(wholeFileTask).iterator();
                        }
                        else {
                            fileTasksIterator = wholeFileTask.split(targetSplitSize).iterator();
                        }
                    }
                    case ALWAYS -> {
                        fileTasksIterator = wholeFileTask.split(targetSplitSize).iterator();
                    }
                    case NEVER -> {
                        fileTasksIterator = List.of(wholeFileTask).iterator();
                    }
                }

                if (recordScannedFiles) {
                    scannedFiles.add(new ScannedDataFiles(ImmutableList.of(wholeFileTask.file())));
                }

                if (invertedIndexQuery.isEmpty()) {
                    // This is the last task for this file and no inverted index query
                    outputRowsLowerBound = saturatedAdd(outputRowsLowerBound, wholeFileTask.file().recordCount() - wholeFileTask.file().deltaRecordCount());
                }
                // In theory, .split() could produce empty iterator, so let's evaluate the outer loop condition again.
                continue;
            }

            FileScanTask scanTask = fileTasksIterator.next();

            ArcherSplit archerSplit;
            if (invertedIndexQuery.isPresent()) {
                Integer indexId = scanTask.file().invertedIndexId();
                checkArgument(indexId != null, "inverted index id is null, should never happen");
                Optional<String> invertedIndexQueryJson = invertedIndexIdToQuery.get(indexId);
                checkArgument(invertedIndexQueryJson != null && invertedIndexQueryJson.isPresent(), "inverted index query is null, should never happen");
                archerSplit = toArcherSplit(scanTask, limit, invertedIndexQueryJson, fileStatisticsDomain);
            }
            else {
                archerSplit = toArcherSplit(scanTask, limit, Optional.empty(), fileStatisticsDomain);
            }

            splits.add(archerSplit);
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    private boolean pruneFileScanTask(FileScanTask fileScanTask, boolean fileHasNoDeletions, TupleDomain<ArcherColumnHandle> dynamicFilterPredicate, TupleDomain<ArcherColumnHandle> fileStatisticsDomain)
    {
        if (fileHasNoDeletions &&
                maxScannedFileSizeInBytes.isPresent() &&
                fileScanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
            return true;
        }

        if (!pathDomain.isAll() && !pathDomain.includesNullableValue(utf8Slice(fileScanTask.file().path().toString()))) {
            return true;
        }
        if (!fileModifiedTimeDomain.isAll()) {
            long fileModifiedTime = getModificationTime(fileScanTask.file().path().toString(), fileSystemFactory.create(session.getIdentity()));
            if (!fileModifiedTimeDomain.includesNullableValue(packDateTimeWithZone(fileModifiedTime, UTC_KEY))) {
                return true;
            }
        }

        Schema fileSchema = fileScanTask.schema();
        Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(fileScanTask);

        Set<ArcherColumnHandle> identityPartitionColumns = partitionKeys.keySet().stream()
                .map(fieldId -> getColumnHandle(fileSchema.findField(fieldId), typeManager))
                .collect(toImmutableSet());

        Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> getPartitionValues(identityPartitionColumns, partitionKeys));

        if (!dynamicFilterPredicate.isAll() && !dynamicFilterPredicate.equals(pushedDownDynamicFilterPredicate)) {
            if (!partitionMatchesPredicate(
                    identityPartitionColumns,
                    partitionValues,
                    dynamicFilterPredicate)) {
                return true;
            }
            if (!fileStatisticsDomain.overlaps(dynamicFilterPredicate)) {
                return true;
            }
        }

        return !partitionConstraintMatcher.matches(identityPartitionColumns, partitionValues);
    }

    private void finish()
    {
        close();
        this.fileScanIterable = CloseableIterable.empty();
        this.fileScanIterator = CloseableIterator.empty();
        this.fileTasksIterator = emptyIterator();
    }

    @Override
    public boolean isFinished()
    {
        return fileScanIterator != null && !fileScanIterator.hasNext() && !fileTasksIterator.hasNext();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.copyOf(scannedFiles.build()));
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean noDataColumnsProjected(FileScanTask fileScanTask)
    {
        return fileScanTask.spec().fields().stream()
                .filter(partitionField -> partitionField.transform().isIdentity())
                .map(PartitionField::sourceId)
                .collect(toImmutableSet())
                .containsAll(projectedBaseColumns);
    }

    private TupleDomain<ArcherColumnHandle> createFileStatisticsDomain(FileScanTask wholeFileTask)
    {
        List<ArcherColumnHandle> predicatedColumns = wholeFileTask.schema().columns().stream()
                .filter(column -> predicatedColumnIds.contains(column.fieldId()))
                .map(column -> getColumnHandle(column, typeManager))
                .collect(toImmutableList());
        return createFileStatisticsDomain(
                fieldIdToType,
                wholeFileTask.file().lowerBounds(),
                wholeFileTask.file().upperBounds(),
                wholeFileTask.file().nullValueCounts(),
                predicatedColumns);
    }

    @VisibleForTesting
    static TupleDomain<ArcherColumnHandle> createFileStatisticsDomain(
            Map<Integer, Type.PrimitiveType> fieldIdToType,
            @Nullable Map<Integer, ByteBuffer> lowerBounds,
            @Nullable Map<Integer, ByteBuffer> upperBounds,
            @Nullable Map<Integer, Long> nullValueCounts,
            List<ArcherColumnHandle> predicatedColumns)
    {
        ImmutableMap.Builder<ArcherColumnHandle, Domain> domainBuilder = ImmutableMap.builder();
        for (ArcherColumnHandle column : predicatedColumns) {
            int fieldId = column.getId();
            boolean mayContainNulls;
            if (nullValueCounts == null) {
                mayContainNulls = true;
            }
            else {
                Long nullValueCount = nullValueCounts.get(fieldId);
                mayContainNulls = nullValueCount == null || nullValueCount > 0;
            }
            Type type = fieldIdToType.get(fieldId);
            domainBuilder.put(
                    column,
                    domainForStatistics(
                            column,
                            lowerBounds == null ? null : fromByteBuffer(type, lowerBounds.get(fieldId)),
                            upperBounds == null ? null : fromByteBuffer(type, upperBounds.get(fieldId)),
                            mayContainNulls));
        }
        return TupleDomain.withColumnDomains(domainBuilder.buildOrThrow());
    }

    private static Domain domainForStatistics(
            ArcherColumnHandle columnHandle,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            boolean mayContainNulls)
    {
        io.trino.spi.type.Type type = columnHandle.getType();
        Type archerType = toArcherType(type, columnHandle.getColumnIdentity());
        if (lowerBound == null && upperBound == null) {
            return Domain.create(ValueSet.all(type), mayContainNulls);
        }

        Range statisticsRange;
        if (lowerBound != null && upperBound != null) {
            statisticsRange = Range.range(
                    type,
                    convertArcherValueToTrino(archerType, lowerBound),
                    true,
                    convertArcherValueToTrino(archerType, upperBound),
                    true);
        }
        else if (upperBound != null) {
            statisticsRange = Range.lessThanOrEqual(type, convertArcherValueToTrino(archerType, upperBound));
        }
        else {
            statisticsRange = Range.greaterThanOrEqual(type, convertArcherValueToTrino(archerType, lowerBound));
        }
        return Domain.create(ValueSet.ofRanges(statisticsRange), mayContainNulls);
    }

    private static class PartitionConstraintMatcher
    {
        private final NonEvictableCache<Map<ColumnHandle, NullableValue>, Boolean> partitionConstraintResults;
        private final Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate;
        private final Optional<Set<ColumnHandle>> predicateColumns;

        private PartitionConstraintMatcher(Constraint constraint)
        {
            // We use Constraint just to pass functional predicate here from DistributedExecutionPlanner
            verify(constraint.getSummary().isAll());
            this.predicate = constraint.predicate();
            this.predicateColumns = constraint.getPredicateColumns();
            this.partitionConstraintResults = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
        }

        boolean matches(
                Set<ArcherColumnHandle> identityPartitionColumns,
                Supplier<Map<ColumnHandle, NullableValue>> partitionValuesSupplier)
        {
            if (predicate.isEmpty()) {
                return true;
            }
            Set<ColumnHandle> predicatePartitionColumns = intersection(predicateColumns.orElseThrow(), identityPartitionColumns);
            if (predicatePartitionColumns.isEmpty()) {
                return true;
            }
            Map<ColumnHandle, NullableValue> partitionValues = partitionValuesSupplier.get();
            return uncheckedCacheGet(
                    partitionConstraintResults,
                    ImmutableMap.copyOf(Maps.filterKeys(partitionValues, predicatePartitionColumns::contains)),
                    () -> predicate.orElseThrow().test(partitionValues));
        }
    }

    @VisibleForTesting
    static boolean partitionMatchesPredicate(
            Set<ArcherColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            TupleDomain<ArcherColumnHandle> dynamicFilterPredicate)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<ArcherColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow();

        for (ArcherColumnHandle partitionColumn : identityPartitionColumns) {
            Domain allowedDomain = domains.get(partitionColumn);
            if (allowedDomain != null) {
                if (!allowedDomain.includesNullableValue(partitionValues.get().get(partitionColumn).getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    private ArcherSplit toArcherSplit(FileScanTask task, OptionalLong limit, Optional<String> invertedIndexQueryJson, TupleDomain<ArcherColumnHandle> fileStatisticsDomain)
    {
        ContentFile<?> file = task.file();
        DeltaStoreType deltaStoreType = file.deltaStoreType();
        Optional<Deletion> deletion;
        List<HostAddress> addresses = localCacheEnabled
                ? cachingHostAddressProvider.getHosts(task.file().path().toString(), preferredNodeCount, ImmutableList.of())
                : ImmutableList.of();
        deletion = switch (deltaStoreType) {
            case NONE -> Optional.empty();
            case DELETION_BITMAP_MEMORY -> Optional.of(new Deletion(file.deltaRecordCount(), ByteBuffers.toByteArray(file.deltaMemory())));
            case DELETION_BITMAP_FILE -> Optional.of(new Deletion(file.deltaRecordCount(), file.deltaFile()));
        };
        Optional<List<FileSummary>> invertedIndexFiles = Optional.ofNullable(file.invertedIndexFiles());
        double weight = Math.min(invertedIndexQuery.isEmpty() ? minimumAssignedSplitWeight : Math.max((double) task.length() / 134217728L, minimumAssignedSplitWeight), 1.0);
        return new ArcherSplit(
                file.path().toString(),
                file.fileName().toString(),
                file.version(),
                task.start(),
                task.length(),
                limit,
                file.fileSizeInBytes(),
                file.recordCount(),
                file.lastModifiedTime(),
                ArcherFileFormat.fromArcher(file.format()),
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.partition()),
                invertedIndexJson,
                invertedIndexQueryJson,
                invertedIndexFiles.map(InvertedIndexFilesParser::toJson),
                deletion.map(DeletionParser::toJson),
                SplitWeight.fromProportion(weight),
                fileStatisticsDomain,
                addresses);
    }

    private static Domain getPathDomain(TupleDomain<ArcherColumnHandle> effectivePredicate)
    {
        ArcherColumnHandle pathColumn = pathColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(pathColumn);
        if (domain == null) {
            return Domain.all(pathColumn.getType());
        }
        return domain;
    }

    private static Domain getFileModifiedTimePathDomain(TupleDomain<ArcherColumnHandle> effectivePredicate)
    {
        ArcherColumnHandle fileModifiedTimeColumn = fileModifiedTimeColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(fileModifiedTimeColumn);
        if (domain == null) {
            return Domain.all(fileModifiedTimeColumn.getType());
        }
        return domain;
    }

    private long getModificationTime(String path, TrinoFileSystem fileSystem)
    {
        try {
            TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(path));
            return inputFile.lastModified().toEpochMilli();
        }
        catch (IOException e) {
            throw new TrinoException(ARCHER_FILESYSTEM_ERROR, "Failed to get file modification time: " + path, e);
        }
    }
}
