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

import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.archer.catalog.TrinoCatalog;
import io.trino.plugin.archer.procedure.ArcherExpireSnapshotsHandle;
import io.trino.plugin.archer.procedure.ArcherOptimizeHandle;
import io.trino.plugin.archer.procedure.ArcherRemoveFilesHandle;
import io.trino.plugin.archer.procedure.ArcherRemoveOrphanFilesHandle;
import io.trino.plugin.archer.procedure.ArcherTableExecuteHandle;
import io.trino.plugin.archer.procedure.ArcherTableProcedureId;
import io.trino.plugin.archer.util.ScannedDataFiles;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.hive.HiveWrittenPartitions;
import io.trino.spi.ErrorCode;
import io.trino.spi.RefreshType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import net.qihoo.archer.AppendFiles;
import net.qihoo.archer.BaseTable;
import net.qihoo.archer.ContentFile;
import net.qihoo.archer.DataFile;
import net.qihoo.archer.DataFiles;
import net.qihoo.archer.DeleteFiles;
import net.qihoo.archer.Deletion;
import net.qihoo.archer.DeletionParser;
import net.qihoo.archer.FileFormat;
import net.qihoo.archer.FileScanTask;
import net.qihoo.archer.GenericPartialFile;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.InvertedIndexField;
import net.qihoo.archer.InvertedIndexFilesParser;
import net.qihoo.archer.InvertedIndexParser;
import net.qihoo.archer.IsolationLevel;
import net.qihoo.archer.ManifestFile;
import net.qihoo.archer.ManifestFiles;
import net.qihoo.archer.ManifestReader;
import net.qihoo.archer.MetadataColumns;
import net.qihoo.archer.OverwriteFiles;
import net.qihoo.archer.PartitionField;
import net.qihoo.archer.PartitionSpec;
import net.qihoo.archer.PartitionSpecParser;
import net.qihoo.archer.RewriteFiles;
import net.qihoo.archer.Schema;
import net.qihoo.archer.SchemaParser;
import net.qihoo.archer.Snapshot;
import net.qihoo.archer.SnapshotRef;
import net.qihoo.archer.Table;
import net.qihoo.archer.TableProperties;
import net.qihoo.archer.TableScan;
import net.qihoo.archer.Transaction;
import net.qihoo.archer.UpdateInvertedIndex;
import net.qihoo.archer.UpdatePartitionSpec;
import net.qihoo.archer.VersionedPath;
import net.qihoo.archer.exceptions.AlreadyExistsException;
import net.qihoo.archer.exceptions.ValidationException;
import net.qihoo.archer.expressions.Expressions;
import net.qihoo.archer.expressions.Term;
import net.qihoo.archer.index.InvertedIndexQuery;
import net.qihoo.archer.index.InvertedIndexQueryParser;
import net.qihoo.archer.index.UserInputAst;
import net.qihoo.archer.io.CloseableIterable;
import net.qihoo.archer.types.Type;
import net.qihoo.archer.types.Types;
import net.qihoo.archer.types.Types.IntegerType;
import net.qihoo.archer.types.Types.LongType;
import net.qihoo.archer.types.Types.NestedField;
import net.qihoo.archer.types.Types.StringType;
import net.qihoo.archer.types.Types.StructType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.difference;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_DYNAMIC_REPARTITIONING_VALUE_ID;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_DYNAMIC_REPARTITIONING_VALUE_NAME;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_DELETION;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_FILE_RECORD_COUNT;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_PARTITION_DATA;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_PARTITION_SPEC_ID;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_MERGE_ROW_ID;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_ROW_ID_NAME;
import static io.trino.plugin.archer.ArcherColumnHandle.dynamicRepartitioningValueColumnHandle;
import static io.trino.plugin.archer.ArcherColumnHandle.dynamicRepartitioningValueColumnMetadata;
import static io.trino.plugin.archer.ArcherColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.archer.ArcherColumnHandle.fileModifiedTimeColumnMetadata;
import static io.trino.plugin.archer.ArcherColumnHandle.pathColumnHandle;
import static io.trino.plugin.archer.ArcherColumnHandle.pathColumnMetadata;
import static io.trino.plugin.archer.ArcherColumnHandle.posColumnHandle;
import static io.trino.plugin.archer.ArcherColumnHandle.posColumnMetadata;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_COMMIT_ERROR;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_FILESYSTEM_ERROR;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_INTERNAL_ERROR;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_INVALID_METADATA;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_MISSING_METADATA;
import static io.trino.plugin.archer.ArcherMetadataColumn.DYNAMIC_REPARTITIONING_VALUE;
import static io.trino.plugin.archer.ArcherMetadataColumn.FILE_MODIFIED_TIME;
import static io.trino.plugin.archer.ArcherMetadataColumn.FILE_PATH;
import static io.trino.plugin.archer.ArcherMetadataColumn.ROW_POS;
import static io.trino.plugin.archer.ArcherMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.archer.ArcherSessionProperties.getExpireSnapshotMinRetention;
import static io.trino.plugin.archer.ArcherSessionProperties.getHiveCatalogName;
import static io.trino.plugin.archer.ArcherSessionProperties.getRemoveOrphanFilesMinRetention;
import static io.trino.plugin.archer.ArcherSessionProperties.isForceEngineRepartitioning;
import static io.trino.plugin.archer.ArcherSessionProperties.isIncrementalRefreshEnabled;
import static io.trino.plugin.archer.ArcherSessionProperties.isOptimizeDynamicRepartitioning;
import static io.trino.plugin.archer.ArcherSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.archer.ArcherTableName.isArcherTableName;
import static io.trino.plugin.archer.ArcherTableName.isDataTable;
import static io.trino.plugin.archer.ArcherTableName.isMaterializedViewStorage;
import static io.trino.plugin.archer.ArcherTableName.tableNameFrom;
import static io.trino.plugin.archer.ArcherTableProperties.INVERTED_INDEXED_BY_PROPERTY;
import static io.trino.plugin.archer.ArcherTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.archer.ArcherTableProperties.getPartitioning;
import static io.trino.plugin.archer.ArcherTableProperties.getTableLocation;
import static io.trino.plugin.archer.ArcherUtil.canEnforceColumnConstraintInSpecs;
import static io.trino.plugin.archer.ArcherUtil.commit;
import static io.trino.plugin.archer.ArcherUtil.deserializePartitionValue;
import static io.trino.plugin.archer.ArcherUtil.fileName;
import static io.trino.plugin.archer.ArcherUtil.firstSnapshot;
import static io.trino.plugin.archer.ArcherUtil.firstSnapshotAfter;
import static io.trino.plugin.archer.ArcherUtil.getArcherTableProperties;
import static io.trino.plugin.archer.ArcherUtil.getColumnHandle;
import static io.trino.plugin.archer.ArcherUtil.getColumns;
import static io.trino.plugin.archer.ArcherUtil.getFileFormat;
import static io.trino.plugin.archer.ArcherUtil.getPartitionKeys;
import static io.trino.plugin.archer.ArcherUtil.getSnapshotIdAsOfTime;
import static io.trino.plugin.archer.ArcherUtil.getTableComment;
import static io.trino.plugin.archer.ArcherUtil.newCreateTableTransaction;
import static io.trino.plugin.archer.ArcherUtil.newDataFileVersion;
import static io.trino.plugin.archer.ArcherUtil.schemaFromMetadata;
import static io.trino.plugin.archer.ConstraintExtractor.andUserInputAstList;
import static io.trino.plugin.archer.ConstraintExtractor.extractTupleDomain;
import static io.trino.plugin.archer.ExpressionConverter.isConvertableToArcherExpression;
import static io.trino.plugin.archer.ExpressionConverter.toArcherExpression;
import static io.trino.plugin.archer.InvertedIndexFieldUtils.parseInvertedIndexFields;
import static io.trino.plugin.archer.PartitionFields.parsePartitionFields;
import static io.trino.plugin.archer.PartitionFields.toPartitionFields;
import static io.trino.plugin.archer.TableType.DATA;
import static io.trino.plugin.archer.TypeConverter.toArcherTypeForNewColumn;
import static io.trino.plugin.archer.TypeConverter.toTrinoType;
import static io.trino.plugin.archer.catalog.hms.TrinoHiveCatalog.DEPENDS_ON_TABLES;
import static io.trino.plugin.archer.catalog.hms.TrinoHiveCatalog.DEPENDS_ON_TABLE_FUNCTIONS;
import static io.trino.plugin.archer.catalog.hms.TrinoHiveCatalog.TRINO_QUERY_START_TIME;
import static io.trino.plugin.archer.procedure.ArcherTableProcedureId.EXPIRE_SNAPSHOTS;
import static io.trino.plugin.archer.procedure.ArcherTableProcedureId.OPTIMIZE;
import static io.trino.plugin.archer.procedure.ArcherTableProcedureId.REMOVE_FILES;
import static io.trino.plugin.archer.procedure.ArcherTableProcedureId.REMOVE_ORPHAN_FILES;
import static io.trino.plugin.archer.procedure.OptimizeTableProcedure.FILE_SIZE_THRESHOLD;
import static io.trino.plugin.archer.procedure.OptimizeTableProcedure.REFRESH_INVERTED_INDEX;
import static io.trino.plugin.archer.procedure.OptimizeTableProcedure.REFRESH_PARTITION;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.UNKNOWN;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.floorDiv;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static net.qihoo.archer.ReachableFileUtil.metadataFileLocations;
import static net.qihoo.archer.SnapshotSummary.DELETED_RECORDS_PROP;
import static net.qihoo.archer.TableProperties.DELETE_ISOLATION_LEVEL;
import static net.qihoo.archer.TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT;
import static net.qihoo.archer.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;
import static net.qihoo.archer.expressions.Expressions.alwaysTrue;
import static net.qihoo.archer.types.TypeUtil.indexParents;
import static net.qihoo.archer.util.LocationUtil.stripTrailingSlash;
import static net.qihoo.archer.util.SnapshotUtil.schemaFor;

public class ArcherMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(ArcherMetadata.class);
    private static final Pattern PATH_PATTERN = Pattern.compile("(.*)/[^/]+");
    private static final int OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION = 2;
    private static final int CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION = 2;
    private static final String RETENTION_THRESHOLD = "retention_threshold";
    private static final String FILES = "files";
    private static final String UNKNOWN_SNAPSHOT_TOKEN = "UNKNOWN";
    public static final Set<String> UPDATABLE_TABLE_PROPERTIES = ImmutableSet.of(PARTITIONING_PROPERTY, INVERTED_INDEXED_BY_PROPERTY);

    private static final Integer DELETE_BATCH_SIZE = 1000;
    public static final int GET_METADATA_BATCH_SIZE = 1000;
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("=");

    private final TypeManager typeManager;
    private final CatalogHandle trinoCatalogHandle;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalog catalog;
    private final TrinoFileSystemFactory fileSystemFactory;

    private final Map<ArcherTableHandle, TableStatistics> tableStatisticsCache = new ConcurrentHashMap<>();

    private Transaction transaction;
    private Optional<Long> fromSnapshotForRefresh = Optional.empty();

    public ArcherMetadata(
            TypeManager typeManager,
            CatalogHandle trinoCatalogHandle,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalog catalog,
            TrinoFileSystemFactory fileSystemFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.trinoCatalogHandle = requireNonNull(trinoCatalogHandle, "trinoCatalogHandle is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return catalog.namespaceExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listNamespaces(session);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return catalog.loadNamespaceMetadata(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return catalog.getNamespacePrincipal(session, schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with start version is not supported");
        }

        if (!isArcherTableName(tableName.getTableName())) {
            return null;
        }

        if (isMaterializedViewStorage(tableName.getTableName())) {
            verify(endVersion.isEmpty(), "Materialized views do not support versioned queries");

            SchemaTableName materializedViewName = new SchemaTableName(tableName.getSchemaName(), tableNameFrom(tableName.getTableName()));
            if (getMaterializedView(session, materializedViewName).isEmpty()) {
                throw new TableNotFoundException(tableName);
            }

            BaseTable storageTable = catalog.getMaterializedViewStorageTable(session, materializedViewName)
                    .orElseThrow(() -> new TrinoException(TABLE_NOT_FOUND, "Storage table metadata not found for materialized view " + tableName));

            return tableHandleForCurrentSnapshot(tableName, storageTable);
        }

        if (!isDataTable(tableName.getTableName())) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }

        BaseTable table;
        try {
            table = (BaseTable) catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), tableName.getTableName()));
        }
        catch (TableNotFoundException e) {
            return null;
        }
        catch (TrinoException e) {
            ErrorCode errorCode = e.getErrorCode();
            if (errorCode.equals(ARCHER_MISSING_METADATA.toErrorCode())
                    || errorCode.equals(ARCHER_INVALID_METADATA.toErrorCode())) {
                return new CorruptedArcherTableHandle(tableName, e);
            }
            throw e;
        }

        if (endVersion.isPresent()) {
            long snapshotId = getSnapshotIdFromVersion(session, table, endVersion.get());
            return tableHandleForSnapshot(
                    tableName,
                    table,
                    Optional.of(snapshotId),
                    schemaFor(table, snapshotId),
                    Optional.empty());
        }
        return tableHandleForCurrentSnapshot(tableName, table);
    }

    private ArcherTableHandle tableHandleForCurrentSnapshot(SchemaTableName tableName, BaseTable table)
    {
        return tableHandleForSnapshot(
                tableName,
                table,
                Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId),
                table.schema(),
                Optional.of(table.spec()));
    }

    private ArcherTableHandle tableHandleForSnapshot(
            SchemaTableName tableName,
            BaseTable table,
            Optional<Long> tableSnapshotId,
            Schema tableSchema,
            Optional<PartitionSpec> partitionSpec)
    {
        Map<String, String> tableProperties = table.properties();
        int partitionSpecId = table.spec().specId();
        int invertedIndexId = table.invertedIndex().indexId();
        Optional<InvertedIndex> invertedIndex = table.invertedIndex().isIndexed() ? Optional.of(table.invertedIndex()) : Optional.empty();
        String nameMappingJson = tableProperties.get(TableProperties.DEFAULT_NAME_MAPPING);
        return new ArcherTableHandle(
                trinoCatalogHandle,
                tableName.getSchemaName(),
                tableName.getTableName(),
                DATA,
                tableSnapshotId,
                SchemaParser.toJson(tableSchema),
                partitionSpec.map(PartitionSpecParser::toJson),
                invertedIndex.map(InvertedIndexParser::toJson),
                table.operations().current().formatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.ofNullable(nameMappingJson),
                table.location(),
                table.properties(),
                tableSchema,
                invertedIndex.orElse(null),
                null,
                NO_RETRIES,
                ImmutableList.of(),
                false,
                Optional.empty(),
                OptionalInt.empty(),
                false,
                OptionalInt.empty(),
                false,
                ImmutableSet.of());
    }

    private static long getSnapshotIdFromVersion(ConnectorSession session, Table table, ConnectorTableVersion version)
    {
        io.trino.spi.type.Type versionType = version.getVersionType();
        return switch (version.getPointerType()) {
            case TEMPORAL -> getTemporalSnapshotIdFromVersion(session, table, version, versionType);
            case TARGET_ID -> getTargetSnapshotIdFromVersion(table, version, versionType);
        };
    }

    private static long getTargetSnapshotIdFromVersion(Table table, ConnectorTableVersion version, io.trino.spi.type.Type versionType)
    {
        long snapshotId;
        if (versionType == BIGINT) {
            snapshotId = (long) version.getVersion();
        }
        else if (versionType instanceof VarcharType) {
            String refName = ((Slice) version.getVersion()).toStringUtf8();
            SnapshotRef ref = table.refs().get(refName);
            if (ref == null) {
                throw new TrinoException(INVALID_ARGUMENTS, "Cannot find snapshot with reference name: " + refName);
            }
            snapshotId = ref.snapshotId();
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
        }

        if (table.snapshot(snapshotId) == null) {
            throw new TrinoException(INVALID_ARGUMENTS, "Archer snapshot ID does not exists: " + snapshotId);
        }
        return snapshotId;
    }

    private static long getTemporalSnapshotIdFromVersion(ConnectorSession session, Table table, ConnectorTableVersion version, io.trino.spi.type.Type versionType)
    {
        if (versionType.equals(DATE)) {
            // Retrieve the latest snapshot made before or at the beginning of the day of the specified date in the session's time zone
            long epochMillis = LocalDate.ofEpochDay((Long) version.getVersion())
                    .atStartOfDay()
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        if (versionType instanceof TimestampType timestampVersionType) {
            long epochMicrosUtc = timestampVersionType.isShort()
                    ? (long) version.getVersion()
                    : ((LongTimestamp) version.getVersion()).getEpochMicros();
            long epochMillisUtc = floorDiv(epochMicrosUtc, MICROSECONDS_PER_MILLISECOND);
            long epochMillis = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillisUtc), ZoneOffset.UTC)
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        if (versionType instanceof TimestampWithTimeZoneType timeZonedVersionType) {
            long epochMillis = timeZonedVersionType.isShort()
                    ? unpackMillisUtc((long) version.getVersion())
                    : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(session, tableName)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (!isArcherTableName(tableName.getTableName()) || isDataTable(tableName.getTableName()) || isMaterializedViewStorage(tableName.getTableName())) {
            return Optional.empty();
        }

        // load the base table for the system table
        String name = tableNameFrom(tableName.getTableName());
        Table table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name));
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }
        catch (UnknownTableTypeException e) {
            // avoid dealing with non Archer tables
            return Optional.empty();
        }

        TableType tableType = ArcherTableName.tableTypeFrom(tableName.getTableName());
        return switch (tableType) {
            case DATA, MATERIALIZED_VIEW_STORAGE -> throw new VerifyException("Unexpected table type: " + tableType); // Handled above.
            case HISTORY -> Optional.of(new HistoryTable(tableName, table));
            case METADATA_LOG_ENTRIES -> Optional.of(new MetadataLogEntriesTable(tableName, table));
            case SNAPSHOTS -> Optional.of(new SnapshotsTable(tableName, typeManager, table));
            case PARTITIONS -> Optional.of(new PartitionTable(tableName, typeManager, table, getCurrentSnapshotId(table)));
            case MANIFESTS -> Optional.of(new ManifestsTable(tableName, table, getCurrentSnapshotId(table)));
            case FILES -> Optional.of(new FilesTable(tableName, typeManager, table, getCurrentSnapshotId(table)));
            case PROPERTIES -> Optional.of(new PropertiesTable(tableName, table));
            case REFS -> Optional.of(new RefsTable(tableName, table));
        };
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArcherTableHandle table = (ArcherTableHandle) tableHandle;

        if (table.getSnapshotId().isEmpty()) {
            // A table with missing snapshot id produces no splits, so we optimize here by returning
            // TupleDomain.none() as the predicate
            return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), ImmutableList.of());
        }

        Table archerTable = catalog.loadTable(session, table.getSchemaTableName());

        // Extract identity partition fields that are present in all partition specs, for creating the discrete predicates.
        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(archerTable);

        TupleDomain<ArcherColumnHandle> enforcedPredicate = table.getEnforcedPredicate();

        DiscretePredicates discretePredicates = null;
        if (!partitionSourceIds.isEmpty()) {
            // Extract identity partition columns
            Map<Integer, ArcherColumnHandle> columns = getColumns(archerTable.schema(), typeManager).stream()
                    .filter(column -> partitionSourceIds.contains(column.getId()))
                    .collect(toImmutableMap(ArcherColumnHandle::getId, identity()));

            Supplier<List<FileScanTask>> lazyFiles = Suppliers.memoize(() -> {
                TableScan tableScan = archerTable.newScan()
                        .useSnapshot(table.getSnapshotId().get())
                        .filter(toArcherExpression(enforcedPredicate));

                try (CloseableIterable<FileScanTask> iterator = tableScan.planFiles()) {
                    return ImmutableList.copyOf(iterator);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            Iterable<FileScanTask> files = () -> lazyFiles.get().iterator();

            Iterable<TupleDomain<ColumnHandle>> discreteTupleDomain = Iterables.transform(files, fileScan -> {
                // Extract partition values in the data file
                Map<Integer, Optional<String>> partitionColumnValueStrings = getPartitionKeys(fileScan);
                Map<ColumnHandle, NullableValue> partitionValues = partitionSourceIds.stream()
                        .filter(partitionColumnValueStrings::containsKey)
                        .collect(toImmutableMap(
                                columns::get,
                                columnId -> {
                                    ArcherColumnHandle column = columns.get(columnId);
                                    Object prestoValue = deserializePartitionValue(
                                            column.getType(),
                                            partitionColumnValueStrings.get(columnId).orElse(null),
                                            column.getName());

                                    return NullableValue.of(column.getType(), prestoValue);
                                }));

                return TupleDomain.fromFixedValues(partitionValues);
            });

            discretePredicates = new DiscretePredicates(
                    columns.values().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList()),
                    discreteTupleDomain);
        }

        return new ConnectorTableProperties(
                // Using the predicate here directly avoids eagerly loading all partition values. Logically, this
                // still keeps predicate and discretePredicates evaluation the same on every row of the table. This
                // can be further optimized by intersecting with partition values at the cost of iterating
                // over all tableScan.planFiles() and caching partition values in table handle.
                enforcedPredicate.transformKeys(ColumnHandle.class::cast),
                // TODO: implement table partitioning
                Optional.empty(),
                Optional.ofNullable(discretePredicates),
                ImmutableList.of());
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        if (table instanceof CorruptedArcherTableHandle corruptedTableHandle) {
            return corruptedTableHandle.schemaTableName();
        }
        return ((ArcherTableHandle) table).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ArcherTableHandle tableHandle = checkValidTableHandle(table);
        Table archerTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        List<ColumnMetadata> columns = getColumnMetadata(tableHandle.getSchema());
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columns, getArcherTableProperties(archerTable), getTableComment(archerTable));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .map(TableInfo::tableName)
                .toList();
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, RelationType> result = ImmutableMap.builder();
        for (TableInfo info : catalog.listTables(session, schemaName)) {
            result.put(info.tableName(), info.extendedRelationType().toRelationType());
        }
        return result.buildKeepingLast();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArcherTableHandle table = checkValidTableHandle(tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ArcherColumnHandle columnHandle : getColumns(table.getSchema(), typeManager)) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        columnHandles.put(FILE_PATH.getColumnName(), pathColumnHandle());
        columnHandles.put(ROW_POS.getColumnName(), posColumnHandle());
        columnHandles.put(FILE_MODIFIED_TIME.getColumnName(), fileModifiedTimeColumnHandle());
        columnHandles.put(DYNAMIC_REPARTITIONING_VALUE.getColumnName(), dynamicRepartitioningValueColumnHandle());
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ArcherColumnHandle column = (ArcherColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setComment(column.getComment())
                .setNullable(column.isNullable())
                .build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> schemaTableNames;
        if (prefix.getTable().isEmpty()) {
            schemaTableNames = catalog.listTables(session, prefix.getSchema()).stream()
                    .map(TableInfo::tableName)
                    .collect(toImmutableList());
        }
        else {
            schemaTableNames = ImmutableList.of(prefix.toSchemaTableName());
        }
        return Lists.partition(schemaTableNames, GET_METADATA_BATCH_SIZE).stream()
                .map(tableBatch -> {
                    ImmutableList.Builder<TableColumnsMetadata> tableMetadatas = ImmutableList.builderWithExpectedSize(tableBatch.size());
                    Set<SchemaTableName> remainingTables = new HashSet<>(tableBatch.size());
                    for (SchemaTableName tableName : tableBatch) {
                        if (redirectTable(session, tableName).isPresent()) {
                            tableMetadatas.add(TableColumnsMetadata.forRedirectedTable(tableName));
                        }
                        else {
                            remainingTables.add(tableName);
                        }
                    }

                    Map<SchemaTableName, List<ColumnMetadata>> loaded = catalog.tryGetColumnMetadata(session, ImmutableList.copyOf(remainingTables));
                    loaded.forEach((tableName, columns) -> {
                        remainingTables.remove(tableName);
                        tableMetadatas.add(TableColumnsMetadata.forTable(tableName, columns));
                    });

                    for (SchemaTableName tableName : remainingTables) {
                        try {
                            Table archerTable = catalog.loadTable(session, tableName);
                            List<ColumnMetadata> columns = getColumnMetadata(archerTable.schema());
                            tableMetadatas.add(TableColumnsMetadata.forTable(tableName, columns));
                        }
                        catch (TableNotFoundException e) {
                            // Table disappeared during listing operation
                        }
                        catch (UnknownTableTypeException e) {
                            // Skip unsupported table type in case that the table redirects are not enabled
                        }
                        catch (RuntimeException e) {
                            // Table can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                            log.warn(e, "Failed to access metadata of table %s during streaming table columns for %s", tableName, prefix);
                        }
                    }
                    return tableMetadatas.build();
                })
                .flatMap(List::stream)
                .iterator();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return catalog.streamRelationColumns(session, schemaName, relationFilter, tableName -> redirectTable(session, tableName).isPresent())
                .orElseGet(() -> {
                    // Catalog does not support streamRelationColumns
                    return ConnectorMetadata.super.streamRelationColumns(session, schemaName, relationFilter);
                });
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return catalog.streamRelationComments(session, schemaName, relationFilter, tableName -> redirectTable(session, tableName).isPresent())
                .orElseGet(() -> {
                    // Catalog does not support streamRelationComments
                    return ConnectorMetadata.super.streamRelationComments(session, schemaName, relationFilter);
                });
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        catalog.createNamespace(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            for (SchemaTableName materializedView : listMaterializedViews(session, Optional.of(schemaName))) {
                dropMaterializedView(session, materializedView);
            }
            for (SchemaTableName viewName : listViews(session, Optional.of(schemaName))) {
                dropView(session, viewName);
            }
            for (SchemaTableName tableName : listTables(session, Optional.of(schemaName))) {
                dropTable(session, getTableHandle(session, tableName, Optional.empty(), Optional.empty()));
            }
        }
        catalog.dropNamespace(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        catalog.renameNamespace(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        catalog.setNamespacePrincipal(session, schemaName, principal);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        Optional<ConnectorTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout, NO_RETRIES, saveMode == SaveMode.REPLACE), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        ArcherTableHandle table = checkValidTableHandle(tableHandle);
        catalog.updateTableComment(session, table.getSchemaTableName(), comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        catalog.updateViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        catalog.updateViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        catalog.updateMaterializedViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Schema schema = schemaFromMetadata(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        boolean forceEngineRepartitioning = isForceEngineRepartitioning(session);
        return getWriteLayout(schema, partitionSpec, false, forceEngineRepartitioning);
    }

    @Override
    public Optional<io.trino.spi.type.Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, io.trino.spi.type.Type type)
    {
        io.trino.spi.type.Type newType = coerceType(type);
        if (type.getTypeSignature().equals(newType.getTypeSignature())) {
            return Optional.empty();
        }
        return Optional.of(newType);
    }

    private io.trino.spi.type.Type coerceType(io.trino.spi.type.Type type)
    {
        if (type instanceof TimestampWithTimeZoneType) {
            return TIMESTAMP_TZ_MICROS;
        }
        if (type instanceof TimestampType) {
            return TIMESTAMP_MICROS;
        }
        if (type instanceof TimeType) {
            return TIME_MICROS;
        }
        if (type instanceof CharType) {
            return VARCHAR;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(coerceType(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(coerceType(mapType.getKeyType()), coerceType(mapType.getValueType()), typeManager.getTypeOperators());
        }
        if (type instanceof RowType rowType) {
            return RowType.from(rowType.getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), coerceType(field.getType())))
                    .collect(toImmutableList()));
        }
        return type;
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        verify(transaction == null, "transaction already set");
        String schemaName = tableMetadata.getTable().getSchemaName();
        if (!schemaExists(session, schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }

        String tableLocation = null;
        if (replace) {
            ArcherTableHandle table = (ArcherTableHandle) getTableHandle(session, tableMetadata.getTableSchema().getTable(), Optional.empty(), Optional.empty());
            if (table != null) {
                verifyTableVersionForUpdate(table);
                Table archerTable = catalog.loadTable(session, table.getSchemaTableName());
                Optional<String> providedTableLocation = getTableLocation(tableMetadata.getProperties());
                if (providedTableLocation.isPresent() && !stripTrailingSlash(providedTableLocation.get()).equals(archerTable.location())) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, format("The provided location '%s' does not match the existing table location '%s'", providedTableLocation.get(), archerTable.location()));
                }
                validateNotModifyingOldSnapshot(table, archerTable);
                tableLocation = archerTable.location();
            }
        }

        if (tableLocation == null) {
            tableLocation = getTableLocation(tableMetadata.getProperties())
                    .orElseGet(() -> catalog.defaultTableLocation(session, tableMetadata.getTable()));
        }
        transaction = newCreateTableTransaction(catalog, tableMetadata, session, replace, tableLocation);
        Location location = Location.of(transaction.table().location());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity());
        try {
            if (!replace && fileSystem.listFiles(location).hasNext()) {
                throw new TrinoException(ARCHER_FILESYSTEM_ERROR, format(
                        "Cannot create a table on a non-empty location: %s, set 'archer.unique-table-location=true' in your Archer catalog properties " +
                                "to use unique table locations for every table.", location));
            }
            return newWritableTableHandle(tableMetadata.getTable(), transaction.table(), retryMode);
        }
        catch (IOException e) {
            throw new TrinoException(ARCHER_FILESYSTEM_ERROR, "Failed checking new table's location: " + location, e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        ArcherWritableTableHandle archerTableHandle = (ArcherWritableTableHandle) tableHandle;
        try {
            if (fragments.isEmpty()) {
                // Commit the transaction if the table is being created without data
                AppendFiles appendFiles = transaction.newFastAppend();
                commit(appendFiles, session);
                transaction.commitTransaction();
                transaction = null;
                return Optional.empty();
            }

            return finishInsert(session, archerTableHandle, fragments, computedStatistics);
        }
        catch (AlreadyExistsException e) {
            // May happen when table has been already created concurrently.
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table %s already exists", archerTableHandle.getName()), e);
        }
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArcherTableHandle table = (ArcherTableHandle) tableHandle;
        Schema schema = table.getSchema();
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(
                schema,
                table.getPartitionSpecJson().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle")));
        boolean forceEngineRepartitioning = isForceEngineRepartitioning(session);
        return getWriteLayout(schema, partitionSpec, false, forceEngineRepartitioning);
    }

    private Optional<ConnectorTableLayout> getWriteLayout(Schema tableSchema, PartitionSpec partitionSpec, boolean forceRepartitioning, boolean forceEngineRepartitioning)
    {
        if (partitionSpec.isUnpartitioned()) {
            return Optional.empty();
        }

        Map<Integer, ArcherColumnHandle> columnById = getColumns(tableSchema, typeManager).stream()
                .collect(toImmutableMap(ArcherColumnHandle::getId, identity()));

        List<ArcherColumnHandle> partitioningColumns = partitionSpec.fields().stream()
                .sorted(Comparator.comparing(PartitionField::sourceId))
                .map(field -> requireNonNull(columnById.get(field.sourceId()), () -> "Cannot find source column for partitioning field " + field))
                .distinct()
                .collect(toImmutableList());
        List<String> partitioningColumnNames = partitioningColumns.stream()
                .map(ArcherColumnHandle::getName)
                .collect(toImmutableList());

        if (!forceRepartitioning && (forceEngineRepartitioning || partitionSpec.fields().stream().allMatch(field -> field.transform().isIdentity()))) {
            // Do not set partitioningHandle, to let engine determine whether to repartition data or not, on stat-based basis.
            return Optional.of(new ConnectorTableLayout(partitioningColumnNames));
        }
        ArcherPartitioningHandle partitioningHandle = new ArcherPartitioningHandle(toPartitionFields(partitionSpec), partitioningColumns);
        return Optional.of(new ConnectorTableLayout(partitioningHandle, partitioningColumnNames, true));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        ArcherTableHandle table = (ArcherTableHandle) tableHandle;
        Table archerTable = catalog.loadTable(session, table.getSchemaTableName());

        validateNotModifyingOldSnapshot(table, archerTable);

        beginTransaction(archerTable);

        return newWritableTableHandle(table.getSchemaTableName(), archerTable, retryMode);
    }

    private ArcherWritableTableHandle newWritableTableHandle(SchemaTableName name, Table table, RetryMode retryMode)
    {
        return new ArcherWritableTableHandle(
                name,
                SchemaParser.toJson(table.schema()),
                transformValues(table.specs(), PartitionSpecParser::toJson),
                table.spec().specId(),
                table.invertedIndex().isIndexed() ? Optional.of(InvertedIndexParser.toJson(table.invertedIndex())) : Optional.empty(),
                getColumns(table.schema(), typeManager),
                table.location(),
                getFileFormat(table),
                table.properties(),
                retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        if (commitTasks.isEmpty()) {
            transaction = null;
            return Optional.empty();
        }

        ArcherWritableTableHandle table = (ArcherWritableTableHandle) insertHandle;
        Table archerTable = transaction.table();
        Type[] partitionColumnTypes = archerTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        archerTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newAppend();
        InvertedIndex invertedIndex = archerTable.invertedIndex();

        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(archerTable.spec())
                    .withPath(task.getSegmentPath())
                    .withFileName(task.getFileName())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withLastModifiedTime(task.getLastModifiedTime())
                    .withFormat(table.getFileFormat().toArcher())
                    .withMetrics(task.getMetrics().metrics());

            // inverted index
            builder.withInvertedIndex(invertedIndex);
            task.getInvertedIndexFilesJson().map(InvertedIndexFilesParser::fromJson).ifPresent(builder::withInvertedIndexFiles);
            // deletion
            task.getDeletionJson().map(DeletionParser::fromJson).ifPresent(builder::withDeletion);

            if (!archerTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            DataFile dataFile = builder.build();
            appendFiles.appendFile(dataFile);
        }

        appendFiles.commit();
        transaction.commitTransaction();
        transaction = null;

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getSegmentPath)
                .collect(toImmutableList())));
    }

    private static void cleanExtraOutputFiles(TrinoFileSystem fileSystem, String queryId, Location location, Set<String> filesToKeep)
    {
        checkArgument(!queryId.contains("-"), "query ID should not contain hyphens: %s", queryId);

        Deque<String> filesToDelete = new ArrayDeque<>();
        try {
            log.debug("Deleting failed attempt files from %s for query %s", location, queryId);

            FileIterator iterator = fileSystem.listFiles(location);
            while (iterator.hasNext()) {
                FileEntry entry = iterator.next();
                String name = entry.location().fileName();
                if (name.startsWith(queryId + "-") && !filesToKeep.contains(name)) {
                    filesToDelete.add(name);
                }
            }

            if (filesToDelete.isEmpty()) {
                return;
            }

            log.info("Found %s files to delete and %s to retain in location %s for query %s", filesToDelete.size(), filesToKeep.size(), location, queryId);
            ImmutableList.Builder<String> deletedFilesBuilder = ImmutableList.builder();
            Iterator<String> filesToDeleteIterator = filesToDelete.iterator();
            while (filesToDeleteIterator.hasNext()) {
                String fileName = filesToDeleteIterator.next();
                log.debug("Deleting failed attempt file %s/%s for query %s", location, fileName, queryId);
                fileSystem.deleteFile(location.appendPath(fileName));
                deletedFilesBuilder.add(fileName);
                filesToDeleteIterator.remove();
            }

            List<String> deletedFiles = deletedFilesBuilder.build();
            if (!deletedFiles.isEmpty()) {
                log.info("Deleted failed attempt files %s from %s for query %s", deletedFiles, location, queryId);
            }
        }
        catch (IOException e) {
            throw new TrinoException(ARCHER_FILESYSTEM_ERROR,
                    format("Could not clean up extraneous output files; remaining files: %s", filesToDelete), e);
        }
    }

    private static Set<String> getOutputFilesLocations(Set<String> writtenFiles)
    {
        return writtenFiles.stream()
                .map(ArcherMetadata::getLocation)
                .collect(toImmutableSet());
    }

    private static String getLocation(String path)
    {
        Matcher matcher = PATH_PATTERN.matcher(path);
        verify(matcher.matches(), "path %s does not match pattern", path);
        return matcher.group(1);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        ArcherTableHandle tableHandle = (ArcherTableHandle) connectorTableHandle;
        checkArgument(tableHandle.getTableType() == DATA, "Cannot execute table procedure %s on non-DATA table: %s", procedureName, tableHandle.getTableType());
        Table aecherTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        if (tableHandle.getSnapshotId().isPresent() && (tableHandle.getSnapshotId().get() != aecherTable.currentSnapshot().snapshotId())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot execute table procedure %s on old snapshot %s".formatted(procedureName, tableHandle.getSnapshotId().get()));
        }

        ArcherTableProcedureId procedureId;
        try {
            procedureId = ArcherTableProcedureId.valueOf(procedureName);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
        }

        return switch (procedureId) {
            case OPTIMIZE -> getTableHandleForOptimize(tableHandle, executeProperties, retryMode);
            case EXPIRE_SNAPSHOTS -> getTableHandleForExpireSnapshots(session, tableHandle, executeProperties);
            case REMOVE_ORPHAN_FILES -> getTableHandleForRemoveOrphanFiles(session, tableHandle, executeProperties);
            case REMOVE_FILES -> getTableHandleForRemoveFiles(session, tableHandle, executeProperties);
        };
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimize(ArcherTableHandle tableHandle, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        DataSize maxScannedFileSize = (DataSize) executeProperties.get(FILE_SIZE_THRESHOLD);
        boolean refreshPartition = (boolean) executeProperties.get(REFRESH_PARTITION);
        boolean refreshInvertedIndex = (boolean) executeProperties.get(REFRESH_INVERTED_INDEX);

        return Optional.of(new ArcherTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE,
                new ArcherOptimizeHandle(
                        tableHandle.getSnapshotId(),
                        tableHandle.getTableSchemaJson(),
                        tableHandle.getPartitionSpecJson().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle")),
                        tableHandle.getInvertedIndexJson(),
                        getColumns(tableHandle.getSchema(), typeManager),
                        getFileFormat(tableHandle.getStorageProperties()),
                        tableHandle.getStorageProperties(),
                        maxScannedFileSize,
                        refreshPartition,
                        refreshInvertedIndex,
                        retryMode != NO_RETRIES),
                tableHandle.getTableLocation()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForExpireSnapshots(ConnectorSession session, ArcherTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table archerTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new ArcherTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                EXPIRE_SNAPSHOTS,
                new ArcherExpireSnapshotsHandle(retentionThreshold),
                archerTable.location()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRemoveOrphanFiles(ConnectorSession session, ArcherTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table archerTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new ArcherTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                REMOVE_ORPHAN_FILES,
                new ArcherRemoveOrphanFilesHandle(retentionThreshold),
                archerTable.location()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRemoveFiles(ConnectorSession session, ArcherTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        List<String> files = (List<String>) executeProperties.get(FILES);
        Table archerTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new ArcherTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                REMOVE_FILES,
                new ArcherRemoveFilesHandle(files),
                archerTable.location()));
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        ArcherTableExecuteHandle executeHandle = (ArcherTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                return getLayoutForOptimize(session, executeHandle);
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case REMOVE_FILES:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private Optional<ConnectorTableLayout> getLayoutForOptimize(ConnectorSession session, ArcherTableExecuteHandle executeHandle)
    {
        Table archerTable = catalog.loadTable(session, executeHandle.schemaTableName());
        // from performance perspective it is better to have lower number of bigger files than other way around
        // thus we force repartitioning for optimize to achieve this
        // Adds session property to control this behavior
        Schema schema = archerTable.schema();
        PartitionSpec spec = archerTable.spec();
        if (isOptimizeDynamicRepartitioning(session)) {
            List<Types.NestedField> columns = new ArrayList<>(schema.columns());
            columns.add(NestedField.required(TRINO_DYNAMIC_REPARTITIONING_VALUE_ID, TRINO_DYNAMIC_REPARTITIONING_VALUE_NAME, IntegerType.get()));
            schema = new Schema(0, columns, schema.getAliases(), schema.primaryKeyFieldIds());

            List<String> fields = new ArrayList<>(toPartitionFields(spec));
            fields.add(TRINO_DYNAMIC_REPARTITIONING_VALUE_NAME);
            spec = parsePartitionFields(schema, fields);
        }
        return getWriteLayout(schema, spec, true, false);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
            ConnectorSession session,
            ConnectorTableExecuteHandle tableExecuteHandle,
            ConnectorTableHandle updatedSourceTableHandle)
    {
        ArcherTableExecuteHandle executeHandle = (ArcherTableExecuteHandle) tableExecuteHandle;
        ArcherTableHandle table = (ArcherTableHandle) updatedSourceTableHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                return beginOptimize(session, executeHandle, table);
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case REMOVE_FILES:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(
            ConnectorSession session,
            ArcherTableExecuteHandle executeHandle,
            ArcherTableHandle table)
    {
        ArcherOptimizeHandle optimizeHandle = (ArcherOptimizeHandle) executeHandle.procedureHandle();
        Table archerTable = catalog.loadTable(session, table.getSchemaTableName());

        validateNotModifyingOldSnapshot(table, archerTable);

        int tableFormatVersion = ((BaseTable) archerTable).operations().current().formatVersion();
        if (tableFormatVersion > OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION) {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "%s is not supported for Archer table format version > %d. Table %s format version is %s.",
                    OPTIMIZE.name(),
                    OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION,
                    table.getSchemaTableName(),
                    tableFormatVersion));
        }

        beginTransaction(archerTable);

        return new BeginTableExecuteResult<>(
                executeHandle,
                table.forOptimize(
                        true,
                        optimizeHandle.maxScannedFileSize(),
                        archerTable.spec().specId(),
                        optimizeHandle.refreshPartition(),
                        archerTable.invertedIndex().indexId(),
                        optimizeHandle.refreshInvertedIndex()));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        ArcherTableExecuteHandle executeHandle = (ArcherTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                finishOptimize(session, executeHandle, fragments, splitSourceInfo);
                return;
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case REMOVE_FILES:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private void finishOptimize(ConnectorSession session, ArcherTableExecuteHandle executeHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        ArcherOptimizeHandle optimizeHandle = (ArcherOptimizeHandle) executeHandle.procedureHandle();
        Table archerTable = transaction.table();
        InvertedIndex invertedIndex = archerTable.invertedIndex();

        // files to be deleted
        ImmutableSet.Builder<DataFile> scannedDataFilesBuilder = ImmutableSet.builder();
        splitSourceInfo.stream().map(ScannedDataFiles.class::cast).forEach(files -> scannedDataFilesBuilder.addAll(files.dataFiles()));

        Set<DataFile> scannedDataFiles = scannedDataFilesBuilder.build();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = archerTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        archerTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        Set<DataFile> newFiles = new HashSet<>();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(archerTable.spec())
                    .withPath(task.getSegmentPath())
                    .withFileName(task.getFileName())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withLastModifiedTime(task.getLastModifiedTime())
                    .withFormat(optimizeHandle.fileFormat().toArcher())
                    .withMetrics(task.getMetrics().metrics());

            // inverted index
            builder.withInvertedIndex(invertedIndex);
            task.getInvertedIndexFilesJson().map(InvertedIndexFilesParser::fromJson).ifPresent(builder::withInvertedIndexFiles);
            // deletion
            task.getDeletionJson().map(DeletionParser::fromJson).ifPresent(builder::withDeletion);

            if (!archerTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            newFiles.add(builder.build());
        }

        if (optimizeHandle.snapshotId().isEmpty() || scannedDataFiles.isEmpty() && newFiles.isEmpty()) {
            // Either the table is empty, or the table scan turned out to be empty, nothing to commit
            transaction = null;
            return;
        }

        RewriteFiles rewriteFiles = transaction.newRewrite();
        scannedDataFiles.forEach(rewriteFiles::deleteFile);
        newFiles.forEach(rewriteFiles::addFile);
        rewriteFiles.commit();
        transaction.commitTransaction();
        transaction = null;
    }

    @Override
    public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        ArcherTableExecuteHandle executeHandle = (ArcherTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case EXPIRE_SNAPSHOTS -> executeExpireSnapshots(session, executeHandle);
            case REMOVE_ORPHAN_FILES -> executeRemoveOrphanFiles(session, executeHandle);
            case REMOVE_FILES -> executeRemoveFiles(session, executeHandle);
            default -> throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
        }
    }

    private void executeExpireSnapshots(ConnectorSession session, ArcherTableExecuteHandle executeHandle)
    {
        ArcherExpireSnapshotsHandle expireSnapshotsHandle = (ArcherExpireSnapshotsHandle) executeHandle.procedureHandle();

        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        Duration retention = requireNonNull(expireSnapshotsHandle.retentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.schemaTableName(),
                EXPIRE_SNAPSHOTS.name(),
                retention,
                getExpireSnapshotMinRetention(session),
                ArcherConfig.EXPIRE_SNAPSHOTS_MIN_RETENTION,
                ArcherSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION);

        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity());
        List<Location> pathsToDelete = new ArrayList<>();
        // deleteFunction is not accessed from multiple threads unless .executeDeleteWith() is used
        Consumer<String> deleteFunction = path -> {
            pathsToDelete.add(Location.of(path));
            if (pathsToDelete.size() == DELETE_BATCH_SIZE) {
                try {
                    fileSystem.deleteFiles(pathsToDelete);
                    pathsToDelete.clear();
                }
                catch (IOException e) {
                    throw new TrinoException(ARCHER_FILESYSTEM_ERROR, "Failed to delete files during snapshot expiration", e);
                }
            }
        };

        long expireTimestampMillis = session.getStart().toEpochMilli() - retention.toMillis();
        table.expireSnapshots()
                .expireOlderThan(expireTimestampMillis)
                .deleteWith(deleteFunction)
                .commit();
        try {
            fileSystem.deleteFiles(pathsToDelete);
        }
        catch (IOException e) {
            throw new TrinoException(ARCHER_FILESYSTEM_ERROR, "Failed to delete files during snapshot expiration", e);
        }
    }

    private static void validateTableExecuteParameters(
            Table table,
            SchemaTableName schemaTableName,
            String procedureName,
            Duration retentionThreshold,
            Duration minRetention,
            String minRetentionParameterName,
            String sessionMinRetentionParameterName)
    {
        int tableFormatVersion = ((BaseTable) table).operations().current().formatVersion();
        if (tableFormatVersion > CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION) {
            // It is not known if future version won't bring any new kind of metadata or data files
            // because of the way procedures are implemented it is safer to fail here than to potentially remove
            // files that should stay there
            throw new TrinoException(NOT_SUPPORTED, format("%s is not supported for Archer table format version > %d. " +
                            "Table %s format version is %s.",
                    procedureName,
                    CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION,
                    schemaTableName,
                    tableFormatVersion));
        }
        Map<String, String> properties = table.properties();
        if (properties.containsKey(WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new TrinoException(NOT_SUPPORTED, "Table " + schemaTableName + " specifies " + properties.get(WRITE_LOCATION_PROVIDER_IMPL) +
                    " as a location provider. Writing to Archer tables with custom location provider is not supported.");
        }

        Duration retention = requireNonNull(retentionThreshold, "retention is null");
        checkProcedureArgument(retention.compareTo(minRetention) >= 0,
                "Retention specified (%s) is shorter than the minimum retention configured in the system (%s). " +
                        "Minimum retention can be changed with %s configuration property or archer.%s session property",
                retention,
                minRetention,
                minRetentionParameterName,
                sessionMinRetentionParameterName);
    }

    public void executeRemoveOrphanFiles(ConnectorSession session, ArcherTableExecuteHandle executeHandle)
    {
        ArcherRemoveOrphanFilesHandle removeOrphanFilesHandle = (ArcherRemoveOrphanFilesHandle) executeHandle.procedureHandle();

        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        Duration retention = requireNonNull(removeOrphanFilesHandle.retentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.schemaTableName(),
                REMOVE_ORPHAN_FILES.name(),
                retention,
                getRemoveOrphanFilesMinRetention(session),
                ArcherConfig.REMOVE_ORPHAN_FILES_MIN_RETENTION,
                ArcherSessionProperties.REMOVE_ORPHAN_FILES_MIN_RETENTION);

        if (table.currentSnapshot() == null) {
            log.debug("Skipping remove_orphan_files procedure for empty table " + table);
            return;
        }

        Instant expiration = session.getStart().minusMillis(retention.toMillis());
        removeOrphanFiles(table, session, executeHandle.schemaTableName(), expiration);
    }

    private void removeOrphanFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, Instant expiration)
    {
        Set<String> processedManifestFilePaths = new HashSet<>();
        // Similarly to issues like https://github.com/trinodb/trino/issues/13759, equivalent paths may have different String
        // representations due to things like double slashes. Using file names may result in retaining files which could be removed.
        // However, in practice Archer metadata and data files have UUIDs in their names which makes this unlikely.
        ImmutableSet.Builder<String> validMetadataFileNames = ImmutableSet.builder();
        ImmutableSet.Builder<String> validSegmentFileNames = ImmutableSet.builder();

        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot.manifestListLocation() != null) {
                validMetadataFileNames.add(fileName(snapshot.manifestListLocation()));
            }

            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                if (!processedManifestFilePaths.add(manifest.path())) {
                    // Already read this manifest
                    continue;
                }

                validMetadataFileNames.add(fileName(manifest.path()));
                try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(table, manifest)) {
                    for (ContentFile<?> contentFile : manifestReader) {
                        contentFile.fullPathFiles().forEach(file -> validSegmentFileNames.add(fileName(file)));
                    }
                }
                catch (IOException e) {
                    throw new TrinoException(ARCHER_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
                }
            }
        }

        metadataFileLocations(table, false).stream()
                .map(ArcherUtil::fileName)
                .forEach(validMetadataFileNames::add);

        validMetadataFileNames.add("version-hint.text");

        scanAndDeleteInvalidFiles(table, session, schemaTableName, expiration, validSegmentFileNames.build(), "data");
        scanAndDeleteInvalidFiles(table, session, schemaTableName, expiration, validMetadataFileNames.build(), "metadata");
    }

    private static ManifestReader<? extends ContentFile<?>> readerForManifest(Table table, ManifestFile manifest)
    {
        return switch (manifest.content()) {
            case DATA -> ManifestFiles.read(manifest, table.io());
            case INDEX -> ManifestFiles.readIndexManifest(manifest, table.io(), table.specs());
        };
    }

    private void scanAndDeleteInvalidFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, Instant expiration, Set<String> validFiles, String subfolder)
    {
        try {
            List<Location> filesToDelete = new ArrayList<>();
            TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity());
            FileIterator allFiles = fileSystem.listFiles(Location.of(table.location()).appendPath(subfolder));
            while (allFiles.hasNext()) {
                FileEntry entry = allFiles.next();
                if (entry.lastModified().isBefore(expiration) && !validFiles.contains(entry.location().fileName())) {
                    filesToDelete.add(entry.location());
                    if (filesToDelete.size() >= DELETE_BATCH_SIZE) {
                        log.debug("Deleting files while removing orphan files for table %s [%s]", schemaTableName, filesToDelete);
                        fileSystem.deleteFiles(filesToDelete);
                        filesToDelete.clear();
                    }
                }
                else {
                    log.debug("%s file retained while removing orphan files %s", entry.location(), schemaTableName.getTableName());
                }
            }
            if (!filesToDelete.isEmpty()) {
                log.debug("Deleting files while removing orphan files for table %s %s", schemaTableName, filesToDelete);
                fileSystem.deleteFiles(filesToDelete);
            }
        }
        catch (IOException e) {
            throw new TrinoException(ARCHER_FILESYSTEM_ERROR, "Failed accessing data for table: " + schemaTableName, e);
        }
    }

    public void executeRemoveFiles(ConnectorSession session, ArcherTableExecuteHandle executeHandle)
    {
        ArcherRemoveFilesHandle removeFilesHandle = (ArcherRemoveFilesHandle) executeHandle.procedureHandle();

        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        List<String> files = requireNonNull(removeFilesHandle.files(), "files is null");

        if (table.currentSnapshot() == null) {
            log.debug("Skipping remove_files procedure for empty table " + table);
            return;
        }

        if (files.isEmpty()) {
            return;
        }

        DeleteFiles deleteFiles = table.newDelete();
        for (String file : files) {
            deleteFiles.deleteFile(file);
        }
        deleteFiles.commit();
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        ArcherTableHandle archerTableHandle = (ArcherTableHandle) tableHandle;
        Optional<Boolean> partitioned = archerTableHandle.getPartitionSpecJson()
                .map(partitionSpecJson -> PartitionSpecParser.fromJson(archerTableHandle.getSchema(), partitionSpecJson).isPartitioned());

        return Optional.of(new ArcherInputInfo(
                archerTableHandle.getSnapshotId(),
                partitioned,
                getFileFormat(archerTableHandle.getStorageProperties()).name()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof CorruptedArcherTableHandle corruptedTableHandle) {
            catalog.dropCorruptedTable(session, corruptedTableHandle.schemaTableName());
        }
        else {
            catalog.dropTable(session, ((ArcherTableHandle) tableHandle).getSchemaTableName());
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        ArcherTableHandle handle = checkValidTableHandle(tableHandle);
        catalog.renameTable(session, handle.getSchemaTableName(), newTable);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        ArcherTableHandle table = (ArcherTableHandle) tableHandle;
        Table archerTable = catalog.loadTable(session, table.getSchemaTableName());

        Set<String> unsupportedProperties = difference(properties.keySet(), UPDATABLE_TABLE_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }

        beginTransaction(archerTable);

        if (properties.containsKey(PARTITIONING_PROPERTY)) {
            @SuppressWarnings("unchecked")
            List<String> partitionColumns = (List<String>) properties.get(PARTITIONING_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The partitioning property cannot be empty"));
            updatePartitioning(archerTable, transaction, partitionColumns);
        }

        if (properties.containsKey(INVERTED_INDEXED_BY_PROPERTY)) {
            @SuppressWarnings("unchecked")
            List<String> invertedIndexColumns = (List<String>) properties.get(INVERTED_INDEXED_BY_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The partitioning property cannot be empty"));
            updateInvertedIndex(archerTable, transaction, invertedIndexColumns);
        }

        try {
            transaction.commitTransaction();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ARCHER_COMMIT_ERROR, "Failed to commit new table properties", e);
        }
    }

    private static void updatePartitioning(Table archerTable, Transaction transaction, List<String> partitionColumns)
    {
        UpdatePartitionSpec updatePartitionSpec = transaction.updateSpec();
        Set<PartitionField> existingPartitionFields = archerTable.spec().fields().stream().collect(toImmutableSet());
        Schema schema = archerTable.schema();
        if (partitionColumns.isEmpty()) {
            existingPartitionFields.stream()
                    .map(partitionField -> toArcherTerm(schema, partitionField))
                    .forEach(updatePartitionSpec::removeField);
        }
        else {
            PartitionSpec partitionSpec = parsePartitionFields(schema, partitionColumns);
            validateNotPartitionedByNestedField(schema, partitionSpec);
            Set<PartitionField> partitionFields = ImmutableSet.copyOf(partitionSpec.fields());
            difference(existingPartitionFields, partitionFields).stream()
                    .map(PartitionField::name)
                    .forEach(updatePartitionSpec::removeField);
            difference(partitionFields, existingPartitionFields).stream()
                    .map(partitionField -> toArcherTerm(schema, partitionField))
                    .forEach(updatePartitionSpec::addField);
        }

        try {
            updatePartitionSpec.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ARCHER_COMMIT_ERROR, "Failed to set new partitioning value", e);
        }
    }

    private static void updateInvertedIndex(Table archerTable, Transaction transaction, List<String> invertedIndexColumns)
    {
        UpdateInvertedIndex updateInvertedIndex = transaction.updateInvertedIndex();
        Set<InvertedIndexField> existingInvertedIndexFields = archerTable.invertedIndex().fields().stream().collect(toImmutableSet());
        Schema schema = archerTable.schema();
        if (invertedIndexColumns.isEmpty()) {
            existingInvertedIndexFields.stream()
                    .map(InvertedIndexField::name)
                    .forEach(updateInvertedIndex::removeField);
        }
        else {
            InvertedIndex invertedIndex = parseInvertedIndexFields(schema, invertedIndexColumns);
            validateNotInvertedIndexedByNestedField(schema, invertedIndex);
            Set<InvertedIndexField> invertedIndexFields = ImmutableSet.copyOf(invertedIndex.fields());
            difference(existingInvertedIndexFields, invertedIndexFields).stream()
                    .map(InvertedIndexField::name)
                    .forEach(updateInvertedIndex::removeField);
            difference(invertedIndexFields, existingInvertedIndexFields)
                    .forEach(field -> updateInvertedIndex.addField(schema.findColumnName(field.sourceId()), field.name(), field.type(), field.properties()));
        }

        try {
            updateInvertedIndex.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ARCHER_COMMIT_ERROR, "Failed to set new inverted index value", e);
        }
    }

    private static Term toArcherTerm(Schema schema, PartitionField partitionField)
    {
        return Expressions.transform(schema.findColumnName(partitionField.sourceId()), partitionField.transform());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        // Spark doesn't support adding a NOT NULL column to Archer tables
        // Also, Spark throws an exception when reading the table if we add such columns and execute a rollback procedure
        // because they keep returning the latest table definition even after the rollback https://github.com/apache/iceberg/issues/5591
        // Even when a table is empty, this connector doesn't support adding not null columns to avoid the above Spark failure
        if (!column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }
        Table archerTable = catalog.loadTable(session, ((ArcherTableHandle) tableHandle).getSchemaTableName());
        AtomicInteger nextFieldId = new AtomicInteger(archerTable.schema().highestFieldId() + 2);
        try {
            archerTable.updateSchema()
                    .addColumn(column.getName(), toArcherTypeForNewColumn(column.getType(), nextFieldId), column.getComment())
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ARCHER_COMMIT_ERROR, "Failed to add column: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        ArcherColumnHandle handle = (ArcherColumnHandle) column;
        Table archerTable = catalog.loadTable(session, ((ArcherTableHandle) tableHandle).getSchemaTableName());
        archerTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        ArcherColumnHandle columnHandle = (ArcherColumnHandle) source;
        Table archerTable = catalog.loadTable(session, ((ArcherTableHandle) tableHandle).getSchemaTableName());
        try {
            archerTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ARCHER_COMMIT_ERROR, "Failed to rename column: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    private List<ColumnMetadata> getColumnMetadata(Schema schema)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();

        List<ColumnMetadata> schemaColumns = schema.columns().stream()
                .map(column ->
                        ColumnMetadata.builder()
                                .setName(column.name())
                                .setType(toTrinoType(column.type(), typeManager))
                                .setNullable(column.isOptional())
                                .setComment(Optional.ofNullable(column.doc()))
                                .build())
                .collect(toImmutableList());
        columns.addAll(schemaColumns);
        columns.add(pathColumnMetadata());
        columns.add(posColumnMetadata());
        columns.add(fileModifiedTimeColumnMetadata());
        columns.add(dynamicRepartitioningValueColumnMetadata());
        return columns.build();
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        ArcherTableHandle table = (ArcherTableHandle) handle;
        // Don't apply delete when predicate contains metadata column in Archer
        TupleDomain<ArcherColumnHandle> metadataColumnPredicate = table.getEnforcedPredicate().filter((column, domain) -> isMetadataColumnId(column.getId()));
        if (!metadataColumnPredicate.isAll()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StructType type = StructType.of(ImmutableList.<NestedField>builder()
                .add(MetadataColumns.FILE_PATH)
                .add(MetadataColumns.ROW_POSITION)
                .add(MetadataColumns.VERSION)
                .add(NestedField.required(TRINO_MERGE_FILE_RECORD_COUNT, "file_record_count", LongType.get()))
                .add(NestedField.required(TRINO_MERGE_PARTITION_SPEC_ID, "partition_spec_id", IntegerType.get()))
                .add(NestedField.required(TRINO_MERGE_PARTITION_DATA, "partition_data", StringType.get()))
                .add(NestedField.required(TRINO_MERGE_DELETION, "deletion", StringType.get()))
                .build());

        NestedField field = NestedField.required(TRINO_MERGE_ROW_ID, TRINO_ROW_ID_NAME, type);
        return getColumnHandle(field, typeManager);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Optional.of(ArcherUpdateHandle.INSTANCE);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        ArcherTableHandle table = (ArcherTableHandle) tableHandle;
        verifyTableVersionForUpdate(table);

        Table archerTable = catalog.loadTable(session, table.getSchemaTableName());
        validateNotModifyingOldSnapshot(table, archerTable);

        beginTransaction(archerTable);

        ArcherWritableTableHandle insertHandle = newWritableTableHandle(table.getSchemaTableName(), archerTable, retryMode);

        return new ArcherMergeTableHandle(table, insertHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        ArcherTableHandle handle = ((ArcherMergeTableHandle) tableHandle).getTableHandle();
        finishWrite(session, handle, fragments, true);
    }

    private static void verifyTableVersionForUpdate(ArcherTableHandle table)
    {
        if (table.getFormatVersion() < 2) {
            throw new TrinoException(NOT_SUPPORTED, "Archer table updates require at least format version 2");
        }
    }

    private static void validateNotModifyingOldSnapshot(ArcherTableHandle table, Table archerTable)
    {
        if (table.getSnapshotId().isPresent() && (table.getSnapshotId().get() != archerTable.currentSnapshot().snapshotId())) {
            throw new TrinoException(NOT_SUPPORTED, "Modifying old snapshot is not supported in Archer");
        }
    }

    private void finishWrite(ConnectorSession session, ArcherTableHandle table, Collection<Slice> fragments, boolean runUpdateValidations)
    {
        Table archerTable = transaction.table();
        InvertedIndex invertedIndex = archerTable.invertedIndex();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        if (commitTasks.isEmpty()) {
            // Avoid recording "empty" write operation
            transaction = null;
            return;
        }

        Schema schema = table.getSchema();

        OverwriteFiles overwriteFiles = transaction.newOverwrite();
        table.getSnapshotId().map(archerTable::snapshot).ifPresent(s -> overwriteFiles.validateFromSnapshot(s.snapshotId()));
        TupleDomain<ArcherColumnHandle> dataColumnPredicate = table.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        if (!dataColumnPredicate.isAll()) {
            overwriteFiles.conflictDetectionFilter(toArcherExpression(dataColumnPredicate));
        }
        IsolationLevel isolationLevel = IsolationLevel.fromName(archerTable.properties().getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT));
        if (isolationLevel == IsolationLevel.SERIALIZABLE) {
            overwriteFiles.validateNoConflictingData();
        }

        if (runUpdateValidations) {
            // Ensure a row that is updated by this commit was not deleted by a separate commit
            overwriteFiles.validateNoConflictingDeletes();
        }

        Set<VersionedPath> versionedPathSet = new HashSet<>();

        for (CommitTaskData task : commitTasks) {
            PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, task.getPartitionSpecJson());
            Type[] partitionColumnTypes = partitionSpec.fields().stream()
                    .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                    .toArray(Type[]::new);
            switch (task.getContent()) {
                case DATA_DELTA:
                    VersionedPath versionedPath = task.getReferencedDataFile().orElseThrow().toArcher();
                    if (versionedPathSet.contains(versionedPath)) {
                        throw new TrinoException(ARCHER_INTERNAL_ERROR, format("Duplicate delta file for data file: %s", versionedPath));
                    }
                    versionedPathSet.add(versionedPath);
                    Deletion deletion = DeletionParser.fromJson(task.getDeletionJson().orElseThrow());

                    DataFiles.Builder toDeleteDataFileBuilder = DataFiles.builder(partitionSpec)
                            .withPath(versionedPath.path().toString())
                            .withVersion(versionedPath.version())
                            .withFileName("")
                            .withFormat(FileFormat.PARQUET)
                            .withFileSizeInBytes(0)
                            .withRecordCount(0);
                    // Use partitionSpec of referenced DataFile
                    if (!partitionSpec.fields().isEmpty()) {
                        String partitionDataJson = task.getPartitionDataJson()
                                .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                        toDeleteDataFileBuilder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                    }
                    overwriteFiles.deleteFile(toDeleteDataFileBuilder.build());

                    long recordCount = task.getMetrics().recordCount();
                    if (deletion.deletedRecordCount() > recordCount) {
                        throw new TrinoException(ARCHER_INTERNAL_ERROR, format("Deleted record count is larger than file record count: %s", versionedPath));
                    }

                    int newVersion = newDataFileVersion(versionedPath.version());
                    GenericPartialFile.Builder partialFileBuilder =
                            new GenericPartialFile.Builder(versionedPath, newVersion)
                                    .withDeletion(deletion);
                    overwriteFiles.addPartialUpdateDataFile(partialFileBuilder.Build());
                    break;
                case DATA:
                    DataFiles.Builder dataFileBuilder = DataFiles.builder(archerTable.spec())
                            .withPath(task.getSegmentPath())
                            .withFileName(task.getFileName())
                            .withFileSizeInBytes(task.getFileSizeInBytes())
                            .withLastModifiedTime(task.getLastModifiedTime())
                            .withFormat(task.getFileFormat().toArcher())
                            .withMetrics(task.getMetrics().metrics());

                    // inverted index
                    dataFileBuilder.withInvertedIndex(invertedIndex);
                    task.getInvertedIndexFilesJson().map(InvertedIndexFilesParser::fromJson).ifPresent(dataFileBuilder::withInvertedIndexFiles);
                    // deletion
                    task.getDeletionJson().map(DeletionParser::fromJson).ifPresent(dataFileBuilder::withDeletion);

                    // Use current Table partitionSpec
                    if (!archerTable.spec().fields().isEmpty()) {
                        String partitionDataJson = task.getPartitionDataJson()
                                .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                        dataFileBuilder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                    }
                    overwriteFiles.addFile(dataFileBuilder.build());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported task content: " + task.getContent());
            }
        }

        try {
            commit(overwriteFiles, session);
            transaction.commitTransaction();
        }
        catch (ValidationException e) {
            throw new TrinoException(ARCHER_COMMIT_ERROR, "Failed to commit Archer update to table: " + table.getSchemaTableName(), e);
        }
        transaction = null;
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        catalog.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        catalog.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        catalog.setViewPrincipal(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_VIEW)
                .map(TableInfo::tableName)
                .toList();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.getViews(session, schemaName);
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getView(session, viewName).isPresent();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getView(session, viewName);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArcherTableHandle handle = (ArcherTableHandle) tableHandle;

        Table archerTable = catalog.loadTable(session, handle.getSchemaTableName());

        DeleteFiles deleteFiles = archerTable.newDelete()
                .deleteFromRowFilter(toArcherExpression(handle.getEnforcedPredicate()));
        commit(deleteFiles, session);

        Map<String, String> summary = archerTable.currentSnapshot().summary();
        String deletedRowsStr = summary.get(DELETED_RECORDS_PROP);
        if (deletedRowsStr == null) {
            // TODO Archer should guarantee this is always present (https://github.com/apache/iceberg/issues/4647)
            return OptionalLong.empty();
        }
        long deletedRecords = Long.parseLong(deletedRowsStr);
        return OptionalLong.of(deletedRecords);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArcherTableHandle table = checkValidTableHandle(tableHandle);
        Table archerTable = catalog.loadTable(session, table.getSchemaTableName());
        DeleteFiles deleteFiles = archerTable.newDelete()
                .deleteFromRowFilter(alwaysTrue());
        commit(deleteFiles, session);
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        ArcherTableHandle table = (ArcherTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        if (!table.getUnenforcedPredicate().isAll()) {
            return Optional.empty();
        }

        table = new ArcherTableHandle(
                table.getCatalog(),
                table.getSchemaName(),
                table.getTableName(),
                table.getTableType(),
                table.getSnapshotId(),
                table.getTableSchemaJson(),
                table.getPartitionSpecJson(),
                table.getInvertedIndexJson(),
                table.getFormatVersion(),
                table.getUnenforcedPredicate(), // known to be ALL
                table.getEnforcedPredicate(),
                table.getInvertedIndexQueryJson(),
                OptionalLong.of(limit),
                table.getProjectedColumns(),
                table.getNameMappingJson(),
                table.getTableLocation(),
                table.getStorageProperties(),
                table.getSchema(),
                table.getInvertedIndex().orElse(null),
                table.getInvertedIndexQuery().orElse(null),
                table.getRetryMode(),
                table.getUpdatedColumns(),
                table.isRecordScannedFiles(),
                table.getMaxScannedFileSize(),
                table.getPartitionSpecId(),
                table.isRefreshPartition(),
                table.getInvertedIndexId(),
                table.isRefreshInvertedIndex(),
                table.getConstraintColumns());

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        ArcherTableHandle table = (ArcherTableHandle) handle;
        ConstraintExtractor.ExtractionResult extractionResult = extractTupleDomain(constraint, table.getInvertedIndex());
        TupleDomain<ArcherColumnHandle> predicate = extractionResult.tupleDomain().transformKeys(ArcherColumnHandle.class::cast);
        Optional<InvertedIndexQuery> query = extractionResult.query();

        if (predicate.isAll() && constraint.getPredicateColumns().isEmpty() && query.isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ArcherColumnHandle> newEnforcedConstraint;
        TupleDomain<ArcherColumnHandle> newUnenforcedConstraint;
        TupleDomain<ArcherColumnHandle> remainingConstraint;

        Optional<String> newInvertedIndexQueryJson;
        InvertedIndexQuery newInvertedIndexQuery;
        if (query.isPresent()) {
            Optional<InvertedIndexQuery> old = table.getInvertedIndexQuery();
            if (old.isPresent()) {
                UserInputAst userInputAst = andUserInputAstList(ImmutableList.of(old.get().userInputAst(), query.get().userInputAst()));
                newInvertedIndexQuery = new InvertedIndexQuery(userInputAst, true);
            }
            else {
                newInvertedIndexQuery = query.get();
            }
            newInvertedIndexQueryJson = Optional.of(InvertedIndexQueryParser.toJson(newInvertedIndexQuery));
        }
        else {
            newInvertedIndexQueryJson = table.getInvertedIndexQueryJson();
            newInvertedIndexQuery = table.getInvertedIndexQuery().orElse(null);
        }

        if (predicate.isNone()) {
            // Engine does not pass none Constraint.summary. It can become none when combined with the expression and connector's domain knowledge.
            newEnforcedConstraint = TupleDomain.none();
            newUnenforcedConstraint = TupleDomain.all();
            remainingConstraint = TupleDomain.all();
        }
        else {
            Table archerTable = catalog.loadTable(session, table.getSchemaTableName());

            Set<Integer> partitionSpecIds = table.getSnapshotId().map(
                            snapshot -> archerTable.snapshot(snapshot).allManifests(archerTable.io()).stream()
                                    .map(ManifestFile::partitionSpecId)
                                    .collect(toImmutableSet()))
                    // No snapshot, so no data. This case doesn't matter.
                    .orElseGet(() -> ImmutableSet.copyOf(archerTable.specs().keySet()));

            Map<ArcherColumnHandle, Domain> unsupported = new LinkedHashMap<>();
            Map<ArcherColumnHandle, Domain> newEnforced = new LinkedHashMap<>();
            Map<ArcherColumnHandle, Domain> newUnenforced = new LinkedHashMap<>();
            Map<ArcherColumnHandle, Domain> domains = predicate.getDomains().orElseThrow(() -> new VerifyException("No domains"));
            domains.forEach((columnHandle, domain) -> {
                if (!isConvertableToArcherExpression(domain)) {
                    unsupported.put(columnHandle, domain);
                }
                else if (canEnforceColumnConstraintInSpecs(typeManager.getTypeOperators(), archerTable, partitionSpecIds, columnHandle, domain)) {
                    newEnforced.put(columnHandle, domain);
                }
                else if (isMetadataColumnId(columnHandle.getId())) {
                    if (columnHandle.isPathColumn() || columnHandle.isFileModifiedTimeColumn()) {
                        newEnforced.put(columnHandle, domain);
                    }
                    else {
                        unsupported.put(columnHandle, domain);
                    }
                }
                else {
                    newUnenforced.put(columnHandle, domain);
                }
            });

            newEnforcedConstraint = TupleDomain.withColumnDomains(newEnforced).intersect(table.getEnforcedPredicate());
            newUnenforcedConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(table.getUnenforcedPredicate());
            remainingConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(TupleDomain.withColumnDomains(unsupported));
        }

        Set<ArcherColumnHandle> newConstraintColumns = Streams.concat(
                        table.getConstraintColumns().stream(),
                        constraint.getPredicateColumns().orElseGet(ImmutableSet::of).stream()
                                .map(columnHandle -> (ArcherColumnHandle) columnHandle))
                .collect(toImmutableSet());

        if (newEnforcedConstraint.equals(table.getEnforcedPredicate())
                && newUnenforcedConstraint.equals(table.getUnenforcedPredicate())
                && newConstraintColumns.equals(table.getConstraintColumns())
                && newInvertedIndexQueryJson.equals(table.getInvertedIndexQueryJson())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                new ArcherTableHandle(
                        table.getCatalog(),
                        table.getSchemaName(),
                        table.getTableName(),
                        table.getTableType(),
                        table.getSnapshotId(),
                        table.getTableSchemaJson(),
                        table.getPartitionSpecJson(),
                        table.getInvertedIndexJson(),
                        table.getFormatVersion(),
                        newUnenforcedConstraint,
                        newEnforcedConstraint,
                        newInvertedIndexQueryJson,
                        table.getLimit(),
                        table.getProjectedColumns(),
                        table.getNameMappingJson(),
                        table.getTableLocation(),
                        table.getStorageProperties(),
                        table.getSchema(),
                        table.getInvertedIndex().orElse(null),
                        newInvertedIndexQuery,
                        table.getRetryMode(),
                        table.getUpdatedColumns(),
                        table.isRecordScannedFiles(),
                        table.getMaxScannedFileSize(),
                        table.getPartitionSpecId(),
                        table.isRefreshPartition(),
                        table.getInvertedIndexId(),
                        table.isRefreshInvertedIndex(),
                        newConstraintColumns),
                remainingConstraint.transformKeys(ColumnHandle.class::cast),
                extractionResult.remainingExpression(),
                false));
    }

    private static Set<Integer> identityPartitionColumnsInAllSpecs(Table table)
    {
        // Extract identity partition column source ids common to ALL specs
        return table.spec().fields().stream()
                .filter(field -> field.transform().isIdentity())
                .filter(field -> table.specs().values().stream().allMatch(spec -> spec.fields().contains(field)))
                .map(PartitionField::sourceId)
                .collect(toImmutableSet());
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }

        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

        ArcherTableHandle archerTableHandle = (ArcherTableHandle) handle;

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<ArcherColumnHandle> projectedColumns = assignments.values().stream()
                    .map(ArcherColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (archerTableHandle.getProjectedColumns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((ArcherColumnHandle) assignment.getValue()).getType()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    archerTableHandle.withProjectedColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<ArcherColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            ArcherColumnHandle baseColumnHandle = (ArcherColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            ArcherColumnHandle projectedColumnHandle = createProjectedColumnHandle(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
            String projectedColumnName = projectedColumnHandle.getQualifiedName();

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.putIfAbsent(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            projectedColumnsBuilder.add(projectedColumnHandle);
        }

        // Modify projections to refer to new variables
        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.buildOrThrow();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        List<Assignment> outputAssignments = ImmutableList.copyOf(newAssignments.values());
        return Optional.of(new ProjectionApplicationResult<>(
                archerTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private static ArcherColumnHandle createProjectedColumnHandle(ArcherColumnHandle column, List<Integer> indices, io.trino.spi.type.Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return column;
        }
        ImmutableList.Builder<Integer> fullPath = ImmutableList.builder();
        fullPath.addAll(column.getPath());

        ColumnIdentity projectedColumnIdentity = column.getColumnIdentity();
        for (int index : indices) {
            // Position based lookup, not FieldId based
            projectedColumnIdentity = projectedColumnIdentity.getChildren().get(index);
            fullPath.add(projectedColumnIdentity.getId());
        }

        return new ArcherColumnHandle(
                column.getBaseColumnIdentity(),
                column.getBaseType(),
                fullPath.build(),
                projectedColumnType,
                column.isNullable(),
                Optional.empty());
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        catalog.setTablePrincipal(session, tableName, principal);
    }

    private Optional<Long> getCurrentSnapshotId(Table table)
    {
        return Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId);
    }

    Table getArcherTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return catalog.loadTable(session, schemaTableName);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        catalog.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropMaterializedView(session, viewName);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode, RefreshType refreshType)
    {
        checkState(fromSnapshotForRefresh.isEmpty(), "From Snapshot must be empty at the start of MV refresh operation.");
        ArcherTableHandle table = (ArcherTableHandle) tableHandle;
        Table archerTable = catalog.loadTable(session, table.getSchemaTableName());
        beginTransaction(archerTable);

        Optional<String> dependencies = Optional.ofNullable(archerTable.currentSnapshot())
                .map(Snapshot::summary)
                .map(summary -> summary.get(DEPENDS_ON_TABLES));

        boolean shouldUseIncremental = isIncrementalRefreshEnabled(session)
                && refreshType == RefreshType.INCREMENTAL
                // there is a single source table
                && sourceTableHandles.size() == 1
                // and source table is an Archer table
                && getOnlyElement(sourceTableHandles) instanceof ArcherTableHandle handle
                // and source table is from the same catalog
                && handle.getCatalog().equals(trinoCatalogHandle)
                // and the source table's fromSnapshot is available in the MV snapshot summary
                && dependencies.isPresent() && !dependencies.get().equals(UNKNOWN_SNAPSHOT_TOKEN);

        if (shouldUseIncremental) {
            Map<String, String> sourceTableToSnapshot = MAP_SPLITTER.split(dependencies.get());
            checkState(sourceTableToSnapshot.size() == 1, "Expected %s to contain only single source table in snapshot summary", sourceTableToSnapshot);
            Map.Entry<String, String> sourceTable = getOnlyElement(sourceTableToSnapshot.entrySet());
            String[] schemaTable = sourceTable.getKey().split("\\.");
            ArcherTableHandle handle = (ArcherTableHandle) getOnlyElement(sourceTableHandles);
            SchemaTableName sourceSchemaTable = new SchemaTableName(schemaTable[0], schemaTable[1]);
            checkState(sourceSchemaTable.equals(handle.getSchemaTableName()), "Source table name %s doesn't match handle table name %s", sourceSchemaTable, handle.getSchemaTableName());
            fromSnapshotForRefresh = Optional.of(Long.parseLong(sourceTable.getValue()));
        }

        return newWritableTableHandle(table.getSchemaTableName(), archerTable, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles,
            List<String> sourceTableFunctions)
    {
        ArcherWritableTableHandle table = (ArcherWritableTableHandle) insertHandle;

        Table archerTable = transaction.table();
        InvertedIndex invertedIndex = archerTable.invertedIndex();
        boolean isFullRefresh = fromSnapshotForRefresh.isEmpty();
        if (isFullRefresh) {
            // delete before insert .. simulating overwrite
            log.info("Performing full MV refresh for storage table: %s", table.getName());
            transaction.newDelete()
                    .deleteFromRowFilter(Expressions.alwaysTrue())
                    .commit();
        }
        else {
            log.info("Performing incremental MV refresh for storage table: %s", table.getName());
        }

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = archerTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        archerTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(archerTable.spec())
                    .withPath(task.getSegmentPath())
                    .withFileName(task.getFileName())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withLastModifiedTime(task.getLastModifiedTime())
                    .withFormat(table.getFileFormat().toArcher())
                    .withMetrics(task.getMetrics().metrics());

            // inverted index
            builder.withInvertedIndex(invertedIndex);
            task.getInvertedIndexFilesJson().map(InvertedIndexFilesParser::fromJson).ifPresent(builder::withInvertedIndexFiles);
            // deletion
            task.getDeletionJson().map(DeletionParser::fromJson).ifPresent(builder::withDeletion);

            if (!archerTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            DataFile dataFile = builder.build();
            appendFiles.appendFile(dataFile);
        }

        String dependencies = sourceTableHandles.stream()
                .map(handle -> (ArcherTableHandle) handle)
                .map(handle -> handle.getSchemaTableName() + "=" + handle.getSnapshotId().map(Object.class::cast).orElse(""))
                .distinct()
                .collect(joining(","));

        // Update the 'dependsOnTables' property that tracks tables on which the materialized view depends and the corresponding snapshot ids of the tables
        appendFiles.set(DEPENDS_ON_TABLES, dependencies);
        appendFiles.set(DEPENDS_ON_TABLE_FUNCTIONS, Boolean.toString(!sourceTableFunctions.isEmpty()));
        appendFiles.set(TRINO_QUERY_START_TIME, session.getStart().toString());
        appendFiles.commit();

        transaction.commitTransaction();
        transaction = null;
        fromSnapshotForRefresh = Optional.empty();
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getSegmentPath)
                .collect(toImmutableList())));
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_MATERIALIZED_VIEW)
                .map(TableInfo::tableName)
                .toList();
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new HashMap<>();
        for (SchemaTableName name : listMaterializedViews(session, schemaName)) {
            try {
                getMaterializedView(session, name).ifPresent(view -> materializedViews.put(name, view));
            }
            catch (RuntimeException e) {
                // Materialized view can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                log.warn(e, "Failed to access metadata of materialized view %s during listing", name);
            }
        }
        return materializedViews;
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getMaterializedView(session, viewName);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        return catalog.getMaterializedViewProperties(session, viewName, definition);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // TODO (https://github.com/trinodb/trino/issues/9594) support rename across schemas
        if (!source.getSchemaName().equals(target.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Materialized View rename across schemas is not supported");
        }
        catalog.renameMaterializedView(session, source, target);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName)
    {
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = getMaterializedView(session, materializedViewName);
        if (materializedViewDefinition.isEmpty()) {
            // View not found, might have been concurrently deleted
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        SchemaTableName storageTableName = materializedViewDefinition.get().getStorageTable()
                .map(CatalogSchemaTableName::getSchemaTableName)
                .orElseThrow(() -> new IllegalStateException("Storage table missing in definition of materialized view " + materializedViewName));

        Table archerTable = catalog.loadTable(session, storageTableName);
        Optional<Snapshot> currentSnapshot = Optional.ofNullable(archerTable.currentSnapshot());
        String dependsOnTables = currentSnapshot
                .map(snapshot -> snapshot.summary().getOrDefault(DEPENDS_ON_TABLES, ""))
                .orElse("");
        boolean dependsOnTableFunctions = currentSnapshot
                .map(snapshot -> Boolean.valueOf(snapshot.summary().getOrDefault(DEPENDS_ON_TABLE_FUNCTIONS, "false")))
                .orElse(false);

        Optional<Instant> refreshTime = currentSnapshot.map(snapshot -> snapshot.summary().get(TRINO_QUERY_START_TIME))
                .map(Instant::parse)
                .or(() -> currentSnapshot.map(snapshot -> Instant.ofEpochMilli(snapshot.timestampMillis())));

        if (dependsOnTableFunctions) {
            // It can't be determined whether a value returned by table function is STALE or not
            return new MaterializedViewFreshness(UNKNOWN, refreshTime);
        }

        if (dependsOnTables.isEmpty()) {
            // Information missing. While it's "unknown" whether storage is stale, we return "stale".
            // Normally dependsOnTables may be missing only when there was no refresh yet.
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        boolean hasUnknownTables = false;
        boolean hasStaleArcherTables = false;
        Optional<Long> firstTableChange = Optional.of(Long.MAX_VALUE);

        Iterable<String> tableToSnapshotIds = Splitter.on(',').split(dependsOnTables);
        for (String entry : tableToSnapshotIds) {
            if (entry.equals(UNKNOWN_SNAPSHOT_TOKEN)) {
                // This is a "federated" materialized view (spanning across connectors). Trust user's choice and assume "fresh or fresh enough".
                hasUnknownTables = true;
                firstTableChange = Optional.empty();
                continue;
            }
            List<String> keyValue = Splitter.on("=").splitToList(entry);
            if (keyValue.size() != 2) {
                throw new TrinoException(ARCHER_INVALID_METADATA, format("Invalid entry in '%s' property: %s'", DEPENDS_ON_TABLES, entry));
            }
            String tableName = keyValue.get(0);
            String value = keyValue.get(1);
            List<String> strings = Splitter.on(".").splitToList(tableName);
            if (strings.size() == 3) {
                strings = strings.subList(1, 3);
            }
            else if (strings.size() != 2) {
                throw new TrinoException(ARCHER_INVALID_METADATA, format("Invalid table name in '%s' property: %s'", DEPENDS_ON_TABLES, strings));
            }
            String schema = strings.get(0);
            String name = strings.get(1);
            SchemaTableName schemaTableName = new SchemaTableName(schema, name);
            ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());

            if (tableHandle == null || tableHandle instanceof CorruptedArcherTableHandle) {
                // Base table is gone or table is corrupted
                return new MaterializedViewFreshness(STALE, Optional.empty());
            }
            Optional<Long> snapshotAtRefresh;
            if (value.isEmpty()) {
                snapshotAtRefresh = Optional.empty();
            }
            else {
                snapshotAtRefresh = Optional.of(Long.parseLong(value));
            }
            switch (getTableChangeInfo(session, (ArcherTableHandle) tableHandle, snapshotAtRefresh)) {
                case NoTableChange() -> {
                    // Fresh
                }
                case FirstChangeSnapshot(Snapshot snapshot) -> {
                    hasStaleArcherTables = true;
                    firstTableChange = firstTableChange
                            .map(epochMilli -> Math.min(epochMilli, snapshot.timestampMillis()));
                }
                case UnknownTableChange() -> {
                    hasStaleArcherTables = true;
                    firstTableChange = Optional.empty();
                }
            }
        }

        Optional<Instant> lastFreshTime = firstTableChange
                .map(Instant::ofEpochMilli)
                .or(() -> refreshTime);
        if (hasStaleArcherTables) {
            return new MaterializedViewFreshness(STALE, lastFreshTime);
        }
        if (hasUnknownTables) {
            return new MaterializedViewFreshness(UNKNOWN, lastFreshTime);
        }
        return new MaterializedViewFreshness(FRESH, Optional.empty());
    }

    private TableChangeInfo getTableChangeInfo(ConnectorSession session, ArcherTableHandle table, Optional<Long> snapshotAtRefresh)
    {
        Table archerTable = catalog.loadTable(session, table.getSchemaTableName());
        Snapshot currentSnapshot = archerTable.currentSnapshot();

        if (snapshotAtRefresh.isEmpty()) {
            // Table had no snapshot at refresh time.
            if (currentSnapshot == null) {
                return new NoTableChange();
            }
            return firstSnapshot(archerTable)
                    .<TableChangeInfo>map(FirstChangeSnapshot::new)
                    .orElse(new UnknownTableChange());
        }

        if (snapshotAtRefresh.get() == currentSnapshot.snapshotId()) {
            return new NoTableChange();
        }
        return firstSnapshotAfter(archerTable, snapshotAtRefresh.get())
                .<TableChangeInfo>map(FirstChangeSnapshot::new)
                .orElse(new UnknownTableChange());
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        catalog.updateColumnComment(session, ((ArcherTableHandle) tableHandle).getSchemaTableName(), ((ArcherColumnHandle) column).getColumnIdentity(), comment);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<String> targetCatalogName = getHiveCatalogName(session);
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        return catalog.redirectTable(session, tableName, targetCatalogName.get());
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        return WriterScalingOptions.ENABLED;
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return WriterScalingOptions.ENABLED;
    }

    public Optional<Long> getIncrementalRefreshFromSnapshot()
    {
        return fromSnapshotForRefresh;
    }

    public void disableIncrementalRefresh()
    {
        fromSnapshotForRefresh = Optional.empty();
    }

    private void cleanExtraOutputFiles(ConnectorSession session, Set<String> locations, Set<String> writtenFiles)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        for (String location : locations) {
            cleanExtraOutputFiles(fileSystem, session.getQueryId(), Location.of(location), writtenFiles);
        }
    }

    private void beginTransaction(Table archerTable)
    {
        verify(transaction == null, "transaction already set");
        transaction = archerTable.newTransaction();
    }

    public static void validateNotPartitionedByNestedField(Schema schema, PartitionSpec partitionSpec)
    {
        Map<Integer, Integer> indexParents = indexParents(schema.asStruct());
        for (PartitionField field : partitionSpec.fields()) {
            if (indexParents.containsKey(field.sourceId())) {
                throw new TrinoException(NOT_SUPPORTED, "Partitioning by nested field is unsupported: " + field.name());
            }
        }
    }

    public static void validateNotInvertedIndexedByNestedField(Schema schema, InvertedIndex invertedIndex)
    {
        Map<Integer, Integer> indexParents = indexParents(schema.asStruct());
        for (InvertedIndexField field : invertedIndex.fields()) {
            if (indexParents.containsKey(field.sourceId())) {
                throw new TrinoException(NOT_SUPPORTED, "Inverted indexed by nested field is unsupported: " + field.name());
            }
        }
    }

    private static ArcherTableHandle checkValidTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (tableHandle instanceof CorruptedArcherTableHandle corruptedTableHandle) {
            throw corruptedTableHandle.createException();
        }
        return ((ArcherTableHandle) tableHandle);
    }

    private sealed interface TableChangeInfo
            permits NoTableChange, FirstChangeSnapshot, UnknownTableChange {}

    private record NoTableChange()
            implements TableChangeInfo {}

    private record FirstChangeSnapshot(Snapshot snapshot)
            implements TableChangeInfo
    {
        FirstChangeSnapshot
        {
            requireNonNull(snapshot, "snapshot is null");
        }
    }

    private record UnknownTableChange()
            implements TableChangeInfo {}
}
