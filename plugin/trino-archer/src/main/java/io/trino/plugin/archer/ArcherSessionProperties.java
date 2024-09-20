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
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class ArcherSessionProperties
        implements SessionPropertiesProvider
{
    public static final String SPLIT_SIZE = "experimental_split_size";
    private static final String SPLIT_MODE = "split_mode";
    private static final String COMPRESSION_CODEC = "compression_codec";
    private static final String USE_FILE_SIZE_FROM_METADATA = "use_file_size_from_metadata";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_WRITER_BATCH_SIZE = "parquet_writer_batch_size";
    private static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    private static final String STATISTICS_ENABLED = "statistics_enabled";
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";
    private static final String TARGET_MAX_FILE_SIZE = "target_max_file_size";
    private static final String HIVE_CATALOG_NAME = "hive_catalog_name";
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    public static final String EXPIRE_SNAPSHOTS_MIN_RETENTION = "expire_snapshots_min_retention";
    public static final String REMOVE_ORPHAN_FILES_MIN_RETENTION = "remove_orphan_files_min_retention";
    private static final String INCREMENTAL_REFRESH_ENABLED = "incremental_refresh_enabled";
    private static final String LOCAL_CACHE_ENABLED = "local_cache_enabled";
    private static final String CACHE_NODE_COUNT = "cache_node_count";
    public static final String DELETE_DATA_AFTER_DROP_TABLE_ENABLED = "delete_data_after_drop_table_enabled";
    public static final String PARTITIONED_BUCKETS_PER_NODE = "partitioned_buckets_per_node";
    public static final String OPTIMIZE_DYNAMIC_REPARTITIONING = "optimize_dynamic_repartitioning";
    public static final String FORCE_ENGINE_REPARTITIONING = "force_engine_repartitioning";
    public static final String INVERTED_INDEX_WRITER_BUFFER_SIZE = "inverted_index_writer_buffer_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ArcherSessionProperties(
            ArcherConfig archerConfig,
            ParquetReaderConfig parquetReaderConfig,
            ParquetWriterConfig parquetWriterConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(dataSizeProperty(
                        SPLIT_SIZE,
                        "Target split size",
                        // Note: this is null by default & hidden, currently mainly for tests.
                        // See https://github.com/trinodb/trino/issues/9018#issuecomment-1752929193 for further discussion.
                        null,
                        true))
                .add(enumProperty(
                        SPLIT_MODE,
                        "How to split data file (auto, always, never)",
                        SplitMode.class,
                        archerConfig.getSplitMode(),
                        false))
                .add(enumProperty(
                        COMPRESSION_CODEC,
                        "Compression codec to use when writing files",
                        HiveCompressionCodec.class,
                        archerConfig.getCompressionCodec(),
                        false))
                .add(booleanProperty(
                        USE_FILE_SIZE_FROM_METADATA,
                        "Use file size stored in Archer metadata",
                        archerConfig.isUseFileSizeFromMetadata(),
                        false))
                .add(booleanProperty(
                        LOCAL_CACHE_ENABLED,
                        "Is local file cache enabled",
                        archerConfig.isLocalCacheEnabled(),
                        false))
                .add(integerProperty(
                        CACHE_NODE_COUNT,
                        "Cache node count when select nodes",
                        archerConfig.getCacheNodeCount(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        parquetReaderConfig.getMaxReadBlockSize(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetWriterConfig.getBlockSize(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetWriterConfig.getPageSize(),
                        false))
                .add(integerProperty(
                        PARQUET_WRITER_BATCH_SIZE,
                        "Parquet: Maximum number of rows passed to the writer in each batch",
                        parquetWriterConfig.getBatchSize(),
                        false))
                .add(durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters during split generation",
                        archerConfig.getDynamicFilteringWaitTimeout(),
                        false))
                .add(booleanProperty(
                        STATISTICS_ENABLED,
                        "Expose table statistics",
                        archerConfig.isTableStatisticsEnabled(),
                        false))
                .add(booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Read only required fields from a struct",
                        archerConfig.isProjectionPushdownEnabled(),
                        false))
                .add(dataSizeProperty(
                        TARGET_MAX_FILE_SIZE,
                        "Target maximum size of written files; the actual size may be larger",
                        archerConfig.getTargetMaxFileSize(),
                        false))
                .add(stringProperty(
                        HIVE_CATALOG_NAME,
                        "Catalog to redirect to when a Hive table is referenced",
                        archerConfig.getHiveCatalogName().orElse(null),
                        // Session-level redirections configuration does not work well with views, as view body is analyzed in context
                        // of a session with properties stripped off. Thus, this property is more of a test-only, or at most POC usefulness.
                        true))
                .add(doubleProperty(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight",
                        archerConfig.getMinimumAssignedSplitWeight(),
                        false))
                .add(durationProperty(
                        EXPIRE_SNAPSHOTS_MIN_RETENTION,
                        "Minimal retention period for expire_snapshot procedure",
                        archerConfig.getExpireSnapshotsMinRetention(),
                        false))
                .add(durationProperty(
                        REMOVE_ORPHAN_FILES_MIN_RETENTION,
                        "Minimal retention period for remove_orphan_files procedure",
                        archerConfig.getRemoveOrphanFilesMinRetention(),
                        false))
                .add(booleanProperty(
                        INCREMENTAL_REFRESH_ENABLED,
                        "Enable Incremental refresh for MVs backed by Archer tables, when possible.",
                        archerConfig.isIncrementalRefreshEnabled(),
                        false))
                .add(booleanProperty(
                        DELETE_DATA_AFTER_DROP_TABLE_ENABLED,
                        "Delete data after drop table",
                        archerConfig.isDeleteDataAfterDropTableEnabled(),
                        false))
                .add(integerProperty(
                        PARTITIONED_BUCKETS_PER_NODE,
                        "Partitioned buckets per node, should keep zero in merge query",
                        archerConfig.getPartitionedBucketsPerNode(),
                        false))
                .add(booleanProperty(
                        OPTIMIZE_DYNAMIC_REPARTITIONING,
                        "Dynamic repartitioning when optimize table",
                        archerConfig.getOptimizeDynamicRepartitioning(),
                        false))
                .add(booleanProperty(
                        FORCE_ENGINE_REPARTITIONING,
                        "Force let engine determine whether to repartition data or not even if not all partition transform is identity",
                        archerConfig.getForceEngineRepartitioning(),
                        false))
                .add(dataSizeProperty(
                        INVERTED_INDEX_WRITER_BUFFER_SIZE,
                        "Target inverted index writer buffer size",
                        archerConfig.getInvertedIndexWriterBufferSize(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Optional<DataSize> getSplitSize(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(SPLIT_SIZE, DataSize.class));
    }

    public static HiveCompressionCodec getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static boolean isUseFileSizeFromMetadata(ConnectorSession session)
    {
        return session.getProperty(USE_FILE_SIZE_FROM_METADATA, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static int getParquetWriterBatchSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BATCH_SIZE, Integer.class);
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static long getTargetMaxFileSize(ConnectorSession session)
    {
        return session.getProperty(TARGET_MAX_FILE_SIZE, DataSize.class).toBytes();
    }

    public static Optional<String> getHiveCatalogName(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(HIVE_CATALOG_NAME, String.class));
    }

    public static Duration getExpireSnapshotMinRetention(ConnectorSession session)
    {
        return session.getProperty(EXPIRE_SNAPSHOTS_MIN_RETENTION, Duration.class);
    }

    public static Duration getRemoveOrphanFilesMinRetention(ConnectorSession session)
    {
        return session.getProperty(REMOVE_ORPHAN_FILES_MIN_RETENTION, Duration.class);
    }

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }

    public static boolean isIncrementalRefreshEnabled(ConnectorSession session)
    {
        return session.getProperty(INCREMENTAL_REFRESH_ENABLED, Boolean.class);
    }

    public static boolean isLocalCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(LOCAL_CACHE_ENABLED, Boolean.class);
    }

    public static int getCacheNodeCount(ConnectorSession session)
    {
        return session.getProperty(CACHE_NODE_COUNT, Integer.class);
    }

    public static SplitMode getSplitMode(ConnectorSession session)
    {
        return session.getProperty(SPLIT_MODE, SplitMode.class);
    }

    public static boolean isDeleteDataAfterDropTableEnabled(ConnectorSession session)
    {
        return session.getProperty(DELETE_DATA_AFTER_DROP_TABLE_ENABLED, Boolean.class);
    }

    public static int getPartitionedBucketsPerNode(ConnectorSession session)
    {
        return session.getProperty(PARTITIONED_BUCKETS_PER_NODE, Integer.class);
    }

    public static boolean isOptimizeDynamicRepartitioning(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_DYNAMIC_REPARTITIONING, Boolean.class);
    }

    public static boolean isForceEngineRepartitioning(ConnectorSession session)
    {
        return session.getProperty(FORCE_ENGINE_REPARTITIONING, Boolean.class);
    }

    public static DataSize getInvertedIndexWriterBufferSize(ConnectorSession session)
    {
        return session.getProperty(INVERTED_INDEX_WRITER_BUFFER_SIZE, DataSize.class);
    }
}
