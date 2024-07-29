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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.trino.plugin.hive.HiveCompressionCodec;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.archer.ArcherFileFormat.PARQUET;
import static io.trino.plugin.archer.CatalogType.HIVE_METASTORE;
import static io.trino.plugin.archer.SplitMode.AUTO;
import static io.trino.plugin.hive.HiveCompressionCodec.ZSTD;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig("archer.allow-legacy-snapshot-syntax")
public class ArcherConfig
{
    public static final int FORMAT_VERSION_SUPPORT_MIN = 2;
    public static final int FORMAT_VERSION_SUPPORT_MAX = 2;
    public static final String EXPIRE_SNAPSHOTS_MIN_RETENTION = "archer.expire_snapshots.min-retention";
    public static final String REMOVE_ORPHAN_FILES_MIN_RETENTION = "archer.remove_orphan_files.min-retention";

    private HiveCompressionCodec compressionCodec = ZSTD;
    private boolean useFileSizeFromMetadata = true;
    private int maxPartitionsPerWriter = 100;
    private boolean uniqueTableLocation = true;
    private CatalogType catalogType = HIVE_METASTORE;
    private Duration dynamicFilteringWaitTimeout = new Duration(0, SECONDS);
    private boolean tableStatisticsEnabled = true;
    private boolean extendedStatisticsEnabled;
    private boolean projectionPushdownEnabled = true;
    private Optional<String> hiveCatalogName = Optional.empty();
    private int formatVersion = FORMAT_VERSION_SUPPORT_MIN;
    private Duration expireSnapshotsMinRetention = new Duration(7, DAYS);
    private Duration removeOrphanFilesMinRetention = new Duration(7, DAYS);
    private DataSize targetMaxFileSize = DataSize.of(1, GIGABYTE);
    // This is meant to protect users who are misusing schema locations (by
    // putting schemas in locations with extraneous files), so default to false
    // to avoid deleting those files if Trino is unable to check.
    private boolean deleteSchemaLocationsFallback;
    private double minimumAssignedSplitWeight = 0.02;
    private Optional<String> materializedViewsStorageSchema = Optional.empty();
    private boolean hideMaterializedViewStorageTable = true;
    private int splitManagerThreads = Runtime.getRuntime().availableProcessors() * 2;
    private boolean incrementalRefreshEnabled = true;

    private int maxMetadataVersions = 5;
    private boolean enableDeleteMetadataAfterCommit = true;
    private boolean localCacheEnabled = true;
    private int cacheNodeCount = 1;
    private ArcherFileFormat fileFormat = PARQUET;
    private SplitMode splitMode = AUTO;
    private boolean deleteDataAfterDropTableEnabled = true;
    private int partitionedBucketsPerNode;
    private boolean optimizeForceRepartitioning = true;
    private boolean forceEngineRepartitioning;
    private DataSize invertedIndexWriterBufferSize = DataSize.of(128, MEGABYTE);

    public CatalogType getCatalogType()
    {
        return catalogType;
    }

    @Config("archer.catalog.type")
    public ArcherConfig setCatalogType(CatalogType catalogType)
    {
        this.catalogType = catalogType;
        return this;
    }

    @NotNull
    public ArcherFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @Config("archer.file-format")
    public ArcherConfig setFileFormat(ArcherFileFormat fileFormat)
    {
        this.fileFormat = fileFormat;
        return this;
    }

    @NotNull
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @Config("archer.compression-codec")
    public ArcherConfig setCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        this.compressionCodec = compressionCodec;
        return this;
    }

    @Deprecated
    public boolean isUseFileSizeFromMetadata()
    {
        return useFileSizeFromMetadata;
    }

    /**
     * Some Archer writers populate incorrect file sizes in the metadata. When
     * this property is set to false, Trino ignores the stored values and fetches
     * them with a getFileStatus call. This means an additional call per split,
     * so it is recommended for a Trino admin to fix the metadata, rather than
     * relying on this property for too long.
     */
    @Deprecated
    @Config("archer.use-file-size-from-metadata")
    public ArcherConfig setUseFileSizeFromMetadata(boolean useFileSizeFromMetadata)
    {
        this.useFileSizeFromMetadata = useFileSizeFromMetadata;
        return this;
    }

    @Min(1)
    public int getMaxPartitionsPerWriter()
    {
        return maxPartitionsPerWriter;
    }

    @Config("archer.max-partitions-per-writer")
    @ConfigDescription("Maximum number of partitions per writer")
    public ArcherConfig setMaxPartitionsPerWriter(int maxPartitionsPerWriter)
    {
        this.maxPartitionsPerWriter = maxPartitionsPerWriter;
        return this;
    }

    public boolean isUniqueTableLocation()
    {
        return uniqueTableLocation;
    }

    @Config("archer.unique-table-location")
    @ConfigDescription("Use randomized, unique table locations")
    public ArcherConfig setUniqueTableLocation(boolean uniqueTableLocation)
    {
        this.uniqueTableLocation = uniqueTableLocation;
        return this;
    }

    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("archer.dynamic-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters during split generation")
    public ArcherConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    public boolean isTableStatisticsEnabled()
    {
        return tableStatisticsEnabled;
    }

    // In case of some queries / tables, retrieving table statistics from Archer
    // can take 20+ seconds. This config allows the user / operator the option
    // to opt out of retrieving table statistics in those cases to speed up query planning.
    @Config("archer.table-statistics-enabled")
    @ConfigDescription("Enable use of table statistics")
    public ArcherConfig setTableStatisticsEnabled(boolean tableStatisticsEnabled)
    {
        this.tableStatisticsEnabled = tableStatisticsEnabled;
        return this;
    }

    public boolean isProjectionPushdownEnabled()
    {
        return projectionPushdownEnabled;
    }

    @Config("archer.projection-pushdown-enabled")
    @ConfigDescription("Read only required fields from a struct")
    public ArcherConfig setProjectionPushdownEnabled(boolean projectionPushdownEnabled)
    {
        this.projectionPushdownEnabled = projectionPushdownEnabled;
        return this;
    }

    public Optional<String> getHiveCatalogName()
    {
        return hiveCatalogName;
    }

    @Config("archer.hive-catalog-name")
    @ConfigDescription("Catalog to redirect to when a Hive table is referenced")
    public ArcherConfig setHiveCatalogName(String hiveCatalogName)
    {
        this.hiveCatalogName = Optional.ofNullable(hiveCatalogName);
        return this;
    }

    @Min(FORMAT_VERSION_SUPPORT_MIN)
    @Max(FORMAT_VERSION_SUPPORT_MAX)
    public int getFormatVersion()
    {
        return formatVersion;
    }

    @Config("archer.format-version")
    @ConfigDescription("Default Archer table format version")
    public ArcherConfig setFormatVersion(int formatVersion)
    {
        this.formatVersion = formatVersion;
        return this;
    }

    @NotNull
    public Duration getExpireSnapshotsMinRetention()
    {
        return expireSnapshotsMinRetention;
    }

    @Config(EXPIRE_SNAPSHOTS_MIN_RETENTION)
    @ConfigDescription("Minimal retention period for expire_snapshot procedure")
    public ArcherConfig setExpireSnapshotsMinRetention(Duration expireSnapshotsMinRetention)
    {
        this.expireSnapshotsMinRetention = expireSnapshotsMinRetention;
        return this;
    }

    @NotNull
    public Duration getRemoveOrphanFilesMinRetention()
    {
        return removeOrphanFilesMinRetention;
    }

    @Config(REMOVE_ORPHAN_FILES_MIN_RETENTION)
    @ConfigDescription("Minimal retention period for remove_orphan_files procedure")
    public ArcherConfig setRemoveOrphanFilesMinRetention(Duration removeOrphanFilesMinRetention)
    {
        this.removeOrphanFilesMinRetention = removeOrphanFilesMinRetention;
        return this;
    }

    public DataSize getTargetMaxFileSize()
    {
        return targetMaxFileSize;
    }

    @Config("archer.target-max-file-size")
    @ConfigDescription("Target maximum raw data size of written files; the actual size may be larger")
    public ArcherConfig setTargetMaxFileSize(DataSize targetMaxFileSize)
    {
        this.targetMaxFileSize = targetMaxFileSize;
        return this;
    }

    public boolean isDeleteSchemaLocationsFallback()
    {
        return this.deleteSchemaLocationsFallback;
    }

    @Config("archer.delete-schema-locations-fallback")
    @ConfigDescription("Whether schema locations should be deleted when Trino can't determine whether they contain external files.")
    public ArcherConfig setDeleteSchemaLocationsFallback(boolean deleteSchemaLocationsFallback)
    {
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
        return this;
    }

    @Config("archer.minimum-assigned-split-weight")
    @ConfigDescription("Minimum weight that a split can be assigned")
    public ArcherConfig setMinimumAssignedSplitWeight(double minimumAssignedSplitWeight)
    {
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        return this;
    }

    @DecimalMax("1")
    @DecimalMin(value = "0", inclusive = false)
    public double getMinimumAssignedSplitWeight()
    {
        return minimumAssignedSplitWeight;
    }

    @NotNull
    public Optional<String> getMaterializedViewsStorageSchema()
    {
        return materializedViewsStorageSchema;
    }

    @Config("archer.materialized-views.storage-schema")
    @ConfigDescription("Schema for creating materialized views storage tables")
    public ArcherConfig setMaterializedViewsStorageSchema(String materializedViewsStorageSchema)
    {
        this.materializedViewsStorageSchema = Optional.ofNullable(materializedViewsStorageSchema);
        return this;
    }

    @Deprecated
    public boolean isHideMaterializedViewStorageTable()
    {
        return hideMaterializedViewStorageTable;
    }

    @Deprecated
    @Config("archer.materialized-views.hide-storage-table")
    @ConfigDescription("Hide materialized view storage tables in metastore")
    public ArcherConfig setHideMaterializedViewStorageTable(boolean hideMaterializedViewStorageTable)
    {
        this.hideMaterializedViewStorageTable = hideMaterializedViewStorageTable;
        return this;
    }

    @Min(0)
    public int getSplitManagerThreads()
    {
        return splitManagerThreads;
    }

    @Config("archer.split-manager-threads")
    @ConfigDescription("Number of threads to use for generating splits")
    public ArcherConfig setSplitManagerThreads(int splitManagerThreads)
    {
        this.splitManagerThreads = splitManagerThreads;
        return this;
    }

    public boolean isIncrementalRefreshEnabled()
    {
        return incrementalRefreshEnabled;
    }

    @Config("archer.incremental-refresh-enabled")
    @ConfigDescription("Enable Incremental refresh for MVs backed by Archer tables, when possible")
    public ArcherConfig setIncrementalRefreshEnabled(boolean incrementalRefreshEnabled)
    {
        this.incrementalRefreshEnabled = incrementalRefreshEnabled;
        return this;
    }

    @NotNull
    public int getMaxMetadataVersions()
    {
        return maxMetadataVersions;
    }

    @Config("archer.metadata.previous-versions-max")
    @ConfigDescription("Maximum number of previous version for metadata")
    public ArcherConfig setMaxMetadataVersions(int maxMetadataVersions)
    {
        this.maxMetadataVersions = maxMetadataVersions;
        return this;
    }

    @NotNull
    public boolean isEnableDeleteMetadataAfterCommit()
    {
        return enableDeleteMetadataAfterCommit;
    }

    @Config("archer.metadata.delete-after-commit.enabled")
    public ArcherConfig setEnableDeleteMetadataAfterCommit(boolean enableDeleteMetadataAfterCommit)
    {
        this.enableDeleteMetadataAfterCommit = enableDeleteMetadataAfterCommit;
        return this;
    }

    public boolean isLocalCacheEnabled()
    {
        return localCacheEnabled;
    }

    @Config("archer.local-cache-enabled")
    public ArcherConfig setLocalCacheEnabled(boolean localCacheEnabled)
    {
        this.localCacheEnabled = localCacheEnabled;
        return this;
    }

    @Min(1)
    public int getCacheNodeCount()
    {
        return cacheNodeCount;
    }

    @Config("archer.cache-node-count")
    public ArcherConfig setCacheNodeCount(int count)
    {
        this.cacheNodeCount = count;
        return this;
    }

    @NotNull
    public SplitMode getSplitMode()
    {
        return splitMode;
    }

    @Config("archer.split-mode")
    public ArcherConfig setSplitMode(SplitMode splitMode)
    {
        this.splitMode = splitMode;
        return this;
    }

    public boolean isDeleteDataAfterDropTableEnabled()
    {
        return deleteDataAfterDropTableEnabled;
    }

    @Config("archer.delete-data-after-drop-table-enabled")
    public ArcherConfig setDeleteDataAfterDropTableEnabled(boolean deleteDataAfterDropTableEnabled)
    {
        this.deleteDataAfterDropTableEnabled = deleteDataAfterDropTableEnabled;
        return this;
    }

    @Min(0)
    @Max(256)
    public int getPartitionedBucketsPerNode()
    {
        return partitionedBucketsPerNode;
    }

    @Config("archer.partitioned-buckets-per-node")
    public ArcherConfig setPartitionedBucketsPerNode(int partitionedBucketsPerNode)
    {
        this.partitionedBucketsPerNode = partitionedBucketsPerNode;
        return this;
    }

    public boolean getOptimizeForceRepartitioning()
    {
        return optimizeForceRepartitioning;
    }

    @Config("archer.optimize-force-repartitioning")
    public ArcherConfig setOptimizeForceRepartitioning(boolean optimizeForceRepartitioning)
    {
        this.optimizeForceRepartitioning = optimizeForceRepartitioning;
        return this;
    }

    public boolean getForceEngineRepartitioning()
    {
        return forceEngineRepartitioning;
    }

    @Config("archer.force-engine-repartitioning")
    public ArcherConfig setForceEngineRepartitioning(boolean forceEngineRepartitioning)
    {
        this.forceEngineRepartitioning = forceEngineRepartitioning;
        return this;
    }

    @MinDataSize("16MB")
    @MaxDataSize("4GB")
    public DataSize getInvertedIndexWriterBufferSize()
    {
        return invertedIndexWriterBufferSize;
    }

    @Config("archer.inverted-index-writer-buffer-size")
    public ArcherConfig setInvertedIndexWriterBufferSize(DataSize invertedIndexWriterBufferSize)
    {
        this.invertedIndexWriterBufferSize = invertedIndexWriterBufferSize;
        return this;
    }
}
