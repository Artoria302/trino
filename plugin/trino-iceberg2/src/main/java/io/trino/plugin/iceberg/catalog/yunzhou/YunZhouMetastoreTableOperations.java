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
package io.trino.plugin.iceberg.catalog.yunzhou;

import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.linkedin.coral.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static io.trino.plugin.iceberg.IcebergUtil.getLocationProvider;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.HIVE_ICEBERG_METASTORE_STORAGE_FORMAT;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.ICEBERG_METASTORE_STORAGE_FORMAT;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.toHiveColumns;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.ENGINE_HIVE_ENABLED;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;

public class YunZhouMetastoreTableOperations
        implements IcebergTableOperations
{
    enum CommitStatus
    {
        FAILURE,
        SUCCESS,
        UNKNOWN
    }

    private static final Logger log = Logger.get(YunZhouMetastoreTableOperations.class);

    public static final String METADATA_FOLDER_NAME = "metadata";
    public static final String METADATA_ID_PROP = "metadata_id";
    public static final String PREVIOUS_METADATA_ID_PROP = "previous_metadata_id";

    public final ConnectorSession session;
    public final String database;
    public final String tableName;
    public final Optional<String> owner;
    public final Optional<String> location;
    public final FileIO fileIo;
    public final CachingHiveMetastore metastore;
    private final ThriftMetastore thriftMetastore;

    public TableMetadata currentMetadata;
    public String currentMetadataId;
    public boolean shouldRefresh = true;
    public int version = -1;

    public YunZhouMetastoreTableOperations(
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ThriftMetastore thriftMetastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        this.fileIo = requireNonNull(fileIo, "fileIo is null");
        this.session = requireNonNull(session, "session is null");
        this.database = requireNonNull(database, "database is null");
        this.tableName = requireNonNull(table, "table is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.location = requireNonNull(location, "location is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.thriftMetastore = requireNonNull(thriftMetastore, "thriftMetastore is null");
    }

    @Override
    public void initializeFromMetadata(TableMetadata tableMetadata)
    {
        checkState(currentMetadata == null, "already initialized");
        currentMetadata = tableMetadata;
        currentMetadataId = tableMetadata.metadataId();
        shouldRefresh = false;
        version = parseVersion(currentMetadataId);
    }

    private static int parseVersion(String metadataId)
    {
        int versionEnd = metadataId.indexOf('-', 0);
        try {
            return parseInt(metadataId.substring(0, versionEnd));
        }
        catch (NumberFormatException | IndexOutOfBoundsException e) {
            log.warn(e, "Unable to parse version from metadata Id: %s", metadataId);
            return -1;
        }
    }

    @Override
    public TableMetadata current()
    {
        if (shouldRefresh) {
            return refresh(false);
        }
        return currentMetadata;
    }

    @Override
    public TableMetadata refresh()
    {
        return refresh(true);
    }

    public TableMetadata refresh(boolean invalidateCaches)
    {
        if (location.isPresent()) {
            refreshFromMetadataId(null);
            return currentMetadata;
        }
        refreshFromMetadataId(getRefreshedId(invalidateCaches));
        return currentMetadata;
    }

    private void refreshFromMetadataId(String newMetadataId)
    {
        // use null-safe equality check because new tables have a null metadata location
        if (Objects.equals(currentMetadataId, newMetadataId)) {
            shouldRefresh = false;
            return;
        }

        AtomicReference<TableMetadata> newMetadata = new AtomicReference<>();
        Tasks.foreach(newMetadataId)
                .retry(20)
                .exponentialBackoff(100, 5000, 600000, 4.0)
                .stopRetryOn(org.apache.iceberg.exceptions.NotFoundException.class) // qualified name, as this is NOT the io.trino.spi.connector.NotFoundException
                .run(metadataId -> newMetadata.set(
                        YunZhouTableMetadataParser.read(this, metadataId, database, tableName)));

        String newUUID = newMetadata.get().uuid();
        if (currentMetadata != null) {
            checkState(newUUID == null || newUUID.equals(currentMetadata.uuid()),
                    "Table UUID does not match: current=%s != refreshed=%s", currentMetadata.uuid(), newUUID);
        }

        currentMetadata = newMetadata.get();
        currentMetadataId = newMetadataId;
        version = parseVersion(newMetadataId);
        shouldRefresh = false;
    }

    private String getRefreshedId(boolean invalidateCaches)
    {
        if (invalidateCaches) {
            metastore.invalidateTable(database, tableName);
        }

        boolean isMaterializedViewStorageTable = isMaterializedViewStorage(tableName);

        Table table;
        if (isMaterializedViewStorageTable) {
            table = getTable(database, tableNameFrom(tableName));
        }
        else {
            table = getTable();
        }

        if (!isMaterializedViewStorageTable && (isTrinoView(table) || isTrinoMaterializedView(table))) {
            // this is a Hive view or Trino/Presto view, or Trino materialized view, hence not a table
            // TODO table operations should not be constructed for views (remove exception-driven code path)
            throw new TableNotFoundException(getSchemaTableName());
        }
        if (!isMaterializedViewStorageTable && !isYunzhouIcebergTable(table)) {
            throw new UnknownTableTypeException(getSchemaTableName());
        }

        String metadataId = table.getParameters().get(METADATA_ID_PROP);
        if (metadataId == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_ID_PROP, getSchemaTableName()));
        }

        return metadataId;
    }

    private Table getTable()
    {
        return metastore.getTable(database, tableName)
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }

    protected Table getTable(String database, String tableName)
    {
        return metastore.getTable(database, tableName)
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }

    private SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(database, tableName);
    }

    @Override
    public void commit(@Nullable TableMetadata base, TableMetadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        // if the metadata is already out of date, reject it
        if (!Objects.equals(base, current())) {
            throw new CommitFailedException("Cannot commit: stale table metadata for %s", getSchemaTableName());
        }

        // if the metadata is not changed, return early
        if (Objects.equals(base, metadata)) {
            return;
        }

        if (base == null) {
            commitNewTable(metadata);
        }
        else {
            commitToExistingTable(base, metadata);
        }

        // Delete previous metadata
        deleteRemovedMetadataIDs(base, metadata);
        shouldRefresh = true;
    }

    /**
     * Deletes the oldest metadata Id from YunZhouMetastore if {@link TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED} is true.
     *
     * @param base table metadata on which previous versions were based
     * @param metadata new table metadata with updated previous versions
     */
    private void deleteRemovedMetadataIDs(TableMetadata base, TableMetadata metadata)
    {
        if (base == null) {
            return;
        }

        boolean deleteAfterCommit = metadata.propertyAsBoolean(
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

        // todo TableMetadata record metadataIds
        Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataEntry = Sets.newHashSet(base.previousFiles());
        removedPreviousMetadataEntry.removeAll(metadata.previousFiles());

        if (deleteAfterCommit) {
            Tasks.foreach(removedPreviousMetadataEntry)
                    .noRetry().suppressFailureWhenFinished()
                    .onFailure((previousMetadataId, exc) ->
                            log.warn("Delete failed for previous metadata Id %s : %s", previousMetadataId, exc.getCause()))
                    .run(previousMetadataEntry -> {
                        try {
                            String metaKey = previousMetadataEntry.metadataId();
                            boolean delete = metastore.deleteMetadata(metaKey);
                            long deleteCounts = metastore.deleteAllSnapshots(metaKey);
                            log.info("Deleted %d snapshots for table %s metadata %s", deleteCounts,
                                    tableName, previousMetadataEntry.metadataId());
                            if (!delete) {
                                log.error("Fail to cleanup metadata ID at %s", previousMetadataEntry.metadataId());
                                throw new RuntimeException("Fail to cleanup metadata ID at " + previousMetadataEntry.metadataId());
                            }
                        }
                        catch (Exception e) {
                            log.error("Fail to cleanup metadata ID at %s : ", previousMetadataEntry.metadataId(), e.getCause());
                            throw new RuntimeException(e.getMessage());
                        }
                    });
        }
    }

    private static boolean hiveEngineEnabled(TableMetadata metadata)
    {
        return metadata.propertyAsBoolean(ENGINE_HIVE_ENABLED, true);
    }

    private void commitNewTable(TableMetadata metadata)
    {
        String newMetadataId = writeNewMetadata(metadata, version + 1);
        boolean hiveEngineEnabled = hiveEngineEnabled(metadata);

        Table.Builder builder = Table.builder()
                .setDatabaseName(database)
                .setTableName(tableName)
                .setOwner(owner)
                // Table needs to be EXTERNAL, otherwise table rename in HMS would rename table directory and break table contents.
                .setTableType(EXTERNAL_TABLE.name())
                .setDataColumns(toHiveColumns(metadata.schema().columns()))
                .withStorage(storage -> storage.setLocation(metadata.location()))
                .withStorage(storage -> storage.setStorageFormat(hiveEngineEnabled ? HIVE_ICEBERG_METASTORE_STORAGE_FORMAT : ICEBERG_METASTORE_STORAGE_FORMAT))
                // This is a must-have property for the EXTERNAL_TABLE table type
                .setParameter("EXTERNAL", "TRUE")
                .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE)
                .setParameter(METADATA_ID_PROP, newMetadataId);
        String tableComment = metadata.properties().get(TABLE_COMMENT);
        if (tableComment != null) {
            builder.setParameter(TABLE_COMMENT, tableComment);
        }

        if (hiveEngineEnabled) {
            builder.setParameter(META_TABLE_STORAGE, "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
        }

        Table table = builder.build();

        PrincipalPrivileges privileges = owner.map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
        try {
            metastore.createTable(table, privileges);
        }
        catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
                throw new RuntimeException("Failed to acquire locks from metastore because the underlying metastore table 'HIVE_LOCKS' does not exist. This can occur when using an embedded metastore which does not support transactions. To fix this use an alternative metastore.", e);
            }
            CommitStatus commitStatus = checkCommitStatus(newMetadataId, metadata);
            // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
            switch (commitStatus) {
                case FAILURE:
                    throw e;
                case UNKNOWN:
                    throw new CommitStateUnknownException(e);
                default:
                    break;
            }
        }
    }

    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        String newMetadataId = writeNewMetadata(metadata, version + 1);

        long lockId = thriftMetastore.acquireTableExclusiveLock(
                new AcidTransactionOwner(session.getUser()),
                session.getQueryId(),
                database,
                tableName);
        try {
            Table currentTable = fromMetastoreApiTable(thriftMetastore.getTable(database, tableName)
                    .orElseThrow(() -> new TableNotFoundException(getSchemaTableName())));

            checkState(currentMetadataId != null, "No current metadata Id for existing table");
            String metadataId = currentTable.getParameters().get(METADATA_ID_PROP);
            if (!currentMetadataId.equals(metadataId)) {
                throw new CommitFailedException("Metadata Id [%s] is not same as table metadata Id [%s] for %s",
                        currentMetadataId, metadataId, getSchemaTableName());
            }

            Table table = Table.builder(currentTable)
                    .setDataColumns(toHiveColumns(metadata.schema().columns()))
                    .withStorage(storage -> storage.setLocation(metadata.location()))
                    .setParameter(METADATA_ID_PROP, newMetadataId)
                    .setParameter(PREVIOUS_METADATA_ID_PROP, currentMetadataId)
                    .build();

            // todo privileges should not be replaced for an alter
            PrincipalPrivileges privileges = table.getOwner().map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
            try {
                metastore.replaceTable(database, tableName, table, privileges);
            }
            catch (RuntimeException e) {
                if (e.getMessage() != null && e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
                    throw new RuntimeException("Failed to acquire locks from metastore because the underlying metastore table 'HIVE_LOCKS' does not exist. This can occur when using an embedded metastore which does not support transactions. To fix this use an alternative metastore.", e);
                }
                CommitStatus commitStatus = checkCommitStatus(newMetadataId, metadata);
                // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
                switch (commitStatus) {
                    case FAILURE:
                        throw e;
                    case UNKNOWN:
                        throw new CommitStateUnknownException(e);
                    default:
                        break;
                }
            }
        }
        finally {
            try {
                thriftMetastore.releaseTableLock(lockId);
            }
            catch (RuntimeException e) {
                // Release lock step has failed. Not throwing this exception, after commit has already succeeded.
                // So, that underlying iceberg API will not do the metadata cleanup, otherwise table will be in unusable state.
                // If configured and supported, the unreleased lock will be automatically released by the metastore after not hearing a heartbeat for a while,
                // or otherwise it might need to be manually deleted from the metastore backend storage.
                log.error(e, "Failed to release lock %s when committing to table %s", lockId, tableName);
            }
        }

        shouldRefresh = true;
    }

    private CommitStatus checkCommitStatus(String newMetadataId, TableMetadata config)
    {
        int maxAttempts = PropertyUtil.propertyAsInt(config.properties(), COMMIT_NUM_STATUS_CHECKS,
                COMMIT_NUM_STATUS_CHECKS_DEFAULT);
        long minWaitMs = PropertyUtil.propertyAsLong(config.properties(), COMMIT_STATUS_CHECKS_MIN_WAIT_MS,
                COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT);
        long maxWaitMs = PropertyUtil.propertyAsLong(config.properties(), COMMIT_STATUS_CHECKS_MAX_WAIT_MS,
                COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT);
        long totalRetryMs = PropertyUtil.propertyAsLong(config.properties(), COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS,
                COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT);

        AtomicReference<CommitStatus> status = new AtomicReference<>(CommitStatus.UNKNOWN);

        Tasks.foreach(newMetadataId)
                .retry(maxAttempts)
                .suppressFailureWhenFinished()
                .exponentialBackoff(minWaitMs, maxWaitMs, totalRetryMs, 2.0)
                .onFailure((location, checkException) ->
                        log.error(checkException, "Cannot check if commit to %s, location: %s.", tableName, location))
                .run(location -> {
                    TableMetadata metadata = refresh();
                    String currentMetadataId = metadata.metadataId();
                    boolean commitSuccess = currentMetadataId.equals(newMetadataId) ||
                            metadata.previousFiles().stream().anyMatch(log -> log.metadataId().equals(newMetadataId));
                    if (commitSuccess) {
                        log.info("Commit status check: Commit to %s of %s succeeded", tableName, newMetadataId);
                        status.set(CommitStatus.SUCCESS);
                    }
                    else {
                        log.warn("Commit status check: Commit to %s of %s unknown, new metadata location is not current " +
                                "or in history", tableName, newMetadataId);
                    }
                });
        if (status.get() == CommitStatus.UNKNOWN) {
            log.error("Cannot determine commit state to %s. Failed during checking %s times. " +
                    "Treating commit state as unknown.", tableName, maxAttempts);
        }
        return status.get();
    }

    private String writeNewMetadata(TableMetadata metadata, int newVersion)
    {
        String newTableMetadataId = newTableMetadataId(newVersion);

        // write the new metadata
        YunZhouTableMetadataParser.write(metadata, metastore, newTableMetadataId, database, tableName);

        return newTableMetadataId;
    }

    private String newTableMetadataId(int newVersion)
    {
        // todo ensure unique id
        return String.format("%05d-%s", newVersion, UUID.randomUUID());
    }

    public CachingHiveMetastore metastore()
    {
        return metastore;
    }

    @Override
    public FileIO io()
    {
        return fileIo;
    }

    @Override
    public String metadataFileLocation(String filename)
    {
        TableMetadata metadata = current();
        String location;
        if (metadata != null) {
            String writeLocation = metadata.properties().get(WRITE_METADATA_LOCATION);
            if (writeLocation != null) {
                return format("%s/%s", writeLocation, filename);
            }
            location = metadata.location();
        }
        else {
            location = this.location.orElseThrow(() -> new IllegalStateException("Location not set"));
        }
        return format("%s/%s/%s", location, METADATA_FOLDER_NAME, filename);
    }

    @Override
    public LocationProvider locationProvider()
    {
        TableMetadata metadata = current();
        return getLocationProvider(getSchemaTableName(), metadata.location(), metadata.properties());
    }

    public static boolean isYunzhouIcebergTable(io.trino.metastore.Table table)
    {
        return isIcebergTable(table) && table.getParameters().containsKey(METADATA_ID_PROP);
    }
}
