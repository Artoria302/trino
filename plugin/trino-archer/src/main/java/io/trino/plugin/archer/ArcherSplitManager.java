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
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.type.TypeManager;
import net.qihoo.archer.CombinedScanTask;
import net.qihoo.archer.DataOperations;
import net.qihoo.archer.FileScanTask;
import net.qihoo.archer.Scan;
import net.qihoo.archer.Snapshot;
import net.qihoo.archer.Table;
import net.qihoo.archer.util.SnapshotUtil;

import java.util.concurrent.ExecutorService;

import static io.trino.plugin.archer.ArcherSessionProperties.getDynamicFilteringWaitTimeout;
import static java.util.Objects.requireNonNull;

public class ArcherSplitManager
        implements ConnectorSplitManager
{
    public static final int ARCHER_DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final ArcherTransactionManager transactionManager;
    private final TypeManager typeManager;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ExecutorService executor;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    @Inject
    public ArcherSplitManager(
            ArcherTransactionManager transactionManager,
            TypeManager typeManager,
            TrinoFileSystemFactory fileSystemFactory,
            @ForArcherSplitManager ExecutorService executor,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        ArcherTableHandle table = (ArcherTableHandle) handle;

        if (table.getSnapshotId().isEmpty()) {
            if (table.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return new FixedSplitSource(ImmutableList.of());
        }

        ArcherMetadata archerMetadata = transactionManager.get(transaction, session.getIdentity());
        Table archerTable = transactionManager.get(transaction, session.getIdentity()).getArcherTable(session, table.getSchemaTableName());
        Duration dynamicFilteringWaitTimeout = getDynamicFilteringWaitTimeout(session);

        Scan<?, FileScanTask, CombinedScanTask> scan = getScan(archerMetadata, archerTable, table, executor);

        ArcherSplitSource splitSource = new ArcherSplitSource(
                fileSystemFactory,
                session,
                table,
                archerTable,
                scan,
                table.getMaxScannedFileSize(),
                dynamicFilter,
                dynamicFilteringWaitTimeout,
                constraint,
                typeManager,
                table.isRecordScannedFiles(),
                table.getPartitionSpecId(),
                table.isRefreshPartition(),
                table.getInvertedIndexId(),
                table.isRefreshInvertedIndex(),
                cachingHostAddressProvider);

        return new ClassLoaderSafeConnectorSplitSource(splitSource, ArcherSplitManager.class.getClassLoader());
    }

    private Scan<?, FileScanTask, CombinedScanTask> getScan(ArcherMetadata archerMetadata, Table archerTable, ArcherTableHandle table, ExecutorService executor)
    {
        Long fromSnapshot = archerMetadata.getIncrementalRefreshFromSnapshot().orElse(null);
        if (fromSnapshot != null) {
            // check if fromSnapshot is still part of the table's snapshot history
            if (SnapshotUtil.isAncestorOf(archerTable, fromSnapshot)) {
                boolean containsModifiedRows = false;
                for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(archerTable, archerTable.currentSnapshot().snapshotId(), fromSnapshot)) {
                    if (snapshot.operation().equals(DataOperations.OVERWRITE) || snapshot.operation().equals(DataOperations.DELETE)) {
                        containsModifiedRows = true;
                        break;
                    }
                }
                if (!containsModifiedRows) {
                    return archerTable.newIncrementalAppendScan().fromSnapshotExclusive(fromSnapshot).planWith(executor);
                }
            }
            // fromSnapshot is missing (could be due to snapshot expiration or rollback), or snapshot range contains modifications
            // (deletes or overwrites), so we cannot perform incremental refresh. Falling back to full refresh.
            archerMetadata.disableIncrementalRefresh();
        }
        return archerTable.newScan().useSnapshot(table.getSnapshotId().get()).planWith(executor);
    }
}
