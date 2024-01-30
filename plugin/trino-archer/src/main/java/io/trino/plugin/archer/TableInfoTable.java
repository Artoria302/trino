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
import io.trino.plugin.archer.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import net.qihoo.archer.DataFile;
import net.qihoo.archer.FileScanTask;
import net.qihoo.archer.Table;
import net.qihoo.archer.TableScan;
import net.qihoo.archer.io.CloseableIterator;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableInfoTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table archerTable;
    private final Optional<Long> snapshotId;
    private final long createTime;

    public TableInfoTable(SchemaTableName tableName, Table archerTable, Optional<Long> snapshotId, long createTime)
    {
        this.archerTable = requireNonNull(archerTable, "archerTable is null");

        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("table_location", VARCHAR))
                        .add(new ColumnMetadata("table_count", BIGINT))
                        .add(new ColumnMetadata("table_size_in_bytes", BIGINT))
                        .add(new ColumnMetadata("create_time", BIGINT))
                        .build());
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (snapshotId.isEmpty()) {
            return new FixedPageSource(ImmutableList.of());
        }
        return new FixedPageSource(buildPages(tableMetadata, archerTable, snapshotId.get(), createTime));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, Table archerTable, long snapshotId, long createTime)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        TableScan tableScan = archerTable.newScan()
                .useSnapshot(snapshotId)
                .includeColumnStats();
        pagesBuilder.beginRow();
        pagesBuilder.appendVarchar(archerTable.location());

        long count = 0;
        long size = 0;

        CloseableIterator<FileScanTask> iterator = tableScan.planFiles().iterator();
        while (iterator.hasNext()) {
            DataFile dataFile = iterator.next().file();
            count += dataFile.recordCount();
            size += dataFile.fileSizeInBytes();
        }
        pagesBuilder.appendBigint(count);
        pagesBuilder.appendBigint(size);
        pagesBuilder.appendBigint(createTime);
        pagesBuilder.endRow();

        return pagesBuilder.build();
    }
}
