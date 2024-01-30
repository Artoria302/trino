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

import io.trino.plugin.archer.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import net.qihoo.archer.DataTask;
import net.qihoo.archer.FileScanTask;
import net.qihoo.archer.MetadataTableType;
import net.qihoo.archer.StructLike;
import net.qihoo.archer.Table;
import net.qihoo.archer.TableScan;
import net.qihoo.archer.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.MetadataTableUtils.createMetadataTableInstance;

public abstract class BaseSystemTable
        implements SystemTable
{
    private final Table archerTable;
    private final ConnectorTableMetadata tableMetadata;
    private final MetadataTableType metadataTableType;

    BaseSystemTable(Table archerTable, ConnectorTableMetadata tableMetadata, MetadataTableType metadataTableType)
    {
        this.archerTable = requireNonNull(archerTable, "archerTable is null");
        this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
        this.metadataTableType = requireNonNull(metadataTableType, "metadataTableType is null");
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
        return new FixedPageSource(buildPages(tableMetadata, session, archerTable, metadataTableType));
    }

    private List<Page> buildPages(ConnectorTableMetadata tableMetadata, ConnectorSession session, Table archerTable, MetadataTableType metadataTableType)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        TableScan tableScan = createMetadataTableInstance(archerTable, metadataTableType).newScan();
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        Map<String, Integer> columnNameToPosition = mapWithIndex(tableScan.schema().columns().stream(),
                (column, position) -> immutableEntry(column.name(), Long.valueOf(position).intValue()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            fileScanTasks.forEach(fileScanTask -> addRows((DataTask) fileScanTask, pagesBuilder, timeZoneKey, columnNameToPosition));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return pagesBuilder.build();
    }

    private void addRows(DataTask dataTask, PageListBuilder pagesBuilder, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        try (CloseableIterable<StructLike> dataRows = dataTask.rows()) {
            dataRows.forEach(dataTaskRow -> addRow(pagesBuilder, dataTaskRow, timeZoneKey, columnNameToPositionInSchema));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected abstract void addRow(PageListBuilder pagesBuilder, StructLike structLike, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema);
}
