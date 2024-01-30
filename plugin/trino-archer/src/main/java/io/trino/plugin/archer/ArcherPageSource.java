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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_BAD_DATA;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ArcherPageSource
        implements ConnectorPageSource
{
    private final int[] expectedColumnIndexes;
    private final ConnectorPageSource delegate;
    private final Optional<ReaderProjectionsAdapter> projectionsAdapter;
    // An array with one element per field in the $pos column. The value in the array points to the
    // channel where the data can be read from.
    private int[] rowIdChildColumnIndexes = new int[0];
    // The $row_id's index in 'expectedColumns', or -1 if there isn't one
    private int rowIdColumnIndex = -1;

    public ArcherPageSource(
            List<ArcherColumnHandle> expectedColumns,
            List<ArcherColumnHandle> requiredColumns,
            ConnectorPageSource delegate,
            Optional<ReaderProjectionsAdapter> projectionsAdapter)
    {
        // expectedColumns should contain columns which should be in the final Page
        // requiredColumns should include all expectedColumns as well as any columns needed by the DeleteFilter
        requireNonNull(expectedColumns, "expectedColumns is null");
        requireNonNull(requiredColumns, "requiredColumns is null");
        this.expectedColumnIndexes = new int[expectedColumns.size()];
        for (int i = 0; i < expectedColumns.size(); i++) {
            ArcherColumnHandle expectedColumn = expectedColumns.get(i);
            checkArgument(expectedColumn.equals(requiredColumns.get(i)), "Expected columns must be a prefix of required columns");
            expectedColumnIndexes[i] = i;

            if (expectedColumn.isUpdateRowIdColumn() || expectedColumn.isMergeRowIdColumn()) {
                this.rowIdColumnIndex = i;

                Map<Integer, Integer> fieldIdToColumnIndex = mapFieldIdsToIndex(requiredColumns);
                List<ColumnIdentity> rowIdFields = expectedColumn.getColumnIdentity().getChildren();
                ImmutableMap.Builder<Integer, Integer> fieldIdToRowIdIndex = ImmutableMap.builder();
                this.rowIdChildColumnIndexes = new int[rowIdFields.size()];
                for (int columnIndex = 0; columnIndex < rowIdFields.size(); columnIndex++) {
                    int fieldId = rowIdFields.get(columnIndex).getId();
                    rowIdChildColumnIndexes[columnIndex] = requireNonNull(fieldIdToColumnIndex.get(fieldId), () -> format("Column %s not found in requiredColumns", fieldId));
                    fieldIdToRowIdIndex.put(fieldId, columnIndex);
                }
            }
        }

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.projectionsAdapter = requireNonNull(projectionsAdapter, "projectionsAdapter is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            if (projectionsAdapter.isPresent()) {
                dataPage = projectionsAdapter.get().adaptPage(dataPage);
            }

            dataPage = withRowIdBlock(dataPage);
            dataPage = dataPage.getColumns(expectedColumnIndexes);

            return dataPage;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(ARCHER_BAD_DATA, e);
        }
    }

    /**
     * The $row_id column used for updates is a composite column of at least one other column in the Page.
     * The indexes of the columns needed for the $row_id are in the updateRowIdChildColumnIndexes array.
     *
     * @param page The raw Page from the Parquet reader.
     * @return A Page where the $row_id channel has been populated.
     */
    private Page withRowIdBlock(Page page)
    {
        if (rowIdColumnIndex == -1) {
            return page;
        }

        Block[] rowIdFields = new Block[rowIdChildColumnIndexes.length];
        for (int childIndex = 0; childIndex < rowIdChildColumnIndexes.length; childIndex++) {
            rowIdFields[childIndex] = page.getBlock(rowIdChildColumnIndexes[childIndex]);
        }

        Block[] fullPage = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            if (channel == rowIdColumnIndex) {
                fullPage[channel] = RowBlock.fromFieldBlocks(page.getPositionCount(), rowIdFields);
                continue;
            }

            fullPage[channel] = page.getBlock(channel);
        }

        return new Page(page.getPositionCount(), fullPage);
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        closeAllSuppress(throwable, this);
    }

    private static Map<Integer, Integer> mapFieldIdsToIndex(List<ArcherColumnHandle> columns)
    {
        ImmutableMap.Builder<Integer, Integer> fieldIdsToIndex = ImmutableMap.builder();
        for (int i = 0; i < columns.size(); i++) {
            fieldIdsToIndex.put(columns.get(i).getId(), i);
        }
        return fieldIdsToIndex.buildOrThrow();
    }
}
