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
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_BAD_DATA;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_CURSOR_ERROR;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ArcherParquetPageSource
        implements ConnectorPageSource
{
    private final ArcherParquetReader parquetReader;
    private final List<ParquetReaderColumn> parquetReaderColumns;
    private final boolean areSyntheticColumnsPresent;

    private boolean closed;

    public ArcherParquetPageSource(
            ArcherParquetReader parquetReader,
            List<ParquetReaderColumn> parquetReaderColumns)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.parquetReaderColumns = ImmutableList.copyOf(requireNonNull(parquetReaderColumns, "parquetReaderColumns is null"));
        this.areSyntheticColumnsPresent = parquetReaderColumns.stream().anyMatch(ParquetReaderColumn::miss);
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetReader.getReadTimeNanos();
    }

    @Override
    public long getMemoryUsage()
    {
        return parquetReader.getMemoryUsage();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        Page page;
        try {
            page = getColumnAdaptationsPage(parquetReader.nextPage());
        }
        catch (Exception e) {
            closeAllSuppress(e, this);
            throw handleException(parquetReader.getDataSource().getId(), e);
        }

        if (closed || page == null) {
            try {
                close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return null;
        }

        return page;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        parquetReader.close();
    }

    private Page getColumnAdaptationsPage(Page page)
    {
        if (!areSyntheticColumnsPresent) {
            return page;
        }
        if (page == null) {
            return null;
        }
        int batchSize = page.getPositionCount();
        Block[] blocks = new Block[parquetReaderColumns.size()];
        int sourceColumn = 0;
        for (int columnIndex = 0; columnIndex < parquetReaderColumns.size(); columnIndex++) {
            ParquetReaderColumn column = parquetReaderColumns.get(columnIndex);
            if (column.miss()) {
                blocks[columnIndex] = RunLengthEncodedBlock.create(column.type(), null, batchSize);
            }
            else {
                Block block = page.getBlock(sourceColumn);
                blocks[columnIndex] = block;
                sourceColumn++;
            }
        }
        return new Page(batchSize, blocks);
    }

    static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(ARCHER_BAD_DATA, exception);
        }
        return new TrinoException(ARCHER_CURSOR_ERROR, format("Failed to read Parquet file: %s, %s", dataSourceId, exception.getMessage()), exception);
    }
}
