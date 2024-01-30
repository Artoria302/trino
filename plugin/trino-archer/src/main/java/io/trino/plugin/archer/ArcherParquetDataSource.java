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

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;

import java.io.IOException;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ArcherParquetDataSource
        extends AbstractParquetDataSource
{
    public static final int MAX_SUPPORTED_PADDING_BYTES = 64;

    private final FileFormatDataSourceStats stats;
    private final TrinoInput input;

    public ArcherParquetDataSource(TrinoInputFile file, long fileSize, ParquetReaderOptions options, FileFormatDataSourceStats stats)
            throws IOException
    {
        super(new ParquetDataSourceId(file.location().toString()), fileSize, options);
        this.stats = requireNonNull(stats, "stats is null");
        this.input = file.newInput();
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    protected Slice readTailInternal(int length)
            throws IOException
    {
        long paddedFileSize = getEstimatedSize();
        int readSize = toIntExact(min(paddedFileSize, (length + MAX_SUPPORTED_PADDING_BYTES)));
        long position = paddedFileSize - readSize;
        long readStart = System.nanoTime();
        Slice tail = input.readFully(position, readSize).slice(max(0, readSize - length), min(readSize, length));
        stats.readDataBytesPerSecond(tail.length(), System.nanoTime() - readStart);
        return tail;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long readStart = System.nanoTime();
        input.readFully(position, buffer, bufferOffset, bufferLength);
        stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - readStart);
    }
}
