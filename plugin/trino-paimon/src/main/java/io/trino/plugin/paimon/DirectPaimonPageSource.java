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
package io.trino.plugin.paimon;

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedList;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_BAD_DATA;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_INTERNAL_ERROR;

/**
 * Trino {@link ConnectorPageSource}.
 */
public class DirectPaimonPageSource
        implements ConnectorPageSource
{
    private ConnectorPageSource current;
    private final LinkedList<ConnectorPageSource> pageSourceQueue;
    private long completedBytes;
    private long readNanos;

    public DirectPaimonPageSource(LinkedList<ConnectorPageSource> pageSourceQueue)
    {
        this.pageSourceQueue = pageSourceQueue;
        this.current = pageSourceQueue.poll();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + (current == null ? 0 : current.getCompletedBytes());
    }

    @Override
    public long getReadTimeNanos()
    {
        return readNanos + (current == null ? 0 : current.getReadTimeNanos());
    }

    @Override
    public boolean isFinished()
    {
        return current == null || (current.isFinished() && pageSourceQueue.isEmpty());
    }

    @Override
    public Page getNextPage()
    {
        try {
            if (current == null) {
                return null;
            }
            Page dataPage = current.getNextPage();
            if (dataPage == null) {
                advance();
                return getNextPage();
            }

            return dataPage;
        }
        catch (RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(PAIMON_BAD_DATA, e.getMessage(), e);
        }
    }

    private void advance()
    {
        if (current == null) {
            throw new TrinoException(PAIMON_INTERNAL_ERROR, "Current is null, should not invoke advance");
        }
        try {
            completedBytes += current.getCompletedBytes();
            readNanos += current.getReadTimeNanos();
            current.close();
        }
        catch (IOException e) {
            current = null;
            close();
            throw new TrinoException(PAIMON_BAD_DATA, "error happens while advance and close old page source.", e);
        }
        current = pageSourceQueue.poll();
    }

    @Override
    public void close()
    {
        try {
            if (current != null) {
                current.close();
            }
            for (ConnectorPageSource source : pageSourceQueue) {
                source.close();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return current == null ? null : current.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return current == null ? 0 : current.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return current == null ? Metrics.EMPTY : current.getMetrics();
    }
}
