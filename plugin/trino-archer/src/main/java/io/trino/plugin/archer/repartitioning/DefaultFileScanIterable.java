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
package io.trino.plugin.archer.repartitioning;

import net.qihoo.archer.FileScanTask;
import net.qihoo.archer.io.CloseableIterable;
import net.qihoo.archer.io.CloseableIterator;

import java.io.IOException;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class DefaultFileScanIterable
        implements CloseableIterable<RepartitionedFileScanTask>
{
    private CloseableIterable<FileScanTask> fileScanIterable;

    public DefaultFileScanIterable(CloseableIterable<FileScanTask> fileScanIterable)
    {
        this.fileScanIterable = requireNonNull(fileScanIterable, "fileScanIterable is null");
    }

    @Override
    public CloseableIterator<RepartitionedFileScanTask> iterator()
    {
        return new CloseableIterator<>()
        {
            private CloseableIterator<FileScanTask> fileScanIterator = fileScanIterable.iterator();

            @Override
            public void close()
                    throws IOException
            {
                if (fileScanIterator != null) {
                    fileScanIterator.close();
                }
                fileScanIterator = null;
            }

            @Override
            public boolean hasNext()
            {
                return fileScanIterator.hasNext();
            }

            @Override
            public RepartitionedFileScanTask next()
            {
                return new RepartitionedFileScanTask(fileScanIterator.next(), OptionalInt.empty());
            }
        };
    }

    @Override
    public void close()
            throws IOException
    {
        if (fileScanIterable != null) {
            fileScanIterable.close();
            fileScanIterable = null;
        }
    }
}
