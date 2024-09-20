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

import java.util.Iterator;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class RepartitionedFileTaskIterator
        implements Iterator<RepartitionedFileScanTask>
{
    private final Iterator<FileScanTask> fileScanTaskIterator;
    private final OptionalInt repartitioningValue;

    public RepartitionedFileTaskIterator(Iterator<FileScanTask> fileScanTaskIterator, OptionalInt repartitioningValue)
    {
        this.fileScanTaskIterator = requireNonNull(fileScanTaskIterator, "fileScanTaskIterator is null");
        this.repartitioningValue = requireNonNull(repartitioningValue, "repartitioningValue is null");
    }

    @Override
    public boolean hasNext()
    {
        return fileScanTaskIterator.hasNext();
    }

    @Override
    public RepartitionedFileScanTask next()
    {
        return new RepartitionedFileScanTask(fileScanTaskIterator.next(), repartitioningValue);
    }
}
