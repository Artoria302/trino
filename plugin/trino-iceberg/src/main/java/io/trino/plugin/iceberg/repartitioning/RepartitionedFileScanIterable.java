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
package io.trino.plugin.iceberg.repartitioning;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class RepartitionedFileScanIterable
        implements CloseableIterable<RepartitionedFileScanTask>
{
    private final Optional<Long> maxScannedFileSizeInBytes;
    private CloseableIterable<FileScanTask> fileScanIterable;

    public RepartitionedFileScanIterable(CloseableIterable<FileScanTask> fileScanIterable, Optional<Long> maxScannedFileSizeInBytes)
    {
        this.fileScanIterable = requireNonNull(fileScanIterable, "fileScanTasks is null");
        this.maxScannedFileSizeInBytes = requireNonNull(maxScannedFileSizeInBytes, "maxScannedFileSizeInBytes is null");
    }

    @Override
    public CloseableIterator<RepartitionedFileScanTask> iterator()
    {
        return new CloseableIterator<>()
        {
            private CloseableIterator<FileScanTask> fileScanIterator = fileScanIterable.iterator();
            private Map<StructLike, PartitionGroup> partitionGroups = new HashMap<>();
            private RepartitionedFileScanTask next1;
            private RepartitionedFileScanTask next2;

            @Override
            public void close()
                    throws IOException
            {
                if (fileScanIterator != null) {
                    fileScanIterator.close();
                    fileScanIterator = null;
                    partitionGroups = null;
                    next1 = null;
                    next2 = null;
                }
            }

            @Override
            public boolean hasNext()
            {
                if (next1 != null || next2 != null) {
                    return true;
                }

                while (fileScanIterator.hasNext()) {
                    FileScanTask task = fileScanIterator.next();
                    boolean hasNoDeletion = task.deletes().isEmpty();
                    if (hasNoDeletion && maxScannedFileSizeInBytes.isPresent() && task.file().fileSizeInBytes() >= maxScannedFileSizeInBytes.get()) {
                        continue;
                    }
                    StructLike partition = task.file().partition();
                    PartitionGroup group = partitionGroups.get(partition);
                    if (group == null) {
                        group = new PartitionGroup(task);
                        partitionGroups.put(partition, group);
                        if (!hasNoDeletion) {
                            next2 = group.value;
                            group.value = null;
                            return true;
                        }
                    }
                    else {
                        if (group.value != null) {
                            next2 = group.value;
                            group.value = null;
                        }
                        next1 = group.next(task);
                        return true;
                    }
                }

                return false;
            }

            @Override
            public RepartitionedFileScanTask next()
            {
                RepartitionedFileScanTask value;
                if (next1 != null) {
                    value = next1;
                    next1 = null;
                }
                else {
                    value = next2;
                    next2 = null;
                }
                return value;
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

    private static class PartitionGroup
    {
        private long thresholdInBytes = 768 * 1024 * 1024;
        private int dynamicRepartitioningBound;
        private long accumulateBytesSnapshot;
        private long accumulateBytes;
        private RepartitionedFileScanTask value;

        PartitionGroup(FileScanTask value)
        {
            requireNonNull(value, "task is null");
            this.dynamicRepartitioningBound = 1;
            this.value = next(value);
        }

        public RepartitionedFileScanTask next(FileScanTask task)
        {
            accumulateBytes += task.file().fileSizeInBytes();
            long diff = accumulateBytes - accumulateBytesSnapshot;
            while (diff >= thresholdInBytes) {
                dynamicRepartitioningBound += 1;
                accumulateBytesSnapshot += thresholdInBytes;
                thresholdInBytes += (long) ((double) thresholdInBytes * 0.4);
                diff = accumulateBytes - accumulateBytesSnapshot;
            }
            return new RepartitionedFileScanTask(task, OptionalInt.of(dynamicRepartitioningBound));
        }
    }
}
