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

import io.trino.spi.TrinoException;
import net.qihoo.archer.FileScanTask;
import net.qihoo.archer.StructLike;
import net.qihoo.archer.io.CloseableIterable;
import net.qihoo.archer.io.CloseableIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class RepartitionedFileScanIterable
        implements CloseableIterable<RepartitionedFileScanTask>
{
    private final long maxScannedFileSizeInBytes;
    private CloseableIterable<FileScanTask> fileScanIterable;
    private final int partitionSpecId;
    private final boolean refreshPartition;
    private final int invertedIndexId;
    private final boolean refreshInvertedIndex;

    public RepartitionedFileScanIterable(
            CloseableIterable<FileScanTask> fileScanIterable,
            long maxScannedFileSizeInBytes,
            OptionalInt partitionSpecId,
            boolean refreshPartition,
            OptionalInt invertedIndexId,
            boolean refreshInvertedIndex)
    {
        this.fileScanIterable = requireNonNull(fileScanIterable, "fileScanTasks is null");
        this.maxScannedFileSizeInBytes = maxScannedFileSizeInBytes;
        this.refreshPartition = refreshPartition;
        if (refreshPartition) {
            if (requireNonNull(partitionSpecId, "partitionSpecId is null").isEmpty()) {
                throw new TrinoException(ARCHER_INTERNAL_ERROR, "No current partition spec id when refresh partition");
            }
            this.partitionSpecId = partitionSpecId.getAsInt();
        }
        else {
            this.partitionSpecId = -1;
        }
        if (refreshInvertedIndex) {
            if (requireNonNull(invertedIndexId, "invertedIndexId is null").isEmpty()) {
                throw new TrinoException(ARCHER_INTERNAL_ERROR, "No current inverted index id when refresh inverted index");
            }
            this.invertedIndexId = invertedIndexId.getAsInt();
        }
        else {
            this.invertedIndexId = -1;
        }
        this.refreshInvertedIndex = refreshInvertedIndex;
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
                    boolean hasNoDeletion = !task.file().hasDelta();
                    if (hasNoDeletion &&
                            task.file().fileSizeInBytes() >= maxScannedFileSizeInBytes &&
                            (!refreshPartition || partitionSpecId == task.file().specId()) &&
                            (!refreshInvertedIndex || invertedIndexId == task.file().invertedIndexId())) {
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
