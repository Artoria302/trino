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
package io.trino.operator.rangepartition;

import io.trino.operator.PagesIndex;
import io.trino.spi.Page;
import org.openjdk.jol.info.ClassLayout;

public class RangePartitionLookupSource
        implements LookupSource
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RangePartitionLookupSource.class).instanceSize();
    private final PagesIndex pagesIndex;
    private final SimplePagesIndexWithPageComparator comparator;
    private final int positionCount;
    private final int[] counts;
    private final int[] roundRobin;
    private final long arraySize;

    RangePartitionLookupSource(PagesIndex pagesIndex, int[] counts, SimplePagesIndexWithPageComparator comparator)
    {
        this.pagesIndex = pagesIndex;
        this.comparator = comparator;
        this.positionCount = pagesIndex.getPositionCount();
        this.counts = counts;
        this.roundRobin = new int[counts.length];
        arraySize = (long) positionCount * Integer.BYTES * 2;
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + arraySize;
    }

    private int upperBound(Page page, int position)
    {
        int i = 0;
        int j = positionCount - 1;
        int mid;
        while (i <= j) {
            mid = i + (j - i) / 2;
            if (comparator.compareTo(page, position, pagesIndex, mid) >= 0) {
                i = mid + 1;
            }
            else {
                j = mid - 1;
            }
        }
        return i;
    }

    @Override
    public int getRangePartition(Page page, int position)
    {
        int partition = 0;
        if (positionCount <= 64) {
            while (partition < positionCount && comparator.compareTo(page, position, pagesIndex, partition) >= 0) {
                partition++;
            }
        }
        else {
            partition = upperBound(page, position);
        }
        int pos = partition - 1;
        if (pos >= 0 && counts[pos] != 1 && comparator.compareTo(page, position, pagesIndex, pos) == 0) {
            partition -= roundRobin[pos];
            roundRobin[pos] = (roundRobin[pos] + 1) % counts[pos];
        }
        return partition;
    }

    @Override
    public void close()
    {
    }
}
