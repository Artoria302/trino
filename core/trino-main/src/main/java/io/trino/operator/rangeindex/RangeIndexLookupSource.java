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
package io.trino.operator.rangeindex;

import io.trino.operator.PagesIndex;
import io.trino.spi.Page;
import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.ThreadLocalRandom;

public class RangeIndexLookupSource
        implements LookupSource
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RangeIndexLookupSource.class).instanceSize();
    private final PagesIndex pagesIndex;
    private final SimplePagesIndexWithPageComparator comparator;
    private final int positionCount;
    private final int[] counts;

    RangeIndexLookupSource(PagesIndex pagesIndex, int[] counts, SimplePagesIndexWithPageComparator comparator)
    {
        this.pagesIndex = pagesIndex;
        this.comparator = comparator;
        this.positionCount = pagesIndex.getPositionCount();
        this.counts = counts;
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE;
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
    public int getRangeIndex(Page page, int position)
    {
        int index = 0;
        if (positionCount <= 64) {
            while (index < positionCount && comparator.compareTo(page, position, pagesIndex, index) >= 0) {
                index++;
            }
        }
        else {
            index = upperBound(page, position);
        }
        if (index - 1 > 0 && counts[index - 1] != 1 && comparator.compareTo(page, position, pagesIndex, index - 1) == 0) {
            index -= ThreadLocalRandom.current().nextInt(0, counts[index - 1]);
        }
        return index;
    }

    @Override
    public void close()
    {
    }
}
