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
package io.trino.operator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.trino.operator.RowReferencePageManager.LoadCursor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class SampleNBuilder
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SampleNBuilder.class).instanceSize();
    private final List<Type> sourceTypes;
    private final int n;
    private final long[] sampleRowIds;
    private final long sampleRowIdsSize;
    private long processedRows;
    private int size;
    private final RowReferencePageManager pageManager = new RowReferencePageManager();

    public SampleNBuilder(List<Type> sourceTypes, int n)
    {
        this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
        this.n = n;
        this.sampleRowIds = new long[n];
        this.sampleRowIdsSize = (long) n * Long.BYTES;
    }

    public void processPage(Page page)
    {
        try (LoadCursor loadCursor = pageManager.add(page)) {
            long dereferenceRowId;
            for (int position = 0; position < page.getPositionCount(); position++) {
                processedRows++;
                loadCursor.advance();
                long rowId = loadCursor.allocateRowId();
                if (size < n) {
                    sampleRowIds[size++] = rowId;
                }
                else {
                    dereferenceRowId = rowId;
                    long replacementIndex = ThreadLocalRandom.current().nextLong(0, processedRows);
                    if (replacementIndex < (long) n) {
                        dereferenceRowId = sampleRowIds[(int) replacementIndex];
                        sampleRowIds[(int) replacementIndex] = rowId;
                    }
                    pageManager.dereference(dereferenceRowId);
                }
            }
            verify(!loadCursor.advance());
        }

        pageManager.compactIfNeeded();
    }

    public Iterator<Page> buildResult()
    {
        return new ResultIterator();
    }

    public long getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE + sampleRowIdsSize + pageManager.sizeOf();
    }

    private class ResultIterator
            extends AbstractIterator<Page>
    {
        private final PageBuilder pageBuilder;

        ResultIterator()
        {
            ImmutableList.Builder<Type> sourceTypesBuilders = ImmutableList.<Type>builder().addAll(sourceTypes);
            pageBuilder = new PageBuilder(sourceTypesBuilders.build());
        }

        @Override
        protected Page computeNext()
        {
            pageBuilder.reset();
            while (!pageBuilder.isFull() && size != 0) {
                long rowId = sampleRowIds[size--];
                Page page = pageManager.getPage(rowId);
                int position = pageManager.getPosition(rowId);
                for (int i = 0; i < sourceTypes.size(); i++) {
                    sourceTypes.get(i).appendTo(page.getBlock(i), position, pageBuilder.getBlockBuilder(i));
                }
                pageBuilder.declarePosition();
                // Deference the row for hygiene, but no need to compact them at this point
                pageManager.dereference(rowId);
            }

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            return pageBuilder.build();
        }
    }
}
