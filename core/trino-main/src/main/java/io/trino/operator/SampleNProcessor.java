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

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class SampleNProcessor
{
    private final LocalMemoryContext localUserMemoryContext;

    @Nullable
    private SampleNBuilder sampleNBuilder;
    private Iterator<Page> outputIterator;

    public SampleNProcessor(
            AggregatedMemoryContext aggregatedMemoryContext,
            List<Type> sourceTypes,
            int n)
    {
        requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null");
        this.localUserMemoryContext = aggregatedMemoryContext.newLocalMemoryContext(SampleNProcessor.class.getSimpleName());
        checkArgument(n >= 0, "n must be positive");

        if (n == 0) {
            outputIterator = emptyIterator();
        }
        else {
            sampleNBuilder = new SampleNBuilder(sourceTypes, n);
        }
    }

    public void addInput(Page page)
    {
        requireNonNull(sampleNBuilder, "sampleNBuilder is null");
        sampleNBuilder.processPage(requireNonNull(page, "page is null"));
        updateMemoryReservation();
    }

    public Page getOutput()
    {
        if (outputIterator == null) {
            // start flushing
            outputIterator = sampleNBuilder.buildResult();
        }

        Page output = null;
        if (outputIterator.hasNext()) {
            output = outputIterator.next();
        }
        else {
            outputIterator = emptyIterator();
        }
        updateMemoryReservation();
        return output;
    }

    public boolean noMoreOutput()
    {
        return outputIterator != null && !outputIterator.hasNext();
    }

    private void updateMemoryReservation()
    {
        requireNonNull(sampleNBuilder, "sampleNBuilder is null");
        localUserMemoryContext.setBytes(sampleNBuilder.getEstimatedSizeInBytes());
    }
}
