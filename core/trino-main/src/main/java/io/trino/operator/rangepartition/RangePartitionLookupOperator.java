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

import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.PageBuffer;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.trino.spi.Page;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.trino.operator.WorkProcessor.flatten;
import static java.util.Objects.requireNonNull;

public class RangePartitionLookupOperator
        implements AdapterWorkProcessorOperator
{
    private final ListenableFuture<LookupSource> lookupSourceFuture;
    private final PageBuffer pageBuffer;
    private final RangePartitionLookupTransformer lookupTransformer;
    private final Runnable afterClose;
    private final WorkProcessor<Page> pages;
    private final WorkProcessor<Page> partitionedPages;

    private boolean closed;

    RangePartitionLookupOperator(
            ProcessorContext processorContext,
            RangePartitionLookupSourceFactory lookupSourceFactory,
            Runnable afterClose,
            List<Integer> sortChannels,
            Optional<WorkProcessor<Page>> sourcePages)
    {
        this.lookupSourceFuture = lookupSourceFactory.createLookupSource();
        pageBuffer = new PageBuffer();
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        lookupTransformer = new RangePartitionLookupTransformer(processorContext, lookupSourceFuture, sortChannels);
        this.partitionedPages = sourcePages.orElse(pageBuffer.pages()).transform(lookupTransformer);

        pages = flatten(WorkProcessor.create(this::process));
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public boolean needsInput()
    {
        return (lookupSourceFuture.isDone()) && pageBuffer.isEmpty() && !pageBuffer.isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
    }

    public WorkProcessor.ProcessState<WorkProcessor<Page>> process()
    {
        // wait for build side to be completed before fetching any data
        if (!lookupSourceFuture.isDone()) {
            return WorkProcessor.ProcessState.blocked(asVoid(lookupSourceFuture));
        }

        if (!partitionedPages.isFinished()) {
            return WorkProcessor.ProcessState.ofResult(partitionedPages);
        }

        close();
        return WorkProcessor.ProcessState.finished();
    }

    @Override
    public void finish()
    {
        pageBuffer.finish();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(lookupTransformer);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
