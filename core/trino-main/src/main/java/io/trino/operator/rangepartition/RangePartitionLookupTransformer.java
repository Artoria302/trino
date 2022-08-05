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

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.IntegerType;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;

import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.trino.operator.WorkProcessor.TransformationState.blocked;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static java.util.Objects.requireNonNull;

public class RangePartitionLookupTransformer
        implements WorkProcessor.Transformation<Page, Page>, Closeable
{
    private final ListenableFuture<LookupSource> lookupSourceFuture;
    private final DriverYieldSignal yieldSignal;
    private final int[] sortChannels;

    private LookupSource lookupSource;

    RangePartitionLookupTransformer(
            ProcessorContext processorContext,
            ListenableFuture<LookupSource> lookupSourceFuture,
            List<Integer> sortChannels)
    {
        this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
        this.yieldSignal = processorContext.getDriverYieldSignal();
        this.sortChannels = Ints.toArray(sortChannels);
    }

    @Override
    public WorkProcessor.TransformationState<Page> process(@Nullable Page page)
    {
        boolean finishing = page == null;
        if (finishing) {
            close();
            return finished();
        }

        if (lookupSource == null) {
            if (!lookupSourceFuture.isDone()) {
                return blocked(asVoid(lookupSourceFuture));
            }
            lookupSource = requireNonNull(getDone(lookupSourceFuture));
        }

        BlockBuilder blockBuilder = IntegerType.INTEGER.createBlockBuilder(
                null,
                page.getPositionCount(),
                IntegerType.INTEGER.getFixedSize());

        Page p = page.getColumns(sortChannels);
        for (int i = 0; i < page.getPositionCount(); i++) {
            int partition = lookupSource.getRangePartition(p, i);
            blockBuilder.writeInt(partition);
        }

        page.appendColumn(blockBuilder.build());

        return ofResult(page, true);
    }

    @Override
    public void close()
    {
        addSuccessCallback(lookupSourceFuture, LookupSource::close);
    }
}
