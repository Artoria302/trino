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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.SimplePagesIndexComparator;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class RangePartitionBuilderOperator
        implements Operator
{
    public static class RangeIndexBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final LookupBridgeManager<RangePartitionLookupSourceFactory> lookupSourceFactoryManager;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final PagesIndex.Factory pagesIndexFactory;
        private final int expectedPositions;
        private final TypeOperators typeOperators;

        private boolean closed;

        public RangeIndexBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                LookupBridgeManager<RangePartitionLookupSourceFactory> lookupSourceFactoryManager,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                PagesIndex.Factory pagesIndexFactory,
                int expectedPositions,
                TypeOperators typeOperators)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannels, "sortChannels cannot be null");
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.expectedPositions = expectedPositions;
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        }

        @Override
        public RangePartitionBuilderOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, RangePartitionBuilderOperator.class.getSimpleName());

            RangePartitionLookupSourceFactory lookupSourceFactory = this.lookupSourceFactoryManager.getLookupBridge();
            return new RangePartitionBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    sortChannels,
                    sortOrders,
                    pagesIndexFactory,
                    expectedPositions,
                    typeOperators);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new RangeIndexBuilderOperatorFactory(
                    operatorId,
                    planNodeId,
                    lookupSourceFactoryManager,
                    sortChannels,
                    sortOrders,
                    pagesIndexFactory,
                    expectedPositions,
                    typeOperators);
        }
    }

    @VisibleForTesting
    public enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * LookupSource has been built
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * No longer needed
         */
        CLOSED
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final RangePartitionLookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<Void> lookupSourceFactoryDestroyed;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final PagesIndex pagesIndex;
    private final TypeOperators typeOperators;

    private State state = State.CONSUMING_INPUT;
    private Optional<ListenableFuture<Void>> lookupSourceNotNeeded = Optional.empty();
    private LookupSource lookupSource;

    RangePartitionBuilderOperator(
            OperatorContext operatorContext,
            RangePartitionLookupSourceFactory lookupSourceFactory,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            PagesIndex.Factory pagesIndexFactory,
            int expectedPositions,
            TypeOperators typeOperators)
    {
        this.operatorContext = operatorContext;
        localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();
        this.sortChannels = sortChannels;
        this.sortOrders = sortOrders;
        this.pagesIndex = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.typeOperators = typeOperators;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        switch (state) {
            case CONSUMING_INPUT:

            case CLOSED:
                return NOT_BLOCKED;

            case LOOKUP_SOURCE_BUILT:
                return lookupSourceNotNeeded.orElseThrow(() -> new IllegalStateException("Lookup source built, but disposal future not set"));
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput()
    {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT);

        return stateNeedsInput && !lookupSourceFactoryDestroyed.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        updateIndex(page);
    }

    private void updateIndex(Page page)
    {
        pagesIndex.addPage(page);

        if (!localUserMemoryContext.trySetBytes(pagesIndex.getEstimatedSize().toBytes())) {
            pagesIndex.compact();
            localUserMemoryContext.setBytes(pagesIndex.getEstimatedSize().toBytes());
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        switch (state) {
            case CONSUMING_INPUT:
                finishInput();
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case CLOSED:
                // no-op
                return;
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void finishInput()
    {
        checkState(state == State.CONSUMING_INPUT);
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        lookupSource = buildLookupSource();
        localUserMemoryContext.setBytes(lookupSource.getInMemorySizeInBytes());
        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.lendLookupSource(lookupSource));

        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(state == State.LOOKUP_SOURCE_BUILT);
        verify(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        pagesIndex.clear();
        localUserMemoryContext.setBytes(pagesIndex.getEstimatedSize().toBytes());
        lookupSource = null;
        close();
    }

    private LookupSource buildLookupSource()
    {
        pagesIndex.sort(sortChannels, sortOrders);
        SimplePagesIndexWithPageComparator comparator = new SimplePagesIndexWithPageComparator(lookupSourceFactory.getTypes(), sortChannels, sortOrders, typeOperators);
        SimplePagesIndexComparator pagesIndexComparator = new SimplePagesIndexComparator(lookupSourceFactory.getTypes(), sortChannels, sortOrders, typeOperators);
        int positionCount = pagesIndex.getPositionCount();
        // origin array [1, 1, 3, 5, 6, 6, 6, 6, 8]
        int[] counts = new int[positionCount];
        Arrays.fill(counts, 1);
        // accumulation count same value [1, 2, 1, 1, 1, 2, 3, 4, 1]
        for (int i = 1; i < positionCount; i++) {
            if (pagesIndexComparator.compareTo(pagesIndex, i, i - 1) == 0) {
                counts[i] = counts[i - 1] + 1;
            }
        }
        this.lookupSource = new RangePartitionLookupSource(pagesIndex, counts, comparator);
        return this.lookupSource;
    }

    @Override
    public boolean isFinished()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            // Finish early when the probe side is empty
            close();
            return true;
        }

        return state == State.CLOSED;
    }

    @Override
    public void close()
    {
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally

        lookupSource = null;
        state = State.CLOSED;

        try (Closer closer = Closer.create()) {
            closer.register(pagesIndex::clear);
            closer.register(() -> localUserMemoryContext.setBytes(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
