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

import com.google.common.collect.ImmutableList;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorAdapter;
import io.trino.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RangePartitionLookupOperatorFactory
        implements OperatorFactory, AdapterWorkProcessorOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final LookupBridgeManager<? extends RangePartitionLookupSourceFactory> lookupSourceFactoryManager;
    private final List<Integer> sortChannels;

    private boolean closed;

    public RangePartitionLookupOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            LookupBridgeManager<? extends RangePartitionLookupSourceFactory> lookupSourceFactoryManager,
            List<Integer> sortChannels)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");
        lookupSourceFactoryManager.incrementLookupFactoryCount();
        this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
    }

    private RangePartitionLookupOperatorFactory(RangePartitionLookupOperatorFactory other)
    {
        this.operatorId = other.operatorId;
        this.planNodeId = other.planNodeId;
        this.lookupSourceFactoryManager = other.lookupSourceFactoryManager;
        lookupSourceFactoryManager.incrementLookupFactoryCount();
        this.sortChannels = ImmutableList.copyOf(other.sortChannels);
        this.closed = false;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        OperatorContext operatorContext = driverContext.addOperatorContext(getOperatorId(), getPlanNodeId(), getOperatorType());
        return new WorkProcessorOperatorAdapter(operatorContext, this);
    }

    @Override
    public void noMoreOperators()
    {
        close();
    }

    // Methods from AdapterWorkProcessorOperatorFactory

    @Override
    public void close()
    {
        checkState(!closed);
        closed = true;
        lookupSourceFactoryManager.lookupOperatorFactoryClosed();
    }

    @Override
    public RangePartitionLookupOperatorFactory duplicate()
    {
        return new RangePartitionLookupOperatorFactory(this);
    }

    @Override
    public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
    {
        lookupSourceFactoryManager.lookupOperatorCreated();
        return new RangePartitionLookupOperator(
                processorContext,
                lookupSourceFactoryManager.getLookupBridge(),
                lookupSourceFactoryManager::lookupOperatorClosed,
                sortChannels,
                Optional.of(sourcePages));
    }

    @Override
    public WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator createAdapterOperator(ProcessorContext processorContext)
    {
        lookupSourceFactoryManager.lookupOperatorCreated();
        return new RangePartitionLookupOperator(
                processorContext,
                lookupSourceFactoryManager.getLookupBridge(),
                lookupSourceFactoryManager::lookupOperatorClosed,
                sortChannels,
                Optional.empty());
    }

    @Override
    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @Override
    public String getOperatorType()
    {
        return RangePartitionLookupOperator.class.getSimpleName();
    }
}
