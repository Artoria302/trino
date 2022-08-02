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

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.BasicWorkProcessorOperatorAdapter.BasicAdapterWorkProcessorOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.BasicWorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static java.util.Objects.requireNonNull;

public class SampleNOperator
        implements WorkProcessorOperator

{
    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            List<Type> sourceTypes,
            int n)
    {
        return createAdapterOperatorFactory(new Factory(operatorId, planNodeId, sourceTypes, n));
    }

    private static class Factory
            implements BasicAdapterWorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final int n;
        private boolean closed;

        private Factory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> sourceTypes,
                int n)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.n = n;
        }

        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new SampleNOperator(
                    processorContext.getMemoryTrackingContext(),
                    sourcePages,
                    sourceTypes,
                    n);
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
            return SampleNOperator.class.getSimpleName();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(operatorId, planNodeId, sourceTypes, n);
        }
    }

    private final SampleNProcessor sampleNProcessor;
    private final WorkProcessor<Page> pages;

    private SampleNOperator(
            MemoryTrackingContext memoryTrackingContext,
            WorkProcessor<Page> sourcePages,
            List<Type> sourceTypes,
            int n)
    {
        this.sampleNProcessor = new SampleNProcessor(
                requireNonNull(memoryTrackingContext, "memoryTrackingContext is null").aggregateUserMemoryContext(),
                sourceTypes,
                n);

        if (n == 0) {
            pages = WorkProcessor.of();
        }
        else {
            pages = sourcePages.transform(new SampleNPages());
        }
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    private class SampleNPages
            implements WorkProcessor.Transformation<Page, Page>
    {
        @Override
        public WorkProcessor.TransformationState<Page> process(Page inputPage)
        {
            if (inputPage != null) {
                sampleNProcessor.addInput(inputPage);
                return WorkProcessor.TransformationState.needsMoreData();
            }

            // no more input, return results
            Page page = null;
            while (page == null && !sampleNProcessor.noMoreOutput()) {
                page = sampleNProcessor.getOutput();
            }

            if (page != null) {
                return WorkProcessor.TransformationState.ofResult(page, false);
            }

            return WorkProcessor.TransformationState.finished();
        }
    }
}
