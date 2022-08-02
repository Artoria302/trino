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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.sql.planner.Symbol;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public class SampleNNode
        extends PlanNode
{
    public enum Step
    {
        SINGLE,
        PARTIAL,
        FINAL
    }

    private final PlanNode source;
    private final long count;
    private final Step step;

    public SampleNNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("step") Step step)
    {
        super(id);

        requireNonNull(source, "source is null");
        checkArgument(count >= 0, "count must be positive");
        checkCondition(count <= Integer.MAX_VALUE, NOT_SUPPORTED, "SAMPLE count > %s is not supported", Integer.MAX_VALUE);

        this.source = source;
        this.count = count;
        this.step = requireNonNull(step, "step is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        // return visitor.visitSampleN(this, context);
        return null;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new SampleNNode(getId(), Iterables.getOnlyElement(newChildren), count, step);
    }
}
