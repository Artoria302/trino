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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RangePartitionNode
        extends PlanNode
{
    private final PlanNode source;
    private final PlanNode sampleSource;
    private final Symbol partitionSymbol;
    private final OrderingScheme orderingScheme;
    private final boolean canPruneSymbol;

    @JsonCreator
    public RangePartitionNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("sampleSource") PlanNode sampleSource,
            @JsonProperty("partitionSymbol") Symbol partitionSymbol,
            @JsonProperty("orderingScheme") OrderingScheme orderingScheme,
            @JsonProperty("canPruneSymbol") boolean canPruneSymbol)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(sampleSource, "sampleSource is null");
        requireNonNull(partitionSymbol, "partitionSymbol is null");
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.sampleSource = sampleSource;
        this.partitionSymbol = partitionSymbol;
        this.orderingScheme = orderingScheme;
        this.canPruneSymbol = canPruneSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source, sampleSource);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("sampleSource")
    public PlanNode getSampleSource()
    {
        return sampleSource;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(source.getOutputSymbols())
                .add(partitionSymbol)
                .build();
    }

    @JsonProperty("orderingScheme")
    public OrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty("canPruneSymbol")
    public boolean canPruneSymbol()
    {
        return canPruneSymbol;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        // return visitor.visitRangePartitionNode(this, context);
        return null;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new RangePartitionNode(getId(), newChildren.get(0), newChildren.get(1), partitionSymbol, orderingScheme, canPruneSymbol);
    }
}
