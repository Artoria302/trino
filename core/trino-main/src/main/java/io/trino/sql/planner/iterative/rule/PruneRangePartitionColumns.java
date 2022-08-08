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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RangePartitionNode;

import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.Patterns.rangePartition;

public class PruneRangePartitionColumns
        extends ProjectOffPushDownRule<RangePartitionNode>
{
    public PruneRangePartitionColumns()
    {
        super(rangePartition());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, RangePartitionNode rangePartitionNode, Set<Symbol> referencedOutputs)
    {
        if (!rangePartitionNode.canPruneSymbol()) {
            return Optional.empty();
        }

        ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();
        Symbol partitionSymbol = rangePartitionNode.getPartitionSymbol();
        builder.addAll(referencedOutputs.stream().filter(symbol -> !symbol.equals(partitionSymbol)).iterator());

        Optional<PlanNode> newSource = restrictOutputs(context.getIdAllocator(), rangePartitionNode.getSource(), builder.build());

        if (newSource.isEmpty()) {
            return Optional.empty();
        }

        ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
        newChildrenBuilder.add(newSource.get());
        newChildrenBuilder.add(rangePartitionNode.getSampleSource());
        return Optional.of(rangePartitionNode.replaceChildren(newChildrenBuilder.build()));
    }
}
