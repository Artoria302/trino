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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static java.util.Objects.requireNonNull;

@Immutable
public class SubPlan
{
    private final PlanFragment fragment;
    private final List<SubPlan> children;
    private final List<PlanFragmentId> childrenPlanFragmentIds;

    public SubPlan(PlanFragment fragment, List<SubPlan> children, List<PlanFragmentId> childrenPlanFragmentIds)
    {
        requireNonNull(fragment, "fragment is null");
        requireNonNull(children, "children is null");
        requireNonNull(childrenPlanFragmentIds, "childrenPlanFragmentId is null");

        this.fragment = fragment;
        this.children = ImmutableList.copyOf(children);
        this.childrenPlanFragmentIds = ImmutableList.copyOf(childrenPlanFragmentIds);
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<SubPlan> getChildren()
    {
        return children;
    }

    public List<PlanFragmentId> getChildrenPlanFragmentIds()
    {
        return childrenPlanFragmentIds;
    }

    /**
     * Flattens the subplan and returns all PlanFragments in the tree
     */
    public List<PlanFragment> getAllFragments()
    {
        ImmutableList.Builder<PlanFragment> fragments = ImmutableList.builder();

        fragments.add(getFragment());
        for (SubPlan child : getChildren()) {
            fragments.addAll(child.getAllFragments());
        }

        return fragments.build();
    }

    public void sanityCheck()
    {
        Multiset<PlanFragmentId> exchangeIds = fragment.getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableMultiset());

        // Multiset<PlanFragmentId> childrenIds = children.stream()
        //         .map(SubPlan::getFragment)
        //         .map(PlanFragment::getId)
        //         .collect(toImmutableMultiset());
        Multiset<PlanFragmentId> childrenIds = childrenPlanFragmentIds.stream().collect(toImmutableMultiset());

        checkState(exchangeIds.equals(childrenIds), "Subplan exchange ids don't match child fragment ids (%s vs %s)", exchangeIds, childrenIds);

        for (SubPlan child : children) {
            child.sanityCheck();
        }
    }
}
