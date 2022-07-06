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
package io.trino.spi.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.QueryId;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class ClusterMemoryPoolInfo
{
    private final MemoryPoolInfo memoryPoolInfo;
    private final int blockedNodes;
    private final int assignedQueries;
    private final Optional<QueryId> largestMemoryQuery;

    public ClusterMemoryPoolInfo(
            MemoryPoolInfo memoryPoolInfo,
            int blockedNodes,
            int assignedQueries)
    {
        this(memoryPoolInfo, blockedNodes, assignedQueries, Optional.empty());
    }

    @JsonCreator
    public ClusterMemoryPoolInfo(
            @JsonProperty("memoryPoolInfo") MemoryPoolInfo memoryPoolInfo,
            int blockedNodes,
            int assignedQueries,
            Optional<QueryId> largestMemoryQuery)
    {
        this.memoryPoolInfo = requireNonNull(memoryPoolInfo, "memoryPoolInfo is null");
        this.blockedNodes = blockedNodes;
        this.assignedQueries = assignedQueries;
        this.largestMemoryQuery = largestMemoryQuery;
    }

    @JsonProperty
    public MemoryPoolInfo getMemoryPoolInfo()
    {
        return memoryPoolInfo;
    }

    @JsonProperty
    public int getBlockedNodes()
    {
        return blockedNodes;
    }

    @JsonProperty
    public int getAssignedQueries()
    {
        return assignedQueries;
    }

    @JsonProperty
    public Optional<QueryId> getLargestMemoryQuery()
    {
        return largestMemoryQuery;
    }
}
