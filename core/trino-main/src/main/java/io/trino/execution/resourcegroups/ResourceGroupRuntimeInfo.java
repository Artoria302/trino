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
package io.trino.execution.resourcegroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.resourcegroups.ResourceGroupId;

import java.util.Optional;

import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;

public class ResourceGroupRuntimeInfo
{
    private final ResourceGroupId resourceGroupId;
    private final long memoryUsageBytes;
    private final int queuedQueries;
    private final int descendantQueuedQueries;
    private final int runningQueries;
    private final int descendantRunningQueries;
    private final Optional<ResourceGroupSpecInfo> resourceGroupConfigSpec;

    @JsonCreator
    public ResourceGroupRuntimeInfo(
            @JsonProperty("resourceGroupId") ResourceGroupId resourceGroupId,
            @JsonProperty("memoryUsageBytes") long memoryUsageBytes,
            @JsonProperty("queuedQueries") int queuedQueries,
            @JsonProperty("descendantQueuedQueries") int descendantQueuedQueries,
            @JsonProperty("runningQueries") int runningQueries,
            @JsonProperty("descendantRunningQueries") int descendantRunningQueries,
            @JsonProperty("resourceGroupConfigSpec") Optional<ResourceGroupSpecInfo> resourceGroupConfigSpec)
    {
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.memoryUsageBytes = memoryUsageBytes;
        this.queuedQueries = queuedQueries;
        this.descendantQueuedQueries = descendantQueuedQueries;
        this.runningQueries = runningQueries;
        this.descendantRunningQueries = descendantRunningQueries;
        this.resourceGroupConfigSpec = requireNonNull(resourceGroupConfigSpec, "resourceGroupConfigSpec is null");
    }

    public static Builder builder(ResourceGroupId resourceGroupId)
    {
        return new Builder(resourceGroupId);
    }

    @JsonProperty
    public ResourceGroupId getResourceGroupId()
    {
        return resourceGroupId;
    }

    @JsonProperty
    public long getMemoryUsageBytes()
    {
        return memoryUsageBytes;
    }

    @JsonProperty
    public int getQueuedQueries()
    {
        return queuedQueries;
    }

    @JsonProperty
    public int getDescendantQueuedQueries()
    {
        return descendantQueuedQueries;
    }

    @JsonProperty
    public int getRunningQueries()
    {
        return runningQueries;
    }

    @JsonProperty
    public int getDescendantRunningQueries()
    {
        return descendantRunningQueries;
    }

    @JsonProperty
    public Optional<ResourceGroupSpecInfo> getResourceGroupConfigSpec()
    {
        return resourceGroupConfigSpec;
    }

    public static class Builder
    {
        private final ResourceGroupId resourceGroupId;
        private ResourceGroupSpecInfo resourceGroupSpecInfo;
        private long userMemoryReservationBytes;
        private int queuedQueries;
        private int descendantQueuedQueries;
        private int runningQueries;
        private int descendantRunningQueries;

        private Builder(ResourceGroupId resourceGroupId)
        {
            this.resourceGroupId = resourceGroupId;
        }

        public Builder addUserMemoryReservationBytes(long userMemoryReservationBytes)
        {
            this.userMemoryReservationBytes = addExact(this.userMemoryReservationBytes, userMemoryReservationBytes);
            return this;
        }

        public Builder addQueuedQueries(int queuedQueries)
        {
            this.queuedQueries = addExact(this.queuedQueries, queuedQueries);
            return this;
        }

        public Builder addDescendantQueuedQueries(int descendantQueuedQueries)
        {
            this.descendantQueuedQueries += descendantQueuedQueries;
            return this;
        }

        public Builder addRunningQueries(int runningQueries)
        {
            this.runningQueries = addExact(this.runningQueries, runningQueries);
            return this;
        }

        public Builder addDescendantRunningQueries(int descendantRunningQueries)
        {
            this.descendantRunningQueries += descendantRunningQueries;
            return this;
        }

        public Builder setResourceGroupSpecInfo(ResourceGroupSpecInfo resourceGroupSpecInfo)
        {
            this.resourceGroupSpecInfo = resourceGroupSpecInfo;
            return this;
        }

        public ResourceGroupRuntimeInfo build()
        {
            return new ResourceGroupRuntimeInfo(resourceGroupId, userMemoryReservationBytes, queuedQueries, descendantQueuedQueries, runningQueries, descendantRunningQueries,
                    Optional.ofNullable(resourceGroupSpecInfo));
        }
    }
}
