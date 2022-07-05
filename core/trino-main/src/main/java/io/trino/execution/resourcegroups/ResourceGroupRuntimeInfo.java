package io.trino.execution.resourcegroups;


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

    public ResourceGroupRuntimeInfo(ResourceGroupId resourceGroupId, long memoryUsageBytes, int queuedQueries, int descendantQueuedQueries, int runningQueries, int descendantRunningQueries, Optional<ResourceGroupSpecInfo> resourceGroupConfigSpec)
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

    public ResourceGroupId getResourceGroupId()
    {
        return resourceGroupId;
    }

    public long getMemoryUsageBytes()
    {
        return memoryUsageBytes;
    }

    public int getQueuedQueries()
    {
        return queuedQueries;
    }

    public int getDescendantQueuedQueries()
    {
        return descendantQueuedQueries;
    }

    public int getRunningQueries()
    {
        return runningQueries;
    }

    public int getDescendantRunningQueries()
    {
        return descendantRunningQueries;
    }

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
