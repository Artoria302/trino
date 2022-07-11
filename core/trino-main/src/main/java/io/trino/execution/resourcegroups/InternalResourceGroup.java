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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.CounterStat;
import io.trino.execution.ManagedQueryExecution;
import io.trino.execution.resourcegroups.WeightedFairQueue.Usage;
import io.trino.server.QueryStateInfo;
import io.trino.server.ResourceGroupInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroup;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.ResourceGroupState;
import io.trino.spi.resourcegroups.SchedulingPolicy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.math.LongMath.saturatedMultiply;
import static com.google.common.math.LongMath.saturatedSubtract;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.SystemSessionProperties.getQueryPriority;
import static io.trino.server.QueryStateInfo.createQueryStateInfo;
import static io.trino.spi.StandardErrorCode.INVALID_RESOURCE_GROUP;
import static io.trino.spi.resourcegroups.ResourceGroupState.CAN_QUEUE;
import static io.trino.spi.resourcegroups.ResourceGroupState.CAN_RUN;
import static io.trino.spi.resourcegroups.ResourceGroupState.FULL;
import static io.trino.spi.resourcegroups.SchedulingPolicy.FAIR;
import static io.trino.spi.resourcegroups.SchedulingPolicy.QUERY_PRIORITY;
import static io.trino.spi.resourcegroups.SchedulingPolicy.WEIGHTED;
import static io.trino.spi.resourcegroups.SchedulingPolicy.WEIGHTED_FAIR;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Resource groups form a tree, and all access to a group is guarded by the root of the tree.
 * Queries are submitted to leaf groups. Never to intermediate groups. Intermediate groups
 * aggregate resource consumption from their children, and may have their own limitations that
 * are enforced.
 */
@ThreadSafe
public class InternalResourceGroup
        implements ResourceGroup
{
    public static final int DEFAULT_WEIGHT = 1;

    private final InternalResourceGroup root;
    private final Optional<InternalResourceGroup> parent;
    private final ResourceGroupId id;
    private final BiConsumer<InternalResourceGroup, Boolean> jmxExportListener;
    private final Executor executor;
    private final Function<ResourceGroupId, Optional<ResourceGroupRuntimeInfo>> additionalRuntimeInfo;
    private final Predicate<InternalResourceGroup> shouldWaitForResourceManagerUpdate;
    // Configuration
    // =============
    @GuardedBy("root")
    private long softMemoryLimitBytes = Long.MAX_VALUE;
    @GuardedBy("root")
    private int softConcurrencyLimit;
    @GuardedBy("root")
    private int hardConcurrencyLimit;
    @GuardedBy("root")
    private int maxQueuedQueries;
    @GuardedBy("root")
    private long softCpuLimitMillis = Long.MAX_VALUE;
    @GuardedBy("root")
    private long hardCpuLimitMillis = Long.MAX_VALUE;
    @GuardedBy("root")
    private long cpuQuotaGenerationMillisPerSecond = Long.MAX_VALUE;
    @GuardedBy("root")
    private int schedulingWeight = DEFAULT_WEIGHT;
    @GuardedBy("root")
    private SchedulingPolicy schedulingPolicy = FAIR;
    @GuardedBy("root")
    private boolean jmxExport;

    // Live data structures
    // ====================
    @GuardedBy("root")
    private final Map<String, InternalResourceGroup> subGroups = new HashMap<>();
    // Sub groups with queued queries, that have capacity to run them
    // That is, they must return true when internalStartNext() is called on them
    @GuardedBy("root")
    private Queue<InternalResourceGroup> eligibleSubGroups = new FifoQueue<>();
    // Sub groups whose memory usage may be out of date. Most likely because they have a running query.
    @GuardedBy("root")
    private final Set<InternalResourceGroup> dirtySubGroups = new HashSet<>();
    @GuardedBy("root")
    private UpdateablePriorityQueue<ManagedQueryExecution> queuedQueries = new FifoQueue<>();
    @GuardedBy("root")
    private final Map<ManagedQueryExecution, ResourceUsage> runningQueries = new HashMap<>();
    @GuardedBy("root")
    private int descendantRunningQueries;
    @GuardedBy("root")
    private int descendantQueuedQueries;
    // CPU and memory usage is cached because it changes very rapidly while queries are running, and would be expensive to track continuously
    @GuardedBy("root")
    private ResourceUsage cachedResourceUsage = new ResourceUsage(0, 0);
    @GuardedBy("root")
    private long lastStartMillis;
    @GuardedBy("root")
    private final CounterStat timeBetweenStartsSec = new CounterStat();

    @GuardedBy("root")
    private final AtomicLong lastRunningQueryStartTime = new AtomicLong(currentTimeMillis());
    @GuardedBy("root")
    private final AtomicBoolean isDirty = new AtomicBoolean();

    public InternalResourceGroup(
            String name,
            BiConsumer<InternalResourceGroup, Boolean> jmxExportListener,
            Executor executor)
    {
        this(Optional.empty(), name, jmxExportListener, executor, ignored -> Optional.empty(), rg -> false);
    }

    public InternalResourceGroup(
            String name,
            BiConsumer<InternalResourceGroup, Boolean> jmxExportListener,
            Executor executor,
            Function<ResourceGroupId, Optional<ResourceGroupRuntimeInfo>> additionalRuntimeInfo,
            Predicate<InternalResourceGroup> shouldWaitForResourceManagerUpdate)
    {
        this(Optional.empty(), name, jmxExportListener, executor, additionalRuntimeInfo, shouldWaitForResourceManagerUpdate);
    }

    protected InternalResourceGroup(
            Optional<InternalResourceGroup> parent,
            String name,
            BiConsumer<InternalResourceGroup, Boolean> jmxExportListener,
            Executor executor,
            Function<ResourceGroupId, Optional<ResourceGroupRuntimeInfo>> additionalRuntimeInfo,
            Predicate<InternalResourceGroup> shouldWaitForResourceManagerUpdate)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.jmxExportListener = requireNonNull(jmxExportListener, "jmxExportListener is null");
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(name, "name is null");
        if (parent.isPresent()) {
            id = new ResourceGroupId(parent.get().id, name);
            root = parent.get().root;
        }
        else {
            id = new ResourceGroupId(name);
            root = this;
        }
        this.additionalRuntimeInfo = requireNonNull(additionalRuntimeInfo, "additionalRuntimeInfo is null");
        this.shouldWaitForResourceManagerUpdate = requireNonNull(shouldWaitForResourceManagerUpdate, "shouldWaitForResourceManagerUpdate is null");
    }

    public ResourceGroupInfo getFullInfo()
    {
        synchronized (root) {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    succinctBytes(softMemoryLimitBytes),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    maxQueuedQueries,
                    succinctBytes(cachedResourceUsage.getMemoryUsageBytes()),
                    succinctDuration(cachedResourceUsage.getCpuUsageMillis(), MILLISECONDS),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    Optional.of(subGroups.values().stream()
                            .filter(group -> group.getRunningQueries() + group.getQueuedQueries() > 0)
                            .map(InternalResourceGroup::getSummaryInfo)
                            .collect(toImmutableList())),
                    Optional.of(getAggregatedRunningQueriesInfo()));
        }
    }

    public ResourceGroupInfo getInfo()
    {
        synchronized (root) {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    succinctBytes(softMemoryLimitBytes),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    maxQueuedQueries,
                    succinctBytes(cachedResourceUsage.getMemoryUsageBytes()),
                    succinctDuration(cachedResourceUsage.getCpuUsageMillis(), MILLISECONDS),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    Optional.of(subGroups.values().stream()
                            .filter(group -> group.getRunningQueries() + group.getQueuedQueries() > 0)
                            .map(InternalResourceGroup::getSummaryInfo)
                            .collect(toImmutableList())),
                    Optional.empty());
        }
    }

    private ResourceGroupInfo getSummaryInfo()
    {
        synchronized (root) {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    succinctBytes(softMemoryLimitBytes),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    maxQueuedQueries,
                    succinctBytes(cachedResourceUsage.getMemoryUsageBytes()),
                    succinctDuration(cachedResourceUsage.getCpuUsageMillis(), MILLISECONDS),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    Optional.empty(),
                    Optional.empty());
        }
    }

    private ResourceGroupState getState()
    {
        synchronized (root) {
            if (canRunMore()) {
                return CAN_RUN;
            }
            else if (canQueueMore()) {
                return CAN_QUEUE;
            }
            else {
                return FULL;
            }
        }
    }

    private List<QueryStateInfo> getAggregatedRunningQueriesInfo()
    {
        synchronized (root) {
            if (subGroups.isEmpty()) {
                return runningQueries.keySet().stream()
                        .map(ManagedQueryExecution::getBasicQueryInfo)
                        .map(queryInfo -> createQueryStateInfo(queryInfo, Optional.of(id)))
                        .collect(toImmutableList());
            }

            return subGroups.values().stream()
                    .map(InternalResourceGroup::getAggregatedRunningQueriesInfo)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }
    }

    public List<ResourceGroupInfo> getPathToRoot()
    {
        synchronized (root) {
            ImmutableList.Builder<ResourceGroupInfo> builder = ImmutableList.builder();
            InternalResourceGroup group = this;
            while (group != null) {
                builder.add(group.getInfo());
                group = group.parent.orElse(null);
            }

            return builder.build();
        }
    }

    @Override
    public ResourceGroupId getId()
    {
        return id;
    }

    @Managed
    public int getRunningQueries()
    {
        synchronized (root) {
            return runningQueries.size() + descendantRunningQueries;
        }
    }

    @Managed
    public int getQueuedQueries()
    {
        synchronized (root) {
            return queuedQueries.size() + descendantQueuedQueries;
        }
    }

    @Managed
    public int getWaitingQueuedQueries()
    {
        synchronized (root) {
            // For leaf group, when no queries can run, all queued queries are waiting for resources on this resource group.
            if (subGroups.isEmpty()) {
                return queuedQueries.size();
            }

            // For internal groups, when no queries can run, only queries that could run on its subgroups are waiting for resources on this group.
            int waitingQueuedQueries = 0;
            for (InternalResourceGroup subGroup : subGroups.values()) {
                if (subGroup.canRunMore()) {
                    waitingQueuedQueries += min(subGroup.getQueuedQueries(), subGroup.getHardConcurrencyLimit() - subGroup.getRunningQueries());
                }
            }

            return waitingQueuedQueries;
        }
    }

    @Override
    public long getSoftMemoryLimitBytes()
    {
        synchronized (root) {
            return softMemoryLimitBytes;
        }
    }

    @Override
    public void setSoftMemoryLimitBytes(long limit)
    {
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.softMemoryLimitBytes = limit;
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Override
    public Duration getSoftCpuLimit()
    {
        synchronized (root) {
            return Duration.ofMillis(softCpuLimitMillis);
        }
    }

    @Override
    public void setSoftCpuLimit(Duration limit)
    {
        synchronized (root) {
            if (limit.toMillis() > hardCpuLimitMillis) {
                setHardCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.softCpuLimitMillis = limit.toMillis();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Override
    public Duration getHardCpuLimit()
    {
        synchronized (root) {
            return Duration.ofMillis(hardCpuLimitMillis);
        }
    }

    @Override
    public void setHardCpuLimit(Duration limit)
    {
        synchronized (root) {
            if (limit.toMillis() < softCpuLimitMillis) {
                setSoftCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.hardCpuLimitMillis = limit.toMillis();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Override
    public long getCpuQuotaGenerationMillisPerSecond()
    {
        synchronized (root) {
            return cpuQuotaGenerationMillisPerSecond;
        }
    }

    @Override
    public void setCpuQuotaGenerationMillisPerSecond(long rate)
    {
        checkArgument(rate > 0, "Cpu quota generation must be positive");
        synchronized (root) {
            cpuQuotaGenerationMillisPerSecond = rate;
        }
    }

    @Override
    public int getSoftConcurrencyLimit()
    {
        synchronized (root) {
            return softConcurrencyLimit;
        }
    }

    @Override
    public void setSoftConcurrencyLimit(int softConcurrencyLimit)
    {
        checkArgument(softConcurrencyLimit >= 0, "softConcurrencyLimit is negative");
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.softConcurrencyLimit = softConcurrencyLimit;
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Managed
    @Override
    public int getHardConcurrencyLimit()
    {
        synchronized (root) {
            return hardConcurrencyLimit;
        }
    }

    @Managed
    @Override
    public void setHardConcurrencyLimit(int hardConcurrencyLimit)
    {
        checkArgument(hardConcurrencyLimit >= 0, "hardConcurrencyLimit is negative");
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.hardConcurrencyLimit = hardConcurrencyLimit;
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
    }

    @Managed
    @Override
    public int getMaxQueuedQueries()
    {
        synchronized (root) {
            return maxQueuedQueries;
        }
    }

    @Managed
    @Override
    public void setMaxQueuedQueries(int maxQueuedQueries)
    {
        checkArgument(maxQueuedQueries >= 0, "maxQueuedQueries is negative");
        synchronized (root) {
            this.maxQueuedQueries = maxQueuedQueries;
        }
    }

    @Managed
    @Nested
    public CounterStat getTimeBetweenStartsSec()
    {
        return timeBetweenStartsSec;
    }

    @Override
    public int getSchedulingWeight()
    {
        synchronized (root) {
            return schedulingWeight;
        }
    }

    @Override
    public void setSchedulingWeight(int weight)
    {
        checkArgument(weight > 0, "weight must be positive");
        synchronized (root) {
            this.schedulingWeight = weight;
            if (parent.isPresent() && parent.get().schedulingPolicy == WEIGHTED && parent.get().eligibleSubGroups.contains(this)) {
                parent.get().addOrUpdateSubGroup(this);
            }
        }
    }

    @Override
    public SchedulingPolicy getSchedulingPolicy()
    {
        synchronized (root) {
            return schedulingPolicy;
        }
    }

    @Override
    public void setSchedulingPolicy(SchedulingPolicy policy)
    {
        synchronized (root) {
            if (policy == schedulingPolicy) {
                return;
            }

            if (parent.isPresent() && parent.get().schedulingPolicy == QUERY_PRIORITY) {
                checkArgument(policy == QUERY_PRIORITY, "Parent of %s uses query priority scheduling, so %s must also", id, id);
            }

            // Switch to the appropriate queue implementation to implement the desired policy
            Queue<InternalResourceGroup> queue;
            UpdateablePriorityQueue<ManagedQueryExecution> queryQueue;
            switch (policy) {
                case FAIR:
                    queue = new FifoQueue<>();
                    queryQueue = new FifoQueue<>();
                    break;
                case WEIGHTED:
                    queue = new StochasticPriorityQueue<>();
                    queryQueue = new StochasticPriorityQueue<>();
                    break;
                case WEIGHTED_FAIR:
                    queue = new WeightedFairQueue<>();
                    queryQueue = new IndexedPriorityQueue<>();
                    break;
                case QUERY_PRIORITY:
                    // Sub groups must use query priority to ensure ordering
                    for (InternalResourceGroup group : subGroups.values()) {
                        group.setSchedulingPolicy(QUERY_PRIORITY);
                    }
                    queue = new IndexedPriorityQueue<>();
                    queryQueue = new IndexedPriorityQueue<>();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported scheduling policy: " + policy);
            }
            schedulingPolicy = policy;
            while (!eligibleSubGroups.isEmpty()) {
                InternalResourceGroup group = eligibleSubGroups.poll();
                addOrUpdateSubGroup(queue, group);
            }
            eligibleSubGroups = queue;
            while (!queuedQueries.isEmpty()) {
                ManagedQueryExecution query = queuedQueries.poll();
                queryQueue.addOrUpdate(query, getQueryPriority(query.getSession()));
            }
            queuedQueries = queryQueue;
        }
    }

    @Override
    public boolean getJmxExport()
    {
        synchronized (root) {
            return jmxExport;
        }
    }

    @Override
    public void setJmxExport(boolean export)
    {
        synchronized (root) {
            jmxExport = export;
        }
        jmxExportListener.accept(this, export);
    }

    public InternalResourceGroup getOrCreateSubGroup(String name)
    {
        requireNonNull(name, "name is null");
        synchronized (root) {
            checkArgument(runningQueries.isEmpty() && queuedQueries.isEmpty(), "Cannot add sub group to %s while queries are running", id);
            if (subGroups.containsKey(name)) {
                return subGroups.get(name);
            }
            InternalResourceGroup subGroup = new InternalResourceGroup(
                    Optional.of(this),
                    name,
                    jmxExportListener,
                    executor,
                    additionalRuntimeInfo,
                    shouldWaitForResourceManagerUpdate);
            // Sub group must use query priority to ensure ordering
            if (schedulingPolicy == QUERY_PRIORITY) {
                subGroup.setSchedulingPolicy(QUERY_PRIORITY);
            }
            subGroups.put(name, subGroup);
            return subGroup;
        }
    }

    private boolean isDirty()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return runningQueries.size() + descendantRunningQueries > 0 || isDirty.get();
        }
    }

    protected void setDirty()
    {
        synchronized (root) {
            this.isDirty.set(true);
            dirtySubGroups.addAll(subGroups());
            subGroups().forEach(InternalResourceGroup::setDirty);
        }
    }

    public void run(ManagedQueryExecution query)
    {
        synchronized (root) {
            if (!subGroups.isEmpty()) {
                throw new TrinoException(INVALID_RESOURCE_GROUP, format("Cannot add queries to %s. It is not a leaf group.", id));
            }
            // Check all ancestors for capacity
            InternalResourceGroup group = this;
            boolean canQueue = true;
            boolean canRun = true;
            while (true) {
                canQueue = canQueue && group.canQueueMore();
                canRun = canRun && group.canRunMore();
                if (group.parent.isEmpty()) {
                    break;
                }
                group = group.parent.get();
            }
            if (!canQueue && !canRun) {
                query.fail(new QueryQueueFullException(id));
                return;
            }
            if (canRun) {
                startInBackground(query);
            }
            else {
                enqueueQuery(query);
            }
            query.addStateChangeListener(state -> {
                if (state.isDone()) {
                    queryFinished(query);
                }
            });
        }
    }

    private void enqueueQuery(ManagedQueryExecution query)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to enqueue a query");
        synchronized (root) {
            queuedQueries.addOrUpdate(query, getQueryPriority(query.getSession()));
            InternalResourceGroup group = this;
            while (group.parent.isPresent()) {
                group.parent.get().descendantQueuedQueries++;
                group = group.parent.get();
            }
            updateEligibility();
        }
    }

    /**
     * Updates eligibility to run more queries for all groups on the path starting from this group up to the root.
     * This method must be called whenever the eligibility may have changed for this group.
     */
    private void updateEligibility()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to update eligibility");
        synchronized (root) {
            if (parent.isEmpty()) {
                return;
            }
            if (isEligibleToStartNext()) {
                parent.get().addOrUpdateSubGroup(this);
            }
            else {
                parent.get().eligibleSubGroups.remove(this);
                lastStartMillis = 0;
            }
            parent.get().updateEligibility();
        }
    }

    private void startInBackground(ManagedQueryExecution query)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to start a query");
        synchronized (root) {
            runningQueries.put(query, new ResourceUsage(0, 0));
            InternalResourceGroup group = this;
            long lastRunningQueryStartTimeMillis = currentTimeMillis();
            lastRunningQueryStartTime.set(lastRunningQueryStartTimeMillis);
            while (group.parent.isPresent()) {
                group.parent.get().lastRunningQueryStartTime.set(lastRunningQueryStartTimeMillis);
                group.parent.get().descendantRunningQueries++;
                group.parent.get().dirtySubGroups.add(group);
                group = group.parent.get();
            }
            updateEligibility();
            executor.execute(query::startWaitingForResources);
        }
    }

    public void updateGroupsAndProcessQueuedQueries()
    {
        synchronized (root) {
            updateResourceUsageAndGetDelta();

            while (internalStartNext()) {
                // start all the queries we can
            }
        }
    }

    public void generateCpuQuota(long elapsedSeconds)
    {
        synchronized (root) {
            if (elapsedSeconds > 0) {
                internalGenerateCpuQuota(elapsedSeconds);
            }
        }
    }

    @VisibleForTesting
    public void triggerProcessQueuedQueries()
    {
        updateGroupsAndProcessQueuedQueries();
    }

    private void queryFinished(ManagedQueryExecution query)
    {
        synchronized (root) {
            if (!runningQueries.containsKey(query) && !queuedQueries.contains(query)) {
                // Query has already been cleaned up
                return;
            }

            ResourceUsage lastUsage = runningQueries.get(query);

            // The query is present in runningQueries
            if (lastUsage != null) {
                // CPU is measured cumulatively (i.e. total CPU used until this moment by the query). Memory is measured
                // instantaneously (how much memory the query is using at this moment). At query completion, memory usage
                // drops to zero.
                ResourceUsage finalUsage = new ResourceUsage(
                        query.getTotalCpuTime().toMillis(),
                        0L);
                ResourceUsage delta = finalUsage.subtract(lastUsage);

                runningQueries.remove(query);

                // Update usage statistics up to the root
                InternalResourceGroup group = this;
                while (group != null) {
                    group.cachedResourceUsage = group.cachedResourceUsage.add(delta);
                    InternalResourceGroup parent = group.parent.orElse(null);
                    if (parent != null) {
                        parent.descendantRunningQueries--;
                        if (parent.descendantRunningQueries == 0) {
                            parent.dirtySubGroups.remove(group);
                        }
                    }
                    group = parent;
                }
            }
            else {
                // The query must be queued
                queuedQueries.remove(query);
                InternalResourceGroup group = this;
                while (group.parent.isPresent()) {
                    group.parent.get().descendantQueuedQueries--;
                    group = group.parent.get();
                }
            }

            updateEligibility();
            root.triggerProcessQueuedQueries();
        }
    }

    protected ResourceUsage updateResourceUsageAndGetDelta()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to refresh stats");
        synchronized (root) {
            ResourceUsage groupUsageDelta = new ResourceUsage(0, 0);

            if (subGroups.isEmpty()) {
                // Leaf resource group
                for (Map.Entry<ManagedQueryExecution, ResourceUsage> entry : runningQueries.entrySet()) {
                    ManagedQueryExecution query = entry.getKey();
                    ResourceUsage oldResourceUsage = entry.getValue();

                    ResourceUsage newResourceUsage = new ResourceUsage(
                            query.getTotalCpuTime().toMillis(),
                            query.getTotalMemoryReservation().toBytes());

                    // Compute delta and update usage
                    ResourceUsage queryUsageDelta = newResourceUsage.subtract(oldResourceUsage);
                    entry.setValue(newResourceUsage);
                    groupUsageDelta = groupUsageDelta.add(queryUsageDelta);
                }

                Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = getAdditionalRuntimeInfo();
                if (resourceGroupRuntimeInfo.isPresent()) {
                    groupUsageDelta = groupUsageDelta.add(new ResourceUsage(0L, resourceGroupRuntimeInfo.get().getMemoryUsageBytes()));
                }

                cachedResourceUsage = cachedResourceUsage.add(groupUsageDelta);
            }
            else {
                // Intermediate resource group
                for (Iterator<InternalResourceGroup> iterator = dirtySubGroups.iterator(); iterator.hasNext(); ) {
                    InternalResourceGroup subGroup = iterator.next();

                    ResourceUsage subGroupUsageDelta = subGroup.updateResourceUsageAndGetDelta();
                    groupUsageDelta = groupUsageDelta.add(subGroupUsageDelta);
                    cachedResourceUsage = cachedResourceUsage.add(subGroupUsageDelta);
                    if (!subGroup.isDirty()) {
                        iterator.remove();
                    }
                    if (!subGroupUsageDelta.equals(new ResourceUsage(0, 0)) || subGroup.isDirty.get()) {
                        subGroup.updateEligibility();
                        subGroup.isDirty.set(false);
                    }
                }
            }

            return groupUsageDelta;
        }
    }

    protected void internalGenerateCpuQuota(long elapsedSeconds)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to generate cpu quota");
        synchronized (root) {
            long newQuota = saturatedMultiply(elapsedSeconds, cpuQuotaGenerationMillisPerSecond);

            long oldUsageMillis = cachedResourceUsage.getCpuUsageMillis();
            long newCpuUsageMillis = saturatedSubtract(oldUsageMillis, newQuota);

            if (newCpuUsageMillis < 0 || newCpuUsageMillis == Long.MAX_VALUE) {
                newCpuUsageMillis = 0;
            }

            cachedResourceUsage = new ResourceUsage(newCpuUsageMillis, cachedResourceUsage.getMemoryUsageBytes());

            if ((newCpuUsageMillis < hardCpuLimitMillis && oldUsageMillis >= hardCpuLimitMillis) ||
                    (newCpuUsageMillis < softCpuLimitMillis && oldUsageMillis >= softCpuLimitMillis)) {
                updateEligibility();
            }

            for (InternalResourceGroup group : subGroups.values()) {
                group.internalGenerateCpuQuota(elapsedSeconds);
            }
        }
    }

    protected boolean internalStartNext()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to find next query");
        synchronized (root) {
            if (!canRunMore()) {
                return false;
            }
            ManagedQueryExecution query = queuedQueries.poll();
            if (query != null) {
                startInBackground(query);
                return true;
            }

            // Remove even if the sub group still has queued queries, so that it goes to the back of the queue
            InternalResourceGroup subGroup = eligibleSubGroups.poll();
            if (subGroup == null) {
                return false;
            }
            boolean started = subGroup.internalStartNext();
            checkState(started, "Eligible sub group had no queries to run");

            long currentTime = System.currentTimeMillis();
            if (lastStartMillis != 0) {
                timeBetweenStartsSec.update(Math.max(0, (currentTime - lastStartMillis) / 1000));
            }
            lastStartMillis = currentTime;

            descendantQueuedQueries--;
            // Don't call updateEligibility here, as we're in a recursive call, and don't want to repeatedly update our ancestors.
            if (subGroup.isEligibleToStartNext()) {
                addOrUpdateSubGroup(subGroup);
            }
            return true;
        }
    }

    private void addOrUpdateSubGroup(Queue<InternalResourceGroup> queue, InternalResourceGroup group)
    {
        if (schedulingPolicy == WEIGHTED_FAIR) {
            ((WeightedFairQueue<InternalResourceGroup>) queue).addOrUpdate(group, new Usage(group.getSchedulingWeight(), group.getRunningQueries()));
        }
        else {
            ((UpdateablePriorityQueue<InternalResourceGroup>) queue).addOrUpdate(group, getSubGroupSchedulingPriority(schedulingPolicy, group));
        }
    }

    private void addOrUpdateSubGroup(InternalResourceGroup group)
    {
        addOrUpdateSubGroup(eligibleSubGroups, group);
    }

    private static long getSubGroupSchedulingPriority(SchedulingPolicy policy, InternalResourceGroup group)
    {
        if (policy == QUERY_PRIORITY) {
            return group.getHighestQueryPriority();
        }
        else {
            return group.computeSchedulingWeight();
        }
    }

    private long computeSchedulingWeight()
    {
        if (runningQueries.size() + descendantRunningQueries >= softConcurrencyLimit) {
            return schedulingWeight;
        }

        return (long) Integer.MAX_VALUE * schedulingWeight;
    }

    private boolean isEligibleToStartNext()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            if (!canRunMore()) {
                return false;
            }
            return !queuedQueries.isEmpty() || !eligibleSubGroups.isEmpty();
        }
    }

    private int getHighestQueryPriority()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            checkState(queuedQueries instanceof IndexedPriorityQueue, "Queued queries not ordered");
            if (queuedQueries.isEmpty()) {
                return 0;
            }
            return getQueryPriority(queuedQueries.peek().getSession());
        }
    }

    private boolean canQueueMore()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = getAdditionalRuntimeInfo();
            if (resourceGroupRuntimeInfo.isPresent()) {
                return descendantQueuedQueries + queuedQueries.size() + resourceGroupRuntimeInfo.get().getQueuedQueries() + resourceGroupRuntimeInfo.get().getDescendantQueuedQueries() < maxQueuedQueries;
            }
            return descendantQueuedQueries + queuedQueries.size() < maxQueuedQueries;
        }
    }

    private boolean canRunMore()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            long cpuUsageMillis = cachedResourceUsage.getCpuUsageMillis();
            long memoryUsageBytes = cachedResourceUsage.getMemoryUsageBytes();

            if ((cpuUsageMillis >= hardCpuLimitMillis) || (memoryUsageBytes > softMemoryLimitBytes)) {
                return false;
            }

            if (shouldWaitForResourceManagerUpdate()) {
                return false;
            }

            int hardConcurrencyLimit = getHardConcurrencyLimitBasedOnCpuUsage();

            int totalRunningQueries = runningQueries.size() + descendantRunningQueries;

            Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = getAdditionalRuntimeInfo();
            if (resourceGroupRuntimeInfo.isPresent()) {
                totalRunningQueries += resourceGroupRuntimeInfo.get().getRunningQueries() + resourceGroupRuntimeInfo.get().getDescendantRunningQueries();
            }

            return totalRunningQueries < hardConcurrencyLimit;
        }
    }

    protected int getHardConcurrencyLimitBasedOnCpuUsage()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            int hardConcurrencyLimit = this.hardConcurrencyLimit;
            long cpuUsageMillis = cachedResourceUsage.getCpuUsageMillis();
            if (cpuUsageMillis >= softCpuLimitMillis) {
                // TODO: Consider whether cpu limit math should be performed on softConcurrency or hardConcurrency
                // Linear penalty between soft and hard limit
                double penalty = (cpuUsageMillis - softCpuLimitMillis) / (double) (hardCpuLimitMillis - softCpuLimitMillis);
                hardConcurrencyLimit = (int) Math.floor(hardConcurrencyLimit * (1 - penalty));
                // Always penalize by at least one
                hardConcurrencyLimit = min(this.hardConcurrencyLimit - 1, hardConcurrencyLimit);
                // Always allow at least one running query
                hardConcurrencyLimit = Math.max(1, hardConcurrencyLimit);
            }

            return hardConcurrencyLimit;
        }
    }

    public Collection<InternalResourceGroup> subGroups()
    {
        synchronized (root) {
            return subGroups.values();
        }
    }

    @VisibleForTesting
    ResourceUsage getResourceUsageSnapshot()
    {
        synchronized (root) {
            return cachedResourceUsage.clone();
        }
    }

    private boolean shouldWaitForResourceManagerUpdate()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return shouldWaitForResourceManagerUpdate.test(this);
        }
    }

    private Optional<ResourceGroupRuntimeInfo> getAdditionalRuntimeInfo()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return additionalRuntimeInfo.apply(getId());
        }
    }

    protected long getLastRunningQueryStartTime()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return lastRunningQueryStartTime.get();
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InternalResourceGroup)) {
            return false;
        }
        InternalResourceGroup that = (InternalResourceGroup) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }
}
