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
package io.trino.resourcemanager;

import io.airlift.units.Duration;
import io.trino.execution.ManagedQueryExecution;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.execution.resourcegroups.ResourceGroupRuntimeInfo;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.server.BasicQueryInfo;
import io.trino.server.NodeStatus;
import io.trino.server.ServerConfig;
import io.trino.server.StatusResource;
import io.trino.spi.QueryId;
import io.trino.util.PeriodicTaskExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ResourceManagerClusterStatusSender
        implements ClusterStatusSender
{
    private final ResourceManagerClient resourceManagerClient;
    private final InternalNodeManager internalNodeManager;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final Supplier<NodeStatus> statusSupplier;
    private final ScheduledExecutorService executor;
    private final Duration queryHeartbeatInterval;

    private final Map<QueryId, PeriodicTaskExecutor> queries = new ConcurrentHashMap<>();

    private final PeriodicTaskExecutor nodeHeartbeatSender;
    private final Optional<PeriodicTaskExecutor> resourceRuntimeHeartbeatSender;

    @Inject
    public ResourceManagerClusterStatusSender(
            @ForResourceManager ResourceManagerClient resourceManagerClient,
            InternalNodeManager internalNodeManager,
            StatusResource statusResource,
            @ForResourceManager ScheduledExecutorService executor,
            ResourceManagerConfig resourceManagerConfig,
            ServerConfig serverConfig,
            ResourceGroupManager<?> resourceGroupManager)
    {
        this(
                resourceManagerClient,
                internalNodeManager,
                requireNonNull(statusResource, "statusResource is null")::getStatus,
                executor,
                resourceManagerConfig,
                serverConfig,
                resourceGroupManager);
    }

    public ResourceManagerClusterStatusSender(
            ResourceManagerClient resourceManagerClient,
            InternalNodeManager internalNodeManager,
            Supplier<NodeStatus> statusResource,
            ScheduledExecutorService executor,
            ResourceManagerConfig resourceManagerConfig,
            ServerConfig serverConfig,
            ResourceGroupManager<?> resourceGroupManager)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerService is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.statusSupplier = requireNonNull(statusResource, "statusResource is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.queryHeartbeatInterval = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getQueryHeartbeatInterval();
        this.nodeHeartbeatSender = new PeriodicTaskExecutor(resourceManagerConfig.getNodeHeartbeatInterval().toMillis(), executor, this::sendNodeHeartbeat);
        this.resourceRuntimeHeartbeatSender = serverConfig.isCoordinator() ? Optional.of(
                new PeriodicTaskExecutor(resourceManagerConfig.getResourceGroupRuntimeHeartbeatInterval().toMillis(), executor, this::sendResourceGroupRuntimeHeartbeat)) : Optional.empty();
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
    }

    @PostConstruct
    public void init()
    {
        nodeHeartbeatSender.start();
        resourceRuntimeHeartbeatSender.ifPresent(PeriodicTaskExecutor::start);
    }

    @PreDestroy
    public void stop()
    {
        queries.values().forEach(PeriodicTaskExecutor::stop);
        if (nodeHeartbeatSender != null) {
            nodeHeartbeatSender.stop();
        }
        resourceRuntimeHeartbeatSender.ifPresent(PeriodicTaskExecutor::stop);
    }

    @Override
    public void registerQuery(ManagedQueryExecution queryExecution)
    {
        QueryId queryId = queryExecution.getBasicQueryInfo().getQueryId();
        queries.computeIfAbsent(queryId, unused -> {
            AtomicLong sequenceId = new AtomicLong();
            PeriodicTaskExecutor taskExecutor = new PeriodicTaskExecutor(
                    queryHeartbeatInterval.toMillis(),
                    executor,
                    () -> sendQueryHeartbeat(queryExecution, sequenceId.incrementAndGet()));
            taskExecutor.start();
            return taskExecutor;
        });
        queryExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queries.computeIfPresent(queryId, (unused, queryHeartbeatSender) -> {
                    queryHeartbeatSender.forceRun();
                    queryHeartbeatSender.stop();
                    return null;
                });
            }
        });
    }

    private void sendQueryHeartbeat(ManagedQueryExecution queryExecution, long sequenceId)
    {
        BasicQueryInfo basicQueryInfo = queryExecution.getBasicQueryInfo();
        String nodeIdentifier = internalNodeManager.getCurrentNode().getNodeIdentifier();
        getResourceManagers().forEach(uri ->
                resourceManagerClient.queryHeartbeat(uri, nodeIdentifier, basicQueryInfo, sequenceId));
    }

    private void sendNodeHeartbeat()
    {
        getResourceManagers().forEach(uri ->
                resourceManagerClient.nodeHeartbeat(uri, statusSupplier.get()));
    }

    private List<URI> getResourceManagers()
    {
        return internalNodeManager.getResourceManagers().stream()
                .map(InternalNode::getInternalUri)
                .collect(toImmutableList());
    }

    public void sendResourceGroupRuntimeHeartbeat()
    {
        List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos = resourceGroupManager.getResourceGroupRuntimeInfos();
        getResourceManagers().forEach(uri ->
                resourceManagerClient.resourceGroupRuntimeHeartbeat(uri, internalNodeManager.getCurrentNode().getNodeIdentifier(), resourceGroupRuntimeInfos));
    }
}
