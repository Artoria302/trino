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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.execution.ManagedQueryExecution;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.resourcegroups.InternalResourceGroup.RootInternalResourceGroup;
import io.trino.resourcemanager.ResourceGroupService;
import io.trino.server.ResourceGroupInfo;
import io.trino.server.ServerConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.memory.ClusterMemoryPoolManager;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManager;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerContext;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import io.trino.util.PeriodicTaskExecutor;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public final class InternalResourceGroupManager<C>
        implements ResourceGroupManager<C>
{
    private static final Logger log = Logger.get(InternalResourceGroupManager.class);

    private static final File CONFIG_FILE = new File("etc/resource-groups.properties");
    private static final String NAME_PROPERTY = "resource-groups.configuration-manager";

    private final ScheduledExecutorService refreshExecutor = newScheduledThreadPool(2, daemonThreadsNamed("ResourceGroupManager"));
    private final PeriodicTaskExecutor resourceGroupRuntimeExecutor;
    private final List<RootInternalResourceGroup> rootGroups = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<ResourceGroupId, InternalResourceGroup> groups = new ConcurrentHashMap<>();
    private final AtomicReference<ResourceGroupConfigurationManager<C>> configurationManager;
    private final ResourceGroupConfigurationManagerContext configurationManagerContext;
    private final ResourceGroupConfigurationManager<?> legacyManager;
    private final MBeanExporter exporter;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong lastCpuQuotaGenerationNanos = new AtomicLong(System.nanoTime());
    private final Map<String, ResourceGroupConfigurationManagerFactory> configurationManagerFactories = new ConcurrentHashMap<>();
    private final AtomicLong lastSchedulingCycleRunTimeMs = new AtomicLong(currentTimeMillis());
    private final ResourceGroupService resourceGroupService;
    private final AtomicReference<Map<ResourceGroupId, ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfos = new AtomicReference<>(ImmutableMap.of());
    private final AtomicReference<Map<ResourceGroupId, ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfosSnapshot = new AtomicReference<>(ImmutableMap.of());
    private final AtomicLong lastUpdatedResourceGroupRuntimeInfo = new AtomicLong(-1L);
    private final double concurrencyThreshold;
    private final Duration resourceGroupRuntimeInfoRefreshInterval;
    private final boolean isResourceManagerEnabled;

    @Inject
    public InternalResourceGroupManager(
            LegacyResourceGroupConfigurationManager legacyManager,
            ClusterMemoryPoolManager memoryPoolManager,
            NodeInfo nodeInfo,
            MBeanExporter exporter,
            ResourceGroupService resourceGroupService,
            QueryManagerConfig queryManagerConfig,
            ServerConfig serverConfig)
    {
        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.configurationManagerContext = new ResourceGroupConfigurationManagerContextInstance(memoryPoolManager, nodeInfo.getEnvironment());
        this.legacyManager = requireNonNull(legacyManager, "legacyManager is null");
        this.configurationManager = new AtomicReference<>(cast(legacyManager));
        this.resourceGroupService = requireNonNull(resourceGroupService, "resourceGroupService is null");
        this.concurrencyThreshold = queryManagerConfig.getConcurrencyThresholdToEnableResourceGroupRefresh();
        this.resourceGroupRuntimeInfoRefreshInterval = queryManagerConfig.getResourceGroupRunTimeInfoRefreshInterval();
        this.isResourceManagerEnabled = requireNonNull(serverConfig, "serverConfig is null").isResourceManagerEnabled();
        this.resourceGroupRuntimeExecutor = new PeriodicTaskExecutor(resourceGroupRuntimeInfoRefreshInterval.toMillis(), refreshExecutor, this::refreshResourceGroupRuntimeInfo);
    }

    @Override
    public Optional<ResourceGroupInfo> tryGetResourceGroupInfo(ResourceGroupId id)
    {
        InternalResourceGroup resourceGroup = groups.get(id);
        return Optional.ofNullable(resourceGroup)
                .map(InternalResourceGroup::getFullInfo);
    }

    @Override
    public Optional<List<ResourceGroupInfo>> tryGetPathToRoot(ResourceGroupId id)
    {
        InternalResourceGroup resourceGroup = groups.get(id);
        return Optional.ofNullable(resourceGroup)
                .map(InternalResourceGroup::getPathToRoot);
    }

    @Override
    public void submit(ManagedQueryExecution queryExecution, SelectionContext<C> selectionContext, Executor executor)
    {
        checkState(configurationManager.get() != null, "configurationManager not set");
        createGroupIfNecessary(selectionContext, executor);
        groups.get(selectionContext.getResourceGroupId()).run(queryExecution);
    }

    @Override
    public SelectionContext<C> selectGroup(SelectionCriteria criteria)
    {
        return configurationManager.get().match(criteria)
                .orElseThrow(() -> new TrinoException(QUERY_REJECTED, "Query did not match any selection rule"));
    }

    @Override
    public void addConfigurationManagerFactory(ResourceGroupConfigurationManagerFactory factory)
    {
        if (configurationManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Resource group configuration manager '%s' is already registered", factory.getName()));
        }
    }

    @Override
    public void loadConfigurationManager()
            throws Exception
    {
        File configFile = CONFIG_FILE.getAbsoluteFile();
        if (!configFile.exists()) {
            return;
        }

        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Resource groups configuration %s does not contain '%s'", configFile, NAME_PROPERTY);

        setConfigurationManager(name, properties);
    }

    @VisibleForTesting
    public void setConfigurationManager(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading resource group configuration manager --");

        ResourceGroupConfigurationManagerFactory factory = configurationManagerFactories.get(name);
        checkState(factory != null, "Resource group configuration manager '%s' is not registered", name);

        ResourceGroupConfigurationManager<C> configurationManager;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            configurationManager = cast(factory.create(ImmutableMap.copyOf(properties), configurationManagerContext));
        }

        checkState(this.configurationManager.compareAndSet(cast(legacyManager), configurationManager), "configurationManager already set");

        log.info("-- Loaded resource group configuration manager %s --", name);
    }

    @SuppressWarnings("ObjectEquality")
    @VisibleForTesting
    public ResourceGroupConfigurationManager<C> getConfigurationManager()
    {
        ResourceGroupConfigurationManager<C> manager = configurationManager.get();
        checkState(manager != legacyManager, "cannot fetch legacy manager");
        return manager;
    }

    @PreDestroy
    public void destroy()
    {
        refreshExecutor.shutdownNow();
        resourceGroupRuntimeExecutor.stop();
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            refreshExecutor.scheduleWithFixedDelay(() -> {
                try {
                    refreshAndStartQueries();
                    lastSchedulingCycleRunTimeMs.getAndSet(currentTimeMillis());
                }
                catch (Throwable t) {
                    log.error(t, "Error while executing refreshAndStartQueries");
                    throw t;
                }
            }, 1, 1, MILLISECONDS);
            if (isResourceManagerEnabled) {
                resourceGroupRuntimeExecutor.start();
            }
        }
    }

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupRuntimeInfos()
    {
        ImmutableList.Builder<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos = ImmutableList.builder();
        rootGroups.forEach(resourceGroup -> buildResourceGroupRuntimeInfo(resourceGroupRuntimeInfos, resourceGroup));
        return resourceGroupRuntimeInfos.build();
    }

    private void buildResourceGroupRuntimeInfo(ImmutableList.Builder<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos, InternalResourceGroup resourceGroup)
    {
        if (!resourceGroup.subGroups().isEmpty()) {
            resourceGroup.subGroups().stream().forEach(subGroup -> buildResourceGroupRuntimeInfo(resourceGroupRuntimeInfos, subGroup));
            return;
        }
        if (resourceGroup.getQueuedQueries() > 0 || resourceGroup.getRunningQueries() > 0) {
            ResourceGroupRuntimeInfo.Builder resourceGroupRuntimeInfo = ResourceGroupRuntimeInfo.builder(resourceGroup.getId());
            resourceGroupRuntimeInfo.addRunningQueries(resourceGroup.getRunningQueries());
            resourceGroupRuntimeInfo.addQueuedQueries(resourceGroup.getQueuedQueries());
            resourceGroupRuntimeInfo.setResourceGroupSpecInfo(new ResourceGroupSpecInfo(resourceGroup.getSoftConcurrencyLimit()));
            resourceGroupRuntimeInfos.add(resourceGroupRuntimeInfo.build());
        }
    }

    private void refreshResourceGroupRuntimeInfo()
    {
        try {
            List<ResourceGroupRuntimeInfo> resourceGroupInfos = resourceGroupService.getResourceGroupInfo();
            resourceGroupRuntimeInfos.set(resourceGroupInfos.stream().collect(toImmutableMap(ResourceGroupRuntimeInfo::getResourceGroupId, i -> i)));
            lastUpdatedResourceGroupRuntimeInfo.set(currentTimeMillis());
            boolean updatedSnapshot = updateResourceGroupsSnapshot();
            if (updatedSnapshot) {
                rootGroups.forEach(group -> group.setDirty());
            }
        }
        catch (Throwable t) {
            log.error(t, "Error while executing refreshAndStartQueries");
        }
    }

    private void refreshAndStartQueries()
    {
        long nanoTime = System.nanoTime();
        long elapsedSeconds = NANOSECONDS.toSeconds(nanoTime - lastCpuQuotaGenerationNanos.get());
        if (elapsedSeconds > 0) {
            // Only advance our clock on second boundaries to avoid calling generateCpuQuota() too frequently, and because it would be a no-op for zero seconds.
            lastCpuQuotaGenerationNanos.addAndGet(elapsedSeconds * 1_000_000_000L);
        }
        else if (elapsedSeconds < 0) {
            // nano time has overflowed
            lastCpuQuotaGenerationNanos.set(nanoTime);
        }
        for (InternalResourceGroup group : rootGroups) {
            try {
                if (elapsedSeconds > 0) {
                    group.generateCpuQuota(elapsedSeconds);
                }
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while generation cpu quota for %s", group);
            }
            try {
                group.updateGroupsAndProcessQueuedQueries();
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while processing queued queries for %s", group);
            }
        }
    }

    private boolean updateResourceGroupsSnapshot()
    {
        if (!isResourceManagerEnabled) {
            return false;
        }
        Map<ResourceGroupId, ResourceGroupRuntimeInfo> snapshotValue = resourceGroupRuntimeInfos.getAndAccumulate(
                resourceGroupRuntimeInfosSnapshot.get(),
                (current, update) -> current != update ? update : null);
        if (snapshotValue != null) {
            resourceGroupRuntimeInfosSnapshot.set(snapshotValue);
            return true;
        }
        return false;
    }

    @VisibleForTesting
    public Map<ResourceGroupId, ResourceGroupRuntimeInfo> getResourceGroupRuntimeInfosSnapshot()
    {
        return resourceGroupRuntimeInfosSnapshot.get();
    }

    private synchronized void createGroupIfNecessary(SelectionContext<C> context, Executor executor)
    {
        ResourceGroupId id = context.getResourceGroupId();
        if (!groups.containsKey(id)) {
            InternalResourceGroup group;
            if (id.getParent().isPresent()) {
                createGroupIfNecessary(configurationManager.get().parentGroupContext(context), executor);
                InternalResourceGroup parent = groups.get(id.getParent().get());
                requireNonNull(parent, "parent is null");
                group = parent.getOrCreateSubGroup(id.getLastSegment());
            }
            else {
                RootInternalResourceGroup root;
                if (!isResourceManagerEnabled) {
                    //root = new RootInternalResourceGroup(id.getSegments().get(0), this::exportGroup, executor, ignored -> Optional.empty(), rg -> false);
                    root = new RootInternalResourceGroup(id.getSegments().get(0), this::exportGroup, executor,  ignored -> Optional.empty(), rg -> false);
                }
                else {
                    root = new RootInternalResourceGroup(
                            id.getSegments().get(0),
                            this::exportGroup,
                            executor,
                            resourceGroupId -> Optional.ofNullable(resourceGroupRuntimeInfosSnapshot.get().get(resourceGroupId)),
                            rg -> shouldWaitForResourceManagerUpdate(
                                    rg,
                                    resourceGroupRuntimeInfosSnapshot::get,
                                    lastUpdatedResourceGroupRuntimeInfo::get,
                                    concurrencyThreshold));
                }
                group = root;
                rootGroups.add(root);
            }
            configurationManager.get().configure(group, context);
            checkState(groups.put(id, group) == null, "Unexpected existing resource group");
        }
    }

    private void exportGroup(InternalResourceGroup group, Boolean export)
    {
        try {
            if (export) {
                exporter.exportWithGeneratedName(group, InternalResourceGroup.class, group.getId().toString());
            }
            else {
                exporter.unexportWithGeneratedName(InternalResourceGroup.class, group.getId().toString());
            }
        }
        catch (JmxException e) {
            log.error(e, "Error %s resource group %s", export ? "exporting" : "unexporting", group.getId());
        }
    }

    private static boolean shouldWaitForResourceManagerUpdate(
            InternalResourceGroup resourceGroup,
            Supplier<Map<ResourceGroupId, ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfos,
            LongSupplier lastUpdatedResourceGroupRuntimeInfo,
            double concurrencyThreshold)
    {
        int hardConcurrencyLimit = resourceGroup.getHardConcurrencyLimitBasedOnCpuUsage();
        int totalRunningQueries = resourceGroup.getRunningQueries();
        ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfos.get().get(resourceGroup.getId());
        if (resourceGroupRuntimeInfo != null) {
            totalRunningQueries += resourceGroupRuntimeInfo.getRunningQueries() + resourceGroupRuntimeInfo.getDescendantRunningQueries();
        }
        return totalRunningQueries >= (hardConcurrencyLimit * concurrencyThreshold) && lastUpdatedResourceGroupRuntimeInfo.getAsLong() <= resourceGroup.getLastRunningQueryStartTime();
    }

    @Managed
    public int getQueriesQueuedOnInternal()
    {
        int queriesQueuedInternal = 0;
        for (InternalResourceGroup rootGroup : rootGroups) {
            synchronized (rootGroup) {
                queriesQueuedInternal += getQueriesQueuedOnInternal(rootGroup);
            }
        }

        return queriesQueuedInternal;
    }

    private int getQueriesQueuedOnInternal(InternalResourceGroup resourceGroup)
    {
        if (resourceGroup.subGroups().isEmpty()) {
            int queuedQueries = resourceGroup.getQueuedQueries();
            int runningQueries = resourceGroup.getRunningQueries();
            if (isResourceManagerEnabled) {
                ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfos.get().get(resourceGroup.getId());
                if (resourceGroupRuntimeInfo != null) {
                    queuedQueries += resourceGroupRuntimeInfo.getQueuedQueries();
                    runningQueries += resourceGroupRuntimeInfo.getRunningQueries();
                }
            }
            return Math.max(Math.min(queuedQueries, resourceGroup.getSoftConcurrencyLimit() - runningQueries), 0);
        }

        int queriesQueuedInternal = 0;
        for (InternalResourceGroup subGroup : resourceGroup.subGroups()) {
            queriesQueuedInternal += getQueriesQueuedOnInternal(subGroup);
        }

        return queriesQueuedInternal;
    }

    @Managed
    public long getLastSchedulingCycleRuntimeDelayMs()
    {
        return currentTimeMillis() - lastSchedulingCycleRunTimeMs.get();
    }

    @SuppressWarnings("unchecked")
    private static <C> ResourceGroupConfigurationManager<C> cast(ResourceGroupConfigurationManager<?> manager)
    {
        return (ResourceGroupConfigurationManager<C>) manager;
    }
}
