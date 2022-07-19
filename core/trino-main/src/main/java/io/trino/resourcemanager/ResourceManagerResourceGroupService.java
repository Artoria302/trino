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

import com.google.common.cache.CacheLoader;
import io.airlift.units.Duration;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.execution.resourcegroups.ResourceGroupRuntimeInfo;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ResourceManagerResourceGroupService
        implements ResourceGroupService
{
    private final ResourceManagerClient resourceManagerClient;
    private final InternalNodeManager internalNodeManager;
    private final Function<InternalNode, List<ResourceGroupRuntimeInfo>> cache;
    private final Executor executor = Executors.newCachedThreadPool();
    private final Boolean resourceGroupServiceCacheEnable;

    @Inject
    public ResourceManagerResourceGroupService(
            @ForResourceManager ResourceManagerClient resourceManagerClient,
            ResourceManagerConfig resourceManagerConfig,
            InternalNodeManager internalNodeManager)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerService is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        Duration cacheExpireDuration = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getResourceGroupServiceCacheExpireInterval();
        Duration cacheRefreshDuration = resourceManagerConfig.getResourceGroupServiceCacheRefreshInterval();
        resourceGroupServiceCacheEnable = resourceManagerConfig.getResourceGroupServiceCacheEnabled();
        if (resourceGroupServiceCacheEnable) {
            this.cache = EvictableCacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireDuration.roundTo(MILLISECONDS), MILLISECONDS)
                    .refreshAfterWrite(cacheRefreshDuration.roundTo(MILLISECONDS), MILLISECONDS)
                    .build(asyncReloading(new CacheLoader<InternalNode, List<ResourceGroupRuntimeInfo>>()
                    {
                        @Override
                        public List<ResourceGroupRuntimeInfo> load(InternalNode internalNode)
                                throws ResourceManagerInconsistentException
                        {
                            return getResourceGroupInfos(internalNode);
                        }
                    }, executor));
        }
        else {
            this.cache = this::getResourceGroupInfos;
        }
    }

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo()
            throws ResourceManagerInconsistentException
    {
        InternalNode currentNode = internalNodeManager.getCurrentNode();
        return cache.apply(currentNode);
    }

    private List<ResourceGroupRuntimeInfo> getResourceGroupInfos(InternalNode internalNode)
            throws ResourceManagerInconsistentException
    {
        return resourceManagerClient.getResourceGroupInfo(internalNode.getNodeIdentifier());
    }
}
