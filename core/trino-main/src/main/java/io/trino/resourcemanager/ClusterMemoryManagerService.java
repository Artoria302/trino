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

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.trino.memory.NodeMemoryConfig;
import io.trino.spi.memory.ClusterMemoryPoolInfo;
import io.trino.spi.memory.MemoryPoolInfo;
import io.trino.util.PeriodicTaskExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class ClusterMemoryManagerService
{
    private static final ClusterMemoryPoolInfo EMPTY_MEMORY_POOL = new ClusterMemoryPoolInfo(
            new MemoryPoolInfo(
                    0, 0, 0,
                    ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()),
            0,
            0);

    private final HttpClient httpClient;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<ClusterMemoryPoolInfo> memoryPool;
    private final long memoryPoolFetchIntervalMillis;
    private final PeriodicTaskExecutor memoryPoolUpdater;

    @Inject
    public ClusterMemoryManagerService(
            @ForResourceManager HttpClient httpClient,
            @ForResourceManager ScheduledExecutorService executorService,
            ResourceManagerConfig resourceManagerConfig,
            NodeMemoryConfig nodeMemoryConfig)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.memoryPoolFetchIntervalMillis = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getMemoryPoolFetchInterval().toMillis();

        this.memoryPool = new AtomicReference<>(EMPTY_MEMORY_POOL);
        this.memoryPoolUpdater = new PeriodicTaskExecutor(memoryPoolFetchIntervalMillis, executorService, () -> memoryPool.set(updateMemoryPoolInfo()));
    }

    @PostConstruct
    public void init()
    {
        memoryPoolUpdater.start();
    }

    @PreDestroy
    public void stop()
    {
        memoryPoolUpdater.stop();
    }

    public ClusterMemoryPoolInfo getMemoryPoolInfo()
    {
        return memoryPool.get();
    }

    private ClusterMemoryPoolInfo updateMemoryPoolInfo()
    {
        ClusterMemoryPoolInfo memoryPoolInfo = null;
        return memoryPoolInfo;
    }
}
