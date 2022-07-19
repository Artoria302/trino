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

import io.trino.execution.resourcegroups.ResourceGroupRuntimeInfo;
import io.trino.server.BasicQueryInfo;
import io.trino.server.NodeStatus;
import io.trino.spi.memory.ClusterMemoryPoolInfo;

import java.net.URI;
import java.util.List;

public interface ResourceManagerClient
{
    void queryHeartbeat(URI uri, String nodeId, BasicQueryInfo basicQueryInfo, long sequenceId);

    void nodeHeartbeat(URI uri, NodeStatus nodeStatus);

    void resourceGroupRuntimeHeartbeat(URI uri, String nodeId, List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo);

    ClusterMemoryPoolInfo getMemoryPoolInfo();

    List<ResourceGroupRuntimeInfo> getResourceGroupInfo(String excludingNode)
            throws ResourceManagerInconsistentException;
}
