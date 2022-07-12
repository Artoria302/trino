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

import com.google.common.util.concurrent.ListeningExecutorService;
import io.trino.execution.resourcegroups.ResourceGroupRuntimeInfo;
import io.trino.server.BasicQueryInfo;
import io.trino.server.NodeStatus;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.memory.ClusterMemoryPoolInfo;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.List;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;

@Path("/v1/resourceManager")
public class ResourceManagerResource
{
    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final ListeningExecutorService executor;

    @Inject
    public ResourceManagerResource(ResourceManagerClusterStateProvider clusterStateProvider, @ForResourceManager ListeningExecutorService executor)
    {
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "internalNodeManager is null");
        this.executor = executor;
    }

    /**
     * This method registers a heartbeat to the resource manager.  A query heartbeat is used for the following purposes:
     * <p>
     * 1) Inform resource managers about current resource group utilization.
     * 2) Inform resource managers about current running queries.
     * 3) Inform resource managers about coordinator status and health.
     */
    @ResourceSecurity(INTERNAL_ONLY)
    @PUT
    @Path("hb/query/{nodeId}/{sequenceId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void queryHeartbeat(
            @PathParam("nodeId") String nodeId,
            BasicQueryInfo basicQueryInfo,
            @PathParam("sequenceId") long sequenceId,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        executor.execute(() -> {
            try {
                clusterStateProvider.registerQueryHeartbeat(nodeId, basicQueryInfo, sequenceId);
                asyncResponse.resume(Response.ok().build());
            }
            catch (Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    /**
     * Registers a node heartbeat with the resource manager.
     */
    @ResourceSecurity(INTERNAL_ONLY)
    @PUT
    @Path("hb/node")
    @Consumes(MediaType.APPLICATION_JSON)
    public void nodeHeartbeat(
            NodeStatus nodeStatus,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        executor.submit(() -> {
            try {
                clusterStateProvider.registerNodeHeartbeat(nodeStatus);
                asyncResponse.resume(Response.ok().build());
            }
            catch (Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @PUT
    @Path("hb/resourceGroup/{nodeId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void resourceGroupRuntimeHeartbeat(
            @PathParam("nodeId") String nodeId,
            List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        executor.execute(() -> {
            try {
                clusterStateProvider.registerResourceGroupRuntimeHeartbeat(nodeId, resourceGroupRuntimeInfos);
                asyncResponse.resume(Response.ok().build());
            }
            catch (Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    /**
     * Returns the resource group information across all clusters except for {@code excludingNode}, which is excluded
     * to prevent redundancy with local resource group information.
     */
    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("resourceGroup/{excludingNode}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getResourceGroupInfo(
            @PathParam("excludingNode") String excludingNode,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        executor.submit(() -> {
            try {
                List<ResourceGroupRuntimeInfo> clusterResourceGroups = clusterStateProvider.getClusterResourceGroups(excludingNode);
                asyncResponse.resume(Response.ok().entity(clusterResourceGroups).build());
            }
            catch (Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("clusterMemoryPoolInfo")
    @Produces(MediaType.APPLICATION_JSON)
    public void getMemoryPoolInfo(
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        executor.submit(() -> {
            try {
                ClusterMemoryPoolInfo info = clusterStateProvider.getClusterMemoryPoolInfo();
                asyncResponse.resume(Response.ok().entity(info).build());
            }
            catch (Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }
}
