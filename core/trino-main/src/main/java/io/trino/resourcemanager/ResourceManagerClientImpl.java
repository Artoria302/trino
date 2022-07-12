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

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.json.JsonCodec;
import io.trino.execution.resourcegroups.ResourceGroupRuntimeInfo;
import io.trino.server.BasicQueryInfo;
import io.trino.server.NodeStatus;
import io.trino.spi.memory.ClusterMemoryPoolInfo;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;

public class ResourceManagerClientImpl
        implements ResourceManagerClient
{
    private final HttpClient httpClient;
    private final JsonCodec<BasicQueryInfo> basicQueryInfoCodec;
    private final JsonCodec<NodeStatus> nodeStatusCodec;
    private final JsonCodec<List<ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfoCodec;
    private final JsonCodec<ClusterMemoryPoolInfo> clusterMemoryPoolInfoCodec;
    private final ResourceManagerAddressSelector addressSelector;

    @Inject
    public ResourceManagerClientImpl(
            @ForResourceManager HttpClient httpClient,
            JsonCodec<BasicQueryInfo> basicQueryInfoCodec,
            JsonCodec<NodeStatus> nodeStatusCodec,
            JsonCodec<List<ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfoCodec,
            JsonCodec<ClusterMemoryPoolInfo> clusterMemoryPoolInfoCodec,
            ResourceManagerAddressSelector addressSelector)
    {
        this.httpClient = httpClient;
        this.basicQueryInfoCodec = basicQueryInfoCodec;
        this.nodeStatusCodec = nodeStatusCodec;
        this.resourceGroupRuntimeInfoCodec = resourceGroupRuntimeInfoCodec;
        this.clusterMemoryPoolInfoCodec = clusterMemoryPoolInfoCodec;
        this.addressSelector = addressSelector;
    }

    @Override
    public void queryHeartbeat(URI uri, String nodeId, BasicQueryInfo basicQueryInfo, long sequenceId)
    {
        byte[] json = basicQueryInfoCodec.toJsonBytes(basicQueryInfo);
        HttpUriBuilder uriBuilder = uriBuilderFrom(uri);
        uriBuilder.appendPath("/hb/query");
        uriBuilder.appendPath(nodeId);
        uriBuilder.appendPath(String.valueOf(sequenceId));
        Request request = preparePut()
                .setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(createStaticBodyGenerator(json))
                .build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Unexpected response code " + response.getStatusCode() + " from resource manager at " + uri);
        }
    }

    @Override
    public void nodeHeartbeat(URI uri, NodeStatus nodeStatus)
    {
        byte[] json = nodeStatusCodec.toJsonBytes(nodeStatus);
        HttpUriBuilder uriBuilder = uriBuilderFrom(uri);
        uriBuilder.appendPath("/hb/node");
        Request request = preparePut()
                .setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(createStaticBodyGenerator(json))
                .build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Unexpected response code " + response.getStatusCode() + " from resource manager at " + uri);
        }
    }

    @Override
    public void resourceGroupRuntimeHeartbeat(URI uri, String nodeId, List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo)
    {
        byte[] json = resourceGroupRuntimeInfoCodec.toJsonBytes(resourceGroupRuntimeInfo);
        HttpUriBuilder uriBuilder = uriBuilderFrom(uri);
        uriBuilder.appendPath("/hb/resourceGroup");
        uriBuilder.appendPath(nodeId);
        Request request = preparePut()
                .setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(createStaticBodyGenerator(json))
                .build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Unexpected response code " + response.getStatusCode() + " from resource manager at " + uri);
        }
    }

    @Override
    public ClusterMemoryPoolInfo getMemoryPoolInfo()
    {
        Optional<URI> uri = addressSelector.selectAddress();
        if (uri.isEmpty()) {
            throw new RuntimeException("no resource manager selected");
        }
        HttpUriBuilder uriBuilder = uriBuilderFrom(uri.get());
        uriBuilder.appendPath("/clusterMemoryPoolInfo");
        Request request = prepareGet()
                .setUri(uriBuilder.build())
                .build();
        return httpClient.execute(request, createJsonResponseHandler(clusterMemoryPoolInfoCodec));
    }

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo(String excludingNode)
            throws ResourceManagerInconsistentException
    {
        Optional<URI> uri = addressSelector.selectAddress();
        if (uri.isEmpty()) {
            throw new RuntimeException("no resource manager selected");
        }
        HttpUriBuilder uriBuilder = uriBuilderFrom(uri.get());
        uriBuilder.appendPath("/resourceGroup");
        uriBuilder.appendPath(excludingNode);
        Request request = prepareGet()
                .setUri(uriBuilder.build())
                .build();
        return httpClient.execute(request, createJsonResponseHandler(resourceGroupRuntimeInfoCodec));
    }
}
