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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.trino.execution.QueryState;
import io.trino.security.AccessControl;
import io.trino.server.BasicQueryInfo;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ProtocolConfig;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.security.AccessControlUtil.filterQueries;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/query")
public class DistributedQueryResource
{
    // Sort returned queries: RUNNING - first, then QUEUED, then other non-completed, then FAILED and in each group we sort by create time.
    private static final Comparator<BasicQueryInfo> QUERIES_ORDERING = Ordering.<BasicQueryInfo>from((o2, o1) -> Boolean.compare(o1.getState() == QueryState.RUNNING, o2.getState() == QueryState.RUNNING))
            .compound((o1, o2) -> Boolean.compare(o1.getState() == QueryState.QUEUED, o2.getState() == QueryState.QUEUED))
            .compound((o1, o2) -> Boolean.compare(!o1.getState().isDone(), !o2.getState().isDone()))
            .compound((o1, o2) -> Boolean.compare(o1.getState() == QueryState.FAILED, o2.getState() == QueryState.FAILED))
            .compound(Comparator.comparing(item -> item.getQueryStats().getCreateTime()));

    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final ResourceManagerProxy proxyHelper;
    private final AccessControl accessControl;
    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final Optional<String> alternateHeaderName;

    @Inject
    public DistributedQueryResource(ResourceManagerClusterStateProvider clusterStateProvider, ResourceManagerProxy proxyHelper,
            AccessControl accessControl, HttpRequestSessionContextFactory sessionContextFactory, ProtocolConfig protocolConfig)
    {
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "nodeStateManager is null");
        this.proxyHelper = requireNonNull(proxyHelper, "proxyHelper is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @GET
    public Response getAllQueryInfo(
            @QueryParam("state") String stateFilter,
            @QueryParam("limit") Integer limitFilter,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));
        List<BasicQueryInfo> queries;
        int limit = firstNonNull(limitFilter, Integer.MAX_VALUE);
        if (limit <= 0) {
            throw new WebApplicationException(Response
                    .status(BAD_REQUEST)
                    .type(MediaType.TEXT_PLAIN)
                    .entity(format("Parameter 'limit' for getAllQueryInfo must be positive. Got %d.", limit))
                    .build());
        }

        queries = clusterStateProvider.getClusterQueries();
        queries = filterQueries(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders, alternateHeaderName), queries, accessControl);

        if (stateFilter != null) {
            ImmutableList.Builder<BasicQueryInfo> builder = ImmutableList.builder();
            for (BasicQueryInfo queryInfo : queries) {
                if (queryInfo.getState() == expectedState) {
                    builder.add(queryInfo);
                }
            }
            queries = builder.build();
        }
        queries = new ArrayList<>(queries);
        if (limit < queries.size()) {
            queries.sort(QUERIES_ORDERING);
        }
        else {
            limit = queries.size();
        }
        queries = ImmutableList.copyOf(queries.subList(0, limit));
        return Response.ok(queries).build();
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @GET
    @Path("{queryId}")
    public void getQueryInfo(
            @PathParam("queryId") QueryId queryId,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        proxyResponse(servletRequest, asyncResponse, uriInfo, queryId);
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @DELETE
    @Path("{queryId}")
    public void cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        proxyResponse(servletRequest, asyncResponse, uriInfo, queryId);
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @PUT
    @Path("{queryId}/killed")
    public void killQuery(
            @PathParam("queryId") QueryId queryId,
            String message,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        proxyResponse(servletRequest, asyncResponse, uriInfo, queryId);
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @PUT
    @Path("{queryId}/preempted")
    public void preemptQuery(
            @PathParam("queryId") QueryId queryId,
            String message,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        proxyResponse(servletRequest, asyncResponse, uriInfo, queryId);
    }

    private void proxyResponse(HttpServletRequest servletRequest, AsyncResponse asyncResponse, UriInfo uriInfo, QueryId queryId)
    {
        Optional<BasicQueryInfo> queryInfo = clusterStateProvider.getClusterQueries().stream()
                .filter(query -> query.getQueryId().equals(queryId))
                .findFirst();

        if (queryInfo.isEmpty()) {
            asyncResponse.resume(Response.status(NOT_FOUND).type(APPLICATION_JSON_TYPE).build());
            return;
        }

        proxyHelper.performRequest(servletRequest, asyncResponse, uriBuilderFrom(queryInfo.get().getSelf()).replacePath(uriInfo.getPath()).build());
    }
}
