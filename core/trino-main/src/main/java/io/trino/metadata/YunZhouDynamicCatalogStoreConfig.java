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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.trino.metadata.YunZhouDynamicCatalogResponseHandler.CatalogResponse;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.metadata.YunZhouDynamicCatalogResponseHandler.createCatalogResponseHandler;

public class YunZhouDynamicCatalogStoreConfig
{
    private static final Logger log = Logger.get(YunZhouDynamicCatalogStoreConfig.class);

    private Optional<String> catalogBaseUrl = Optional.empty();

    private Optional<String> tenant = Optional.empty();

    public Optional<String> getCatalogBaseUrl()
    {
        return catalogBaseUrl;
    }

    @Config("yz.catalog-base-url")
    public YunZhouDynamicCatalogStoreConfig setCatalogBaseUrl(String catalogBaseUrl)
    {
        this.catalogBaseUrl = Optional.of(catalogBaseUrl);
        return this;
    }

    public Optional<String> getTenant()
    {
        return tenant;
    }

    @Config("yz.tenant")
    public YunZhouDynamicCatalogStoreConfig setTenant(String tenant)
    {
        this.tenant = Optional.of(tenant);
        return this;
    }

    public String getCatalogOneUrl(String catalogName)
    {
        return catalogBaseUrl.map(u -> u + "/" + tenant.get() + "/" + catalogName).orElse(null);
    }

    public String getCatalogListUrl()
    {
        return catalogBaseUrl.map(u -> u + "/" + tenant.get()).orElse(null);
    }

    public List<Map<String, String>> getCatalogConfigList()
    {
        final String url = getCatalogListUrl();
        if (Objects.nonNull(url)) {
            return httpRequest(url);
        }
        return ImmutableList.of();
    }

    public Map<String, String> getCatalogConfigOne(String catalogName)
    {
        final String url = getCatalogOneUrl(catalogName);
        if (Objects.nonNull(url)) {
            final List<Map<String, String>> list = httpRequest(url);
            if (list == null || list.isEmpty()) {
                return null;
            }
            else {
                return list.get(0);
            }
        }
        return ImmutableMap.of();
    }

    private List<Map<String, String>> httpRequest(String url)
    {
        log.debug("request catalog url [%s]", url);
        try (JettyHttpClient httpClient = new JettyHttpClient()) {
            Request request = Request.Builder
                    .prepareGet()
                    .setUri(new URI(url))
                    .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .build();
            final CatalogResponse catalogResponse = httpClient.execute(request, createCatalogResponseHandler());
            if (catalogResponse.getCode() == 200) {
                return catalogResponse.getResult();
            }
            else {
                log.error("request catalog url [%s] failure. msg : %s", url, catalogResponse.getMsg());
                return null;
            }
        }
        catch (Exception e) {
            log.error(e, "request catalog url [%s] error.", url);
            return null;
        }
    }
}
