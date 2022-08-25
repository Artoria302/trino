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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.trino.metadata.YunZhouDynamicCatalogResponseHandler.CatalogResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.ResponseHandlerUtils.readResponseBytes;

public class YunZhouDynamicCatalogResponseHandler
        implements ResponseHandler<CatalogResponse, RuntimeException>
{
    public static final YunZhouDynamicCatalogResponseHandler CATALOG_RESPONSE_HANDLER = new YunZhouDynamicCatalogResponseHandler();

    public static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
    }

    public static YunZhouDynamicCatalogResponseHandler createCatalogResponseHandler()
    {
        return CATALOG_RESPONSE_HANDLER;
    }

    private YunZhouDynamicCatalogResponseHandler()
    {
    }

    @Override
    public CatalogResponse handleException(Request request, Exception exception)
            throws RuntimeException
    {
        throw propagate(request, exception);
    }

    @Override
    public CatalogResponse handle(Request request, Response response)
            throws RuntimeException
    {
        byte[] bytes = readResponseBytes(request, response);
        try {
            return objectMapper.readValue(bytes, CatalogResponse.class);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CatalogResponse
    {
        private Integer code;
        private String msg;
        private String msgDescribe;
        private String dateTime;
        private List<Map<String, String>> result;

        public CatalogResponse()
        {
        }

        public Integer getCode()
        {
            return code;
        }

        public void setCode(Integer code)
        {
            this.code = code;
        }

        public String getMsg()
        {
            return msg;
        }

        public void setMsg(String msg)
        {
            this.msg = msg;
        }

        public String getMsgDescribe()
        {
            return msgDescribe;
        }

        public void setMsgDescribe(String msgDescribe)
        {
            this.msgDescribe = msgDescribe;
        }

        public String getDateTime()
        {
            return dateTime;
        }

        public void setDateTime(String dateTime)
        {
            this.dateTime = dateTime;
        }

        public List<Map<String, String>> getResult()
        {
            return result;
        }

        public void setResult(List<Map<String, String>> result)
        {
            this.result = result;
        }
    }
}
