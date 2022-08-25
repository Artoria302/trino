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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class YunZhouDynamicCatalogStoreConfigTest
{
    private YunZhouDynamicCatalogStoreConfig config;

    @BeforeEach
    void setUp()
    {
        config = new YunZhouDynamicCatalogStoreConfig();
        config.setCatalogBaseUrl("http://localhost:8081/catalog");
        config.setTenant("lxh");
    }

    @Test
    void getCatalogConfigList()
    {
        final List<Map<String, String>> catalogConfigList = config.getCatalogConfigList();
        System.out.println(catalogConfigList);
    }

    @Test
    void getCatalogConfigOne()
    {
        final Map<String, String> xxx = config.getCatalogConfigOne("xxx");
        System.out.println(xxx);
    }
}
