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
package io.trino.plugin.paimon;

import io.airlift.configuration.Config;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public class PaimonConfig
{
    private String warehouse;
    private boolean catalogLockEnabled;
    private CatalogType catalogType;
    private boolean localCacheEnabled = true;
    private int cacheNodeCount = 1;

    @Nullable
    public String getWarehouse()
    {
        return warehouse;
    }

    @Config("paimon.catalog.warehouse")
    public PaimonConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public boolean getCatalogLockEnabled()
    {
        return catalogLockEnabled;
    }

    @Config("paimon.catalog.lock-enabled")
    public PaimonConfig setCatalogLockEnabled(boolean catalogLockEnabled)
    {
        this.catalogLockEnabled = catalogLockEnabled;
        return this;
    }

    public boolean isLocalCacheEnabled()
    {
        return localCacheEnabled;
    }

    @NotNull
    public CatalogType getCatalogType()
    {
        return catalogType;
    }

    @Config("paimon.catalog.type")
    public PaimonConfig setCatalogType(CatalogType catalogType)
    {
        this.catalogType = catalogType;
        return this;
    }

    @Config("paimon.local-cache-enabled")
    public PaimonConfig setLocalCacheEnabled(boolean localCacheEnabled)
    {
        this.localCacheEnabled = localCacheEnabled;
        return this;
    }

    @Min(1)
    public int getCacheNodeCount()
    {
        return cacheNodeCount;
    }

    @Config("paimon.cache-node-count")
    public PaimonConfig setCacheNodeCount(int count)
    {
        this.cacheNodeCount = count;
        return this;
    }
}
