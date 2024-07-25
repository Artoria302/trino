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
package io.trino.localcache;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.localcache.filemerge.FileMergeCacheConfig;
import io.trino.localcache.filemerge.FileMergeCacheManager;
import io.trino.localcache.filemerge.FileMergeCacheStats;
import io.trino.spi.localcache.CacheManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CacheManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(CacheConfig.class);
        CacheConfig cacheConfig = buildConfigObject(CacheConfig.class);
        if (cacheConfig.isCachingEnabled()) {
            bindCacheManager(binder, cacheConfig);
        }
        else {
            binder.bind(CacheManager.class).to(NoOpCacheManager.class).in(Scopes.SINGLETON);
        }
    }

    private void bindCacheManager(Binder binder, CacheConfig cacheConfig)
    {
        switch (cacheConfig.getCacheType()) {
            case FILE_MERGE -> {
                configBinder(binder).bindConfig(FileMergeCacheConfig.class);
                binder.bind(FileMergeCacheStats.class).in(Scopes.SINGLETON);
                newExporter(binder).export(FileMergeCacheStats.class).withGeneratedName();
                binder.bind(CacheManager.class).to(FileMergeCacheManager.class).in(Scopes.SINGLETON);
            }
            case NONE -> binder.bind(CacheManager.class).to(NoOpCacheManager.class).in(Scopes.SINGLETON);
        }
    }
}
