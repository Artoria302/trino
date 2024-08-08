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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.plugin.paimon.cache.PaimonCacheKeyProvider;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Module for binding instance.
 */
public class PaimonModule
        implements Module
{
    public PaimonModule()
    {
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PaimonConfig.class);
        binder.bind(PaimonTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(PaimonMetadataFactory.class).in(SINGLETON);
        binder.bind(PaimonSplitManager.class).in(SINGLETON);
        binder.bind(PaimonPageSourceProvider.class).in(SINGLETON);
        binder.bind(PaimonSessionProperties.class).in(SINGLETON);
        binder.bind(PaimonTableOptions.class).in(SINGLETON);

        newOptionalBinder(binder, CacheKeyProvider.class).setBinding().to(PaimonCacheKeyProvider.class).in(Scopes.SINGLETON);
    }
}
