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
package io.trino.plugin.archer.catalog;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.archer.ArcherConfig;
import io.trino.plugin.archer.CatalogType;
import io.trino.plugin.archer.catalog.file.ArcherFileMetastoreCatalogModule;
import io.trino.plugin.archer.catalog.hms.ArcherHiveMetastoreCatalogModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.trino.plugin.archer.CatalogType.HIVE_METASTORE;
import static io.trino.plugin.archer.CatalogType.TESTING_FILE_METASTORE;

public class ArcherCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindCatalogModule(HIVE_METASTORE, new ArcherHiveMetastoreCatalogModule());
        bindCatalogModule(TESTING_FILE_METASTORE, new ArcherFileMetastoreCatalogModule());
    }

    private void bindCatalogModule(CatalogType catalogType, Module module)
    {
        install(conditionalModule(
                ArcherConfig.class,
                config -> config.getCatalogType() == catalogType,
                module));
    }
}
