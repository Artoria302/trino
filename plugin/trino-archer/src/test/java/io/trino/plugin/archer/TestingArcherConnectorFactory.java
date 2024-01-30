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
package io.trino.plugin.archer;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.archer.ArcherConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingArcherConnectorFactory
        implements ConnectorFactory
{
    private final Optional<Module> archerCatalogModule;
    private final Module module;

    public TestingArcherConnectorFactory(Path localFileSystemRootPath)
    {
        this(localFileSystemRootPath, Optional.empty());
    }

    public TestingArcherConnectorFactory(
            Path localFileSystemRootPath,
            Optional<Module> archerCatalogModule)
    {
        boolean ignored = localFileSystemRootPath.toFile().mkdirs();
        this.archerCatalogModule = requireNonNull(archerCatalogModule, "archerCatalogModule is null");
        this.module = binder -> {
            newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                    .addBinding("local").toInstance(new LocalFileSystemFactory(localFileSystemRootPath));
            configBinder(binder).bindConfigDefaults(FileHiveMetastoreConfig.class, config -> config.setCatalogDirectory("local:///"));
        };
    }

    @Override
    public String getName()
    {
        return "archer";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        if (!config.containsKey("archer.catalog.type")) {
            config = ImmutableMap.<String, String>builder()
                    .putAll(config)
                    .put("archer.catalog.type", "TESTING_FILE_METASTORE")
                    .buildOrThrow();
        }
        return createConnector(catalogName, config, context, module, archerCatalogModule);
    }
}
