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
package io.trino.plugin.udf;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class UdfProviderConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "udf_provider";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        return createConnector(catalogName, config, context, EMPTY_MODULE);
    }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module)
    {
        ClassLoader classLoader = UdfProviderConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new ConnectorObjectNameGeneratorModule("io.trino.plugin.udf", "trino.plugin.udf"),
                    new JsonModule(),
                    new UdfProviderModule(),
                    new FileSystemModule(catalogName, context.getNodeManager(), context.getOpenTelemetry()),
                    binder -> {
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                        binder.bind(CacheManager.class).toInstance(context.getCacheManager());
                    },
                    module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            UdfProviderConfig udfProviderConfig = injector.getInstance(UdfProviderConfig.class);
            TrinoFileSystemFactory fileSystemFactory = injector.getInstance(TrinoFileSystemFactory.class);

            return new UdfProviderConnector(udfProviderConfig, fileSystemFactory);
        }
    }
}
