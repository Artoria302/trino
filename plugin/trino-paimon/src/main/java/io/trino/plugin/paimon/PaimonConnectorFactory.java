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
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.paimon.catalog.PaimonCatalogModule;
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

/**
 * Trino {@link ConnectorFactory}.
 */
public class PaimonConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "paimon";
    }

    @Override
    public Connector create(
            String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return create(catalogName, config, context, new EmptyModule());
    }

    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module)
    {
        ClassLoader classLoader = PaimonConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new ConnectorObjectNameGeneratorModule("io.trino.plugin.paimon", "trino.plugin.paimon"),
                    new JsonModule(),
                    new PaimonSecurityModule(),
                    new PaimonModule(),
                    new FileSystemModule(catalogName, context.getNodeManager(), context.getOpenTelemetry()),
                    new PaimonCatalogModule(),
                    binder -> {
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(OrcReaderConfig.class).toInstance(new OrcReaderConfig());
                        binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                        binder.bind(CacheManager.class).toInstance(context.getCacheManager());
                    },
                    module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            PaimonTransactionManager transactionManager = injector.getInstance(PaimonTransactionManager.class);
            PaimonSplitManager paimonSplitManager = injector.getInstance(PaimonSplitManager.class);
            PaimonPageSourceProvider paimonPageSourceProvider = injector.getInstance(PaimonPageSourceProvider.class);
            PaimonSessionProperties paimonSessionProperties = injector.getInstance(PaimonSessionProperties.class);
            PaimonTableOptions paimonTableOptions = injector.getInstance(PaimonTableOptions.class);

            return new PaimonConnector(
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(paimonSplitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(
                            paimonPageSourceProvider, classLoader),
                    paimonTableOptions,
                    paimonSessionProperties);
        }
    }

    /**
     * Empty module for paimon connector factory.
     */
    public static class EmptyModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}
    }
}
