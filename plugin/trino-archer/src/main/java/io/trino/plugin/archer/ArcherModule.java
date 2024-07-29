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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.plugin.archer.cache.ArcherCacheKeyProvider;
import io.trino.plugin.archer.procedure.ExpireSnapshotsTableProcedure;
import io.trino.plugin.archer.procedure.OptimizeTableProcedure;
import io.trino.plugin.archer.procedure.RemoveFilesTableProcedure;
import io.trino.plugin.archer.procedure.RemoveOrphanFilesTableProcedure;
import io.trino.plugin.archer.runtime.ArcherRuntimeManager;
import io.trino.plugin.archer.runtime.DefaultArcherRuntimeManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.metastore.thrift.TranslateHiveViews;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.procedure.Procedure;

import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ArcherModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ArcherTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, TranslateHiveViews.class)).toInstance(false);
        configBinder(binder).bindConfig(ArcherConfig.class);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(ArcherSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ArcherTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ArcherMaterializedViewProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(ArcherSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSourceProviderFactory.class).setDefault().to(ArcherPageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(ArcherPageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(ArcherPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(ArcherNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        binder.bind(ArcherMetadataFactory.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        binder.bind(ArcherFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ArcherFileWriterFactory.class).withGeneratedName();

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(RollbackToSnapshotProcedure.class).in(Scopes.SINGLETON);

        Multibinder<TableProcedureMetadata> tableProcedures = newSetBinder(binder, TableProcedureMetadata.class);
        tableProcedures.addBinding().toProvider(OptimizeTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(ExpireSnapshotsTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(RemoveOrphanFilesTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(RemoveFilesTableProcedure.class).in(Scopes.SINGLETON);

        binder.bind(ArcherRuntimeManager.class).to(DefaultArcherRuntimeManager.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, CacheKeyProvider.class).setBinding().to(ArcherCacheKeyProvider.class).in(Scopes.SINGLETON);

        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForArcherSplitManager.class));
    }

    @Provides
    @Singleton
    @ForArcherSplitManager
    public ExecutorService createSplitManagerExecutor(CatalogName catalogName, ArcherConfig config)
    {
        if (config.getSplitManagerThreads() == 0) {
            return newDirectExecutorService();
        }
        return newFixedThreadPool(
                config.getSplitManagerThreads(),
                daemonThreadsNamed("archer-split-manager-" + catalogName + "-%s"));
    }
}
