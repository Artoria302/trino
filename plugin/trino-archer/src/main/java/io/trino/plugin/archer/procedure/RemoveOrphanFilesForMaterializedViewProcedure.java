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
package io.trino.plugin.archer.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.archer.ArcherMetadata;
import io.trino.plugin.archer.CommitTaskData;
import io.trino.plugin.archer.ForArcherMetadata;
import io.trino.plugin.archer.catalog.TrinoCatalog;
import io.trino.plugin.archer.catalog.TrinoCatalogFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.TypeManager;
import net.qihoo.archer.BaseTable;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.ExecutorService;

import static io.trino.plugin.archer.procedure.ArcherTableProcedureId.REMOVE_ORPHAN_FILES;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class RemoveOrphanFilesForMaterializedViewProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REMOVE_ORPHAN_FILES_METHOD;

    static {
        try {
            REMOVE_ORPHAN_FILES_METHOD = lookup().unreflect(RemoveOrphanFilesForMaterializedViewProcedure.class.getMethod("removeOrphanFiles", ConnectorSession.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TrinoCatalogFactory catalogFactory;
    private final ClassLoader classLoader;
    private final TypeManager typeManager;
    private final CatalogHandle trinoCatalogHandle;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ExecutorService metadataExecutorService;

    @Inject
    public RemoveOrphanFilesForMaterializedViewProcedure(
            TrinoCatalogFactory catalogFactory,
            TypeManager typeManager,
            CatalogHandle trinoCatalogHandle,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoFileSystemFactory fileSystemFactory,
            @ForArcherMetadata ExecutorService metadataExecutorService)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        // this class is loaded by PluginClassLoader and we need its reference to be stored
        this.classLoader = getClass().getClassLoader();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.trinoCatalogHandle = requireNonNull(trinoCatalogHandle, "trinoCatalogHandle is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.metadataExecutorService = requireNonNull(metadataExecutorService, "metadataExecutorService is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "remove_orphan_files_for_materialized_view",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA", VARCHAR),
                        new Procedure.Argument("VIEW", VARCHAR),
                        new Procedure.Argument("RETENTION_THRESHOLD", VARCHAR)),
                REMOVE_ORPHAN_FILES_METHOD.bindTo(this));
    }

    public void removeOrphanFiles(ConnectorSession session, String schema, String view, String retentionThreshold)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            SchemaTableName viewName = new SchemaTableName(schema, view);
            TrinoCatalog catalog = catalogFactory.create(session.getIdentity());
            BaseTable archerTable = catalog.getMaterializedViewStorageTable(session, viewName)
                    .orElseThrow(() -> new TrinoException(TABLE_NOT_FOUND, "Storage table metadata not found for materialized view " + viewName));
            ArcherTableExecuteHandle executeHandle = new ArcherTableExecuteHandle(
                    viewName,
                    REMOVE_ORPHAN_FILES,
                    new ArcherRemoveOrphanFilesHandle(Duration.valueOf(retentionThreshold)),
                    archerTable.location());
            ArcherMetadata metadata = new ArcherMetadata(typeManager, trinoCatalogHandle, commitTaskCodec, catalog, fileSystemFactory, metadataExecutorService);
            metadata.executeRemoveOrphanFiles(session, archerTable, executeHandle);
        }
    }
}
