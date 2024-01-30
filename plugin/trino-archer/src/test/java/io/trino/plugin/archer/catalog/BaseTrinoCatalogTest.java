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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.metastore.TableInfo;
import io.trino.plugin.archer.ArcherConfig;
import io.trino.plugin.archer.ArcherMetadata;
import io.trino.plugin.archer.ArcherSessionProperties;
import io.trino.plugin.archer.CommitTaskData;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.PartitionSpec;
import net.qihoo.archer.Schema;
import net.qihoo.archer.SortOrder;
import net.qihoo.archer.Table;
import net.qihoo.archer.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.metastore.TableInfo.ExtendedRelationType.TABLE;
import static io.trino.metastore.TableInfo.ExtendedRelationType.TRINO_VIEW;
import static io.trino.plugin.archer.ArcherSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.archer.ArcherUtil.quotedTableName;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(BaseTrinoCatalogTest.class);

    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new ArcherSessionProperties(
                    new ArcherConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    protected abstract TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations);

    protected Map<String, Object> defaultNamespaceProperties(String newNamespaceName)
    {
        return ImmutableMap.of();
    }

    @Test
    public void testCreateNamespaceWithLocation()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_create_namespace_with_location_" + randomNameSuffix();
        Map<String, Object> namespaceProperties = new HashMap<>(defaultNamespaceProperties(namespace));
        String namespaceLocation = (String) namespaceProperties.computeIfAbsent(LOCATION_PROPERTY, _ -> "local:///a/path/");
        namespaceProperties = ImmutableMap.copyOf(namespaceProperties);
        catalog.createNamespace(SESSION, namespace, namespaceProperties, new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        assertThat(catalog.listNamespaces(SESSION)).contains(namespace);
        assertThat(catalog.loadNamespaceMetadata(SESSION, namespace)).isEqualTo(namespaceProperties);
        assertThat(catalog.defaultTableLocation(SESSION, new SchemaTableName(namespace, "table"))).isEqualTo(namespaceLocation.replaceAll("/$", "") + "/table");
        catalog.dropNamespace(SESSION, namespace);
        assertThat(catalog.listNamespaces(SESSION)).doesNotContain(namespace);
    }

    @Test
    public void testNonLowercaseNamespace()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "testNonLowercaseNamespace" + randomNameSuffix();
        // Trino schema names are always lowercase (until https://github.com/trinodb/trino/issues/17)
        String schema = namespace.toLowerCase(ENGLISH);

        // Currently this is actually stored in lowercase by all Catalogs
        catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            assertThat(catalog.namespaceExists(SESSION, namespace)).as("catalog.namespaceExists(namespace)")
                    .isFalse();
            assertThat(catalog.namespaceExists(SESSION, schema)).as("catalog.namespaceExists(schema)")
                    .isTrue();
            assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                    // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                    .doesNotContain(namespace)
                    .contains(schema);

            // Test with ArcherMetadata, should the ConnectorMetadata implementation behavior depend on that class
            ConnectorMetadata archerMetadata = new ArcherMetadata(
                    PLANNER_CONTEXT.getTypeManager(),
                    CatalogHandle.fromId("archer:NORMAL:v12345"),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    connectorIdentity -> {
                        throw new UnsupportedOperationException();
                    });
            assertThat(archerMetadata.schemaExists(SESSION, namespace)).as("archerMetadata.schemaExists(namespace)")
                    .isFalse();
            assertThat(archerMetadata.schemaExists(SESSION, schema)).as("archerMetadata.schemaExists(schema)")
                    .isTrue();
            assertThat(archerMetadata.listSchemaNames(SESSION)).as("archerMetadata.listSchemaNames")
                    .doesNotContain(namespace)
                    .contains(schema);
        }
        finally {
            catalog.dropNamespace(SESSION, namespace);
        }
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_create_table_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        Map<String, String> tableProperties = Map.of("test_key", "test_value");
        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            String tableLocation = arbitraryTableLocation(catalog, SESSION, schemaTableName);
            catalog.newCreateTableTransaction(
                            SESSION,
                            schemaTableName,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            InvertedIndex.unindexed(),
                            tableLocation,
                            tableProperties)
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(new TableInfo(schemaTableName, TABLE));
            assertThat(catalog.listTables(SESSION, Optional.empty())).contains(new TableInfo(schemaTableName, TABLE));

            Table archerTable = catalog.loadTable(SESSION, schemaTableName);
            assertThat(archerTable.name()).isEqualTo(quotedTableName(schemaTableName));
            assertThat(archerTable.schema().columns().size()).isEqualTo(1);
            assertThat(archerTable.schema().columns().get(0).name()).isEqualTo("col1");
            assertThat(archerTable.schema().columns().get(0).type()).isEqualTo(Types.LongType.get());
            assertThat(archerTable.location()).isEqualTo(tableLocation);
            assertThat(archerTable.sortOrder().isUnsorted()).isEqualTo(true);
            assertThat(archerTable.properties()).containsAllEntriesOf(tableProperties);

            catalog.dropTable(SESSION, schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty()).stream().map(TableInfo::tableName).toList()).doesNotContain(schemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_rename_table_" + randomNameSuffix();
        String targetNamespace = "test_rename_table_" + randomNameSuffix();

        String table = "tableName";
        SchemaTableName sourceSchemaTableName = new SchemaTableName(namespace, table);
        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createNamespace(SESSION, targetNamespace, defaultNamespaceProperties(targetNamespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.newCreateTableTransaction(
                            SESSION,
                            sourceSchemaTableName,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            InvertedIndex.unindexed(),
                            arbitraryTableLocation(catalog, SESSION, sourceSchemaTableName),
                            ImmutableMap.of())
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(new TableInfo(sourceSchemaTableName, TABLE));

            // Rename within the same schema
            SchemaTableName targetSchemaTableName = new SchemaTableName(sourceSchemaTableName.getSchemaName(), "newTableName");
            catalog.renameTable(SESSION, sourceSchemaTableName, targetSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty()).stream().map(TableInfo::tableName).toList()).doesNotContain(sourceSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(new TableInfo(targetSchemaTableName, TABLE));

            // Move to a different schema
            sourceSchemaTableName = targetSchemaTableName;
            targetSchemaTableName = new SchemaTableName(targetNamespace, sourceSchemaTableName.getTableName());
            catalog.renameTable(SESSION, sourceSchemaTableName, targetSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace)).stream().map(TableInfo::tableName).toList()).doesNotContain(sourceSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(targetNamespace))).contains(new TableInfo(targetSchemaTableName, TABLE));

            catalog.dropTable(SESSION, targetSchemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
                catalog.dropNamespace(SESSION, targetNamespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespaces: %s, %s", namespace, targetNamespace);
            }
        }
    }

    @Test
    public void testUseUniqueTableLocations()
    {
        TrinoCatalog catalog = createTrinoCatalog(true);
        String namespace = "test_unique_table_locations_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        Map<String, Object> namespaceProperties = new HashMap<>(defaultNamespaceProperties(namespace));
        String namespaceLocation = (String) namespaceProperties.computeIfAbsent(
                LOCATION_PROPERTY,
                _ -> "local:///archer_catalog_test_rename_table_" + UUID.randomUUID());

        catalog.createNamespace(SESSION, namespace, namespaceProperties, new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            String location1 = catalog.defaultTableLocation(SESSION, schemaTableName);
            String location2 = catalog.defaultTableLocation(SESSION, schemaTableName);
            assertThat(location1)
                    .isNotEqualTo(location2);

            assertThat(location1)
                    .startsWith(namespaceLocation + "/");
            assertThat(location2)
                    .startsWith(namespaceLocation + "/");
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    @Test
    public void testView()
            throws IOException
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        Path tmpDirectory = Files.createTempDirectory("archer_catalog_test_create_view_");
        tmpDirectory.toFile().deleteOnExit();

        String namespace = "test_create_view_" + randomNameSuffix();
        String viewName = "viewName";
        String renamedViewName = "renamedViewName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, viewName);
        SchemaTableName renamedSchemaTableName = new SchemaTableName(namespace, renamedViewName);
        ConnectorViewDefinition viewDefinition = new ConnectorViewDefinition(
                "SELECT name FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createUnboundedVarcharType().getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(SESSION.getUser()),
                false,
                ImmutableList.of());

        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createView(SESSION, schemaTableName, viewDefinition, false);

            assertThat(catalog.listTables(SESSION, Optional.of(namespace)).stream()).contains(new TableInfo(schemaTableName, TRINO_VIEW));

            Map<SchemaTableName, ConnectorViewDefinition> views = catalog.getViews(SESSION, Optional.of(schemaTableName.getSchemaName()));
            assertThat(views.size()).isEqualTo(1);
            assertViewDefinition(views.get(schemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION, schemaTableName).orElseThrow(), viewDefinition);

            catalog.renameView(SESSION, schemaTableName, renamedSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace)).stream().map(TableInfo::tableName).toList()).doesNotContain(schemaTableName);
            views = catalog.getViews(SESSION, Optional.of(schemaTableName.getSchemaName()));
            assertThat(views.size()).isEqualTo(1);
            assertViewDefinition(views.get(renamedSchemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION, renamedSchemaTableName).orElseThrow(), viewDefinition);
            assertThat(catalog.getView(SESSION, schemaTableName)).isEmpty();

            catalog.dropView(SESSION, renamedSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty()).stream().map(TableInfo::tableName).toList())
                    .doesNotContain(renamedSchemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    @Test
    public void testListTables()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        TrinoPrincipal principal = new TrinoPrincipal(PrincipalType.USER, SESSION.getUser());

        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            String ns1 = "ns1" + randomNameSuffix();
            String ns2 = "ns2" + randomNameSuffix();
            catalog.createNamespace(SESSION, ns1, defaultNamespaceProperties(ns1), principal);
            closer.register(() -> catalog.dropNamespace(SESSION, ns1));
            catalog.createNamespace(SESSION, ns2, defaultNamespaceProperties(ns2), principal);
            closer.register(() -> catalog.dropNamespace(SESSION, ns2));

            SchemaTableName table1 = new SchemaTableName(ns1, "t1");
            SchemaTableName table2 = new SchemaTableName(ns2, "t2");
            catalog.newCreateTableTransaction(
                            SESSION,
                            table1,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            InvertedIndex.unindexed(),
                            arbitraryTableLocation(catalog, SESSION, table1),
                            ImmutableMap.of())
                    .commitTransaction();
            closer.register(() -> catalog.dropTable(SESSION, table1));

            catalog.newCreateTableTransaction(
                            SESSION,
                            table2,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            InvertedIndex.unindexed(),
                            arbitraryTableLocation(catalog, SESSION, table2),
                            ImmutableMap.of())
                    .commitTransaction();
            closer.register(() -> catalog.dropTable(SESSION, table2));

            // No namespace provided, all tables across all namespaces should be returned
            assertThat(catalog.listTables(SESSION, Optional.empty())).containsAll(ImmutableList.of(new TableInfo(table1, TABLE), new TableInfo(table2, TABLE)));
            // Namespace is provided and exists
            assertThat(catalog.listTables(SESSION, Optional.of(ns1))).containsExactly(new TableInfo(table1, TABLE));
            // Namespace is provided and does not exist
            assertThat(catalog.listTables(SESSION, Optional.of("non_existing"))).isEmpty();
        }
    }

    protected void assertViewDefinition(ConnectorViewDefinition actualView, ConnectorViewDefinition expectedView)
    {
        assertThat(actualView.getOriginalSql()).isEqualTo(expectedView.getOriginalSql());
        assertThat(actualView.getCatalog()).isEqualTo(expectedView.getCatalog());
        assertThat(actualView.getSchema()).isEqualTo(expectedView.getSchema());
        assertThat(actualView.getColumns().size()).isEqualTo(expectedView.getColumns().size());
        for (int i = 0; i < actualView.getColumns().size(); i++) {
            assertViewColumnDefinition(actualView.getColumns().get(i), expectedView.getColumns().get(i));
        }
        assertThat(actualView.getOwner()).isEqualTo(expectedView.getOwner());
        assertThat(actualView.isRunAsInvoker()).isEqualTo(expectedView.isRunAsInvoker());
    }

    private String arbitraryTableLocation(TrinoCatalog catalog, ConnectorSession session, SchemaTableName schemaTableName)
            throws Exception
    {
        try {
            return catalog.defaultTableLocation(session, schemaTableName);
        }
        catch (TrinoException e) {
            if (!e.getErrorCode().equals(HIVE_DATABASE_LOCATION_ERROR.toErrorCode())) {
                throw e;
            }
        }
        Path tmpDirectory = Files.createTempDirectory("archer_catalog_test_arbitrary_location");
        tmpDirectory.toFile().deleteOnExit();
        return tmpDirectory.toString();
    }

    private void assertViewColumnDefinition(ConnectorViewDefinition.ViewColumn actualViewColumn, ConnectorViewDefinition.ViewColumn expectedViewColumn)
    {
        assertThat(actualViewColumn.getName()).isEqualTo(expectedViewColumn.getName());
        assertThat(actualViewColumn.getType()).isEqualTo(expectedViewColumn.getType());
    }
}
