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
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArcherPlugin
{
    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getConnectorFactory();
        // simplest possible configuration
        factory.create("test", Map.of("hive.metastore.uri", "thrift://foo:1234"), new TestingConnectorContext()).shutdown();
    }

    @Test
    public void testThriftMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        Map.of(
                                "archer.catalog.type", "HIVE_METASTORE",
                                "hive.metastore.uri", "thrift://foo:1234",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testHiveMetastoreRejected()
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                "test",
                Map.of(
                        "hive.metastore", "thrift",
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Configuration property 'hive.metastore' was not used");
    }

    @Test
    public void testAllowAllAccessControl()
    {
        ConnectorFactory connectorFactory = getConnectorFactory();

        connectorFactory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("archer.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("archer.security", "allow-all")
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testReadOnlyAllAccessControl()
    {
        ConnectorFactory connectorFactory = getConnectorFactory();

        connectorFactory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("archer.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("archer.security", "read-only")
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testSystemAccessControl()
    {
        ConnectorFactory connectorFactory = getConnectorFactory();

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("archer.catalog.type", "HIVE_METASTORE")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("archer.security", "system")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThatThrownBy(connector::getAccessControl).isInstanceOf(UnsupportedOperationException.class);
        connector.shutdown();
    }

    @Test
    public void testFileBasedAccessControl()
            throws Exception
    {
        ConnectorFactory connectorFactory = getConnectorFactory();
        File tempFile = File.createTempFile("test-archer-plugin-access-control", ".json");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), "{}".getBytes(UTF_8));

        connectorFactory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("archer.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("archer.security", "file")
                                .put("bootstrap.quiet", "true")
                                .put("security.config-file", tempFile.getAbsolutePath())
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testArcherPluginFailsWhenIncorrectPropertyProvided()
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                        "test",
                        Map.of(
                                "archer.catalog.type", "HIVE_METASTORE",
                                "hive.hive-views.enabled", "true",
                                "hive.metastore.uri", "thrift://foo:1234",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown())
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Configuration property 'hive.hive-views.enabled' was not used");
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new ArcherPlugin().getConnectorFactories());
    }
}
