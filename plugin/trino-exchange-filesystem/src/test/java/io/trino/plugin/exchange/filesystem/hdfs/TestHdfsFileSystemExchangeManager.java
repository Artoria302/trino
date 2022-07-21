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
package io.trino.plugin.exchange.filesystem.hdfs;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.plugin.exchange.filesystem.AbstractTestExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.plugin.exchange.filesystem.hdfs.containers.HiveHadoop;
import io.trino.spi.exchange.ExchangeManager;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestHdfsFileSystemExchangeManager
        extends AbstractTestExchangeManager
{
    private HiveHadoop hiveHadoop;

    private String baseDir;

    @Override
    protected ExchangeManager createExchangeManager()
    {
        Path hadoopCoreSiteXmlTempFile = null;
        try {
            hadoopCoreSiteXmlTempFile = createHadoopCoreSiteXmlTempFile();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        hiveHadoop = HiveHadoop.builder()
                .withImage(HiveHadoop.HIVE3_IMAGE)
                .withNetwork(Network.newNetwork())
                .withFilesToMount(ImmutableMap.of(
                        "/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.toString()))
                .build();
        hiveHadoop.start();

        baseDir = "hdfs:///user/hadoop";

        hiveHadoop.executeInContainerFailOnError("hadoop", "fs", "-mkdir", "-p", baseDir);

        String dir1 = baseDir + "/trino-hdfs-file-system-exchange-manager-1";
        String dir2 = baseDir + "/trino-hdfs-file-system-exchange-manager-2";
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("exchange.base-directories", dir1 + "," + dir2)
                .put("exchange.encryption-enabled", "false")
                // to trigger file split in some tests
                .put("exchange.sink-max-file-size", "16MB")
                .put("exchange.hdfs.block-size", "4MB")
                .buildOrThrow();
        return new FileSystemExchangeManagerFactory().create(config);
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        if (hiveHadoop != null) {
            hiveHadoop.executeInContainerFailOnError("hadoop", "fs", "-rm", "-f", "-r", baseDir);
            hiveHadoop.stop();
            hiveHadoop = null;
        }
    }

    private Path createHadoopCoreSiteXmlTempFile()
            throws Exception
    {
        String coreSiteXmlContent = Resources.toString(Resources.getResource("containers/hive_hadoop/hdp3.1-core-site.xml.template"), UTF_8);

        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path coreSiteXml = Files.createTempFile("core-site", ".xml", posixFilePermissions);
        coreSiteXml.toFile().deleteOnExit();
        Files.write(coreSiteXml, coreSiteXmlContent.getBytes(UTF_8));

        return coreSiteXml;
    }
}
