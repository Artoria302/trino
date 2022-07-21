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
import io.trino.plugin.exchange.filesystem.AbstractTestExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.spi.exchange.ExchangeManager;
import org.testng.annotations.AfterClass;

import java.util.Map;

public class TestHdfsFileSystemExchangeManager
        extends AbstractTestExchangeManager
{
    @Override
    protected ExchangeManager createExchangeManager()
    {
        String tmpDir = System.getProperty("java.io.tmpdir");
        String dir1 = "file://" + tmpDir + "/trino-hdfs-file-system-exchange-manager-1";
        String dir2 = "file://" + tmpDir + "/trino-hdfs-file-system-exchange-manager-2";
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
    }
}
