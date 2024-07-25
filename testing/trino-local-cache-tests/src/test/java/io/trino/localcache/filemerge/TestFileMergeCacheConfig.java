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
package io.trino.localcache.filemerge;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestFileMergeCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileMergeCacheConfig.class)
                .setMaxCachedEntries(50_000)
                .setMaxInMemoryCacheSize(DataSize.of(2, GIGABYTE))
                .setCacheTtl(new Duration(7, DAYS))
                .setDiskQuota(null)
                .setDiskQuotaRate(0.7)
                .setMinBlockSize(DataSize.of(4, MEGABYTE))
                .setMaxBlockSize(DataSize.of(128, MEGABYTE))
                .setMergeEnabled(true)
                .setMergeInterval(Duration.succinctDuration(15, TimeUnit.MINUTES))
                .setIoThreads(Math.max(Runtime.getRuntime().availableProcessors() / 2, 1)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("cache.max-cached-entries", "5")
                .put("cache.max-in-memory-cache-size", "42MB")
                .put("cache.ttl", "10s")
                .put("cache.disk-quota", "5kB")
                .put("cache.disk-quota-rate", "0.5")
                .put("cache.min-block-size", "16MB")
                .put("cache.max-block-size", "512MB")
                .put("cache.merge-enabled", "false")
                .put("cache.merge-interval", "1h")
                .put("cache.io-threads", "1")
                .buildOrThrow();

        FileMergeCacheConfig expected = new FileMergeCacheConfig()
                .setMaxCachedEntries(5)
                .setMaxInMemoryCacheSize(DataSize.of(42, MEGABYTE))
                .setCacheTtl(new Duration(10, SECONDS))
                .setDiskQuota(DataSize.of(5, KILOBYTE))
                .setDiskQuotaRate(0.5)
                .setMinBlockSize(DataSize.of(16, MEGABYTE))
                .setMaxBlockSize(DataSize.of(512, MEGABYTE))
                .setMergeEnabled(false)
                .setMergeInterval(Duration.succinctDuration(1, HOURS))
                .setIoThreads(1);
        assertFullMapping(properties, expected);
    }
}
