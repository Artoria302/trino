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

import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.localcache.CacheConfig;
import io.trino.localcache.SimpleCachingTrinoFileSystem;
import io.trino.spi.localcache.CacheManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.localcache.TestingCacheUtils.validateBuffer;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

@Test(singleThreaded = true)
public class TestFileMergeCachingInputStream
{
    private static final int DATA_LENGTH = (int) DataSize.of(20, KILOBYTE).toBytes();
    private final byte[] data = new byte[DATA_LENGTH];

    private java.nio.file.Path cacheDirectory;
    private Path fileDirectory;
    private String dataFile;
    private TrinoFileSystem localFileSystem;

    @BeforeClass
    public void setup()
            throws IOException
    {
        new Random().nextBytes(data);

        this.cacheDirectory = createTempDirectory("cache");
        this.fileDirectory = createTempDirectory("file");
        this.dataFile = "local:" + new File(fileDirectory + "/data").getAbsolutePath();
        this.localFileSystem = localFileSystem(fileDirectory);
        try (OutputStream outputStream = localFileSystem.newOutputFile(Location.of(dataFile)).create()) {
            outputStream.write(data);
        }
    }

    @AfterClass
    public void close()
            throws IOException, InterruptedException
    {
        checkState(cacheDirectory != null);
        checkState(fileDirectory != null);

        localFileSystem.deleteDirectory(Location.of("local:" + cacheDirectory));
        localFileSystem.deleteDirectory(Location.of("local:" + fileDirectory));
    }

    @Test(timeOut = 30_000)
    private void testSeek()
            throws IOException
    {
        TestingCacheStats stats = new TestingCacheStats();
        FileMergeCacheManager cacheManager = fileMergeCacheManager(stats);

        TrinoFileSystem fs = cachingFileSystem(localFileSystem, cacheManager);
        try (TrinoInputStream in = fs.newInputFile(Location.of(dataFile)).newStream()) {
            in.seek(-1);
            fail("Expected failure to seek a negative offset");
        }
        catch (EOFException e) {
            assertThat(e.getMessage()).isEqualTo("Cannot seek to a negative offset");
        }

        cacheManager.destroy();
    }

    @Test(timeOut = 30_000)
    private void testReadAll()
            throws IOException
    {
        byte[] bytes;
        TestingCacheStats stats = new TestingCacheStats();
        FileMergeCacheManager cacheManager = fileMergeCacheManager(stats);

        TrinoFileSystem fs = cachingFileSystem(localFileSystem, cacheManager);
        try (TrinoInputStream in = fs.newInputFile(Location.of(dataFile)).newStream()) {
            bytes = in.readAllBytes();
            validateBuffer(data, 0, bytes, 0, DATA_LENGTH);
            stats.trigger();
            assertThat(stats.getCacheHitTotal()).isEqualTo(0);
            assertThat(stats.getCacheMissTotal()).isGreaterThan(0);

            long miss = stats.getCacheMissTotal();
            in.seek(0);
            bytes = in.readAllBytes();
            validateBuffer(data, 0, bytes, 0, DATA_LENGTH);
            assertThat(stats.getCacheHitTotal()).isGreaterThan(0);
            assertThat(stats.getCacheMissTotal()).isEqualTo(miss);
        }
        cacheManager.destroy();
    }

    @Test(timeOut = 30_000)
    private void testRead()
            throws IOException
    {
        byte[] buf = new byte[2048];
        long miss;
        long hit;
        int bytesRead;
        TestingCacheStats stats = new TestingCacheStats();
        FileMergeCacheManager cacheManager = fileMergeCacheManager(stats);

        TrinoFileSystem fs = cachingFileSystem(localFileSystem, cacheManager);
        try (TrinoInputStream in = fs.newInputFile(Location.of(dataFile)).newStream()) {
            in.seek(100);
            bytesRead = read(in, buf, 0, 100);
            assertThat(bytesRead).isEqualTo(100);
            validateBuffer(data, 100, buf, 0, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheMissTotal()).isGreaterThan(0);

            // read all cached data
            miss = stats.getCacheMissTotal();
            in.seek(100);
            bytesRead = read(in, buf, 100, 100);
            assertThat(bytesRead).isEqualTo(100);
            validateBuffer(data, 100, buf, 100, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheHitTotal()).isGreaterThan(0);
            assertThat(stats.getCacheMissTotal()).isEqualTo(miss);

            // read data in cached data
            in.seek(120);
            bytesRead = read(in, buf, 200, 50);
            assertThat(bytesRead).isEqualTo(50);
            validateBuffer(data, 120, buf, 200, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheMissTotal()).isEqualTo(miss);

            // read data from cached data, but end is out of range
            hit = stats.getCacheHitTotal();
            in.seek(150);
            bytesRead = read(in, buf, 300, 100);
            assertThat(bytesRead).isEqualTo(100);
            validateBuffer(data, 150, buf, 300, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheHitTotal()).isGreaterThan(hit);

            // read data from cached data, but begin is out of range
            miss = stats.getCacheMissTotal();
            in.seek(50);
            bytesRead = read(in, buf, 300, 100);
            assertThat(bytesRead).isEqualTo(100);
            validateBuffer(data, 50, buf, 300, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheMissTotal()).isGreaterThan(miss);

            // read data from cached data
            miss = stats.getCacheMissTotal();
            in.seek(50);
            bytesRead = read(in, buf, 400, 200);
            assertThat(bytesRead).isEqualTo(200);
            validateBuffer(data, 50, buf, 400, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheMissTotal()).isEqualTo(miss);

            // read end of data
            hit = stats.getCacheHitTotal();
            miss = stats.getCacheMissTotal();
            in.seek(DATA_LENGTH - 50);
            bytesRead = read(in, buf, 500, 100);
            assertThat(bytesRead).isEqualTo(50);
            validateBuffer(data, DATA_LENGTH - 50, buf, 500, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheHitTotal()).isEqualTo(hit);
            assertThat(stats.getCacheMissTotal()).isGreaterThan(miss);

            // read end of data, begin is cached, hit
            hit = stats.getCacheHitTotal();
            miss = stats.getCacheMissTotal();
            in.seek(DATA_LENGTH - 20);
            bytesRead = read(in, buf, 550, 100);
            assertThat(bytesRead).isEqualTo(20);
            validateBuffer(data, DATA_LENGTH - 20, buf, 550, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheHitTotal()).isGreaterThan(hit);
            assertThat(stats.getCacheMissTotal()).isEqualTo(miss);

            // read end of data, begin is not cached, can not hit
            miss = stats.getCacheMissTotal();
            in.seek(DATA_LENGTH - 100);
            bytesRead = read(in, buf, 600, 200);
            assertThat(bytesRead).isEqualTo(100);
            validateBuffer(data, DATA_LENGTH - 100, buf, 600, bytesRead);
            stats.trigger();
            assertThat(stats.getCacheMissTotal()).isGreaterThan(miss);
        }

        cacheManager.destroy();
    }

    private FileMergeCacheManager fileMergeCacheManager(FileMergeCacheStats cacheStats)
    {
        CacheConfig cacheConfig = new CacheConfig();
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig()
                .setMinBlockSize(DataSize.of(16, BYTE))
                .setMaxBlockSize(DataSize.of(128, BYTE));
        return new FileMergeCacheManager(cacheConfig.setBaseDirectory(cacheDirectory.toUri()), fileMergeCacheConfig, cacheStats);
    }

    private TrinoFileSystem localFileSystem(java.nio.file.Path root)
    {
        return new LocalFileSystem(root);
    }

    private TrinoFileSystem cachingFileSystem(TrinoFileSystem delegate, CacheManager cacheManager)
    {
        return new SimpleCachingTrinoFileSystem(delegate, new DefaultCacheKeyProvider(), cacheManager);
    }

    private int read(TrinoInputStream in, byte[] buf, int off, int len)
            throws IOException
    {
        int bytesRead = 0;
        int n;
        while (bytesRead < len) {
            n = in.read(buf, off + bytesRead, len - bytesRead);
            if (n < 0) {
                break;
            }
            bytesRead += n;
        }
        return bytesRead;
    }
}
