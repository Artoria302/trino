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
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.localcache.CacheConfig;
import io.trino.localcache.SimpleCachingTrinoFileSystem;
import io.trino.localcache.collect.Range;
import io.trino.spi.localcache.CacheManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.localcache.TestingCacheUtils.validateBuffer;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestFileMergeCacheLRUTracker
{
    private static final int DATA_LENGTH = (int) DataSize.of(64, KILOBYTE).toBytes();
    private final byte[] data = new byte[DATA_LENGTH];
    private final ExecutorService lruExecutor = newCachedThreadPool(daemonThreadsNamed("test-lru-%s"));

    private Path cacheDirectory;
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
        lruExecutor.shutdown();
        lruExecutor.awaitTermination(5, TimeUnit.SECONDS);

        checkState(cacheDirectory != null);
        checkState(fileDirectory != null);

        localFileSystem.deleteDirectory(Location.of("local:" + cacheDirectory));
        localFileSystem.deleteDirectory(Location.of("local:" + fileDirectory));
    }

    @AfterMethod(alwaysRun = true)
    private void tearDown()
            throws IOException
    {
        File[] files = cacheDirectory.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                Files.delete(file.toPath());
            }
        }
    }

    @Test(timeOut = 30_000)
    private void testBasic()
            throws IOException
    {
        byte[] buf = new byte[2048];
        TestingCacheStats stats = new TestingCacheStats();
        DataSize quota = DataSize.of(2, KILOBYTE);
        // block size should greater than quota, block size is not tested here
        DataSize minBlockSize = DataSize.of(256, BYTE);
        DataSize maxBlockSize = DataSize.of(1, KILOBYTE);
        FileMergeCacheManager cacheManager = fileMergeCacheManager(stats, quota, minBlockSize, maxBlockSize);
        LRUTracker<FileMergeCacheManager.LocalCacheFile> lruTracker = cacheManager.lruTracker;

        FileMergeCacheManager.LocalCacheFile file1;
        FileMergeCacheManager.LocalCacheFile file2;

        TrinoFileSystem fs = cachingFileSystem(localFileSystem, cacheManager);
        try (TrinoInput in = fs.newInputFile(Location.of(dataFile)).newInput()) {
            // range: [0, 100)
            // file:  [0, 256)
            in.readFully(0, buf, 0, 100);
            validateBuffer(data, 0, buf, 0, 100);
            stats.trigger();

            assertThat(stats.getDiskUsedBytes()).isEqualTo(100);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(256);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(getCacheDirectorySpace());
            file1 = lruTracker.peekMost();
            assertThat(file1).isEqualTo(lruTracker.peekLeast());
            assertThat(lruTracker.size()).isEqualTo(1);
            assertThat(lruTracker.size()).isEqualTo(lruTracker.listSize());

            // range: [0, 100), [200, 256), [256, 300)
            // file:  [0, 256), [0,   256), [256, 512)
            in.readFully(200, buf, 0, 100);
            validateBuffer(data, 200, buf, 0, 100);
            stats.trigger();

            assertThat(stats.getDiskUsedBytes()).isEqualTo(200);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(512);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(getCacheDirectorySpace());
            assertThat(file1).isEqualTo(lruTracker.peekLeast());
            assertThat(lruTracker.size()).isEqualTo(2);
            assertThat(lruTracker.size()).isEqualTo(lruTracker.listSize());

            // range: [0, 100), [200, 256), [256, 300), [400, 500)
            // file:  [0, 256), [0,   256), [256, 512), [256, 512)
            in.readFully(400, buf, 0, 100);
            validateBuffer(data, 400, buf, 0, 100);
            stats.trigger();

            assertThat(stats.getDiskUsedBytes()).isEqualTo(300);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(512);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(getCacheDirectorySpace());
            file2 = lruTracker.peekMost();

            // range: [0, 100), [200, 256), [256, 300), [400, 500), [788,  800)
            // file:  [0, 256), [0,   256), [256, 512), [256, 512), [768, 1024)
            in.readFully(788, buf, 0, 12);
            validateBuffer(data, 788, buf, 0, 12);
            stats.trigger();

            assertThat(stats.getDiskUsedBytes()).isEqualTo(312);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(768);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(getCacheDirectorySpace());
            assertThat(lruTracker.size()).isEqualTo(3);
            assertThat(lruTracker.size()).isEqualTo(lruTracker.listSize());

            // read range1 again
            // range: [0, 100), [200, 256), [256, 300), [400, 500), [788,  800)
            // file:  [0, 256), [0,   256), [256, 512), [256, 512), [768, 1024)
            in.readFully(5, buf, 0, 10);
            validateBuffer(data, 5, buf, 0, 10);
            stats.trigger();

            assertThat(stats.getDiskUsedBytes()).isEqualTo(312);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(768);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(getCacheDirectorySpace());
            assertThat(file1).isEqualTo(lruTracker.peekMost());
            assertThat(file2).isEqualTo(lruTracker.peekLeast());
            assertThat(lruTracker.size()).isEqualTo(3);
            assertThat(lruTracker.size()).isEqualTo(lruTracker.listSize());

            // test evict
            // before
            // range: [0, 100), [200, 256), [256, 300), [400, 500), [788,  800)
            // file:  [0, 256), [0,   256), [256, 512), [256, 512), [768, 1024)
            // after
            // range: [0, 100), [200, 256), [788,  800), [900, 1024), [1024, 2048), [2048, 2448)
            // file:  [0, 256), [0,   256), [768, 1024), [768, 1024), [1024, 2048), [2048, 2560)
            in.readFully(900, buf, 0, 1548);
            validateBuffer(data, 900, buf, 0, 1548);
            stats.trigger();

            assertThat(stats.getDiskUsedBytes()).isEqualTo(1716);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(2048);
            assertThat(stats.getDiskAllocatedBytes()).isEqualTo(getCacheDirectorySpace());
            assertThat(file1).isEqualTo(lruTracker.peekLeast());
            assertThat(cacheManager.rangeSize()).isEqualTo(6);
            assertThat(lruTracker.size()).isEqualTo(4);
            assertThat(lruTracker.size()).isEqualTo(lruTracker.listSize());
        }
        cacheManager.destroy();
    }

    @Test(timeOut = 300_000, invocationCount = 50)
    private void testReadFullyMultiThreads()
            throws InterruptedException
    {
        TestingCacheStats stats = new TestingCacheStats(50);
        DataSize quota = DataSize.of(8, KILOBYTE);
        DataSize minBlockSize = DataSize.of(16, BYTE);
        long minBlockSizeBytes = minBlockSize.toBytes();
        DataSize maxBlockSize = DataSize.of(128, BYTE);
        long maxBlockSizeBytes = maxBlockSize.toBytes();
        FileMergeCacheManager cacheManager = fileMergeCacheManager(stats, quota, minBlockSize, maxBlockSize);
        LRUTracker<FileMergeCacheManager.LocalCacheFile> lruTracker = cacheManager.lruTracker;

        int nThreads = 16;
        int maxLen = toIntExact(DataSize.of(436, BYTE).toBytes());
        CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++) {
            lruExecutor.submit(() -> {
                byte[] buf = new byte[maxLen];
                TrinoFileSystem fs = cachingFileSystem(localFileSystem, cacheManager);
                try (TrinoInput in = fs.newInputFile(Location.of(dataFile)).newInput()) {
                    for (int j = 0; j < 100; j++) {
                        int idx = ThreadLocalRandom.current().nextInt(DATA_LENGTH - maxLen);
                        int len = ThreadLocalRandom.current().nextInt(0, maxLen);
                        in.readFully(idx, buf, 0, len);
                        validateBuffer(data, idx, buf, 0, len);
                        if (ThreadLocalRandom.current().nextInt(100) == 0) {
                            cacheManager.mergeCacheFiles();
                        }
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        stats.trigger();

        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(getCacheDirectorySpace());
        assertThat(stats.getDiskAllocatedBytes()).isLessThanOrEqualTo(quota.toBytes());
        assertThat(lruTracker.size()).isEqualTo(lruTracker.listSize());
        assertThat(stats.getDiskUsedBytes()).isGreaterThan(0);

        FileMergeCacheManager.CacheRange cacheRange = cacheManager.persistedRanges.get(lruTracker.peekLeast().getFileIdentifier());
        assertThat(cacheRange).isNotNull();
        Map<Range<Long>, FileMergeCacheManager.LocalCacheFile> map = cacheRange.getRange().asMapOfRanges();

        assertThat(lruTracker.size()).isEqualTo(new HashSet<>(map.values()).size());
        assertThat(lruTracker.containsAll()).isTrue();

        long diskUsed = 0;
        long upper = -1;
        for (Map.Entry<Range<Long>, FileMergeCacheManager.LocalCacheFile> entry : map.entrySet()) {
            FileMergeCacheManager.LocalCacheFile file = entry.getValue();
            long lower = entry.getKey().lowerEndpoint();
            assertThat(upper).isLessThanOrEqualTo(lower);
            upper = entry.getKey().upperEndpoint();
            assertThat(lruTracker.contains(file)).isTrue();
            diskUsed += upper - lower;
            long blockSize = file.getBlockSize();
            assertThat(minBlockSizeBytes <= blockSize && blockSize <= maxBlockSizeBytes).isTrue();
            assertThat((file.getOffset() % minBlockSizeBytes)).isEqualTo(0);
            assertThat(file.getRefCount()).isEqualTo(0);
        }

        assertThat(diskUsed).isEqualTo(stats.getDiskUsedBytes());

        cacheManager.destroy();
    }

    private long getCacheDirectorySpace()
    {
        long size = 0;
        for (File f : requireNonNull(cacheDirectory.toFile().listFiles())) {
            if (f.isFile()) {
                size += f.length();
            }
        }
        return size;
    }

    private FileMergeCacheManager fileMergeCacheManager(FileMergeCacheStats cacheStats, DataSize quota, DataSize minBlockSize, DataSize maxBlockSize)
    {
        CacheConfig cacheConfig = new CacheConfig();
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig()
                .setDiskQuota(quota)
                .setMinBlockSize(minBlockSize)
                .setMaxBlockSize(maxBlockSize);
        return new FileMergeCacheManager(cacheConfig.setBaseDirectory(cacheDirectory.toUri()), fileMergeCacheConfig, cacheStats);
    }

    private TrinoFileSystem localFileSystem(Path root)
    {
        return new LocalFileSystem(root);
    }

    private TrinoFileSystem cachingFileSystem(TrinoFileSystem delegate, CacheManager cacheManager)
    {
        return new SimpleCachingTrinoFileSystem(delegate, new DefaultCacheKeyProvider(), cacheManager);
    }
}
