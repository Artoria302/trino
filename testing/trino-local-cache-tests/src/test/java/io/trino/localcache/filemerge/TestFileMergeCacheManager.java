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
import io.airlift.units.Duration;
import io.trino.localcache.CacheConfig;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.localcache.CacheResult;
import io.trino.spi.localcache.FileIdentifier;
import io.trino.spi.localcache.FileReadRequest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.localcache.TestingCacheUtils.stressTest;
import static io.trino.localcache.TestingCacheUtils.validateBuffer;
import static io.trino.spi.localcache.CacheResult.Result.MISS;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestFileMergeCacheManager
{
    private static final int DATA_LENGTH = (int) DataSize.of(20, KILOBYTE).toBytes();
    private final byte[] data = new byte[DATA_LENGTH];

    private URI cacheDirectory;
    private URI fileDirectory;
    private File dataFile;
    private long modificationTime;

    @BeforeClass
    public void setup()
            throws IOException
    {
        new Random().nextBytes(data);

        this.cacheDirectory = createTempDirectory("cache").toUri();
        this.fileDirectory = createTempDirectory("file").toUri();
        this.dataFile = new File(fileDirectory.getPath() + "/data");

        Files.write((new File(dataFile.getPath())).toPath(), data, CREATE_NEW);
        this.modificationTime = new File(dataFile.getPath()).lastModified();
    }

    @AfterClass
    public void close()
            throws IOException, InterruptedException
    {
        checkState(cacheDirectory != null);
        checkState(fileDirectory != null);

        Files.deleteIfExists(dataFile.toPath());
        File[] files = new File(cacheDirectory).listFiles();
        if (files != null) {
            for (File file : files) {
                Files.delete(file.toPath());
            }
        }

        Files.deleteIfExists(new File(cacheDirectory).toPath());
        Files.deleteIfExists(new File(fileDirectory).toPath());
    }

    @Test(timeOut = 30_000)
    public void testReadFully()
            throws IOException
    {
        TestingCacheStats stats = new TestingCacheStats();
        FileMergeCacheManager cacheManager = fileMergeCacheManager(stats);
        byte[] buffer = new byte[1024];

        // new read
        // [42, 142)
        readFully(cacheManager, 42, buffer, 0, 100);
        stats.trigger();
        assertThat(stats.getCacheMissTotal()).isEqualTo(1);
        assertThat(stats.getCacheHitTotal()).isEqualTo(0);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // within the range of the cache
        // [42, 142)
        readFully(cacheManager, 47, buffer, 0, 90);
        assertThat(stats.getCacheMissTotal()).isEqualTo(1);
        assertThat(stats.getCacheHitTotal()).isEqualTo(1);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // partially within the range of the cache
        // [42, 152)
        readFully(cacheManager, 52, buffer, 0, 100);
        stats.trigger();
        assertThat(stats.getCacheMissTotal()).isEqualTo(2);
        assertThat(stats.getCacheHitTotal()).isEqualTo(1);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // partially within the range of the cache
        // [32, 42), [42, 152)
        readFully(cacheManager, 32, buffer, 10, 50);
        stats.trigger();
        assertThat(stats.getCacheMissTotal()).isEqualTo(3);
        assertThat(stats.getCacheHitTotal()).isEqualTo(1);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // create a hole within two caches
        // [32, 42), [42, 152), [200, 250)
        readFully(cacheManager, 200, buffer, 40, 50);
        stats.trigger();
        assertThat(stats.getCacheMissTotal()).isEqualTo(4);
        assertThat(stats.getCacheHitTotal()).isEqualTo(1);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // use a range to cover the hole
        // [32, 42), [42, 152), [153, 200), [200, 250)
        readFully(cacheManager, 40, buffer, 400, 200);
        stats.trigger();
        assertThat(stats.getCacheMissTotal()).isEqualTo(5);
        assertThat(stats.getCacheHitTotal()).isEqualTo(1);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // [32, 42), [42, 152), [153, 200), [200, 250)
        readFully(cacheManager, 32, buffer, 0, 218);
        stats.trigger();
        assertThat(stats.getCacheMissTotal()).isEqualTo(5);
        assertThat(stats.getCacheHitTotal()).isEqualTo(2);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        cacheManager.destroy();
    }

    @Test(timeOut = 30_000)
    public void testRead()
            throws IOException
    {
        TestingCacheStats stats = new TestingCacheStats();
        FileMergeCacheManager cacheManager = fileMergeCacheManager(stats);
        byte[] buffer = new byte[1024];

        // new read, then cache range will be: [42, 142)
        TestingReadResult result = read(cacheManager, 42, buffer, 0, 100);
        assertThat(result.isHit).isFalse();
        assertThat(result.n).isEqualTo(100);
        assertThat(stats.getCacheMissTotal()).isEqualTo(1);
        assertThat(stats.getCacheHitTotal()).isEqualTo(0);
        stats.trigger();
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // within the range of the cache, then cache range will be: [42, 142)
        result = read(cacheManager, 47, buffer, 0, 90);
        assertThat(result.isHit).isTrue();
        assertThat(result.n).isEqualTo(90);
        assertThat(stats.getCacheMissTotal()).isEqualTo(1);
        assertThat(stats.getCacheHitTotal()).isEqualTo(1);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // partially within the range of the cache, begin is include, then cache range will be: [42, 142)
        result = read(cacheManager, 52, buffer, 0, 100);
        assertThat(result.isHit).isTrue();
        assertThat(result.n).isEqualTo(90);
        assertThat(stats.getCacheMissTotal()).isEqualTo(1);
        assertThat(stats.getCacheHitTotal()).isEqualTo(2);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // partially within the range of the cache, begin is exclude, then cache range will be: [32, 142)
        result = read(cacheManager, 32, buffer, 10, 50);
        assertThat(result.isHit).isFalse();
        assertThat(result.n).isEqualTo(50);
        assertThat(stats.getCacheMissTotal()).isEqualTo(2);
        assertThat(stats.getCacheHitTotal()).isEqualTo(2);
        stats.trigger();
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // create a hole within two caches, then cache range will be: [32, 142), [200, 250)
        result = read(cacheManager, 200, buffer, 40, 50);
        assertThat(result.isHit).isFalse();
        assertThat(result.n).isEqualTo(50);
        assertThat(stats.getCacheMissTotal()).isEqualTo(3);
        assertThat(stats.getCacheHitTotal()).isEqualTo(2);
        stats.trigger();
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // use a range to cover the hole, begin is include, then cache range will be: [32, 142), [200, 250)
        result = read(cacheManager, 40, buffer, 200, 200);
        assertThat(result.isHit).isTrue();
        assertThat(result.n).isEqualTo(102);
        assertThat(stats.getCacheMissTotal()).isEqualTo(3);
        assertThat(stats.getCacheHitTotal()).isEqualTo(3);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // use a range to cover the hole, then cache range will be: [30, 250)
        result = read(cacheManager, 30, buffer, 400, 200);
        assertThat(result.isHit).isFalse();
        assertThat(result.n).isEqualTo(200);
        assertThat(stats.getCacheMissTotal()).isEqualTo(4);
        assertThat(stats.getCacheHitTotal()).isEqualTo(3);
        stats.trigger();
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // excess end of the file, then cache range will be: [30, 250), [DATA_LENGTH - 100, DATA_LENGTH)
        result = read(cacheManager, DATA_LENGTH - 100, buffer, 600, 200);
        assertThat(result.isHit).isFalse();
        assertThat(result.n).isEqualTo(100);
        assertThat(stats.getCacheMissTotal()).isEqualTo(5);
        assertThat(stats.getCacheHitTotal()).isEqualTo(3);
        stats.trigger();
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        // read after end of the file, then cache range will be: [30, 250), [DATA_LENGTH - 100, DATA_LENGTH)
        result = read(cacheManager, DATA_LENGTH, buffer, 800, 100);
        assertThat(result.isHit).isTrue();
        assertThat(result.n).isEqualTo(-1);
        assertThat(stats.getCacheMissTotal()).isEqualTo(5);
        assertThat(stats.getCacheHitTotal()).isEqualTo(3);
        assertThat(stats.getInMemoryRetainedBytes()).isEqualTo(0);

        cacheManager.destroy();
    }

    @Test(timeOut = 30_000)
    public void testBlockSize()
            throws IOException
    {
        TestingCacheStats stats = new TestingCacheStats();
        CacheConfig cacheConfig = new CacheConfig().setBaseDirectory(cacheDirectory);
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig()
                .setMergeInterval(Duration.succinctDuration(0, MILLISECONDS))
                .setMinBlockSize(DataSize.of(16, BYTE))
                .setMaxBlockSize(DataSize.of(64, BYTE))
                .setDiskQuota(DataSize.of(2, GIGABYTE));
        FileMergeCacheManager cacheManager = fileMergeCacheManager(cacheConfig, fileMergeCacheConfig, stats);
        LRUTracker<FileMergeCacheManager.LocalCacheFile> lruTracker = cacheManager.lruTracker;

        FileMergeCacheManager.LocalCacheFile file1;
        FileMergeCacheManager.LocalCacheFile file2;

        byte[] buffer = new byte[1024];
        // range: [0,  8)
        // file:  [0, 16)
        readFully(cacheManager, 0, buffer, 0, 8);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(1);
        assertThat(lruTracker.size()).isEqualTo(1);
        file1 = lruTracker.peekMost();
        assertThat(stats.getDiskUsedBytes()).isEqualTo(8);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(16);
        assertThat(file1.getOffset()).isEqualTo(0);
        assertThat(file1.getBlockSize()).isEqualTo(16);

        // range: [0,  8), [56, 64)
        // file:  [0, 16), [48, 64)
        readFully(cacheManager, 56, buffer, 56, 8);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(2);
        assertThat(lruTracker.size()).isEqualTo(2);
        file2 = lruTracker.peekMost();
        assertThat(stats.getDiskUsedBytes()).isEqualTo(16);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(32);
        assertThat(file2.getOffset()).isEqualTo(48);
        assertThat(file2.getBlockSize()).isEqualTo(16);

        // previous and current in same block should merge when overlap
        // range: [0, 10), [56, 64)
        // file:  [0, 16), [48, 64)
        readFully(cacheManager, 7, buffer, 7, 3);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(2);
        assertThat(lruTracker.size()).isEqualTo(2);
        file2 = lruTracker.peekMost();
        assertThat(file2.getOffset()).isEqualTo(0);
        assertThat(file1).isEqualTo(file2);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(18);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(32);

        // current and following in same block should merge when overlap
        // range: [0, 10), [54, 64)
        // file:  [0, 16), [48, 64)
        file1 = lruTracker.peekLeast();
        readFully(cacheManager, 54, buffer, 54, 3);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(2);
        assertThat(lruTracker.size()).isEqualTo(2);
        file2 = lruTracker.peekMost();
        assertThat(file1).isEqualTo(file2);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(20);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(32);

        // previous and current in same block should merge when next to
        // range: [0, 12), [54, 64)
        // file:  [0, 16), [48, 64)
        readFully(cacheManager, 10, buffer, 10, 2);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(2);
        assertThat(lruTracker.size()).isEqualTo(2);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(22);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(32);

        // current and following im same block should merge when next to
        // range: [0, 12), [52, 64)
        // file:  [0, 16), [48, 64)
        readFully(cacheManager, 52, buffer, 52, 2);
        stats.trigger();
        file1 = lruTracker.peekMost();
        assertThat(cacheManager.rangeSize()).isEqualTo(2);
        assertThat(lruTracker.size()).isEqualTo(2);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(24);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(32);
        assertThat(file1.getBlockSize()).isEqualTo(16);

        // current and following im same block should merge when next to
        // range: [0, 12), [14, 15), [52, 64)
        // file:  [0, 16), [0,  16), [48, 64)
        readFully(cacheManager, 14, buffer, 14, 1);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(3);
        assertThat(lruTracker.size()).isEqualTo(2);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(25);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(32);

        // make gap
        // range: [0, 12), [14, 15), [22, 25), [52, 64)
        // file:  [0, 16), [0,  16), [16, 32), [48, 64)
        readFully(cacheManager, 22, buffer, 22, 3);
        stats.trigger();
        assertThat(lruTracker.size()).isEqualTo(3);
        assertThat(cacheManager.rangeSize()).isEqualTo(4);
        file1 = lruTracker.peekMost();
        assertThat(file1.getOffset()).isEqualTo(16);
        assertThat(file1.getBlockSize()).isEqualTo(16);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(28);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(48);

        // range: [0, 12), [14, 15), [22, 25), [52, 64), [88, 90)
        // file:  [0, 16), [0,  16), [16, 32), [48, 64), [80, 96)
        readFully(cacheManager, 88, buffer, 88, 2);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(5);
        assertThat(lruTracker.size()).isEqualTo(4);
        file1 = lruTracker.peekMost();
        assertThat(file1.getOffset()).isEqualTo(80);
        assertThat(file1.getBlockSize()).isEqualTo(16);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(30);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(64);

        // read length greater than min block size and less than max block size, should allocate 1 block
        // range: [0, 12), [14, 15), [22, 25), [52, 64), [88, 90), [156, 184)
        // file:  [0, 16), [0,  16), [16, 32), [48, 64), [80, 96), [144, 192)
        readFully(cacheManager, 156, buffer, 156, 28);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(6);
        assertThat(lruTracker.size()).isEqualTo(5);
        file1 = lruTracker.peekMost();
        assertThat(file1.getOffset()).isEqualTo(144);
        assertThat(file1.getBlockSize()).isEqualTo(48);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(58);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(112);

        // read range stride aligned max block size should allocate 2 block
        // range: [0, 12), [14, 15), [22, 25), [52, 64), [88, 90), [156, 184), [350, 384), [384, 426)
        // file:  [0, 16), [0,  16), [16, 32), [48, 64), [80, 96), [144, 192), [336, 384), [384, 432)
        readFully(cacheManager, 350, buffer, 350, 76);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(8);
        assertThat(lruTracker.size()).isEqualTo(7);
        file1 = lruTracker.peekMost();
        assertThat(file1.getOffset()).isEqualTo(384);
        assertThat(file1.getBlockSize()).isEqualTo(48);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(134);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(208);

        // range: [0, 16), [16, 32), [32, 43), [52, 64), [88, 90), [156, 184), [350, 384), [384, 426)
        // file:  [0, 16), [16, 32), [32, 48), [48, 64), [80, 96), [144, 192), [336, 384), [384, 432)
        readFully(cacheManager, 11, buffer, 11, 32);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(8);
        assertThat(lruTracker.size()).isEqualTo(8);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(161);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(224);

        // range: [0, 16), [16, 32), [32, 48), [48, 64), [64, 80), [80, 82), [88, 90), [156, 184), [350, 384), [384, 426)
        // file:  [0, 16), [16, 32), [32, 48), [48, 64), [64, 80), [80, 96), [80, 96), [144, 192), [336, 384), [384, 432)
        readFully(cacheManager, 40, buffer, 40, 42);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(10);
        assertThat(lruTracker.size()).isEqualTo(9);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(188);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(240);

        // cache should hit
        buffer = new byte[1024];
        readFully(cacheManager, 0, buffer, 0, 82);
        readFully(cacheManager, 88, buffer, 88, 2);
        readFully(cacheManager, 156, buffer, 156, 28);
        readFully(cacheManager, 350, buffer, 350, 76);

        // merge
        // range: [0, 64), [64, 82), [88, 90), [156, 184), [350, 384), [384, 426)
        // file:  [0, 64), [64, 96), [64, 96), [144, 192), [336, 384), [384, 432)
        cacheManager.mergeCacheFiles();
        assertThat(cacheManager.rangeSize()).isEqualTo(6);
        assertThat(lruTracker.size()).isEqualTo(5);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(188);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(240);

        buffer = new byte[1024];
        readFully(cacheManager, 0, buffer, 0, 82);
        readFully(cacheManager, 88, buffer, 88, 2);
        readFully(cacheManager, 156, buffer, 156, 28);
        readFully(cacheManager, 350, buffer, 350, 76);

        // merge
        // range: [0, 64), [64, 82), [88, 90), [156, 184), [310, 311), [350, 384), [384, 426)
        // file:  [0, 64), [64, 96), [64, 96), [144, 192), [304, 320), [336, 384), [384, 432)
        readFully(cacheManager, 310, buffer, 310, 1);
        stats.trigger();

        // read all
        // range: [0, 64), [64, 96), [96, 128), [128, 144), [144, 192), [192, 256), [256, 304), [304, 320), [320, 336), [336, 384), [384, 426)
        // file:  [0, 64), [64, 96), [96, 128), [128, 144), [144, 192), [192, 256), [256, 304), [304, 320), [320, 336), [336, 384), [384, 432)
        buffer = new byte[1024];
        readFully(cacheManager, 0, buffer, 0, 426);
        stats.trigger();
        assertThat(cacheManager.rangeSize()).isEqualTo(11);
        assertThat(lruTracker.size()).isEqualTo(11);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(426);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(432);
        readFully(cacheManager, 0, buffer, 0, 426);

        // merge
        // range: [0, 64), [64, 128), [128, 192), [192, 256), [256, 320), [320, 384), [384, 426)
        // file:  [0, 64), [64, 128), [128, 192), [192, 256), [256, 320), [320, 384), [384, 432)
        cacheManager.mergeCacheFiles();
        assertThat(cacheManager.rangeSize()).isEqualTo(7);
        assertThat(lruTracker.size()).isEqualTo(7);
        assertThat(stats.getDiskUsedBytes()).isEqualTo(426);
        assertThat(stats.getDiskAllocatedBytes()).isEqualTo(432);

        buffer = new byte[1024];
        readFully(cacheManager, 0, buffer, 0, 426);

        cacheManager.destroy();
    }

    @Test(timeOut = 30_000)
    public void testReadContinuous()
            throws IOException
    {
        TestingCacheStats stats = new TestingCacheStats();
        CacheConfig cacheConfig = new CacheConfig().setBaseDirectory(cacheDirectory);
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig()
                .setMergeInterval(Duration.succinctDuration(0, MILLISECONDS))
                .setMinBlockSize(DataSize.of(4, BYTE))
                .setMaxBlockSize(DataSize.of(16, BYTE))
                .setDiskQuota(DataSize.of(2, GIGABYTE));
        FileMergeCacheManager cacheManager = fileMergeCacheManager(cacheConfig, fileMergeCacheConfig, stats);
        LRUTracker<FileMergeCacheManager.LocalCacheFile> lruTracker = cacheManager.lruTracker;

        byte[] buffer = new byte[64];
        readFully(cacheManager, 0, buffer, 0, 8);
        stats.trigger();
        assertThat(lruTracker.size()).isEqualTo(1);

        readFully(cacheManager, 8, buffer, 8, 8);
        stats.trigger();
        assertThat(lruTracker.size()).isEqualTo(2);

        readFully(cacheManager, 16, buffer, 16, 8);
        stats.trigger();
        assertThat(lruTracker.size()).isEqualTo(3);

        readFully(cacheManager, 24, buffer, 24, 8);
        stats.trigger();
        assertThat(lruTracker.size()).isEqualTo(4);

        readFully(cacheManager, 32, buffer, 32, 8);
        stats.trigger();
        assertThat(lruTracker.size()).isEqualTo(5);

        long hit;
        TestingReadResult result;
        // [0, 8), [8, 16), [16, 24), [24, 32), [32, 40)
        buffer = new byte[64];
        hit = stats.getCacheHitTotal();
        readFully(cacheManager, 4, buffer, 4, 14);
        assertThat(stats.getCacheHitTotal()).isEqualTo(hit + 1);

        hit = stats.getCacheHitTotal();
        result = read(cacheManager, 20, buffer, 20, 14);
        assertThat(result.isHit).isTrue();
        assertThat(stats.getCacheHitTotal()).isEqualTo(hit + 1);

        cacheManager.mergeCacheFiles();
        assertThat(cacheManager.lruTracker.size()).isEqualTo(3);

        buffer = new byte[64];
        hit = stats.getCacheHitTotal();
        readFully(cacheManager, 4, buffer, 4, 14);
        assertThat(stats.getCacheHitTotal()).isEqualTo(hit + 1);

        hit = stats.getCacheHitTotal();
        result = read(cacheManager, 20, buffer, 20, 14);
        assertThat(result.isHit).isTrue();
        assertThat(stats.getCacheHitTotal()).isEqualTo(hit + 1);

        cacheManager.destroy();
    }

    @Test(timeOut = 60_000, invocationCount = 10)
    public void testStress()
            throws ExecutionException, InterruptedException
    {
        CacheConfig cacheConfig = new CacheConfig().setBaseDirectory(cacheDirectory);
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig()
                .setMinBlockSize(DataSize.of(32, BYTE))
                .setMaxBlockSize(DataSize.of(512, BYTE))
                .setMergeEnabled(false)
                .setCacheTtl(new Duration(10, MILLISECONDS));

        TestingCacheStats stats = new TestingCacheStats(10);
        FileMergeCacheManager cacheManager = fileMergeCacheManager(cacheConfig, fileMergeCacheConfig, stats);

        stressTest(data, (position, buffer, offset, length) -> readFully(cacheManager, position, buffer, offset, length));

        stats.trigger();
        for (Map.Entry<FileIdentifier, FileMergeCacheManager.CacheRange> entry : cacheManager.persistedRanges.entrySet()) {
            entry.getValue().getRange().asMapOfRanges().values().forEach(f -> assertThat(f.getRefCount()).isEqualTo(0));
        }

        cacheManager.destroy();
    }

    private FileMergeCacheManager fileMergeCacheManager(CacheConfig cacheConfig, FileMergeCacheConfig fileMergeCacheConfig, FileMergeCacheStats cacheStats)
    {
        return new FileMergeCacheManager(cacheConfig, fileMergeCacheConfig, cacheStats);
    }

    private FileMergeCacheManager fileMergeCacheManager(FileMergeCacheStats cacheStats)
    {
        CacheConfig cacheConfig = new CacheConfig();
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig();
        fileMergeCacheConfig.setDiskQuota(DataSize.of(2, GIGABYTE));
        return new FileMergeCacheManager(cacheConfig.setBaseDirectory(cacheDirectory), fileMergeCacheConfig, cacheStats);
    }

    private void readFully(CacheManager cacheManager, long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        FileReadRequest key = new FileReadRequest(dataFile.getPath(), modificationTime, position, length);
        if (requireNonNull(cacheManager.readFully(key, buffer, offset)).getResult() == MISS) {
            try (RandomAccessFile file = new RandomAccessFile(dataFile.getAbsolutePath(), "r")) {
                file.seek(position);
                file.readFully(buffer, offset, length);
            }
            cacheManager.write(key, wrappedBuffer(buffer, offset, length));
        }
        validateBuffer(data, key.getOffset(), buffer, offset, length);
    }

    private TestingReadResult read(CacheManager cacheManager, long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        if (position >= DATA_LENGTH) {
            return new TestingReadResult(true, -1);
        }
        FileReadRequest key = new FileReadRequest(dataFile.getPath(), modificationTime, position, length);
        CacheResult result = cacheManager.read(key, buffer, offset);
        TestingReadResult readResult;
        switch (result.getResult()) {
            case HIT:
                readResult = new TestingReadResult(true, result.getLength());
                break;
            case MISS:
                int n;
                int bytesRead = 0;
                try (RandomAccessFile file = new RandomAccessFile(dataFile.getAbsolutePath(), "r")) {
                    file.seek(position);
                    while (bytesRead < length) {
                        n = file.read(buffer, offset + bytesRead, length - bytesRead);
                        if (n < 0) {
                            break;
                        }
                        bytesRead += n;
                    }
                }
                if (bytesRead > 0) {
                    cacheManager.write(new FileReadRequest(key.getFileIdentifier(), position, bytesRead), wrappedBuffer(buffer, offset, bytesRead));
                }
                readResult = new TestingReadResult(false, bytesRead);
                break;
            default:
                throw new RuntimeException("Code should not reach here");
        }
        if (readResult.n > 0) {
            validateBuffer(data, position, buffer, offset, readResult.n);
        }
        return readResult;
    }

    static class TestingReadResult
    {
        final boolean isHit;
        final int n;

        public TestingReadResult(boolean isHit, int n)
        {
            this.isHit = isHit;
            this.n = n;
        }
    }
}
