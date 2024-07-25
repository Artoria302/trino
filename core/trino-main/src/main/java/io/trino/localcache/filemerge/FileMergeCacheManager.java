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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.localcache.CacheConfig;
import io.trino.localcache.collect.Range;
import io.trino.localcache.collect.TreeRangeMap;
import io.trino.spi.TrinoException;
import io.trino.spi.localcache.CacheManager;
import io.trino.spi.localcache.CacheResult;
import io.trino.spi.localcache.FileIdentifier;
import io.trino.spi.localcache.FileReadRequest;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

// TODO: Make cache state persistent on disk so we do not need to wipe out cache every time we reboot a server.
@SuppressWarnings("UnstableApiUsage")
public class FileMergeCacheManager
        implements CacheManager
{
    private static final Logger log = Logger.get(FileMergeCacheManager.class);

    private static final String EXTENSION = ".cache";

    // write, merge and remove operator (except evict) on same file always in same thread, for reduce lock contention
    private final ExecutorService[] threadPools;
    private final int threadPoolSize;
    private final ScheduledExecutorService scheduledExecutorService;

    // a mapping from remote file `F` to a range map `M`; the corresponding local cache file for each range in `M` represents the cached chunk of `F`
    @VisibleForTesting final Map<FileIdentifier, CacheRange> persistedRanges = new ConcurrentHashMap<>();
    private final Map<FileIdentifier, Long> cacheUpdated = new ConcurrentHashMap<>();
    // a local cache only to control the lifecycle of persisted
    // Path and its corresponding cacheScope identifier
    private final Cache<FileIdentifier, Integer> cache;
    // stats
    private final FileMergeCacheStats stats;
    // config
    private final URI baseDirectory;
    private final long maxInflightBytes;
    @VisibleForTesting final long diskQuotaBytes;
    @VisibleForTesting final LRUTracker<LocalCacheFile> lruTracker = new LRUTracker<>();
    private final int minBlockSizeBytes;
    private final int maxBlockSizeBytes;
    private final long mergeIntervalMillis;

    @Inject
    public FileMergeCacheManager(
            CacheConfig cacheConfig,
            FileMergeCacheConfig fileMergeCacheConfig,
            FileMergeCacheStats fileMergeCacheStats)
    {
        requireNonNull(cacheConfig, "directory is null");
        this.cache = buildUnsafeCache(CacheBuilder.newBuilder()
                .maximumSize(fileMergeCacheConfig.getMaxCachedEntries())
                .expireAfterAccess(fileMergeCacheConfig.getCacheTtl().toMillis(), MILLISECONDS)
                .removalListener(new CacheRemovalListener())
                .recordStats());
        this.stats = requireNonNull(fileMergeCacheStats, "fileMergeCacheStats is null");
        requireNonNull(cacheConfig.getBaseDirectory(), "the cache config base directory is null");
        this.baseDirectory = requireNonNull(cacheConfig.getBaseDirectory(), "baseDirectory is null");
        checkArgument(fileMergeCacheConfig.getMaxInMemoryCacheSize().toBytes() >= 0, "maxInflightBytes is negative");
        this.maxInflightBytes = fileMergeCacheConfig.getMaxInMemoryCacheSize().toBytes();

        this.minBlockSizeBytes = toIntExact(fileMergeCacheConfig.getMinBlockSize().toBytes());
        this.maxBlockSizeBytes = toIntExact(fileMergeCacheConfig.getMaxBlockSize().toBytes());
        checkArgument(minBlockSizeBytes <= maxBlockSizeBytes, "minBlockSizeBytes must less than or equal to maxBlockSizeBytes");
        checkArgument(maxBlockSizeBytes % minBlockSizeBytes == 0, "maxBlockSizeBytes must be a multiple of minBlockSizeBytes");

        this.threadPoolSize = fileMergeCacheConfig.getIoThreads();
        this.threadPools = new ExecutorService[this.threadPoolSize];
        for (int i = 0; i < threadPoolSize; i++) {
            threadPools[i] = newSingleThreadExecutor(daemonThreadsNamed("local-cache-thread-%s"));
        }

        boolean enableMerge = fileMergeCacheConfig.isMergeEnabled();
        this.scheduledExecutorService = newScheduledThreadPool(1, daemonThreadsNamed("local-cache-merge-schedule-thread-%s"));
        this.mergeIntervalMillis = fileMergeCacheConfig.getMergeInterval().toMillis();
        if (enableMerge) {
            this.scheduledExecutorService.scheduleWithFixedDelay(this::mergeCacheFiles, 1, 1, MINUTES);
        }

        File target = new File(baseDirectory);
        try {
            if (!target.exists()) {
                Files.createDirectories(target.toPath());
            }
            else {
                File[] files = target.listFiles();
                if (files == null) {
                    return;
                }
                Arrays.stream(files).forEach(file -> {
                    try {
                        Files.delete(file.toPath());
                    }
                    catch (IOException e) {
                        // ignore
                    }
                });
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "cannot create cache directory " + target, e);
        }
        finally {
            Optional<DataSize> diskQuota = fileMergeCacheConfig.getDiskQuota();
            // getTotalSpace() return 0 if directory is not exist
            this.diskQuotaBytes = diskQuota.map(DataSize::toBytes).orElseGet(() -> (long) (new File(baseDirectory).getTotalSpace() * fileMergeCacheConfig.getDiskQuotaRate()));
            this.stats.setDiskQuotaBytes(diskQuotaBytes);
        }
        log.info("cache directory: %s, disk quota bytes: %d", this.baseDirectory.getPath(), diskQuotaBytes);
        checkArgument(diskQuotaBytes > 0, "disk quota bytes must greater than 0");
    }

    @SuppressModernizer
    private static <K, V> Cache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder)
    {
        return cacheBuilder.build();
    }

    @PreDestroy
    @Override
    public void destroy()
    {
        scheduledExecutorService.shutdownNow();
        for (int i = 0; i < threadPoolSize; i++) {
            threadPools[i].shutdown();
        }
        awaitQuietly(scheduledExecutorService);
        for (int i = 0; i < threadPoolSize; i++) {
            awaitQuietly(threadPools[i]);
        }
    }

    public void awaitQuietly(@Nonnull ExecutorService executor)
    {
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException ignore) {
        }
    }

    private static void acquire(Semaphore semaphore, int permits)
    {
        try {
            semaphore.acquire(permits);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    void mergeCacheFiles()
    {
        if (cacheUpdated.isEmpty()) {
            return;
        }
        int parallelism = threadPoolSize * 2;
        Semaphore semaphore = new Semaphore(parallelism);
        long mergeStartTime = System.currentTimeMillis();
        for (Map.Entry<FileIdentifier, Long> entry : cacheUpdated.entrySet()) {
            FileIdentifier identifier = entry.getKey();
            long updateTime = entry.getValue();
            if ((mergeStartTime - updateTime) < mergeIntervalMillis) {
                continue;
            }
            acquire(semaphore, 1);
            cacheUpdated.remove(identifier);
            try {
                int mod = (identifier.hashCode() & Integer.MAX_VALUE) % threadPoolSize;
                threadPools[mod].submit(() -> {
                    try {
                        long start = System.nanoTime();
                        mergeCacheFile(identifier);
                        stats.updateFileMerge();
                        stats.addFileMergeTime(System.nanoTime() - start);
                    }
                    catch (Throwable t) {
                        log.warn(t, "failed to merge file %s", identifier.getPath());
                        stats.updateFileMergeFailed();
                    }
                    finally {
                        semaphore.release();
                    }
                });
            }
            catch (Throwable t) {
                log.warn(t, "failed to submit task");
                semaphore.release();
            }
        }
        acquire(semaphore, parallelism);
    }

    private void transferFile(FileChannel channelTo, long position, String path, long count)
            throws IOException
    {
        try (RandomAccessFile file = new RandomAccessFile(new File(path), "r")) {
            try (FileChannel channelFrom = file.getChannel()) {
                channelTo.transferFrom(channelFrom, position, count);
            }
        }
    }

    private boolean mergeCacheFile(
            FileIdentifier fileIdentifier,
            List<Range<Long>> tmpRanges,
            List<LocalCacheFile> tmpLocalCacheFiles)
    {
        int size = tmpRanges.size();
        checkArgument(size > 1 && tmpRanges.size() == tmpLocalCacheFiles.size(), "ranges and files size must be equal and at least two");
        List<Range<Long>> newRanges = new ArrayList<>();
        List<LocalCacheFile> deleteLocalCacheFiles = new ArrayList<>();

        boolean updated = false;
        LocalCacheFile lastFile = tmpLocalCacheFiles.get(tmpLocalCacheFiles.size() - 1);
        Range<Long> range = Range.closedOpen(tmpLocalCacheFiles.get(0).getOffset(), lastFile.getOffset() + lastFile.getBlockSize());
        CacheRange cacheRange = persistedRanges.get(fileIdentifier);
        if (cacheRange == null) {
            return true;
        }

        String path = createNewPath();
        try (RandomAccessFile file = new RandomAccessFile(new File(path), "rw")) {
            long position = 0;
            try (FileChannel fileChannel = file.getChannel()) {
                Range<Long> prevRange = null;
                Range<Long> lastRange;
                LocalCacheFile prevFile = null;
                for (int i = 0; i < size; i++) {
                    Range<Long> curRange = tmpRanges.get(i);
                    LocalCacheFile curFile = tmpLocalCacheFiles.get(i);
                    if (curFile != prevFile) {
                        transferFile(fileChannel, position, curFile.getPath(), curFile.getBlockSize());
                        position += curFile.getBlockSize();
                        deleteLocalCacheFiles.add(curFile);
                    }
                    if (prevRange != null && prevRange.upperEndpoint().longValue() == curRange.lowerEndpoint().longValue()) {
                        lastRange = newRanges.get(newRanges.size() - 1);
                        newRanges.set(newRanges.size() - 1, Range.closedOpen(lastRange.lowerEndpoint(), curRange.upperEndpoint()));
                    }
                    else {
                        newRanges.add(curRange);
                    }
                    prevRange = curRange;
                    prevFile = curFile;
                }
            }
        }
        catch (IOException e) {
            log.warn(e, "failed to copy local cache file");
            tryDeleteFile(path);
            return false;
        }
        LocalCacheFile newFile = new LocalCacheFile(range.lowerEndpoint(), path, toIntExact(range.upperEndpoint() - range.lowerEndpoint()), fileIdentifier);

        Lock writeLock = cacheRange.getLock().writeLock();
        writeLock.lock();
        try {
            if (cacheRange.isRemoved()) {
                return true;
            }

            // check if the range has updated
            TreeRangeMap<Long, LocalCacheFile> rangeMap = cacheRange.getRange();
            boolean equal = true;
            Map<Range<Long>, LocalCacheFile> curRanges = rangeMap.subRangeMap(range).asMapOfRanges();
            if (curRanges.size() != tmpRanges.size()) {
                equal = false;
            }
            else {
                int i = 0;
                for (Map.Entry<Range<Long>, LocalCacheFile> entry : curRanges.entrySet()) {
                    Range<Long> r1 = entry.getKey();
                    LocalCacheFile f1 = entry.getValue();
                    Range<Long> r2 = tmpRanges.get(i);
                    LocalCacheFile f2 = tmpLocalCacheFiles.get(i);
                    if (!r1.equals(r2) || !f1.equals(f2) || f1.hasMarkDelete()) {
                        equal = false;
                        break;
                    }
                    i++;
                }
            }

            if (equal) {
                updated = true;

                // update the range map
                rangeMap.remove(range);
                newRanges.forEach(r -> rangeMap.put(r, newFile));
                lruTracker.put(newFile);
                deleteLocalCacheFiles.forEach(LocalCacheFile::markDelete);
            }
        }
        finally {
            writeLock.unlock();
        }

        if (!updated) {
            deleteLocalCacheFile(newFile);
        }
        else {
            stats.addDiskAllocatedBytes(newFile.getBlockSize());
        }

        return false;
    }

    private void mergeCacheFile(FileIdentifier fileIdentifier)
    {
        CacheRange cacheRange = persistedRanges.get(fileIdentifier);
        if (cacheRange == null) {
            return;
        }
        fileIdentifier = cacheRange.getFileIdentifier();

        long maxBlockSize = this.maxBlockSizeBytes;
        Range<Long> span;
        List<Map.Entry<Range<Long>, LocalCacheFile>> diskRanges;
        Lock readLock = cacheRange.getLock().readLock();
        readLock.lock();
        try {
            if (cacheRange.isRemoved()) {
                return;
            }
            if (cacheRange.getRange().asMapOfRanges().isEmpty()) {
                return;
            }
            span = cacheRange.getRange().span();
        }
        finally {
            readLock.unlock();
        }

        long lowerAlign = span.lowerEndpoint() & -maxBlockSize;
        long upperAlign = (span.upperEndpoint() + maxBlockSize - 1) & -maxBlockSize;

        for (; lowerAlign < upperAlign; lowerAlign += maxBlockSize) {
            Range<Long> range = Range.closedOpen(lowerAlign, lowerAlign + maxBlockSize);
            readLock.lock();
            try {
                if (cacheRange.isRemoved()) {
                    return;
                }
                diskRanges = new ArrayList<>(cacheRange.getRange().subRangeMap(range).asMapOfRanges().entrySet());
                if (diskRanges.isEmpty()) {
                    continue;
                }
                incRef(diskRanges);
            }
            finally {
                readLock.unlock();
            }

            try {
                // combine
                List<Range<Long>> tmpRanges = new ArrayList<>();
                List<LocalCacheFile> tmpLocalCacheFiles = new ArrayList<>();
                Range<Long> curRange;
                LocalCacheFile curFile;
                Range<Long> prevRange = null;
                LocalCacheFile prevFile = null;
                boolean combine = false;
                for (Map.Entry<Range<Long>, LocalCacheFile> entry : diskRanges) {
                    curRange = entry.getKey();
                    curFile = entry.getValue();
                    if (curFile != prevFile && prevRange != null && prevRange.upperEndpoint().longValue() == curRange.lowerEndpoint().longValue()) {
                        combine = true;
                    }
                    else if (curFile != prevFile) {
                        if (combine) {
                            if (mergeCacheFile(fileIdentifier, tmpRanges, tmpLocalCacheFiles)) {
                                return;
                            }
                            combine = false;
                        }
                        tmpRanges.clear();
                        tmpLocalCacheFiles.clear();
                    }
                    tmpRanges.add(curRange);
                    tmpLocalCacheFiles.add(curFile);
                    prevRange = curRange;
                    prevFile = curFile;
                }

                if (combine) {
                    if (mergeCacheFile(fileIdentifier, tmpRanges, tmpLocalCacheFiles)) {
                        return;
                    }
                }
            }
            finally {
                decRef(diskRanges);
            }
        }
    }

    private int addInCacheFileCount()
    {
        stats.addInCacheFileCount(1);
        return 1;
    }

    private void descInCacheFileCount()
    {
        stats.addInCacheFileCount(-1);
    }

    @Override
    public CacheResult readFully(FileReadRequest request, byte[] buffer, int offset)
    {
        boolean result = internalReadFully(request, buffer, offset);

        try {
            // hint the cache
            cache.get(request.getFileIdentifier(), this::addInCacheFileCount);
        }
        catch (ExecutionException e) {
            // ignore
        }

        if (result) {
            stats.updateCacheHit();
            return CacheResult.hit(request.getLength());
        }

        stats.updateCacheMiss();
        return CacheResult.miss();
    }

    @Override
    public CacheResult read(FileReadRequest request, byte[] buffer, int offset)
    {
        int n = internalRead(request, buffer, offset);

        try {
            // hint the cache
            cache.get(request.getFileIdentifier(), this::addInCacheFileCount);
        }
        catch (ExecutionException e) {
            // ignore
        }

        if (n >= 0) {
            stats.updateCacheHit();
            return CacheResult.hit(n);
        }

        stats.updateCacheMiss();
        return CacheResult.miss();
    }

    private boolean internalReadFully(FileReadRequest request, byte[] buffer, int offset)
    {
        int reqLen = request.getLength();
        long reqOff = request.getOffset();
        if (reqLen <= 0) {
            // no-op
            return true;
        }

        // check if the file is cached on local disk
        CacheRange cacheRange = persistedRanges.get(request.getFileIdentifier());
        if (cacheRange == null) {
            return false;
        }

        List<Map.Entry<Range<Long>, LocalCacheFile>> diskRanges;
        long reqEnd = reqOff + reqLen;
        Lock readLock = cacheRange.getLock().readLock();
        boolean locked = readLock.tryLock();
        if (!locked) {
            return false;
        }
        try {
            diskRanges = new ArrayList<>(cacheRange.getRange().subRangeMap(Range.closedOpen(reqOff, reqEnd)).asMapOfRanges().entrySet());
            if (diskRanges.isEmpty()) {
                // no range
                return false;
            }

            // read lock is ok here
            incRef(diskRanges);
        }
        finally {
            readLock.unlock();
        }

        try {
            // there is chance file has deleted
            // check if files continuous
            long start = reqOff;
            for (Map.Entry<Range<Long>, LocalCacheFile> entry : diskRanges) {
                Range<Long> range = entry.getKey();
                if (start != range.lowerEndpoint()) {
                    return false;
                }
                start = range.upperEndpoint();
            }
            if (start != reqEnd) {
                return false;
            }

            start = reqOff;
            int off = offset;
            int remain = reqLen;
            int len;
            for (Map.Entry<Range<Long>, LocalCacheFile> entry : diskRanges) {
                Range<Long> range = entry.getKey();
                LocalCacheFile cacheFile = entry.getValue();
                try (RandomAccessFile file = new RandomAccessFile(new File(cacheFile.getPath()), "r")) {
                    long fileStart = start - cacheFile.getOffset();
                    len = Math.min(remain, toIntExact(range.upperEndpoint() - start));
                    file.seek(fileStart);
                    file.readFully(buffer, off, len);
                    off += len;
                    remain -= len;
                    start += len;
                    lruTracker.get(cacheFile);
                }
                catch (IOException e) {
                    log.warn(e, "failed to read cache data, exception should not happen here");
                    return false;
                }
            }
            return true;
        }
        finally {
            // do not need lock here
            decRef(diskRanges);
        }
    }

    private int internalRead(FileReadRequest request, byte[] buffer, int offset)
    {
        long reqOff = request.getOffset();
        int reqLen = request.getLength();
        if (reqLen <= 0) {
            // no-op
            return 0;
        }

        // check if the file is cached on local disk
        CacheRange cacheRange = persistedRanges.get(request.getFileIdentifier());
        if (cacheRange == null) {
            return -1;
        }

        List<Map.Entry<Range<Long>, LocalCacheFile>> diskRanges;
        long reqEnd = reqOff + reqLen;
        Lock readLock = cacheRange.getLock().readLock();
        boolean locked = readLock.tryLock();
        if (!locked) {
            return -1;
        }
        try {
            diskRanges = new ArrayList<>(cacheRange.getRange().subRangeMap(Range.closedOpen(reqOff, reqEnd)).asMapOfRanges().entrySet());
            if (diskRanges.isEmpty()) {
                // no range
                return -1;
            }
            // read lock is ok here
            incRef(diskRanges);
        }
        finally {
            readLock.unlock();
        }

        long start = reqOff;
        int off = offset;
        int remain = reqLen;
        int len;
        int nRead = -1;
        try {
            for (Map.Entry<Range<Long>, LocalCacheFile> entry : diskRanges) {
                Range<Long> range = entry.getKey();
                LocalCacheFile cacheFile = entry.getValue();
                if (start != range.lowerEndpoint()) {
                    return nRead;
                }

                try (RandomAccessFile file = new RandomAccessFile(new File(cacheFile.getPath()), "r")) {
                    long fileStart = start - cacheFile.getOffset();
                    len = Math.min(remain, toIntExact(range.upperEndpoint() - start));
                    file.seek(fileStart);
                    file.readFully(buffer, off, len);
                    off += len;
                    remain -= len;
                    start += len;
                    nRead = (nRead == -1) ? len : (nRead + len);
                    lruTracker.get(cacheFile);
                }
                catch (IOException e) {
                    log.warn(e, "failed to read cache data, exception should not happen here");
                    return nRead;
                }
            }
        }
        finally {
            // do not need lock here
            decRef(diskRanges);
        }

        return nRead;
    }

    @Override
    public void write(FileReadRequest key, Slice data)
    {
        if (data.length() == 0 || stats.getInMemoryRetainedBytes() + data.length() >= maxInflightBytes) {
            return;
        }

        // make a copy given the input data could be a reusable buffer
        stats.addInMemoryRetainedBytes(data.length());
        byte[] copy = data.getBytes();

        int mod = (key.getFileIdentifier().hashCode() & Integer.MAX_VALUE) % threadPoolSize;
        try {
            threadPools[mod].submit(() -> {
                try {
                    long start = System.nanoTime();
                    if (internalWrite(key, copy)) {
                        stats.updateDiskWrite();
                        stats.addDiskWriteTime(System.nanoTime() - start);
                    }
                    else {
                        stats.updateDiskWriteFailed();
                        log.info("%s fail to persist cache with length %s", Thread.currentThread().getName(), key.getLength());
                    }
                }
                catch (Throwable t) {
                    stats.updateDiskWriteFailed();
                    log.warn(t, "%s fail to persist cache with length %s ", Thread.currentThread().getName(), key.getLength());
                }
                finally {
                    stats.addInMemoryRetainedBytes(-copy.length);
                }
            });
        }
        catch (Throwable t) {
            log.warn("failed to submit write task");
            stats.addInMemoryRetainedBytes(-copy.length);
        }
    }

    private LocalCacheFile createLocalCacheFile(long blockOffset, int blockSize, FileIdentifier fileIdentifier, long fileOffset, byte[] buf, int off, int len)
    {
        String path = createNewPath();
        try (RandomAccessFile file = new RandomAccessFile(new File(path), "rw")) {
            file.setLength(blockSize);
            file.seek(fileOffset);
            file.write(buf, off, len);
        }
        catch (IOException e) {
            log.warn(e, "failed to create local cache file");
            tryDeleteFile(path);
            return null;
        }
        return new LocalCacheFile(blockOffset, path, blockSize, fileIdentifier);
    }

    private String createNewPath()
    {
        return URI.create(baseDirectory + "/" + randomUUID() + EXTENSION).getPath();
    }

    private static boolean isConnected(long thisLower, long thisUpper, long thatLower, long thatUpper)
    {
        return thisLower < thatUpper && thatLower < thisUpper;
    }

    private static boolean isCover(long thisLower, long thisUpper, long thatLower, long thatUpper)
    {
        return thisLower <= thatLower && thatUpper <= thisUpper;
    }

    private static boolean isCover(List<Map.Entry<Range<Long>, LocalCacheFile>> ranges, long rangeLower, long rangeUpper)
    {
        if (ranges.isEmpty()) {
            return false;
        }
        Range<Long> r = ranges.get(0).getKey();
        long lower = r.lowerEndpoint();
        long upper = r.upperEndpoint();
        if (isCover(lower, upper, rangeLower, rangeUpper)) {
            return true;
        }
        int i = 1;
        int size = ranges.size();
        while (i < size) {
            r = ranges.get(i).getKey();
            if (upper < r.lowerEndpoint()) {
                lower = r.lowerEndpoint();
            }
            upper = r.upperEndpoint();
            if (isCover(lower, upper, rangeLower, rangeUpper)) {
                return true;
            }
            i++;
        }
        return false;
    }

    private boolean internalWrite(FileReadRequest key, byte[] data)
    {
        if (key.getLength() == 0) {
            return true;
        }
        FileIdentifier fileIdentifier = key.getFileIdentifier();

        CacheRange cacheRange = persistedRanges.computeIfAbsent(fileIdentifier, CacheRange::new);
        // use old object first
        fileIdentifier = cacheRange.getFileIdentifier();

        // try to find all exists blocks to allocate data
        long lMinBlockSizeBytes = minBlockSizeBytes;
        long reqEnd = key.getOffset() + key.getLength();
        long reqOffAligned = key.getOffset() & -lMinBlockSizeBytes;
        long reqEndAligned = (reqEnd + lMinBlockSizeBytes - 1) & -lMinBlockSizeBytes;
        Range<Long> range;
        List<Map.Entry<Range<Long>, LocalCacheFile>> diskRanges;
        Lock readLock = cacheRange.getLock().readLock();
        readLock.lock();
        try {
            if (cacheRange.isRemoved()) {
                log.info("failed to persist cache, this file just expired");
                return false;
            }
            // check if there are blocks to write already
            // get entry first to shrink range otherwise we need to use minBlockSizeBytes to align
            TreeRangeMap<Long, LocalCacheFile> rangeMap = cacheRange.getRange();
            Map.Entry<Range<Long>, LocalCacheFile> prev = rangeMap.getFloorEntry(key.getOffset() - 1);
            Map.Entry<Range<Long>, LocalCacheFile> next = rangeMap.getCeilingEntry(reqEnd);
            range = Range.closedOpen(
                    prev == null ? reqOffAligned : prev.getKey().lowerEndpoint(),
                    next == null ? reqEndAligned : next.getKey().upperEndpoint());

            diskRanges = new ArrayList<>(cacheRange.getRange().subRangeMap(range).asMapOfRanges().entrySet());
            incRef(diskRanges);
        }
        finally {
            readLock.unlock();
        }

        // write data
        List<Range<Long>> newRanges = new ArrayList<>();
        List<LocalCacheFile> newLocalCacheFiles = new ArrayList<>();
        List<LocalCacheFile> newCreatedLocalCacheFiles = new ArrayList<>();

        boolean updated = false;
        long preAllocatedDisk = 0;

        try {
            if (isCover(diskRanges, key.getOffset(), reqEnd)) {
                return true;
            }

            long curOffset;
            Range<Long> curRange;
            LocalCacheFile curFile;
            long curEnd;
            Range<Long> prevRange;
            LocalCacheFile prevFile;

            long reqOff = key.getOffset();
            int off = 0;
            int len;
            int remain = key.getLength();

            // calculate range and file for new data
            prevFile = null;
            List<LocalCacheFile> existFiles = new ArrayList<>(diskRanges.size());
            // deduplication
            for (Map.Entry<Range<Long>, LocalCacheFile> entry : diskRanges) {
                curFile = entry.getValue();
                if (prevFile == null || curFile != prevFile) {
                    existFiles.add(curFile);
                }
                prevFile = curFile;
            }

            List<Range<Long>> tmpRanges = new ArrayList<>();
            List<LocalCacheFile> tmpLocalCacheFiles = new ArrayList<>();
            int i = 0;
            int size = existFiles.size();
            long lMaxBlockSizeBytes = maxBlockSizeBytes;
            while (remain > 0) {
                if (i >= size || reqOff < existFiles.get(i).getOffset()) {
                    long maxEnd = (reqOff & -lMaxBlockSizeBytes) + lMaxBlockSizeBytes;
                    if (i < size) {
                        maxEnd = Math.min(existFiles.get(i).getOffset(), maxEnd);
                    }
                    long expectedEnd = (reqOff + remain + lMinBlockSizeBytes - 1) & -lMinBlockSizeBytes;
                    long blockSize = Math.min(maxEnd, expectedEnd) - reqOffAligned;
                    if (!evictFor(blockSize)) {
                        log.warn("failed to persist cache, not enough free space after evict, block size: %d", blockSize);
                        return false;
                    }
                    stats.addDiskAllocatedBytes(blockSize);
                    preAllocatedDisk += blockSize;
                    // length should not exceed a block
                    len = toIntExact(Math.min(reqOff + remain, reqOffAligned + blockSize) - reqOff);
                    curFile = createLocalCacheFile(reqOffAligned, toIntExact(blockSize), cacheRange.getFileIdentifier(), reqOff - reqOffAligned, data, off, len);
                    if (curFile == null) {
                        return false;
                    }
                    tmpRanges.add(Range.closedOpen(reqOff, reqOff + len));
                    tmpLocalCacheFiles.add(curFile);
                    newCreatedLocalCacheFiles.add(curFile);
                }
                else if (reqOff >= existFiles.get(i).getOffset() + existFiles.get(i).getBlockSize()) {
                    len = 0;
                    i++;
                }
                else {
                    curFile = existFiles.get(i);
                    curOffset = curFile.getOffset();
                    curEnd = curFile.getOffset() + curFile.getBlockSize();
                    checkArgument(curOffset <= reqOff && reqOff < curEnd, "current reqOffset should always in this block");
                    len = toIntExact(Math.min(reqOff + remain, curEnd) - reqOff);
                    try (RandomAccessFile file = new RandomAccessFile(new File(curFile.getPath()), "rw")) {
                        file.seek(reqOff - curOffset);
                        file.write(data, off, len);
                    }
                    catch (IOException e) {
                        log.warn(e, "failed to persist cache, exception should not happen here");
                        return false;
                    }
                    tmpRanges.add(Range.closedOpen(reqOff, reqOff + len));
                    tmpLocalCacheFiles.add(curFile);
                    i++;
                }

                off += len;
                remain -= len;
                reqOff += len;
                reqOffAligned = reqOff & -lMinBlockSizeBytes;
            }

            // combine range
            i = 0;
            size = diskRanges.size();
            int j = 0;
            int size2 = tmpRanges.size();
            while (i < size && j < size2) {
                prevRange = diskRanges.get(i).getKey();
                prevFile = diskRanges.get(i).getValue();
                curRange = tmpRanges.get(j);
                curFile = tmpLocalCacheFiles.get(j);
                if (prevFile.getOffset() == curFile.getOffset()) {
                    checkArgument(prevFile == curFile, "when offset equal, cache file must be same object");
                    long lower = curRange.lowerEndpoint();
                    long upper = curRange.upperEndpoint();
                    while (i < size && diskRanges.get(i).getValue() == curFile && diskRanges.get(i).getKey().upperEndpoint() < lower) {
                        newRanges.add(diskRanges.get(i).getKey());
                        newLocalCacheFiles.add(diskRanges.get(i).getValue());
                        i++;
                    }

                    while (i < size && diskRanges.get(i).getValue() == curFile && diskRanges.get(i).getKey().lowerEndpoint() <= upper) {
                        prevRange = diskRanges.get(i).getKey();
                        lower = Math.min(lower, prevRange.lowerEndpoint());
                        upper = Math.max(upper, prevRange.upperEndpoint());
                        i++;
                    }
                    newRanges.add(Range.closedOpen(lower, upper));
                    newLocalCacheFiles.add(curFile);

                    while (i < size && diskRanges.get(i).getValue() == curFile) {
                        newRanges.add(diskRanges.get(i).getKey());
                        newLocalCacheFiles.add(diskRanges.get(i).getValue());
                        i++;
                    }
                    j++;
                }
                else if (prevFile.getOffset() < curFile.getOffset()) {
                    newRanges.add(prevRange);
                    newLocalCacheFiles.add(prevFile);
                    i++;
                }
                else {
                    newRanges.add(curRange);
                    newLocalCacheFiles.add(curFile);
                    j++;
                }
            }
            while (i < size) {
                newRanges.add(diskRanges.get(i).getKey());
                newLocalCacheFiles.add(diskRanges.get(i).getValue());
                i++;
            }
            while (j < size2) {
                newRanges.add(tmpRanges.get(j));
                newLocalCacheFiles.add(tmpLocalCacheFiles.get(j));
                j++;
            }

            // update range
            cacheRange = persistedRanges.get(fileIdentifier);
            if (cacheRange == null) {
                return false;
            }
            Lock writeLock = cacheRange.getLock().writeLock();
            writeLock.lock();
            try {
                if (cacheRange.isRemoved()) {
                    return false;
                }

                // check if the range has updated
                // signal thread foreach file, but maybe
                TreeRangeMap<Long, LocalCacheFile> rangeMap = cacheRange.getRange();
                long prevLength = 0;
                boolean equal = true;
                Map.Entry<Range<Long>, LocalCacheFile> prev = rangeMap.getFloorEntry(key.getOffset() - 1);
                Map.Entry<Range<Long>, LocalCacheFile> next = rangeMap.getCeilingEntry(key.getOffset() + key.getLength());
                reqOffAligned = key.getOffset() & -lMinBlockSizeBytes;
                range = Range.closedOpen(
                        prev == null ? reqOffAligned : prev.getKey().lowerEndpoint(),
                        next == null ? reqEndAligned : next.getKey().upperEndpoint());
                Map<Range<Long>, LocalCacheFile> curRanges = rangeMap.subRangeMap(range).asMapOfRanges();
                if (curRanges.size() != diskRanges.size()) {
                    equal = false;
                }
                else {
                    i = 0;
                    for (Map.Entry<Range<Long>, LocalCacheFile> entry : curRanges.entrySet()) {
                        Range<Long> r = entry.getKey();
                        Map.Entry<Range<Long>, LocalCacheFile> listEntry = diskRanges.get(i);
                        if (!r.equals(listEntry.getKey()) || !entry.getValue().equals(listEntry.getValue())) {
                            equal = false;
                            break;
                        }
                        prevLength += r.upperEndpoint() - r.lowerEndpoint();
                        i++;
                    }
                }

                if (!equal) {
                    log.info("failed to persist cache, index has been modified");
                    return false;
                }
                else {
                    updated = true;

                    // update the range map and track disk usage
                    rangeMap.remove(range);

                    long curLength = 0;
                    reqOff = key.getOffset();
                    reqEnd = reqOff + key.getLength();
                    prevFile = null;
                    size = newRanges.size();
                    for (i = 0; i < size; i++) {
                        Range<Long> r = newRanges.get(i);
                        LocalCacheFile f = newLocalCacheFiles.get(i);
                        rangeMap.put(r, f);
                        curLength += r.upperEndpoint() - r.lowerEndpoint();
                        if (f != prevFile) {
                            long offset = f.getOffset();
                            long end = offset + f.getBlockSize();
                            // isConnected
                            if (isConnected(reqOff, reqEnd, offset, end)) {
                                lruTracker.put(f);
                            }
                        }
                        prevFile = f;
                    }

                    stats.addDiskUsedBytes(curLength - prevLength);

                    return true;
                }
            }
            finally {
                writeLock.unlock();
            }
        }
        finally {
            // no lock is needed for the following operation
            decRef(diskRanges);
            if (!updated) {
                stats.addDiskAllocatedBytes(-preAllocatedDisk);
                newCreatedLocalCacheFiles.forEach(FileMergeCacheManager::deleteLocalCacheFile);
            }
            else {
                cacheUpdated.compute(fileIdentifier, (k, v) -> v == null ? System.currentTimeMillis() : v);
            }
        }
    }

    // TODO: evict same file in same thread, then write and merge operator will need not to check range again in write lock
    private boolean evictFor(long blockSize)
    {
        if (blockSize > diskQuotaBytes) {
            return false;
        }
        long diskAllocatedBytes = stats.getDiskAllocatedBytes();
        if (diskAllocatedBytes + blockSize <= diskQuotaBytes) {
            return true;
        }

        LocalCacheFile file;
        while (diskAllocatedBytes + blockSize > diskQuotaBytes && (file = lruTracker.pollLeast()) != null) {
            CacheRange cacheRange = persistedRanges.get(file.getFileIdentifier());
            if (cacheRange == null) {
                continue;
            }
            long usedDisk = 0;
            Lock writeLock = cacheRange.getLock().writeLock();
            writeLock.lock();
            try {
                if (cacheRange.isRemoved() || !file.markDelete()) {
                    continue;
                }

                long off = file.getOffset();
                TreeRangeMap<Long, LocalCacheFile> rangeMap = cacheRange.getRange();
                Range<Long> fileRange = Range.closedOpen(off, off + file.getBlockSize());
                for (Map.Entry<Range<Long>, LocalCacheFile> entry : rangeMap.subRangeMap(fileRange).asMapOfRanges().entrySet()) {
                    Range<Long> r = entry.getKey();
                    usedDisk += r.upperEndpoint() - r.lowerEndpoint();
                }
                rangeMap.remove(fileRange);
                file.addAndGetRefCount(1);
            }
            finally {
                writeLock.unlock();
            }
            diskAllocatedBytes -= file.getBlockSize();
            stats.addDiskUsedBytes(-usedDisk);
            int ref = file.addAndGetRefCount(-1);
            if (ref == 0) {
                tryDeleteLocalCacheFile(file);
            }
        }

        return diskAllocatedBytes + blockSize <= diskQuotaBytes;
    }

    private boolean tryDeleteLocalCacheFile(LocalCacheFile localCacheFile)
    {
        try {
            if (localCacheFile.hasMarkDelete() && localCacheFile.transferToDeleted()) {
                lruTracker.remove(localCacheFile);
                File file = new File(localCacheFile.getPath());
                Files.delete(file.toPath());
                stats.addDiskAllocatedBytes(-localCacheFile.getBlockSize());
                return true;
            }
        }
        catch (IOException e) {
            log.warn(e, "failed to delete local cache file, exception should not happen here");
        }
        return false;
    }

    private static void deleteLocalCacheFile(LocalCacheFile localCacheFile)
    {
        try {
            File file = new File(localCacheFile.getPath());
            Files.delete(file.toPath());
        }
        catch (IOException ignore) {
            // ignore
        }
    }

    private static void tryDeleteFile(String path)
    {
        try {
            File file = new File(path);
            if (file.exists()) {
                Files.delete(file.toPath());
            }
        }
        catch (IOException e) {
            // ignore
        }
    }

    private void incRef(List<Map.Entry<Range<Long>, LocalCacheFile>> ranges)
    {
        for (Map.Entry<Range<Long>, LocalCacheFile> entry : ranges) {
            entry.getValue().addAndGetRefCount(1);
        }
    }

    private void decRef(List<Map.Entry<Range<Long>, LocalCacheFile>> ranges)
    {
        for (Map.Entry<Range<Long>, LocalCacheFile> entry : ranges) {
            LocalCacheFile localCacheFile = entry.getValue();
            int ref = localCacheFile.addAndGetRefCount(-1);
            if (ref == 0) {
                tryDeleteLocalCacheFile(localCacheFile);
            }
        }
    }

    @VisibleForTesting
    static class LocalCacheFile
    {
        // each LocalCacheFile object about 156 bytes: 76 bytes (this object field and 2 AtomicInteger) + 80 bytes (String path object)
        // fileIdentifier is shared and not calculated here
        private final long offset;  // the original file offset
        private final String path;    // the cache location on disk
        private final int blockSize;    // the cache file length on disk
        private final FileIdentifier fileIdentifier;    // the target file
        private final AtomicInteger refCount = new AtomicInteger(0);
        private final AtomicInteger status = new AtomicInteger(0);
        private static final int STATUS_MARK_DELETE = 1;
        private static final int STATUS_HAS_DELETED = STATUS_MARK_DELETE << 1;

        public LocalCacheFile(long offset, String path, int blockSize, FileIdentifier fileIdentifier)
        {
            this.offset = offset;
            this.path = requireNonNull(path, "path is null");
            this.blockSize = blockSize;
            this.fileIdentifier = requireNonNull(fileIdentifier, "fileIdentifier is null");
        }

        public long getOffset()
        {
            return offset;
        }

        public String getPath()
        {
            return path;
        }

        public FileIdentifier getFileIdentifier()
        {
            return fileIdentifier;
        }

        public int getRefCount()
        {
            return refCount.get();
        }

        public int addAndGetRefCount(int delta)
        {
            return refCount.addAndGet(delta);
        }

        public boolean hasMarkDelete()
        {
            return (status.get() & STATUS_MARK_DELETE) == STATUS_MARK_DELETE;
        }

        public boolean markDelete()
        {
            int oldVal = status.get();
            int newVal;
            while ((oldVal & STATUS_MARK_DELETE) != STATUS_MARK_DELETE) {
                newVal = oldVal | STATUS_MARK_DELETE;
                if (status.compareAndSet(oldVal, newVal)) {
                    return true;
                }
                oldVal = status.get();
            }
            return false;
        }

        public boolean hasDeleted()
        {
            return (status.get() & STATUS_HAS_DELETED) == STATUS_HAS_DELETED;
        }

        //  guard code to protect from double delete, theoretically is not needed
        public boolean transferToDeleted()
        {
            int oldVal = status.get();
            int newVal;
            while ((oldVal & STATUS_HAS_DELETED) != STATUS_HAS_DELETED) {
                newVal = oldVal | STATUS_HAS_DELETED;
                if (status.compareAndSet(oldVal, newVal)) {
                    return true;
                }
                oldVal = status.get();
            }
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LocalCacheFile that = (LocalCacheFile) o;
            return Objects.equals(offset, that.offset) && Objects.equals(path, that.path);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(offset, path);
        }

        public int getBlockSize()
        {
            return blockSize;
        }
    }

    @VisibleForTesting
    static class CacheRange
    {
        private final TreeRangeMap<Long, LocalCacheFile> range = TreeRangeMap.create();
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final FileIdentifier fileIdentifier;
        private boolean removed;

        private CacheRange(FileIdentifier fileIdentifier)
        {
            this.fileIdentifier = fileIdentifier;
        }

        public TreeRangeMap<Long, LocalCacheFile> getRange()
        {
            return range;
        }

        public ReadWriteLock getLock()
        {
            return lock;
        }

        public FileIdentifier getFileIdentifier()
        {
            return fileIdentifier;
        }

        public void markRemoved()
        {
            removed = true;
        }

        public boolean isRemoved()
        {
            return removed;
        }
    }

    private class CacheRemovalListener
            implements RemovalListener<FileIdentifier, Integer>
    {
        @Override
        public void onRemoval(RemovalNotification<FileIdentifier, Integer> notification)
        {
            FileIdentifier identifier = notification.getKey();
            if (identifier == null) {
                return;
            }
            descInCacheFileCount();
            try {
                int mod = (identifier.hashCode() & Integer.MAX_VALUE) % threadPoolSize;
                threadPools[mod].submit(() -> removeFile(identifier));
            }
            catch (Throwable t) {
                log.warn(t, "failed to submit task, remove file on current thread %s", Thread.currentThread().getName());
                removeFile(identifier);
            }
        }

        public void removeFile(FileIdentifier identifier)
        {
            CacheRange cacheRange = persistedRanges.remove(identifier);
            if (cacheRange == null) {
                return;
            }
            cacheUpdated.remove(identifier);

            List<Map.Entry<Range<Long>, LocalCacheFile>> ranges;
            Lock writeLock = cacheRange.getLock().writeLock();
            writeLock.lock();
            try {
                // to protect from some code add new entry to this RangeMap
                cacheRange.markRemoved();
                ranges = new ArrayList<>(cacheRange.getRange().asMapOfRanges().entrySet());
                cacheRange.getRange().clear();
            }
            finally {
                writeLock.unlock();
            }

            long usedBytes = 0;
            for (Map.Entry<Range<Long>, LocalCacheFile> entry : ranges) {
                Range<Long> range = entry.getKey();
                LocalCacheFile file = entry.getValue();
                usedBytes += range.upperEndpoint() - range.lowerEndpoint();
                file.addAndGetRefCount(1);
                file.markDelete();
            }
            stats.addDiskUsedBytes(-usedBytes);

            // do not combine with above for loop, LocalCacheFile maybe referenced by many range,
            // although tryDeleteLocalCacheFile has guard code to protect from double delete
            int ref;
            for (Map.Entry<Range<Long>, LocalCacheFile> entry : ranges) {
                LocalCacheFile file = entry.getValue();
                ref = file.addAndGetRefCount(-1);
                if (ref == 0) {
                    tryDeleteLocalCacheFile(file);
                }
            }
        }
    }

    @VisibleForTesting
    long rangeSize()
    {
        return persistedRanges.values().stream().map(r -> {
            Lock lock = r.getLock().readLock();
            lock.lock();
            try {
                return (long) r.getRange().asMapOfRanges().size();
            }
            finally {
                lock.unlock();
            }
        }).mapToLong(v -> v).sum();
    }

    @Override
    public boolean isValid()
    {
        return true;
    }
}
