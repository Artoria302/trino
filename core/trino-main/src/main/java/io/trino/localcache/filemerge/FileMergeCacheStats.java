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

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class FileMergeCacheStats
{
    private final AtomicLong inMemoryRetainedBytes = new AtomicLong();
    private final AtomicLong inCacheFileCount = new AtomicLong();
    private final CounterStat hit = new CounterStat();
    private final CounterStat miss = new CounterStat();
    private final AtomicLong diskUsedBytes = new AtomicLong();
    private final AtomicLong diskAllocatedBytes = new AtomicLong();
    private final AtomicLong diskQuotaBytes = new AtomicLong();
    private final CounterStat diskWrite = new CounterStat();
    private final CounterStat diskWriteFailed = new CounterStat();
    private final TimeStat diskWriteTime = new TimeStat(MILLISECONDS);
    private final CounterStat fileMerge = new CounterStat();
    private final CounterStat fileMergeFailed = new CounterStat();
    private final TimeStat fileMergeTime = new TimeStat(MILLISECONDS);

    @Managed
    public long getInMemoryRetainedBytes()
    {
        return inMemoryRetainedBytes.get();
    }

    public void addInMemoryRetainedBytes(long bytes)
    {
        inMemoryRetainedBytes.addAndGet(bytes);
    }

    @Managed
    public long getInCacheFileCount()
    {
        return inCacheFileCount.get();
    }

    public void addInCacheFileCount(long cnt)
    {
        inCacheFileCount.addAndGet(cnt);
    }

    @Managed
    @Nested
    public CounterStat getCacheHit()
    {
        return hit;
    }

    public long getCacheHitTotal()
    {
        return hit.getTotalCount();
    }

    public void updateCacheHit()
    {
        hit.update(1);
    }

    @Managed
    @Nested
    public CounterStat getCacheMiss()
    {
        return miss;
    }

    public long getCacheMissTotal()
    {
        return miss.getTotalCount();
    }

    public void updateCacheMiss()
    {
        miss.update(1);
    }

    @Managed
    public long getDiskQuotaBytes()
    {
        return diskQuotaBytes.get();
    }

    public void setDiskQuotaBytes(long diskQuotaBytes)
    {
        this.diskQuotaBytes.set(diskQuotaBytes);
    }

    @Managed
    public long getDiskUsedBytes()
    {
        return diskUsedBytes.get();
    }

    public void addDiskUsedBytes(long bytes)
    {
        diskUsedBytes.addAndGet(bytes);
    }

    @Managed
    public long getDiskAllocatedBytes()
    {
        return diskAllocatedBytes.get();
    }

    public void addDiskAllocatedBytes(long bytes)
    {
        diskAllocatedBytes.addAndGet(bytes);
    }

    @Managed
    @Nested
    public CounterStat getDiskWrite()
    {
        return diskWrite;
    }

    public void updateDiskWrite()
    {
        diskWrite.update(1);
    }

    @Managed
    @Nested
    public CounterStat getDiskWriteFailed()
    {
        return diskWriteFailed;
    }

    public void updateDiskWriteFailed()
    {
        diskWriteFailed.update(1);
    }

    @Managed
    @Nested
    public TimeStat getDiskWriteTime()
    {
        return diskWriteTime;
    }

    public void addDiskWriteTime(long nano)
    {
        diskWriteTime.add(nano, NANOSECONDS);
    }

    @Managed
    @Nested
    public CounterStat getFileMerge()
    {
        return fileMerge;
    }

    public void updateFileMerge()
    {
        fileMerge.update(1);
    }

    @Managed
    @Nested
    public CounterStat getFileMergeFailed()
    {
        return fileMergeFailed;
    }

    public void updateFileMergeFailed()
    {
        fileMergeFailed.update(1);
    }

    @Managed
    @Nested
    public TimeStat getFileMergeTime()
    {
        return fileMergeTime;
    }

    public void addFileMergeTime(long nano)
    {
        fileMergeTime.add(nano, NANOSECONDS);
    }
}
