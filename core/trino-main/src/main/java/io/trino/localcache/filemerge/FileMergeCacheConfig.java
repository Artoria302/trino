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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import io.trino.util.PowerOfTwoDataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;

public class FileMergeCacheConfig
{
    private int maxCachedEntries = 50_000;
    private Duration cacheTtl = new Duration(7, DAYS);
    private DataSize maxInMemoryCacheSize = DataSize.of(2L, GIGABYTE);
    private Optional<DataSize> diskQuota = Optional.empty();
    private double diskQuotaRate = 0.7;
    private DataSize minBlockSize = DataSize.of(4, MEGABYTE);
    private DataSize maxBlockSize = DataSize.of(128, MEGABYTE);
    private boolean mergeEnabled = true;
    private Duration mergeInterval = Duration.succinctDuration(15, TimeUnit.MINUTES);
    private int ioThreads = Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);

    @Min(1)
    public int getMaxCachedEntries()
    {
        return maxCachedEntries;
    }

    @Config("cache.max-cached-entries")
    @ConfigDescription("Number of entries allowed in the cache")
    public FileMergeCacheConfig setMaxCachedEntries(int maxCachedEntries)
    {
        this.maxCachedEntries = maxCachedEntries;
        return this;
    }

    public DataSize getMaxInMemoryCacheSize()
    {
        return maxInMemoryCacheSize;
    }

    @Config("cache.max-in-memory-cache-size")
    @ConfigDescription("The maximum cache size allowed in memory")
    public FileMergeCacheConfig setMaxInMemoryCacheSize(DataSize maxInMemoryCacheSize)
    {
        this.maxInMemoryCacheSize = maxInMemoryCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("cache.ttl")
    @ConfigDescription("Time-to-live for a cache entry")
    public FileMergeCacheConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    public Optional<DataSize> getDiskQuota()
    {
        return diskQuota;
    }

    @Config("cache.disk-quota")
    @ConfigDescription("max disk space to cache data, default to using cache.disk-quota-rate of the total space on the disk where base directory is located")
    public FileMergeCacheConfig setDiskQuota(DataSize diskQuota)
    {
        if (diskQuota != null) {
            this.diskQuota = Optional.of(diskQuota);
        }
        return this;
    }

    @DecimalMin("0.05")
    @DecimalMax("0.95")
    public double getDiskQuotaRate()
    {
        return diskQuotaRate;
    }

    @Config("cache.disk-quota-rate")
    @ConfigDescription("max disk space rate to cache data, default to using 80% of the total space on the disk where base directory is located")
    public FileMergeCacheConfig setDiskQuotaRate(double diskQuotaRate)
    {
        this.diskQuotaRate = diskQuotaRate;
        return this;
    }

    @MinDataSize("1MB")
    @PowerOfTwoDataSize
    public DataSize getMinBlockSize()
    {
        return minBlockSize;
    }

    @Config("cache.min-block-size")
    @ConfigDescription("min size of each local file")
    public FileMergeCacheConfig setMinBlockSize(DataSize blockSize)
    {
        this.minBlockSize = blockSize;
        return this;
    }

    @MaxDataSize("1GB")
    @PowerOfTwoDataSize
    public DataSize getMaxBlockSize()
    {
        return maxBlockSize;
    }

    @Config("cache.max-block-size")
    @ConfigDescription("max size of each local file")
    public FileMergeCacheConfig setMaxBlockSize(DataSize blockSize)
    {
        this.maxBlockSize = blockSize;
        return this;
    }

    public boolean isMergeEnabled()
    {
        return mergeEnabled;
    }

    @Config("cache.merge-enabled")
    @ConfigDescription("if auto merge adjacent cache file to max block size or not")
    public FileMergeCacheConfig setMergeEnabled(boolean enable)
    {
        this.mergeEnabled = enable;
        return this;
    }

    public Duration getMergeInterval()
    {
        return mergeInterval;
    }

    @Config("cache.merge-interval")
    @ConfigDescription("merge task schedule interval")
    public FileMergeCacheConfig setMergeInterval(Duration mergeInterval)
    {
        this.mergeInterval = mergeInterval;
        return this;
    }

    @Min(1)
    public int getIoThreads()
    {
        return ioThreads;
    }

    @Config("cache.io-threads")
    @ConfigDescription("threads to write/merge cache file, the default value is number of available processors")
    public FileMergeCacheConfig setIoThreads(int ioThreads)
    {
        this.ioThreads = ioThreads;
        return this;
    }
}
