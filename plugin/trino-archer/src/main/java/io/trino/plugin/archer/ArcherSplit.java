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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class ArcherSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(ArcherSplit.class);

    private final String segmentPath;
    private final String fileName;
    private final int version;
    private final long start;
    private final long length;
    private final OptionalLong limit;
    private final long fileSize;
    private final long fileRecordCount;
    private final long lastModifiedTime;
    private final ArcherFileFormat fileFormat;
    private final String partitionSpecJson;
    private final String partitionDataJson;
    private final Optional<String> invertedIndexJson;
    private final Optional<String> invertedIndexQueryJson;
    private final Optional<String> invertedIndexFilesJson;
    private final Optional<String> deletionJson;
    private final SplitWeight splitWeight;
    private final TupleDomain<ArcherColumnHandle> fileStatisticsDomain;
    private final List<HostAddress> addresses;

    @JsonCreator
    public ArcherSplit(
            @JsonProperty("segmentPath") String segmentPath,
            @JsonProperty("fileName") String fileName,
            @JsonProperty("version") int version,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileRecordCount") long fileRecordCount,
            @JsonProperty("lastModifiedTime") long lastModifiedTime,
            @JsonProperty("fileFormat") ArcherFileFormat fileFormat,
            @JsonProperty("partitionSpecJson") String partitionSpecJson,
            @JsonProperty("partitionDataJson") String partitionDataJson,
            @JsonProperty("invertedIndexJson") Optional<String> invertedIndexJson,
            @JsonProperty("invertedIndexQueryJson") Optional<String> invertedIndexQueryJson,
            @JsonProperty("invertedIndexFilesJson") Optional<String> invertedIndexFilesJson,
            @JsonProperty("deletionJson") Optional<String> deletionJson,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("fileStatisticsDomain") TupleDomain<ArcherColumnHandle> fileStatisticsDomain,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.segmentPath = requireNonNull(segmentPath, "segmentPath is null");
        this.fileName = requireNonNull(fileName, "fileName is null");
        this.version = version;
        this.start = start;
        this.length = length;
        this.limit = requireNonNull(limit, "limit is null");
        this.fileSize = fileSize;
        this.fileRecordCount = fileRecordCount;
        this.lastModifiedTime = lastModifiedTime;
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.invertedIndexJson = requireNonNull(invertedIndexJson, "invertedIndexJson is null");
        this.invertedIndexQueryJson = requireNonNull(invertedIndexQueryJson, "invertedIndexQueryJson is null");
        this.invertedIndexFilesJson = requireNonNull(invertedIndexFilesJson, "invertedIndexFilesJson is null");
        this.deletionJson = requireNonNull(deletionJson, "deletionJson is null");
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        this.fileStatisticsDomain = requireNonNull(fileStatisticsDomain, "fileStatisticsDomain is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public String getSegmentPath()
    {
        return segmentPath;
    }

    @JsonProperty
    public String getFileName()
    {
        return fileName;
    }

    @JsonProperty
    public int getVersion()
    {
        return version;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public long getFileRecordCount()
    {
        return fileRecordCount;
    }

    @JsonProperty
    public long getLastModifiedTime()
    {
        return lastModifiedTime;
    }

    @JsonProperty
    public ArcherFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public String getPartitionSpecJson()
    {
        return partitionSpecJson;
    }

    @JsonProperty
    public String getPartitionDataJson()
    {
        return partitionDataJson;
    }

    @JsonProperty
    public Optional<String> getInvertedIndexJson()
    {
        return invertedIndexJson;
    }

    @JsonProperty
    public Optional<String> getInvertedIndexQueryJson()
    {
        return invertedIndexQueryJson;
    }

    @JsonProperty
    public Optional<String> getInvertedIndexFilesJson()
    {
        return invertedIndexFilesJson;
    }

    @JsonProperty
    public Optional<String> getDeletionJson()
    {
        return deletionJson;
    }

    @JsonProperty
    public TupleDomain<ArcherColumnHandle> getFileStatisticsDomain()
    {
        return fileStatisticsDomain;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.<String, String>builder()
                .put("segmentPath", segmentPath)
                .put("start", String.valueOf(start))
                .put("length", String.valueOf(length))
                .buildOrThrow();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(segmentPath)
                + estimatedSizeOf(partitionSpecJson)
                + estimatedSizeOf(partitionDataJson)
                + splitWeight.getRetainedSizeInBytes()
                + fileStatisticsDomain.getRetainedSizeInBytes(ArcherColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .addValue(segmentPath)
                .add("records", fileRecordCount);
        return helper.toString();
    }
}
