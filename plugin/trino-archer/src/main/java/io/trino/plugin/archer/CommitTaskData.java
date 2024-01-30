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
import io.trino.plugin.archer.util.VersionedPath;
import net.qihoo.archer.FileContent;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CommitTaskData
{
    private final String segmentPath;
    private final String fileName;
    private final ArcherFileFormat fileFormat;
    private final long fileSizeInBytes;
    private final long lastModifiedTime;
    private final Optional<String> invertedIndexFilesJson;
    private final Optional<String> deletionJson;
    private final MetricsWrapper metrics;
    private final String partitionSpecJson;
    private final Optional<String> partitionDataJson;
    private final FileContent content;
    private final Optional<VersionedPath> referencedDataFile;

    @JsonCreator
    public CommitTaskData(
            @JsonProperty("segmentPath") String segmentPath,
            @JsonProperty("fileName") String fileName,
            @JsonProperty("fileFormat") ArcherFileFormat fileFormat,
            @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
            @JsonProperty("lastModifiedTime") long lastModifiedTime,
            @JsonProperty("invertedIndexFilesJson") Optional<String> invertedIndexFilesJson,
            @JsonProperty("deletionJson") Optional<String> deletionJson,
            @JsonProperty("metrics") MetricsWrapper metrics,
            @JsonProperty("partitionSpecJson") String partitionSpecJson,
            @JsonProperty("partitionDataJson") Optional<String> partitionDataJson,
            @JsonProperty("content") FileContent content,
            @JsonProperty("referencedDataFile") Optional<VersionedPath> referencedDataFile)
    {
        this.segmentPath = requireNonNull(segmentPath, "segmentPath is null");
        this.fileName = requireNonNull(fileName, "fileName is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.fileSizeInBytes = fileSizeInBytes;
        this.lastModifiedTime = lastModifiedTime;
        this.invertedIndexFilesJson = requireNonNull(invertedIndexFilesJson, "invertedIndexFilesJson is null");
        this.deletionJson = requireNonNull(deletionJson, "deletionJson is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.content = requireNonNull(content, "content is null");
        this.referencedDataFile = requireNonNull(referencedDataFile, "referencedDataFile is null");
        checkArgument(fileSizeInBytes >= 0, "fileSizeInBytes is negative");
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
    public ArcherFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public long getFileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @JsonProperty
    public long getLastModifiedTime()
    {
        return lastModifiedTime;
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
    public MetricsWrapper getMetrics()
    {
        return metrics;
    }

    @JsonProperty
    public String getPartitionSpecJson()
    {
        return partitionSpecJson;
    }

    @JsonProperty
    public Optional<String> getPartitionDataJson()
    {
        return partitionDataJson;
    }

    @JsonProperty
    public FileContent getContent()
    {
        return content;
    }

    @JsonProperty
    public Optional<VersionedPath> getReferencedDataFile()
    {
        return referencedDataFile;
    }
}
