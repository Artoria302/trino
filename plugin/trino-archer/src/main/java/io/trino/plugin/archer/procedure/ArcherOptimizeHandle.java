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
package io.trino.plugin.archer.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.plugin.archer.ArcherColumnHandle;
import io.trino.plugin.archer.ArcherFileFormat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record ArcherOptimizeHandle(
        Optional<Long> snapshotId,
        String schemaAsJson,
        String partitionSpecAsJson,
        Optional<String> invertedIndexAsJson,
        List<ArcherColumnHandle> tableColumns,
        ArcherFileFormat fileFormat,
        Map<String, String> tableStorageProperties,
        DataSize maxScannedFileSize,
        boolean retriesEnabled)
        implements ArcherProcedureHandle
{
    public ArcherOptimizeHandle
    {
        requireNonNull(snapshotId, "snapshotId is null");
        requireNonNull(schemaAsJson, "schemaAsJson is null");
        requireNonNull(partitionSpecAsJson, "partitionSpecAsJson is null");
        requireNonNull(invertedIndexAsJson, "invertedIndexAsJson is null");
        ImmutableList.copyOf(requireNonNull(tableColumns, "tableColumns is null"));
        requireNonNull(fileFormat, "fileFormat is null");
        ImmutableMap.copyOf(requireNonNull(tableStorageProperties, "tableStorageProperties is null"));
        requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
    }
}
