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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.archer.ArcherConfig.FORMAT_VERSION_SUPPORT_MAX;
import static io.trino.plugin.archer.ArcherConfig.FORMAT_VERSION_SUPPORT_MIN;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class ArcherTableProperties
{
    public static final String FILE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONING_PROPERTY = "partitioning";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String INVERTED_INDEXED_BY_PROPERTY = "inverted_indexed_by";
    public static final String LOCATION_PROPERTY = "location";
    public static final String FORMAT_VERSION_PROPERTY = "format_version";
    public static final String ENABLE_DELETE_METADATA_AFTER_COMMIT = "enable_delete_metadata_after_commit";
    public static final String MAX_PREVIOUS_METADATA_VERSIONS = "max_previous_metadata_versions";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public ArcherTableProperties(ArcherConfig archerConfig)
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        FILE_FORMAT_PROPERTY,
                        "File format for the table",
                        ArcherFileFormat.class,
                        archerConfig.getFileFormat(),
                        false))
                .add(new PropertyMetadata<>(
                        PARTITIONING_PROPERTY,
                        "Partition transforms",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(new PropertyMetadata<>(
                        INVERTED_INDEXED_BY_PROPERTY,
                        "Inverted indexed columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .add(integerProperty(
                        FORMAT_VERSION_PROPERTY,
                        "Archer table format version",
                        archerConfig.getFormatVersion(),
                        ArcherTableProperties::validateFormatVersion,
                        false))
                .add(booleanProperty(
                        ENABLE_DELETE_METADATA_AFTER_COMMIT,
                        "Delete metadata after commit",
                        archerConfig.isEnableDeleteMetadataAfterCommit(),
                        false))
                .add(integerProperty(
                        MAX_PREVIOUS_METADATA_VERSIONS,
                        "Maximum number of previous version for metadata",
                        archerConfig.getMaxMetadataVersions(),
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitioning(Map<String, Object> tableProperties)
    {
        List<String> partitioning = (List<String>) tableProperties.get(PARTITIONING_PROPERTY);
        return partitioning == null ? ImmutableList.of() : ImmutableList.copyOf(partitioning);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getSortOrder(Map<String, Object> tableProperties)
    {
        List<String> sortedBy = (List<String>) tableProperties.get(SORTED_BY_PROPERTY);
        return sortedBy == null ? ImmutableList.of() : ImmutableList.copyOf(sortedBy);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getInvertedIndex(Map<String, Object> tableProperties)
    {
        List<String> indexedBy = (List<String>) tableProperties.get(INVERTED_INDEXED_BY_PROPERTY);
        return indexedBy == null ? ImmutableList.of() : ImmutableList.copyOf(indexedBy);
    }

    public static Optional<String> getTableLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }

    public static int getFormatVersion(Map<String, Object> tableProperties)
    {
        return (int) tableProperties.get(FORMAT_VERSION_PROPERTY);
    }

    public static ArcherFileFormat getFileFormat(Map<String, Object> tableProperties)
    {
        return (ArcherFileFormat) tableProperties.get(FILE_FORMAT_PROPERTY);
    }

    private static void validateFormatVersion(int version)
    {
        if (version < FORMAT_VERSION_SUPPORT_MIN || version > FORMAT_VERSION_SUPPORT_MAX) {
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    format("format_version must be between %d and %d", FORMAT_VERSION_SUPPORT_MIN, FORMAT_VERSION_SUPPORT_MAX));
        }
    }

    public static boolean enableDeleteMetadataAfterCommit(Map<String, Object> tableProperties)
    {
        return (Boolean) tableProperties.get(ENABLE_DELETE_METADATA_AFTER_COMMIT);
    }

    public static int getMaxMetadataVersions(Map<String, Object> tableProperties)
    {
        return (Integer) tableProperties.get(MAX_PREVIOUS_METADATA_VERSIONS);
    }
}
