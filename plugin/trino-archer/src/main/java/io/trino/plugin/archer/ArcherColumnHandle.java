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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.archer.ArcherMetadataColumn.DYNAMIC_REPARTITIONING_VALUE;
import static io.trino.plugin.archer.ArcherMetadataColumn.FILE_MODIFIED_TIME;
import static io.trino.plugin.archer.ArcherMetadataColumn.FILE_PATH;
import static io.trino.plugin.archer.ArcherMetadataColumn.ROW_POS;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.MetadataColumns.IS_DELETED;
import static net.qihoo.archer.MetadataColumns.ROW_POSITION;

public class ArcherColumnHandle
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(ArcherColumnHandle.class);

    // Archer reserved row ids begin at INTEGER.MAX_VALUE and count down. Starting with MIN_VALUE here to avoid conflicts.
    public static final int TRINO_UPDATE_ROW_ID = Integer.MIN_VALUE;
    public static final int TRINO_MERGE_ROW_ID = Integer.MIN_VALUE + 1;
    public static final String TRINO_ROW_ID_NAME = "$row_id";

    public static final int TRINO_MERGE_FILE_RECORD_COUNT = Integer.MIN_VALUE + 2;
    public static final int TRINO_MERGE_PARTITION_SPEC_ID = Integer.MIN_VALUE + 3;
    public static final int TRINO_MERGE_PARTITION_DATA = Integer.MIN_VALUE + 4;
    public static final int TRINO_MERGE_DELETION = Integer.MIN_VALUE + 5;

    public static final String TRINO_DYNAMIC_REPARTITIONING_VALUE_NAME = "_dynamic_repartitioning_value";
    public static final int TRINO_DYNAMIC_REPARTITIONING_VALUE_ID = Integer.MIN_VALUE + 6;

    private final ColumnIdentity baseColumnIdentity;
    private final Type baseType;
    // The list of field ids to indicate the projected part of the top-level column represented by baseColumnIdentity
    private final List<Integer> path;
    private final Type type;
    private final boolean nullable;
    private final Optional<String> comment;
    // Cache of ColumnIdentity#getId to ensure quick access, even with dereferences
    private final int id;

    @JsonCreator
    public ArcherColumnHandle(
            @JsonProperty("baseColumnIdentity") ColumnIdentity baseColumnIdentity,
            @JsonProperty("baseType") Type baseType,
            @JsonProperty("path") List<Integer> path,
            @JsonProperty("type") Type type,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.baseColumnIdentity = requireNonNull(baseColumnIdentity, "baseColumnIdentity is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.nullable = nullable;
        this.id = path.isEmpty() ? baseColumnIdentity.getId() : Iterables.getLast(path);
    }

    @JsonIgnore
    public ColumnIdentity getColumnIdentity()
    {
        ColumnIdentity columnIdentity = baseColumnIdentity;
        for (int fieldId : path) {
            columnIdentity = columnIdentity.getChildByFieldId(fieldId);
        }
        return columnIdentity;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public ColumnIdentity getBaseColumnIdentity()
    {
        return baseColumnIdentity;
    }

    @JsonProperty
    public Type getBaseType()
    {
        return baseType;
    }

    @JsonIgnore
    public ArcherColumnHandle getBaseColumn()
    {
        return new ArcherColumnHandle(getBaseColumnIdentity(), getBaseType(), ImmutableList.of(), getBaseType(), isNullable(), Optional.empty());
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonIgnore
    public int getId()
    {
        return id;
    }

    /**
     * For nested columns, this is the unqualified name of the last field in the path
     */
    @JsonIgnore
    public String getName()
    {
        return getColumnIdentity().getName();
    }

    @JsonProperty
    public List<Integer> getPath()
    {
        return path;
    }

    /**
     * The dot separated path components used to address this column, including all dereferences and the column name.
     */
    @JsonIgnore
    public String getQualifiedName()
    {
        ImmutableList.Builder<String> pathNames = ImmutableList.builder();
        ColumnIdentity columnIdentity = baseColumnIdentity;
        pathNames.add(columnIdentity.getName());
        for (int fieldId : path) {
            columnIdentity = columnIdentity.getChildByFieldId(fieldId);
            pathNames.add(columnIdentity.getName());
        }
        // Archer tables are guaranteed not to have ambiguous column names so joining them like this must uniquely identify a single column.
        return String.join(".", pathNames.build());
    }

    @JsonIgnore
    public boolean isBaseColumn()
    {
        return path.isEmpty();
    }

    @JsonIgnore
    public boolean isRowPositionColumn()
    {
        return id == ROW_POS.getId();
    }

    @JsonIgnore
    public boolean isUpdateRowIdColumn()
    {
        return id == TRINO_UPDATE_ROW_ID;
    }

    @JsonIgnore
    public boolean isMergeRowIdColumn()
    {
        return id == TRINO_MERGE_ROW_ID;
    }

    /**
     * Marker column used by the Archer DeleteFilter to indicate rows which are deleted by equality deletes.
     */
    @JsonIgnore
    public boolean isIsDeletedColumn()
    {
        return id == IS_DELETED.fieldId();
    }

    @JsonIgnore
    public boolean isFileModifiedTimeColumn()
    {
        return id == FILE_MODIFIED_TIME.getId();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseColumnIdentity, baseType, path, type, nullable, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ArcherColumnHandle other = (ArcherColumnHandle) obj;
        return Objects.equals(this.baseColumnIdentity, other.baseColumnIdentity) &&
                Objects.equals(this.baseType, other.baseType) &&
                Objects.equals(this.path, other.path) &&
                Objects.equals(this.type, other.type) &&
                this.nullable == other.nullable &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return getId() + ":" + getName() + ":" + type.getDisplayName();
    }

    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE
                + baseColumnIdentity.getRetainedSizeInBytes()
                + estimatedSizeOf(path, SizeOf::sizeOf)
                + sizeOf(nullable)
                + sizeOf(comment, SizeOf::estimatedSizeOf)
                + sizeOf(id);
    }

    public static ArcherColumnHandle pathColumnHandle()
    {
        return new ArcherColumnHandle(
                columnIdentity(FILE_PATH),
                FILE_PATH.getType(),
                ImmutableList.of(),
                FILE_PATH.getType(),
                false,
                Optional.empty());
    }

    public static ColumnMetadata pathColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(FILE_PATH.getColumnName())
                .setType(FILE_PATH.getType())
                .setHidden(true)
                .build();
    }

    public static ArcherColumnHandle posColumnHandle()
    {
        return new ArcherColumnHandle(
                columnIdentity(ROW_POS),
                ROW_POS.getType(),
                ImmutableList.of(),
                ROW_POS.getType(),
                false,
                Optional.of(ROW_POSITION.doc()));
    }

    public static ColumnMetadata posColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(ROW_POS.getColumnName())
                .setType(ROW_POS.getType())
                .setHidden(true)
                .build();
    }

    public static ArcherColumnHandle fileModifiedTimeColumnHandle()
    {
        return new ArcherColumnHandle(
                columnIdentity(FILE_MODIFIED_TIME),
                FILE_MODIFIED_TIME.getType(),
                ImmutableList.of(),
                FILE_MODIFIED_TIME.getType(),
                false,
                Optional.empty());
    }

    public static ColumnMetadata fileModifiedTimeColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(FILE_MODIFIED_TIME.getColumnName())
                .setType(FILE_MODIFIED_TIME.getType())
                .setHidden(true)
                .build();
    }

    public static ArcherColumnHandle dynamicRepartitioningValueColumnHandle()
    {
        return new ArcherColumnHandle(
                columnIdentity(DYNAMIC_REPARTITIONING_VALUE),
                DYNAMIC_REPARTITIONING_VALUE.getType(),
                ImmutableList.of(),
                DYNAMIC_REPARTITIONING_VALUE.getType(),
                false,
                Optional.empty());
    }

    public static ColumnMetadata dynamicRepartitioningValueColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(DYNAMIC_REPARTITIONING_VALUE.getColumnName())
                .setType(DYNAMIC_REPARTITIONING_VALUE.getType())
                .setHidden(true)
                .build();
    }

    private static ColumnIdentity columnIdentity(ArcherMetadataColumn metadata)
    {
        return new ColumnIdentity(metadata.getId(), metadata.getColumnName(), metadata.getTypeCategory(), ImmutableList.of());
    }

    public boolean isPathColumn()
    {
        return getColumnIdentity().getId() == FILE_PATH.getId();
    }
}
