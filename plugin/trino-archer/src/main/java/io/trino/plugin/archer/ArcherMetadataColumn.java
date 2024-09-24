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

import io.trino.spi.type.Type;
import net.qihoo.archer.MetadataColumns;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_DYNAMIC_REPARTITIONING_VALUE_ID;
import static io.trino.plugin.archer.ArcherColumnHandle.TRINO_DYNAMIC_REPARTITIONING_VALUE_NAME;
import static io.trino.plugin.archer.ColumnIdentity.TypeCategory;
import static io.trino.plugin.archer.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;

public enum ArcherMetadataColumn
{
    FILE_PATH(MetadataColumns.FILE_PATH.fieldId(), "$path", VARCHAR, PRIMITIVE),
    FILE_MODIFIED_TIME(Integer.MAX_VALUE - 1001, "$file_modified_time", TIMESTAMP_TZ_MILLIS, PRIMITIVE), // https://github.com/apache/iceberg/issues/5240
    ROW_POS(MetadataColumns.ROW_POSITION.fieldId(), "$pos", BIGINT, PRIMITIVE),
    DYNAMIC_REPARTITIONING_VALUE(TRINO_DYNAMIC_REPARTITIONING_VALUE_ID, TRINO_DYNAMIC_REPARTITIONING_VALUE_NAME, INTEGER, PRIMITIVE), // https://github.com/apache/iceberg/issues/5240
    /**/;

    private static final Set<Integer> COLUMNS_ID = Stream.of(values())
            .map(ArcherMetadataColumn::getId)
            .collect(toImmutableSet());
    private final int id;
    private final String columnName;
    private final Type type;
    private final TypeCategory typeCategory;

    ArcherMetadataColumn(int id, String columnName, Type type, TypeCategory typeCategory)
    {
        this.id = id;
        this.columnName = columnName;
        this.type = type;
        this.typeCategory = typeCategory;
    }

    public int getId()
    {
        return id;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Type getType()
    {
        return type;
    }

    public TypeCategory getTypeCategory()
    {
        return typeCategory;
    }

    public static boolean isMetadataColumnId(int id)
    {
        return COLUMNS_ID.contains(id);
    }
}
