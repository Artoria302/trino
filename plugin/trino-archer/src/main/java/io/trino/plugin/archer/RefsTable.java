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
import io.trino.plugin.archer.util.PageListBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TimeZoneKey;
import net.qihoo.archer.StructLike;
import net.qihoo.archer.Table;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static net.qihoo.archer.MetadataTableType.REFS;

public class RefsTable
        extends BaseSystemTable
{
    private static final List<ColumnMetadata> COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("name", VARCHAR))
            .add(new ColumnMetadata("type", VARCHAR))
            .add(new ColumnMetadata("snapshot_id", BIGINT))
            .add(new ColumnMetadata("max_reference_age_in_ms", BIGINT))
            .add(new ColumnMetadata("min_snapshots_to_keep", INTEGER))
            .add(new ColumnMetadata("max_snapshot_age_in_ms", BIGINT))
            .build();

    public RefsTable(SchemaTableName tableName, Table archerTable)
    {
        super(
                requireNonNull(archerTable, "archerTable is null"),
                new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS),
                REFS);
    }

    @Override
    protected void addRow(PageListBuilder pagesBuilder, StructLike structLike, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        pagesBuilder.beginRow();
        pagesBuilder.appendVarchar(structLike.get(columnNameToPositionInSchema.get("name"), String.class));
        pagesBuilder.appendVarchar(structLike.get(columnNameToPositionInSchema.get("type"), String.class));
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get("snapshot_id"), Long.class));
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get("max_reference_age_in_ms"), Long.class));
        pagesBuilder.appendInteger(structLike.get(columnNameToPositionInSchema.get("min_snapshots_to_keep"), Integer.class));
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get("max_snapshot_age_in_ms"), Long.class));
        pagesBuilder.endRow();
    }
}
