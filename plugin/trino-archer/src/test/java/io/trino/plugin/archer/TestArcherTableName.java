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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArcherTableName
{
    @Test
    public void testFrom()
    {
        assertParseNameAndType("abc", "abc", TableType.DATA);
        assertParseNameAndType("abc$history", "abc", TableType.HISTORY);
        assertParseNameAndType("abc$snapshots", "abc", TableType.SNAPSHOTS);

        assertInvalid("abc$data");
        assertInvalid("abc@123");
        assertInvalid("abc@xyz");
        assertInvalid("abc$what");
        assertInvalid("abc@123$data@456");
        assertInvalid("abc@123$snapshots");
        assertInvalid("abc$snapshots@456");
        assertInvalid("xyz$data@456");
        assertInvalid("abc$partitions@456");
        assertInvalid("abc$manifests@456");
    }

    @Test
    public void testIsDataTable()
    {
        assertThat(ArcherTableName.isDataTable("abc")).isTrue();

        assertThatThrownBy(() -> ArcherTableName.isDataTable("abc$data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Archer table name: abc$data");

        assertThat(ArcherTableName.isDataTable("abc$history")).isFalse();

        assertThatThrownBy(() -> ArcherTableName.isDataTable("abc$invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Archer table name: abc$invalid");
    }

    @Test
    public void testTableNameFrom()
    {
        assertThat(ArcherTableName.tableNameFrom("abc")).isEqualTo("abc");

        assertThatThrownBy(() -> ArcherTableName.tableNameFrom("abc$data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Archer table name: abc$data");

        assertThat(ArcherTableName.tableNameFrom("abc$history")).isEqualTo("abc");

        assertThatThrownBy(() -> ArcherTableName.tableNameFrom("abc$invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Archer table name: abc$invalid");
    }

    @Test
    public void testTableTypeFrom()
    {
        assertThat(ArcherTableName.tableTypeFrom("abc")).isEqualTo(TableType.DATA);

        assertThatThrownBy(() -> ArcherTableName.tableTypeFrom("abc$data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Archer table name: abc$data");

        assertThat(ArcherTableName.tableTypeFrom("abc$history")).isEqualTo(TableType.HISTORY);

        assertThatThrownBy(() -> ArcherTableName.tableTypeFrom("abc$invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Archer table name: abc$invalid");
    }

    @Test
    public void testTableNameWithType()
    {
        assertThat(ArcherTableName.tableNameWithType("abc", TableType.DATA)).isEqualTo("abc$data");
        assertThat(ArcherTableName.tableNameWithType("abc", TableType.HISTORY)).isEqualTo("abc$history");
    }

    private static void assertInvalid(String inputName)
    {
        assertThat(ArcherTableName.isArcherTableName(inputName)).isFalse();

        assertThatThrownBy(() -> ArcherTableName.tableTypeFrom(inputName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid Archer table name: " + inputName);
    }

    private static void assertParseNameAndType(String inputName, String tableName, TableType tableType)
    {
        assertThat(ArcherTableName.isArcherTableName(inputName)).isTrue();
        assertThat(ArcherTableName.tableNameFrom(inputName)).isEqualTo(tableName);
        assertThat(ArcherTableName.tableTypeFrom(inputName)).isEqualTo(tableType);
    }
}
