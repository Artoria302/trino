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
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import net.qihoo.archer.FileFormat;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseArcherConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected final FileFormat format;

    public BaseArcherConnectorSmokeTest(FileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN -> false;
            case SUPPORTS_CREATE_VIEW -> true;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW -> true;
            case SUPPORTS_DELETE -> true;
            case SUPPORTS_ROW_LEVEL_DELETE -> true;
            case SUPPORTS_UPDATE, SUPPORTS_MERGE -> true;
            case SUPPORTS_CREATE_OR_REPLACE_TABLE -> true;
            case SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("" +
                        "CREATE TABLE archer." + schemaName + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        format("   location = '.*/" + schemaName + "/region.*'\n") +
                        "\\)");
    }

    @Test
    public void testHiddenPathColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "hidden_file_path", "(a int, b VARCHAR)", ImmutableList.of("(1, 'a')"))) {
            String filePath = (String) computeScalar(format("SELECT file_path FROM \"%s$files\"", table.getName()));

            assertQuery("SELECT DISTINCT \"$path\" FROM " + table.getName(), "VALUES " + "'" + filePath + "'");

            // Check whether the "$path" hidden column is correctly evaluated in the filter expression
            assertQuery(format("SELECT a FROM %s WHERE \"$path\" = '%s'", table.getName(), filePath), "VALUES 1");
        }
    }

    protected String expectedValues3(String values)
    {
        return format("SELECT CAST(a AS bigint), CAST(b AS double), d FROM (VALUES %s) AS t (a, b, d)", values);
    }

    @Test
    public void testCreateInvertedIndexTextTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b double, d varchar(512)) with (inverted_indexed_by = ARRAY['d TEXT'])");
        assertThat(query("SELECT a, b, d FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("insert into " + tableName + " (a, b, d) VALUES (1, 3.4, 'slc sGhh soaf'), (13, 99.9, 'sle sUJ sBP sTj shh')", 2);
        assertThat(query("select a, b, d from " + tableName + " where inv_match(d, 'soaf')"))
                .skippingTypesCheck()
                .matches(expectedValues3("(1, 3.4, 'slc sGhh soaf')"));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateInvertedIndexJsonTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b double, c varchar(1024), d varchar(102400), e varchar(512)) with (inverted_indexed_by = ARRAY['c JSON PROPERTIES(tokenizer=raw)', 'c JSON as c_jieba PROPERTIES(tokenizer=jieba_search)', 'd TEXT', 'e IP'])");
        assertThat(query("SELECT a, b, d FROM " + tableName))
                .returnsEmptyResult();

        assertUpdate("insert into " + tableName + " (a, b, c, d, e) VALUES (1, 3.4, '{\"title\": \"aaa bbb ccc\",\"year\": 1937}', 'slc sGhh soaf', '192.168.0.90'), (13, 99.9, '{\"title\": \"The Modern Prometheus\",\"year\": 1818, }','sle sUJ sBP sTj shh', '114.114.114.114')", 2);
        assertThat(query("select a, b, d from " + tableName + " where inv_match_json(c, 'year', '1937')"))
                .skippingTypesCheck()
                .matches(expectedValues3("(1, 3.4, 'slc sGhh soaf')"));
        assertThat(query("select a, b, d from " + tableName + " where inv_match_json(c, 'year', '1937') and a=1"))
                .skippingTypesCheck()
                .matches(expectedValues3("(1, 3.4, 'slc sGhh soaf')"));

        assertUpdate("insert into " + tableName + " (a, b, c, d, e) VALUES (2, 6.7, '{\"title\": \"ddd eee fff\",\"year\": 2024}', 'eee fff dir', '192.168.1.90'), (15, 99.9, '{\"title\": \"The Modern Prometheus\",\"year\": 2018, }','sle sUJ sBP sTj shh', '114.114.114.114')", 2);
        assertThat(query("select a, b, d from " + tableName + " where inv_match_json(c, 'year', '1937')"))
                .skippingTypesCheck()
                .matches(expectedValues3("(1, 3.4, 'slc sGhh soaf')"));

        assertUpdate("insert into " + tableName + " (a, b, c, d, e) VALUES (3, 6.74443, '{\"title\": \"ggg hhh iii\",\"year\": 2026}', " +
                "'You will rejoice to hear that no disaster has accompanied the commencement of an enterprise which you have regarded with such evil forebodings. " +
                "I arrived here yesterday, and my first task is to assure my dear sister of my welfare and increasing confidence in the success of my undertaking.', " +
                "'192.168.1.180')", 1);
        assertUpdate("insert into " + tableName + " (a, b, c, d, e) VALUES (5, 18.7, '{\"title\": [\"jjj kkk lll\", \"The Modern Prometheus\"],\"year\": 1984}', " +
                "'He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.', " +
                "'2001:0db8:85a3:0000:0000:8a2e:0370:7334')", 1);
        assertThat(query("select a, b, e from " + tableName + " where inv_match_index(d, 'd', 'Gulf')"))
                .skippingTypesCheck()
                .matches(expectedValues3("(5, 18.7, '2001:0db8:85a3:0000:0000:8a2e:0370:7334')"));
        // json in set
        assertThat(query("select a from " + tableName + " where inv_match_json_in_set(c, 'year', ARRAY['1937', '2024'])"))
                .skippingTypesCheck()
                .matches(expectedOneBigint("(1), (2)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_json_in_set(c, 'c_jieba', 'title', ARRAY['aaa', 'hhh'])"))
                .skippingTypesCheck()
                .matches(expectedOneBigint("(1), (3)"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateInvertedIndexIPTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b double, d varchar(512)) with (inverted_indexed_by = ARRAY['d IP'])");
        assertThat(query("SELECT a, b, d FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("insert into " + tableName + " (a, b, d) VALUES (1, 3.4, '192.168.0.90'), (13, 99.9, '114.114.114.114')", 2);
        assertThat(query("select a, b, d from " + tableName + " " + "where inv_match(d, '192.168.0.90')"))
                .skippingTypesCheck()
                .matches(expectedValues3("(1, 3.4, '192.168.0.90')"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInvertedIndexQueryWithRowId()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b double, d varchar(512)) with (inverted_indexed_by = ARRAY['d IP'])");
        assertThat(query("SELECT a, b, d FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("insert into " + tableName + " (a, b, d) VALUES (1, 3.4, '192.168.0.90'), (13, 99.9, '114.114.114.114')", 2);

        assertThat(query("select \"$pos\", b, d from " + tableName + " " + "where inv_match(d, '192.168.0.90')"))
                .skippingTypesCheck()
                .matches(expectedValues3("(0, 3.4, '192.168.0.90')"));
        assertThat(query("select \"$pos\", b, d from " + tableName + " " + "where inv_match(d, '114.114.114.114')"))
                .skippingTypesCheck()
                .matches(expectedValues3("(1, 99.9, '114.114.114.114')"));
        assertUpdate("DROP TABLE " + tableName);
    }

    protected String expectedValues1(String values)
    {
        return format("SELECT CAST(a AS bigint) FROM (VALUES %s) AS t (a)", values);
    }

    @Test
    public void testAllType()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(\n" +
                "  dt_type date,\n" +
                "  str_type varchar,\n" +
                "  uuid_type UUID,\n" +
                "  time_type TIME(6),\n" +
                "  ts_type TIMESTAMP(6),\n" +
                "  ts_tz_type TIMESTAMP(6) with time zone,\n" +
                "  int_type int,\n" +
                "  bigint_type bigint,\n" +
                "  real_type real,\n" +
                "  double_type double,\n" +
                "  decimal_type DECIMAL(35, 3),\n" +
                "  boolean_type boolean,\n" +
                "  varbinary_type VARBINARY,\n" +
                "  array_type1 Array(UUID),\n" +
                "  array_type2 Array(Array(varchar)),\n" +
                "  array_type3 Array(Map(varchar, varchar)),\n" +
                "  array_type4 Array(Row(col1 varchar, col2 Array(varchar))),\n" +
                "  map_type1 Map(varchar, varchar),\n" +
                "  map_type2 Map(varchar, Array(DECIMAL(10, 3))),\n" +
                "  map_type3 Map(varchar, Row(col1 varchar, col2 int)),\n" +
                "  map_type4 Map(varchar, Map(varchar, varchar)),\n" +
                "  row_type Row(col1 UUID, col2 Array(int), col3 Map(varchar, varchar), col4 Row(col1 int, col2 varchar))\n" +
                ")");
        assertThat(query("SELECT dt_type FROM " + tableName))
                .returnsEmptyResult();

        assertUpdate("insert into " + tableName + " values (\n" +
                "      date '2023-06-10',\n" +
                "      'abc',\n" +
                "\t  UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59',\n" +
                "\t  TIME '01:02:03.456',\n" +
                "\t  TIMESTAMP '2023-06-10 15:55:23',\n" +
                "\t  TIMESTAMP '2023-06-10 15:55:23+0800',\n" +
                "\t  1,\n" +
                "\t  2,\n" +
                "\t  0.3,\n" +
                "\t  0.4,\n" +
                "      cast('1234.555' as DECIMAL(35, 3)),\n" +
                "      true,\n" +
                "\t  x'65683FAA',\n" +
                "\t  Array[UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59', UUID '12151fd2-7586-11e9-8f9e-2a86e4085a5a'],\n" +
                "      Array[Array['2a86e4085a59', '2a86e4085a5a'], Array['2a86e4085a59', '2a86e4085a5a']],\n" +
                "\t  Array[Map(Array['a', 'b', 'c'], Array['va', 'vb', 'bc']), Map(Array['d', 'e', 'f'], Array['vd', 've', 'bf'])],\n" +
                "      Array[ROW('2aa59', Array['2a86e4085a59', '2a86e4085a5a']), ROW('adf23', Array['afdbfba', '213fdfv'])],\n" +
                "      Map(Array['a', 'b', 'c'], Array['va', 'vb', 'bc']),\n" +
                "\t  Map(Array['a', 'b', 'c'], Array[Array[cast(1234567.888 as DECIMAL(35, 3))], null, null]),\n" +
                "\t  Map(Array['a', 'b', 'c'], Array[ROW('a', 1),  ROW('b', 2),  ROW('c', 3)]),\n" +
                "\t  Map(Array['a', 'b', 'c'], Array[Map(Array['a', 'b', 'c'], Array['va', 'vb', 'bc']), Map(Array['d', 'e', 'f'], Array['vd', 've', 'bf']), null]),\n" +
                "      ROW(UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59', Array[1, 2, 3], Map(Array['a', 'b', 'c'], Array['va', 'vb', 'bc']), ROW(123, 'row2'))\n" +
                "  ),\n" +
                "  (\n" +
                "      date '2023-07-10',\n" +
                "      'def',\n" +
                "\t  UUID '12151fd2-7586-11e9-8f9e-2a86e4085a5a',\n" +
                "\t  TIME '02:02:03.456',\n" +
                "\t  TIMESTAMP '2023-07-10 15:55:23',\n" +
                "\t  TIMESTAMP '2023-07-10 15:55:23+0800',\n" +
                "\t  3,\n" +
                "\t  4,\n" +
                "\t  0.5,\n" +
                "\t  0.6,\n" +
                "      cast('4321.555' as DECIMAL(35, 3)),\n" +
                "      false,\n" +
                "\t  x'65683F',\n" +
                "\t  Array[UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59', UUID '12151fd2-7586-11e9-8f9e-2a86e4085a5a'],\n" +
                "      Array[Array['2a86e4085a59', '2a86e4085a5a'], Array['2a86e4085a59', '2a86e4085a5a']],\n" +
                "\t  Array[Map(Array['a', 'b', 'c'], Array['va', 'vb', 'bc']), Map(Array['d', 'e', 'f'], Array['vd', 've', 'bf'])],\n" +
                "      Array[ROW('2aa59', Array['2a86e4085a59', '2a86e4085a5a']), ROW('adf23', Array['afdbfba', '213fdfv'])],\n" +
                "      Map(Array['a', 'b', 'c'], Array['va', 'vb', 'bc']),\n" +
                "\t  Map(Array['a', 'b', 'c'], Array[Array[cast(1234567.888 as DECIMAL(35, 3))], null, null]),\n" +
                "\t  Map(Array['a', 'b', 'c'], Array[ROW('a', 1),  ROW('b', 2),  ROW('c', 3)]),\n" +
                "\t  Map(Array['a', 'b', 'c'], Array[Map(Array['a', 'b', 'c'], Array['va', 'vb', 'bc']), Map(Array['d', 'e', 'f'], Array['vd', 've', 'bf']), null]),\n" +
                "      ROW(UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59', Array[1, 2, 3], Map(Array['a', 'b', 'c'], Array['va', 'vb', 'bc']), ROW(123, 'row2'))\n" +
                "  )", 2);

        assertThat(query("select \"$pos\" from " + tableName + " " + "where dt_type = date '2023-06-10'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(0)"));
        assertThat(query("select \"$pos\" from " + tableName + " " + "where dt_type = date '2023-07-10'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(1)"));

        assertThat(query("select \"$pos\" from " + tableName + " " + "where time_type = TIME '01:02:03.456'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(0)"));
        assertThat(query("select \"$pos\" from " + tableName + " " + "where time_type = TIME '02:02:03.456'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(1)"));

        assertThat(query("select \"$pos\" from " + tableName + " " + "where ts_type = TIMESTAMP '2023-06-10 15:55:23'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(0)"));
        assertThat(query("select \"$pos\" from " + tableName + " " + "where ts_type = TIMESTAMP '2023-07-10 15:55:23'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(1)"));

        assertThat(query("select \"$pos\" from " + tableName + " " + "where ts_tz_type = TIMESTAMP '2023-06-10 15:55:23+0800'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(0)"));
        assertThat(query("select \"$pos\" from " + tableName + " " + "where ts_tz_type = TIMESTAMP '2023-07-10 15:55:23+0800'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(1)"));

        assertThat(query("select \"$pos\" from " + tableName + " " + "where uuid_type = UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(0)"));
        assertThat(query("select \"$pos\" from " + tableName + " " + "where uuid_type = UUID '12151fd2-7586-11e9-8f9e-2a86e4085a5a'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(1)"));

        assertThat(query("select \"$pos\" from " + tableName + " " + "where varbinary_type = x'65683FAA'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(0)"));
        assertThat(query("select \"$pos\" from " + tableName + " " + "where varbinary_type = x'65683F'"))
                .skippingTypesCheck()
                .matches(expectedValues1("(1)"));

        assertThat(query("select \"$pos\" from " + tableName + " " + "where decimal_type = cast('1234.555' as DECIMAL(35, 3))"))
                .skippingTypesCheck()
                .matches(expectedValues1("(0)"));
        assertThat(query("select \"$pos\" from " + tableName + " " + "where decimal_type = cast('4321.555' as DECIMAL(35, 3))"))
                .skippingTypesCheck()
                .matches(expectedValues1("(1)"));
    }

    @Test
    public void testRowLevelDelete2()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_row_delete", "AS SELECT * FROM region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE regionkey = 2"))
                    .returnsEmptyResult();
            assertThat(query("SELECT cast(regionkey AS integer) FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES 0, 1, 3, 4");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 3", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE regionkey = 3"))
                    .returnsEmptyResult();
            assertThat(query("SELECT cast(regionkey AS integer) FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES 0, 1, 4");
        }
    }

    @Test
    public void testUpdate2()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_", getCreateTableDefaultDefinition())) {
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) SELECT regionkey, regionkey * 2.5 FROM region", "SELECT count(*) FROM region");
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 0.0), (1, 2.5), (2, 5.0), (3, 7.5), (4, 10.0)"));

            assertUpdate("UPDATE " + table.getName() + " SET b = b + 1.2 WHERE a % 2 = 0", 3);
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 1.2), (1, 2.5), (2, 6.2), (3, 7.5), (4, 11.2)"));

            assertUpdate("UPDATE " + table.getName() + " SET b = b + 1.2 WHERE a >= 3", 2);
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 1.2), (1, 2.5), (2, 6.2), (3, 8.7), (4, 12.4)"));
        }
    }

    @Test
    public void testMerge2()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_merge_", getCreateTableDefaultDefinition())) {
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) SELECT regionkey, regionkey * 2.5 FROM region", "SELECT count(*) FROM region");
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 0.0), (1, 2.5), (2, 5.0), (3, 7.5), (4, 10.0)"));

            assertUpdate("MERGE INTO " + table.getName() + " t " +
                            "USING (VALUES (0, 1.3), (2, 2.9), (3, 0.0), (4, -5.0), (5, 5.7)) AS s (a, b) " +
                            "ON (t.a = s.a) " +
                            "WHEN MATCHED AND s.b > 0 THEN UPDATE SET b = t.b + s.b " +
                            "WHEN MATCHED AND s.b = 0 THEN DELETE " +
                            "WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b)",
                    4);
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 1.3), (1, 2.5), (2, 7.9), (4, 10.0), (5, 5.7)"));

            assertUpdate("MERGE INTO " + table.getName() + " t " +
                            "USING (VALUES (1, -1.0), (4, -5.0), (6, 7.7)) AS s (a, b) " +
                            "ON (t.a = s.a) " +
                            "WHEN MATCHED AND s.a = 1 THEN UPDATE SET b = t.b + s.b " +
                            "WHEN MATCHED AND s.a = 4 THEN DELETE " +
                            "WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b)",
                    3);
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 1.3), (1, 1.5), (2, 7.9), (5, 5.7), (6, 7.7)"));
        }
    }

    @Test
    public void testInvertedIndexQueryWithMergeDelete()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b double) with (inverted_indexed_by = ARRAY['a RAW'])");
        assertUpdate("INSERT INTO " + tableName + " (a, b) SELECT regionkey, regionkey * 2.5 FROM region", "SELECT count(*) FROM region");
        assertThat(query("SELECT a, b FROM " + tableName))
                .matches(expectedValues("(0, 0.0), (1, 2.5), (2, 5.0), (3, 7.5), (4, 10.0)"));

        assertThat(query("select a, b from " + tableName + " " + "where inv_match(a, '3')"))
                .skippingTypesCheck()
                .matches(expectedValues("(3, 7.5)"));

        assertUpdate("MERGE INTO " + tableName + " t " +
                        "USING (VALUES (0, 1.3), (2, 2.9), (3, 0.0), (4, -5.0), (5, 5.7)) AS s (a, b) " +
                        "ON (t.a = s.a) " +
                        "WHEN MATCHED AND s.b > 0 THEN UPDATE SET b = t.b + s.b " +
                        "WHEN MATCHED AND s.b = 0 THEN DELETE " +
                        "WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b)",
                4);
        assertThat(query("SELECT a, b FROM " + tableName))
                .matches(expectedValues("(0, 1.3), (1, 2.5), (2, 7.9), (4, 10.0), (5, 5.7)"));

        assertThat(query("select a, b from " + tableName + " " + "where inv_match(a, '3')"))
                .returnsEmptyResult();
        assertThat(query("select a, b from " + tableName + " " + "where inv_match(a, '5')"))
                .skippingTypesCheck().matches(expectedValues("(5, 5.7)"));
        assertThat(query("select a, b from " + tableName + " " + "where inv_match(a, '2')"))
                .skippingTypesCheck().matches(expectedValues("(2, 7.9)"));
        assertThat(query("select a, b from " + tableName + " " + "where inv_match(a, '1')"))
                .skippingTypesCheck().matches(expectedValues("(1, 2.5)"));

        assertUpdate("DELETE FROM " + tableName + " WHERE a = 5", 1);
        assertThat(query("SELECT * FROM " + tableName + " WHERE a = 5"))
                .returnsEmptyResult();
        assertThat(query("select a, b from " + tableName + " " + "where inv_match(a, '5')"))
                .returnsEmptyResult();

        assertThat(query("SELECT a, b FROM " + tableName))
                .matches(expectedValues("(0, 1.3), (1, 2.5), (2, 7.9), (4, 10.0)"));

        assertUpdate("DROP TABLE " + tableName);
    }

    private static String expectedTwoBigint(String values)
    {
        return format("SELECT CAST(a AS bigint), CAST(b AS bigint) FROM (VALUES %s) AS t (a, b)", values);
    }

    @Test
    public void testInvertedIndexQuery()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b varchar(512), c varchar(512)) with (inverted_indexed_by = ARRAY['a RAW', 'b TEXT', 'c TEXT'])");
        assertThat(query("SELECT a, b, c FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("insert into " + tableName + " (a, b, c) VALUES (1, 'aaa ddd', 'xxx yyy'), (10, 'ggg hhh', 'zzz ttt'), (1, '777', 'nnn mmm')", 3);

        assertThat(query("select \"$pos\", a from " + tableName + " " + "where inv_match_index_in_set(a, 'a', array['1']) and inv_match(b, 'ddd')"))
                .skippingTypesCheck().matches(expectedTwoBigint("(0, 1)"));

        assertThat(query("select \"$pos\", a from " + tableName + " " + "where inv_match_in_set(a, array['1']) and inv_match(b, 'ddd')"))
                .skippingTypesCheck().matches(expectedTwoBigint("(0, 1)"));

        assertThat(query("select \"$pos\", a from " + tableName + " " + "where inv_match(b, 'hhh')"))
                .skippingTypesCheck().matches(expectedTwoBigint("(1, 10)"));

        // not
        assertThat(query("select \"$pos\", a from " + tableName + " " + "where b like '%hhh%' and a not in (1)"))
                .skippingTypesCheck().matches(expectedTwoBigint("(1, 10)"));
        assertThat(query("select \"$pos\", a from " + tableName + " " + "where inv_match(b, 'hhh') and not inv_match_in_set(a, array['1'])"))
                .skippingTypesCheck().matches(expectedTwoBigint("(1, 10)"));
        assertThat(query("select \"$pos\", a from " + tableName + " " + "where not inv_match(b, 'hhh') and not inv_match(c, 'mmm')"))
                .skippingTypesCheck().matches(expectedTwoBigint("(0, 1)"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testComplexInvertedIndexQuery()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b bigint, c bigint) with (inverted_indexed_by = ARRAY['a RAW', 'b RAW', 'c RAW'])");
        assertThat(query("SELECT a, b, c FROM " + tableName))
                .returnsEmptyResult();

        assertUpdate("insert into " + tableName + " (a, b, c) VALUES " +
                        "(1, 2, 3), (10, 11, 12), (100, 201, 302)," +
                        "(1, 2, 3), (10, 11, 12), (101, 201, 302)," +
                        "(1, 2, 3), (10, 11, 12), (102, 201, 302)",
                9);

        assertThat(query("select \"$pos\" from " + tableName + " " + "where inv_match(a, '100') and not (inv_match(a, '1') or inv_match(b, '11'))"))
                .skippingTypesCheck().matches(expectedOneBigint("(2)"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRangeInvertedIndexQuery()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b varchar) with (inverted_indexed_by = ARRAY['a RAW', 'b IP'])");
        assertThat(query("SELECT a, b FROM " + tableName))
                .returnsEmptyResult();

        assertUpdate("insert into " + tableName + " (a, b) VALUES " +
                        "(1, '192.168.0.1')," +
                        "(2, '192.168.0.2')," +
                        "(3, '192.168.0.3')," +
                        "(4, '192.168.0.4')," +
                        "(5, '192.168.0.5')," +
                        "(6, '192.168.0.6')," +
                        "(7, '192.168.0.7')," +
                        "(8, '192.168.0.8')," +
                        "(9, '192.168.0.9')",
                9);

        assertThat(query("select a from " + tableName + " where inv_match_gt(a, '7')"))
                .skippingTypesCheck().matches(expectedOneBigint("(8), (9)"));
        assertThat(query("select a from " + tableName + " where inv_match_gte(a, '7')"))
                .skippingTypesCheck().matches(expectedOneBigint("(7), (8), (9)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_gt(b, 'b', '192.168.0.7')"))
                .skippingTypesCheck().matches(expectedOneBigint("(8), (9)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_gte(b, 'b', '192.168.0.7')"))
                .skippingTypesCheck().matches(expectedOneBigint("(7), (8), (9)"));

        assertThat(query("select a from " + tableName + " where inv_match_lt(a, '3')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1), (2)"));
        assertThat(query("select a from " + tableName + " where inv_match_lte(a, '3')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1), (2), (3)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_lt(b, 'b', '192.168.0.3')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1), (2)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_lte(b, 'b', '192.168.0.3')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1), (2), (3)"));

        assertThat(query("select a from " + tableName + " where inv_match_gt(a, '3') and inv_match_lt(a, '5')"))
                .skippingTypesCheck().matches(expectedOneBigint("(4)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_gt(a, 'a', '3') and inv_match_index_lte(a, 'a', '5')"))
                .skippingTypesCheck().matches(expectedOneBigint("(4), (5)"));
        assertThat(query("select a from " + tableName + " where inv_match_gte(a, '3') and inv_match_lt(a, '5')"))
                .skippingTypesCheck().matches(expectedOneBigint("(3), (4)"));
        assertThat(query("select a from " + tableName + " where inv_match_gte(a, '3') and inv_match_lte(a, '5')"))
                .skippingTypesCheck().matches(expectedOneBigint("(3), (4), (5)"));

        assertThat(query("select a from " + tableName + " where inv_match_between(b, '192.168.0.3', false, '192.168.0.5', false)"))
                .skippingTypesCheck().matches(expectedOneBigint("(4)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_between(b, 'b', '192.168.0.3', false, '192.168.0.5', true)"))
                .skippingTypesCheck().matches(expectedOneBigint("(4), (5)"));
        assertThat(query("select a from " + tableName + " where inv_match_between(b, '192.168.0.3', true, '192.168.0.5', false)"))
                .skippingTypesCheck().matches(expectedOneBigint("(3), (4)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_between(b, 'b', '192.168.0.3', true, '192.168.0.5', true)"))
                .skippingTypesCheck().matches(expectedOneBigint("(3), (4), (5)"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNetworkInvertedIndexQuery()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b varchar) with (inverted_indexed_by = ARRAY['a RAW', 'b IP'])");
        assertThat(query("SELECT a, b FROM " + tableName))
                .returnsEmptyResult();

        assertUpdate("insert into " + tableName + " (a, b) VALUES " +
                        "(1, '192.168.0.0')," +
                        "(2, '192.168.0.1')," +
                        "(3, '192.168.0.2')," +
                        "(4, '192.168.255.255')," +
                        "(5, '10.168.0.5')," +
                        "(6, '2001:db8:3c0d:5b6d:0:0:42:8329')," +
                        "(7, '2001:db8:3c0d:5b40::')," +
                        "(8, '2001:db8:3c0d:5b7f:ffff:ffff:ffff:ffff')," +
                        "(9, '2001:db8:3c0d:5b53:0:0:0:1')," +
                        "(10, '2001:db8:3c0d:5b3f:ffff:ffff:ffff:ffff')," +
                        "(11, '2001:db8:3c0d:5b80::')," +
                        "(12, '10.168.0.9')",
                12);

        assertThat(query("select a from " + tableName + " where inv_match_network(b, '192.168.0.0/16', true)"))
                .skippingTypesCheck().matches(expectedOneBigint("(1), (2), (3), (4)"));
        assertThat(query("select a from " + tableName + " where inv_match_index_network(b, 'b', '192.168.0.0/16', false)"))
                .skippingTypesCheck().matches(expectedOneBigint("(2), (3)"));

        assertThat(query("select a from " + tableName + " where inv_match_network(b, '2001:db8:3c0d:5b6d:0:0:42:8329/58', true)"))
                .skippingTypesCheck().matches(expectedOneBigint("(6), (7), (8), (9)"));

        assertUpdate("DROP TABLE " + tableName);
    }

    private static String expectedOneBigint(String values)
    {
        return format("SELECT CAST(a AS bigint) FROM (VALUES %s) AS t (a)", values);
    }

    @Test
    public void testInvertedIndexEvolution()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + "(a bigint, b varchar(512), c varchar(512)) with (inverted_indexed_by = ARRAY['a RAW', 'b TEXT'])");
        assertThat(query("SELECT a, b, c FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("insert into " + tableName + " (a, b, c) VALUES (1, 'b1', 'c1'), (2, 'b2', 'c2'), (3, 'b3', 'c3')", 3);

        // query a and b USE AND
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['1']) and inv_match(b, 'b1')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1)"));
        // query a and b USE OR
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['1']) or inv_match(b, 'b2')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1), (2)"));

        // add index
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES inverted_indexed_by = ARRAY['a RAW', 'b TEXT', 'c TEXT']");

        assertUpdate("insert into " + tableName + " (a, b, c) VALUES (4, 'b4', 'c5'), (5, 'b5', 'c5'), (6, 'b3', 'c3')", 3);

        // query b
        assertThat(query("select a from " + tableName + " " + "where inv_match(b, 'b3')"))
                .skippingTypesCheck().matches(expectedOneBigint("(3), (6)"));
        // query c
        assertThat(query("select a from " + tableName + " " + "where inv_match(c, 'c3')"))
                .skippingTypesCheck().matches(expectedOneBigint("(6)"));

        // query a and b again
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['1']) and inv_match(b, 'b1')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1)"));

        // query a and c use AND
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['1']) and inv_match(c, 'c1')"))
                .skippingTypesCheck().returnsEmptyResult();
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['4']) and inv_match(c, 'c5')"))
                .skippingTypesCheck().matches(expectedOneBigint("(4)"));
        // query a and c use OR
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['1']) or inv_match(c, 'c3')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1), (6)"));
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['1']) or inv_match(c, 'c2')"))
                .skippingTypesCheck().matches(expectedOneBigint("(1)"));
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['4']) or inv_match(c, 'c2')"))
                .skippingTypesCheck().matches(expectedOneBigint("(4)"));
        assertThat(query("select a from " + tableName + " " + "where inv_match_in_set(a, array['4']) or inv_match(c, 'c3')"))
                .skippingTypesCheck().matches(expectedOneBigint("(4), (6)"));

        // rename index
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES inverted_indexed_by = ARRAY['a RAW', 'b TEXT AS bi', 'c TEXT']");
        assertUpdate("insert into " + tableName + " (a, b, c) VALUES (7, 'b7', 'c7'), (8, 'b8', 'c8'), (9, 'b9', 'c9')", 3);
        assertThat(query("select a from " + tableName + " " + "where inv_match_index_in_set(b, 'bi', array['b3', 'b9'])"))
                .skippingTypesCheck().matches(expectedOneBigint("(3), (6), (9)"));

        // delete and create new index
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES inverted_indexed_by = ARRAY['a RAW', 'b TEXT as bi2 PROPERTIES(tokenizer=raw)', 'c TEXT']");
        assertUpdate("insert into " + tableName + " (a, b, c) VALUES (10, 'b10', 'c10'), (11, 'b11', 'c11'), (12, 'b12', 'c12')", 3);
        assertThat(query("select a from " + tableName + " " + "where inv_match_index_in_set(b, 'bi2', array['b3', 'b9', 'b12'])"))
                .skippingTypesCheck().matches(expectedOneBigint("(12)"));

        assertUpdate("DROP TABLE " + tableName);
    }
}
