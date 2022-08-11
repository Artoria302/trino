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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.DISTRIBUTED_SORT;
import static io.trino.SystemSessionProperties.ENABLE_WRITE_TABLE_ORDER_BY;
import static io.trino.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;

public class TestRangePartition
        extends BasePlanTest
{
    TestRangePartition()
    {
        super(ImmutableMap.of(
                ENABLE_WRITE_TABLE_ORDER_BY, "true",
                DISTRIBUTED_SORT, "false",
                SCALE_WRITERS, "false",
                REDISTRIBUTE_WRITES, "false"));
    }

    @Test
    public void testPlan()
    {
        plan("SELECT c FROM (VALUES(1, 0.01, 'red')) AS t(a, b, c) ORDER BY a, CAST(b AS INT)");
    }

    @Test
    public void testExchange()
    {
        plan("SELECT nationkey, row_number() OVER (PARTITION BY nationkey order by name) AS r1, row_number() OVER (PARTITION BY regionkey, nationkey) AS r2 FROM nation");
    }

    @Test
    public void testWith()
    {
        plan("WITH t as (SELECT nationkey, count(*) AS cnt FROM nation GROUP BY nationkey) SELECT cnt FROM t UNION ALL SELECT cnt FROM t");
    }

    @Test
    public void testCreate()
    {
        plan("create table aaa as select nationkey from nation ORDER BY nationkey");
    }
}
