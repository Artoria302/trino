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
package io.trino.plugin.archer.function;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;

public final class PartitionFunction
{
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final int EPOCH_YEAR = EPOCH.getYear();

    public static final List<Class<?>> PARTITION_FUNCTIONS = ImmutableList.of(
            HumanYear.class,
            HumanMonth.class,
            HumanDay.class,
            HumanHour.class);

    private PartitionFunction() {}

    @Description("Transform partition year to human readable time")
    @ScalarFunction("human_year")
    public static class HumanYear
    {
        @SqlType(StandardTypes.VARCHAR)
        public static Slice transform(@SqlType(StandardTypes.INTEGER) long yearOrdinal)
        {
            return utf8Slice(String.format("%04d", EPOCH_YEAR + yearOrdinal));
        }
    }

    @Description("Transform partition month to human readable time")
    @ScalarFunction("human_month")
    public static class HumanMonth
    {
        @SqlType(StandardTypes.VARCHAR)
        public static Slice transform(@SqlType(StandardTypes.INTEGER) long monthOrdinal)
        {
            return utf8Slice(String.format("%04d-%02d",
                    EPOCH_YEAR + Math.floorDiv(monthOrdinal, 12), 1 + Math.floorMod(monthOrdinal, 12)));
        }
    }

    @Description("Transform partition day to human readable time")
    @ScalarFunction("human_day")
    public static class HumanDay
    {
        @SqlType(StandardTypes.VARCHAR)
        public static Slice transform(@SqlType(StandardTypes.INTEGER) long dayOrdinal)
        {
            OffsetDateTime day = EPOCH.plusDays(dayOrdinal);
            return utf8Slice(String.format("%04d-%02d-%02d",
                    day.getYear(), day.getMonth().getValue(), day.getDayOfMonth()));
        }
    }

    @Description("Transform partition hour to human readable time")
    @ScalarFunction("human_hour")
    public static class HumanHour
    {
        @SqlType(StandardTypes.VARCHAR)
        public static Slice transform(@SqlType(StandardTypes.INTEGER) long hourOrdinal)
        {
            OffsetDateTime time = EPOCH.plusHours(toIntExact(hourOrdinal));
            return utf8Slice(String.format("%04d-%02d-%02d-%02d",
                    time.getYear(), time.getMonth().getValue(), time.getDayOfMonth(), time.getHour()));
        }
    }
}
