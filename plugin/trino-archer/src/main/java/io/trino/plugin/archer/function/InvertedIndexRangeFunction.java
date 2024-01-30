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
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;

import java.util.List;

import static io.trino.plugin.archer.function.InvertedIndexMatchFunction.ERROR_MESSAGE;

public final class InvertedIndexRangeFunction
{
    public static final List<Class<?>> INV_MATCH_RANGE_FUNCTIONS = ImmutableList.of(
            MatchIndexNetwork.class,
            MatchNetwork.class,
            MatchIndexGreater.class,
            MatchGreater.class,
            MatchIndexGreaterEqual.class,
            MatchGreaterEqual.class,
            MatchIndexLess.class,
            MatchLess.class,
            MatchIndexLessEqual.class,
            MatchLessEqual.class,
            MatchIndexBetween.class,
            MatchBetween.class);

    private InvertedIndexRangeFunction() {}

    @ScalarFunction("inv_match_index_network")
    public static class MatchIndexNetwork
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchNetwork(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlType(StandardTypes.VARCHAR) Slice network,
                @SqlType(StandardTypes.BOOLEAN) boolean inclusiveHostCount)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_network")
    public static class MatchNetwork
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchNetwork(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice network,
                @SqlType(StandardTypes.BOOLEAN) boolean inclusiveHostCount)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_gt")
    public static class MatchIndexGreater
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_gt")
    public static class MatchGreater
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_gte")
    public static class MatchIndexGreaterEqual
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_gte")
    public static class MatchGreaterEqual
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_lt")
    public static class MatchIndexLess
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_lt")
    public static class MatchLess
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_lte")
    public static class MatchIndexLessEqual
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_lte")
    public static class MatchLessEqual
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_between")
    public static class MatchIndexBetween
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice lower,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean lowerInclusive,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice upper,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean upperInclusive)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice lower,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean lowerInclusive,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice upper,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean upperInclusive)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice lower,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean lowerInclusive,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice upper,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean upperInclusive)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_between")
    public static class MatchBetween
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice lower,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean lowerInclusive,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice upper,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean upperInclusive)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice lower,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean lowerInclusive,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice upper,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean upperInclusive)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice lower,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean lowerInclusive,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice upper,
                @SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean upperInclusive)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }
}
