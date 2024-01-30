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
import io.trino.spi.block.Block;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;

import java.util.List;

public final class InvertedIndexMatchFunction
{
    public static final String ERROR_MESSAGE = "Inverted index function should always be pushdown and not be projected";
    public static final List<Class<?>> INV_MATCH_FUNCTIONS = ImmutableList.of(
            MatchIndex.class,
            Match.class,
            MatchIndexTextSlop.class,
            MatchTextSlop.class,
            MatchIndexTextPrefix.class,
            MatchTextPrefix.class,
            MatchIndexJson.class,
            MatchJson.class,
            MatchIndexJsonSlop.class,
            MatchJsonSlop.class,
            MatchIndexJsonPrefix.class,
            MatchJsonPrefix.class,
            MatchIndexInSet.class,
            MatchInSet.class,
            MatchIndexJsonInSet.class,
            MatchJsonInSet.class);

    private InvertedIndexMatchFunction() {}

    @ScalarFunction("inv_match_index")
    public static class MatchIndex
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match")
    public static class Match
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_text_slop")
    public static class MatchIndexTextSlop
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlType(StandardTypes.VARCHAR) Slice phrase,
                @SqlType(StandardTypes.INTEGER) long slop)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_text_slop")
    public static class MatchTextSlop
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice phrase,
                @SqlType(StandardTypes.INTEGER) long slop)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_text_prefix")
    public static class MatchIndexTextPrefix
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_text_prefix")
    public static class MatchTextPrefix
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_json")
    public static class MatchIndexJson
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlType(StandardTypes.VARCHAR) Slice jsonPath,
                @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_json")
    public static class MatchJson
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice jsonPath,
                @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_json_slop")
    public static class MatchIndexJsonSlop
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlType(StandardTypes.VARCHAR) Slice jsonPath,
                @SqlType(StandardTypes.VARCHAR) Slice phrase,
                @SqlType(StandardTypes.INTEGER) long slop)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_json_slop")
    public static class MatchJsonSlop
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice jsonPath,
                @SqlType(StandardTypes.VARCHAR) Slice phrase,
                @SqlType(StandardTypes.INTEGER) long slop)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_json_prefix")
    public static class MatchIndexJsonPrefix
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlType(StandardTypes.VARCHAR) Slice jsonPath,
                @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_json_prefix")
    public static class MatchJsonPrefix
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice jsonPath,
                @SqlType(StandardTypes.VARCHAR) Slice phrase)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_in_set")
    public static class MatchIndexInSet
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType("array(varchar)") Block array)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType("array(varchar)") Block array)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlNullable @SqlType("array(varchar)") Block array)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_in_set")
    public static class MatchInSet
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchInt(
                @SqlNullable @SqlType("T") Long column,
                @SqlNullable @SqlType("array(varchar)") Block array)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchSlice(
                @SqlNullable @SqlType("T") Slice column,
                @SqlNullable @SqlType("array(varchar)") Block array)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean matchDouble(
                @SqlNullable @SqlType("T") Double column,
                @SqlNullable @SqlType("array(varchar)") Block array)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_index_json_in_set")
    public static class MatchIndexJsonInSet
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice indexName,
                @SqlType(StandardTypes.VARCHAR) Slice jsonPath,
                @SqlType("array(varchar)") Block array)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }

    @ScalarFunction("inv_match_json_in_set")
    public static class MatchJsonInSet
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean match(
                @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice column,
                @SqlType(StandardTypes.VARCHAR) Slice jsonPath,
                @SqlType("array(varchar)") Block array)
        {
            throw new UnsupportedOperationException(ERROR_MESSAGE);
        }
    }
}
