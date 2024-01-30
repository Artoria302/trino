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
import com.google.common.collect.ImmutableMap;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.Schema;
import net.qihoo.archer.types.Types;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static io.trino.plugin.archer.InvertedIndexFieldUtils.parseInvertedIndexFields;
import static net.qihoo.archer.InvertedIndexFieldProperties.TOKENIZER_KEY;
import static net.qihoo.archer.InvertedIndexType.IP;
import static net.qihoo.archer.InvertedIndexType.JSON;
import static net.qihoo.archer.InvertedIndexType.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestInvertedIndexFieldUtils
{
    @Test
    public void testParse()
    {
        assertParse("long_column", invertedIndex(builder -> builder.index("long_column")));
        assertParse("long_column RAW", invertedIndex(builder -> builder.index("long_column")));

        // lowercase
        assertParse("long_column raw", invertedIndex(builder -> builder.index("long_column")));
        assertParse("\"long_column\"", invertedIndex(builder -> builder.index("long_column")));
        assertParse("\"long_column\" raw", invertedIndex(builder -> builder.index("long_column")));

        // uppercase
        assertParse("LONG_COLUMN", invertedIndex(builder -> builder.index("long_column")));
        assertParse("LONG_COLUMN RAW", invertedIndex(builder -> builder.index("long_column")));
        assertDoesNotParse("\"LONG_COLUMN\" RAW", "Uppercase characters in identifier '\"LONG_COLUMN\"' are not supported.");

        // mixed case
        assertParse("lOng_ColumN", invertedIndex(builder -> builder.index("long_column")));
        assertParse("lOng_ColumN RaW", invertedIndex(builder -> builder.index("long_column")));
        assertDoesNotParse("\"lOng_ColumN\"", "Uppercase characters in identifier '\"lOng_ColumN\"' are not supported.");
        assertDoesNotParse("\"lOng_ColumN\" Raw", "Uppercase characters in identifier '\"lOng_ColumN\"' are not supported.");

        assertParse("comment", invertedIndex(builder -> builder.index("comment")));
        assertParse("\"comment\"", invertedIndex(builder -> builder.index("comment")));
        assertParse("\"quoted field\"", invertedIndex(builder -> builder.index("quoted field")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"", invertedIndex(builder -> builder.index("\"another\" \"quoted\" \"field\"")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"  TEXT", invertedIndex(builder -> builder.index("\"another\" \"quoted\" \"field\"", TEXT, ImmutableMap.of())));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"  JSON  ", invertedIndex(builder -> builder.index("\"another\" \"quoted\" \"field\"", JSON, ImmutableMap.of())));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"  IP  ", invertedIndex(builder -> builder.index("\"another\" \"quoted\" \"field\"", IP, ImmutableMap.of())));

        assertParse("\"comment\" JSON  ", invertedIndex(builder -> builder.index("comment", JSON, ImmutableMap.of())));
        assertParse("\"comment\" JSON", invertedIndex(builder -> builder.index("comment", JSON, ImmutableMap.of())));

        assertException("long_column TEXT");
        assertException("long_column JSON");
        assertException("long_column IP");

        assertParse("comment JSON  AS json_Index ", invertedIndex(builder -> builder.index("comment", "json_index", JSON, ImmutableMap.of())));
        assertParse("comment JSoN aS Json_Index ", invertedIndex(builder -> builder.index("comment", "json_index", JSON, ImmutableMap.of())));
        assertParse("comment JSON as jSon_indEx properties (tokenizer =jieba_search)", invertedIndex(builder -> builder.index("comment", "json_index", JSON, ImmutableMap.of(TOKENIZER_KEY, "jieba_search"))));
        assertParse("comment JSON properties(tokenizer = jieba_search)", invertedIndex(builder -> builder.index("comment", "comment", JSON, ImmutableMap.of(TOKENIZER_KEY, "jieba_search"))));

        assertDoesNotParse("comment JSON Properties(tokenizer = jieba_search, a = b)", "Unsupported inverted index field property");
        assertDoesNotParse("comment a json_index");
        assertDoesNotParse("comment JSON a json_index");
        assertDoesNotParse("comment JSON s json_index ");
        assertDoesNotParse("comment JSON prperties( tokenizer = jieba_search)");
        assertDoesNotParse("comment JSON properties( tokenizeR = jieba_seaRch)", "Unsupported tokenizer");
    }

    private static void assertParse(@Language("SQL") String value, InvertedIndex expected)
    {
        assertThat(expected.fields().size()).isEqualTo(1);
        assertThat(parseField(value)).isEqualTo(expected);
    }

    private static void assertException(@Language("SQL") String value)
    {
        assertThatThrownBy(() -> parseField(value)).hasMessageStartingWith("Only support inverted index");
    }

    private static void assertDoesNotParse(@Language("SQL") String value)
    {
        assertDoesNotParse(value, "Unable to parse inverted index field: [%s]".formatted(value));
    }

    private static void assertDoesNotParse(@Language("SQL") String value, String expectedMessage)
    {
        assertThatThrownBy(() -> parseField(value))
                .hasMessageContaining(expectedMessage);
    }

    private static InvertedIndex parseField(String value)
    {
        return invertedIndex(builder -> parseInvertedIndexFields(builder, ImmutableList.of(value)));
    }

    private static InvertedIndex invertedIndex(Consumer<InvertedIndex.Builder> consumer)
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "long_column", Types.LongType.get()),
                Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.required(3, "price", Types.DoubleType.get()),
                Types.NestedField.optional(4, "comment", Types.StringType.get()),
                Types.NestedField.optional(5, "notes", Types.ListType.ofRequired(6, Types.StringType.get())),
                Types.NestedField.optional(7, "quoted field", Types.StringType.get()),
                Types.NestedField.optional(8, "quoted ts", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(9, "\"another\" \"quoted\" \"field\"", Types.StringType.get()));

        InvertedIndex.Builder builder = InvertedIndex.builderFor(schema);
        consumer.accept(builder);
        return builder.build();
    }
}
