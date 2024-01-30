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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.InvertedIndexField;
import net.qihoo.archer.InvertedIndexType;
import net.qihoo.archer.Schema;
import net.qihoo.archer.types.Types;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.archer.PartitionFields.fromIdentifierToColumn;
import static io.trino.plugin.archer.PartitionFields.quotedName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static net.qihoo.archer.InvertedIndexFieldProperties.SUPPORTED_PROPERTIES_KEY;
import static net.qihoo.archer.InvertedIndexFieldProperties.SUPPORTED_TOKENIZER;
import static net.qihoo.archer.InvertedIndexFieldProperties.TOKENIZER_KEY;

public final class InvertedIndexFieldUtils
{
    private InvertedIndexFieldUtils() {}

    private static final String KEY_VALUE = "\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*=\\s*([a-zA-Z_0-9.]+)\\s*";
    private static final String KEY_VALUES = KEY_VALUE + "(" + "\\s*,\\s*" + KEY_VALUE + ")*";
    private static final Pattern KEY_VALUES_PATTERN = Pattern.compile(KEY_VALUE);
    private static final Pattern PATTERN = Pattern.compile(
            "\\s*(?<identifier>" + PartitionFields.IDENTIFIER + ")"
                    + "(?i:\\s+(?<type>RAW|IP|DATETIME|JSON|TEXT))?"
                    + "(?i:\\s+AS\\s+(?-i:(?<name>" + PartitionFields.IDENTIFIER + ")))?"
                    + "(?i:\\s+PROPERTIES\\s*\\(\\s*(?-i:(?<properties>" + KEY_VALUES + ")*)\\s*\\))?"
                    + "\\s*");

    public static InvertedIndex parseInvertedIndexFields(Schema schema, List<String> fields)
    {
        InvertedIndex.Builder builder = InvertedIndex.builderFor(schema);
        parseInvertedIndexFields(builder, fields);
        InvertedIndex invertedIndex;
        try {
            invertedIndex = builder.build();
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Invalid " + INVALID_TABLE_PROPERTY + " definition", e);
        }

        Set<Integer> baseColumnFieldIds = schema.columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(toImmutableSet());
        for (InvertedIndexField field : invertedIndex.fields()) {
            if (!baseColumnFieldIds.contains(field.sourceId())) {
                throw new TrinoException(COLUMN_NOT_FOUND, "Column not found: " + schema.findColumnName(field.sourceId()));
            }
        }

        return invertedIndex;
    }

    public static void parseInvertedIndexFields(InvertedIndex.Builder invertedIndexBuilder, List<String> fields)
    {
        fields.forEach(field -> parseInvertedIndexField(invertedIndexBuilder, field));
    }

    private static void parseInvertedIndexField(InvertedIndex.Builder builder, String field)
    {
        Matcher matcher = PATTERN.matcher(field);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Unable to parse inverted index field: [%s]".formatted(field));
        }

        String columnName = fromIdentifierToColumn(matcher.group("identifier"));

        InvertedIndexType type = switch (firstNonNull(matcher.group("type"), "RAW").toUpperCase(ENGLISH)) {
            case "RAW" -> InvertedIndexType.fromString("RAW");
            case "IP" -> InvertedIndexType.fromString("IP");
            case "DATETIME" -> InvertedIndexType.fromString("DATETIME");
            case "JSON" -> InvertedIndexType.fromString("JSON");
            case "TEXT" -> InvertedIndexType.fromString("TEXT");
            default -> throw new IllegalArgumentException("Unexpected index type, should be RAW/IP/DATETIME/JSON/TEXT");
        };

        String name = matcher.group("name");
        String indexName = name == null ? columnName : fromIdentifierToColumn(name);

        String propsString = matcher.group("properties");
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        if (propsString != null) {
            Matcher propsMatcher = KEY_VALUES_PATTERN.matcher(propsString);
            while (propsMatcher.find()) {
                String key = propsMatcher.group(1).toLowerCase(ENGLISH);
                String value = propsMatcher.group(2);
                if (!SUPPORTED_PROPERTIES_KEY.contains(key)) {
                    throw new IllegalArgumentException("Unsupported inverted index field property: " + key);
                }
                if (key.equals(TOKENIZER_KEY)) {
                    if (type != InvertedIndexType.JSON && type != InvertedIndexType.TEXT) {
                        throw new IllegalArgumentException("Unexpected index type, only JSON/TEXT support tokenizer");
                    }
                    if (!SUPPORTED_TOKENIZER.contains(value)) {
                        throw new IllegalArgumentException("Unsupported tokenizer: " + value);
                    }
                }
                properties.put(key, value);
            }
        }
        builder.index(columnName, indexName, type, properties.buildOrThrow());
    }

    public static List<String> toInvertedIndexFields(InvertedIndex invertedIndex)
    {
        return invertedIndex.fields().stream()
                .map(field -> toInvertedIndexField(invertedIndex, field))
                .collect(toImmutableList());
    }

    private static String toInvertedIndexField(InvertedIndex spec, InvertedIndexField field)
    {
        String name = quotedName(spec.schema().findColumnName(field.sourceId()));
        String indexName = quotedName(field.name());
        return format("%s %s AS %s%s", name, field.type(), indexName, formatPropertiesPart(field.properties()));
    }

    private static String formatPropertiesPart(Map<String, String> properties)
    {
        if (properties.isEmpty()) {
            return "";
        }
        else {
            return " PROPERTIES(" + properties.entrySet().stream().map(pair -> format("%s=%s", pair.getKey(), pair.getValue())).collect(Collectors.joining(", ")) + ")";
        }
    }
}
