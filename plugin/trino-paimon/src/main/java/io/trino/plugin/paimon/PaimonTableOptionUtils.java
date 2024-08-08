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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_OPTIONS_ERROR;
import static java.lang.String.format;

/**
 * options utils.
 */
public class PaimonTableOptionUtils
{
    private static final Set<String> VALID_CONFIG_OPTIONS = extractValidConfigOptions();
    private static final Set<String> VALID_FIELD_SUFFIX = ImmutableSet.of(CoreOptions.DEFAULT_VALUE_SUFFIX, CoreOptions.STATS_MODE_SUFFIX, CoreOptions.AGG_FUNCTION, CoreOptions.DISTINCT, CoreOptions.NESTED_KEY, CoreOptions.IGNORE_RETRACT);

    private PaimonTableOptionUtils() {}

    private static boolean shouldSkip(String fieldName)
    {
        return switch (fieldName) {
            case "primary-key", "partition", "location" -> true;
            default -> false;
        };
    }

    private static Set<String> extractValidConfigOptions()
    {
        try {
            ImmutableSet.Builder<String> configOptions = ImmutableSet.builder();
            Field[] fields = CoreOptions.class.getFields();
            for (Field field : fields) {
                if (isConfigOption(field)) {
                    String key = ((ConfigOption<?>) field.get(null)).key();
                    if (!shouldSkip(key)) {
                        configOptions.add(key);
                    }
                }
            }
            return configOptions.build();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to extract config options from class " + ConfigOption.class + '.', e);
        }
    }

    private static boolean isConfigOption(Field field)
    {
        return field.getType().equals(ConfigOption.class);
    }

    public static void checkConfigOptions(Map<String, String> options, Set<String> fieldNames)
    {
        options.keySet().forEach(key -> {
            if (key.startsWith(CoreOptions.FIELDS_PREFIX + ".")) {
                checkFieldOptions(key, fieldNames);
            }
            else if (!VALID_CONFIG_OPTIONS.contains(key)) {
                throw new TrinoException(PAIMON_OPTIONS_ERROR, "Unknown table option: " + key);
            }
        });
    }

    private static void checkFieldOptions(String key, Set<String> fieldNames)
    {
        int first = key.indexOf('.');
        int last = key.lastIndexOf('.');
        if (first < 0 || last < 0 || first == last) {
            throw new TrinoException(PAIMON_OPTIONS_ERROR, "Not a valid fields option: " + key);
        }

        String prefix = key.substring(0, first);
        String suffix = key.substring(last + 1);
        String fieldName = key.substring(first + 1, last);

        if (!fieldNames.contains(fieldName)) {
            throw new TrinoException(PAIMON_OPTIONS_ERROR, format("Not found field '%s' in table schema", fieldName));
        }
        if (!prefix.equals(CoreOptions.FIELDS_PREFIX)) {
            throw new TrinoException(PAIMON_OPTIONS_ERROR, format("Unknown fields option prefix '%s'", prefix));
        }
        if (!VALID_FIELD_SUFFIX.contains(suffix)) {
            throw new TrinoException(PAIMON_OPTIONS_ERROR, format("Unknown fields option suffix '%s'", prefix));
        }
    }
}
