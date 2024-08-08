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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

/**
 * Trino table options.
 */
public class PaimonTableOptions
{
    public static final String LOCATION_PROPERTY = "location";
    public static final String PRIMARY_KEY_IDENTIFIER = "primary_key";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String OPTIONS_PROPERTY = "options";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public PaimonTableOptions(TypeManager typeManager)
    {
        ImmutableList.Builder<PropertyMetadata<?>> builder = ImmutableList.builder();

        builder
                .add(new PropertyMetadata<>(
                        PRIMARY_KEY_IDENTIFIER,
                        "Primary keys for the table.",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition keys for the table.",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "Table location URI",
                        null,
                        false))
                .add(new PropertyMetadata<>(
                        OPTIONS_PROPERTY,
                        "Table options",
                        new MapType(VARCHAR, VARCHAR, typeManager.getTypeOperators()),
                        Map.class,
                        null,
                        true,
                        value -> {
                            Map<String, String> extraProperties = (Map<String, String>) value;
                            if (extraProperties.containsValue(null)) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, format("Extra table property value cannot be null '%s'", extraProperties));
                            }
                            if (extraProperties.containsKey(null)) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, format("Extra table property key cannot be null '%s'", extraProperties));
                            }
                            return extraProperties;
                        },
                        value -> value));

        tableProperties = builder.build();
    }

    public List<PropertyMetadata<?>> getTableOptions()
    {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPrimaryKeys(Map<String, Object> tableProperties)
    {
        List<String> primaryKeys = (List<String>) tableProperties.get(PRIMARY_KEY_IDENTIFIER);
        return primaryKeys == null ? ImmutableList.of() : ImmutableList.copyOf(primaryKeys);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedKeys(Map<String, Object> tableProperties)
    {
        List<String> partitionedKeys = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedKeys == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedKeys);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getTableOptions(Map<String, Object> tableProperties)
    {
        Map<String, String> options = (Map<String, String>) tableProperties.get(OPTIONS_PROPERTY);
        return options == null ? ImmutableMap.of() : ImmutableMap.copyOf(options);
    }

    public static Optional<String> getTableLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }
}
