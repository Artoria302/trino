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
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.archer.ColumnIdentity.TypeCategory.ARRAY;
import static io.trino.plugin.archer.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.archer.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.archer.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestArcherColumnHandle
{
    @Test
    public void testRoundTrip()
    {
        testRoundTrip(new ArcherColumnHandle(primitiveColumnIdentity(12, "blah"), BIGINT, ImmutableList.of(), BIGINT, true, Optional.of("this is a comment")));

        // Nested column
        ColumnIdentity foo1 = new ColumnIdentity(1, "foo1", PRIMITIVE, ImmutableList.of());
        ColumnIdentity foo2 = new ColumnIdentity(2, "foo2", PRIMITIVE, ImmutableList.of());
        ColumnIdentity foo3 = new ColumnIdentity(3, "foo3", ARRAY, ImmutableList.of(foo1));
        Type nestedColumnType = RowType.from(ImmutableList.of(
                RowType.field("foo2", BIGINT),
                RowType.field("foo3", new ArrayType(BIGINT))));
        ArcherColumnHandle nestedColumn = new ArcherColumnHandle(
                new ColumnIdentity(
                        5,
                        "foo5",
                        STRUCT,
                        ImmutableList.of(foo2, foo3)),
                nestedColumnType,
                ImmutableList.of(),
                nestedColumnType,
                true,
                Optional.empty());
        testRoundTrip(nestedColumn);

        ArcherColumnHandle partialColumn = new ArcherColumnHandle(
                new ColumnIdentity(
                        5,
                        "foo5",
                        STRUCT,
                        ImmutableList.of(foo2, foo3)),
                nestedColumnType,
                ImmutableList.of(2),
                BIGINT,
                true,
                Optional.empty());
        testRoundTrip(partialColumn);
    }

    private void testRoundTrip(ArcherColumnHandle expected)
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        JsonCodec<ArcherColumnHandle> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(ArcherColumnHandle.class);

        String json = codec.toJson(expected);
        ArcherColumnHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getBaseColumnIdentity()).isEqualTo(expected.getBaseColumnIdentity());
        assertThat(actual.getBaseType()).isEqualTo(expected.getBaseType());
        assertThat(actual.getQualifiedName()).isEqualTo(expected.getQualifiedName());
        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.getColumnIdentity()).isEqualTo(expected.getColumnIdentity());
        assertThat(actual.getId()).isEqualTo(expected.getId());
        assertThat(actual.getType()).isEqualTo(expected.getType());
        assertThat(actual.getComment()).isEqualTo(expected.getComment());
    }
}
