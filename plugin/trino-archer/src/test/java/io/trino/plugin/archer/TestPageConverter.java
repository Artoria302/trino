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
import io.trino.operator.PageAssertions;
import io.trino.operator.PageTestUtils;
import io.trino.plugin.archer.util.ArrowToPageConverter;
import io.trino.plugin.archer.util.PageToArrowConverter;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.DateTimeTestingUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestPageConverter
{
    private void writeList(BaseWriter.ListWriter listWriter)
    {
        String[] data = new String[] {"1", "2", "3", "10", "20", "30", "100", "200", "300"};
        int idx = 0;
        for (int i = 0; i < 3; i++) {
            listWriter.startList();
            for (int j = 0; j < 3; j++) {
                listWriter.varChar().writeVarChar(data[idx++]);
            }
            listWriter.endList();
        }
    }

    private void writeListOfList(ListVector rootList)
    {
        UnionListWriter unionListWriter = rootList.getWriter();
        for (int i = 0; i < 3; i++) {
            unionListWriter.setPosition(i);
            unionListWriter.startList();
            writeList(unionListWriter.list());
            unionListWriter.endList();
        }
        unionListWriter.setValueCount(3);
    }

    private void writeList1(ListVector listVector)
    {
        UnionListWriter listWriter = listVector.getWriter();
        int[] data = new int[] {1, 2, 3, 10, 20, 30, 100, 200, 300};
        int tmpIndex = 0;
        for (int i = 0; i < 3; i++) {
            listWriter.setPosition(i);
            listWriter.startList();
            for (int j = 0; j < 3; j++) {
                listWriter.writeInt(data[tmpIndex]);
                tmpIndex = tmpIndex + 1;
            }
            listWriter.endList();
        }
        listWriter.setValueCount(3);
    }

    private void writeList2(ListVector listVector)
    {
        UnionListWriter listWriter = listVector.getWriter();

        String[] data3 = new String[] {"1", "2", "3", "10", "20", "30"};
        int tmpIndex = 0;
        for (int i = 0; i < 3; i++) {
            if (i != 2) {
                listWriter.setPosition(i);
                listWriter.startList();
                for (int j = 0; j < i + 1; j++) {
                    if (j == 1) {
                        listWriter.writeNull();
                    }
                    else {
                        listWriter.varChar().writeVarChar(data3[tmpIndex]);
                    }
                    tmpIndex = tmpIndex + 1;
                }
                listWriter.endList();
            }
        }
        listWriter.setValueCount(3);
    }

    private void writeRow(StructVector structVector)
    {
        int[] data1 = new int[] {101, 102, 103};
        String[] data2 = new String[] {"zz", "ggg", "bbbbbb"};
        NullableStructWriter structWriter = structVector.getWriter();
        for (int i = 0; i < 3; i++) {
            structWriter.setPosition(i);
            structWriter.start();
            structWriter.integer("int32").writeInt(data1[i]);
            structWriter.varChar("varchar").writeVarChar(data2[i]);
            structWriter.end();
        }
        structVector.setValueCount(3);
    }

    private void writeMap(MapVector mapVector)
    {
        UnionMapWriter mapWriter = mapVector.getWriter();
        for (int i = 0; i < 3; i++) {
            mapWriter.setPosition(i);
            mapWriter.startMap();

            mapWriter.startEntry();
            mapWriter.key().varChar().writeVarChar("i0" + i);
            mapWriter.value().integer().writeInt(i);
            mapWriter.endEntry();

            mapWriter.startEntry();
            mapWriter.key().varChar().writeVarChar("i1" + i);
            mapWriter.value().integer().writeInt(i);
            mapWriter.endEntry();

            mapWriter.endMap();
        }
        mapVector.setValueCount(3);
    }

    @Test
    public void testConvert()
    {
        Field intField = new Field("int32", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field varcharField = new Field("varchar", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);

        List<Field> fields = new ArrayList<>();

        fields.add(intField);
        fields.add(varcharField);
        fields.add(new Field("int32_list", FieldType.nullable(ArrowType.List.INSTANCE), List.of(new Field(ListVector.DATA_VECTOR_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null))));
        fields.add(new Field("varchar_list", FieldType.nullable(ArrowType.List.INSTANCE), List.of(new Field(ListVector.DATA_VECTOR_NAME, FieldType.nullable(ArrowType.Utf8.INSTANCE), null))));

        fields.add(new Field("list_varchar_list", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(new Field(ListVector.DATA_VECTOR_NAME, FieldType.nullable(ArrowType.List.INSTANCE),
                        List.of(new Field(ListVector.DATA_VECTOR_NAME, FieldType.nullable(ArrowType.Utf8.INSTANCE), null))))));

        fields.add(new Field("row", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(intField, varcharField)));

        Field kv = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                new Field(MapVector.KEY_NAME, FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
                new Field(MapVector.VALUE_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null)));
        // key: varchar, value: int
        fields.add(new Field("map", FieldType.nullable(new ArrowType.Map(false)), List.of(kv)));

        Schema schema = new Schema(fields);

        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.setRowCount(3);

            // int vector
            IntVector intVector = (IntVector) root.getVector(0);
            intVector.allocateNew(3);
            intVector.set(1, 2);
            intVector.set(2, 3);
            intVector.setValueCount(3);

            // varchar vector
            VarCharVector varCharVector = (VarCharVector) root.getVector(1);
            varCharVector.allocateNew(3);
            varCharVector.setSafe(0, "aaa".getBytes(StandardCharsets.UTF_8));
            varCharVector.setSafe(2, "ccc".getBytes(StandardCharsets.UTF_8));
            varCharVector.setValueCount(3);

            writeList1((ListVector) root.getVector(2));
            writeList2((ListVector) root.getVector(3));
            writeListOfList((ListVector) root.getVector(4));
            writeRow((StructVector) root.getVector(5));
            writeMap((MapVector) root.getVector(6));

            assertThat(root.syncSchema()).isFalse();

            /// System.out.println(root.getSchema());
            /// System.out.println(root.contentToTSVString());

            ArrowToPageConverter arrowToPageConverter = new ArrowToPageConverter(allocator, root.getSchema());
            Page page1 = arrowToPageConverter.convert(root);

            List<String> columnNames = arrowToPageConverter.getColumnNames();
            List<Type> columnTypes = arrowToPageConverter.getColumnTypes();

            PageToArrowConverter pageToArrowConverter = new PageToArrowConverter(allocator, columnNames, columnTypes, null);
            try (VectorSchemaRoot root2 = pageToArrowConverter.convert(page1)) {
                /// System.out.println(root2.getSchema());
                /// System.out.println(root2.contentToTSVString());
                ArrowToPageConverter converter2 = new ArrowToPageConverter(allocator, root2.getSchema());
                Page page2 = converter2.convert(root2);

                PageAssertions.assertPageEquals(arrowToPageConverter.getColumnTypes(), page1, page2);
            }
        }
    }

    private void testHelper(BufferAllocator allocator, List<String> columnNames, List<Type> types, List<TestParameter> cases)
    {
        PageToArrowConverter pageToArrowConverter = new PageToArrowConverter(allocator, columnNames, types, null);
        ArrowToPageConverter arrowToPageConverter = new ArrowToPageConverter(allocator, pageToArrowConverter.getSchema());

        for (TestParameter p : cases) {
            Page page = PageTestUtils.createRandomPage(types, p.positionCount, Optional.empty(), p.nullRate, ImmutableList.of());
            try (VectorSchemaRoot root = pageToArrowConverter.convert(page)) {
                Page convertedPage = arrowToPageConverter.convert(root);
                PageAssertions.assertPageEquals(types, convertedPage, page);
            }
        }
    }

    private record TestParameter(int positionCount, float nullRate)
    {
        static TestParameter of(int positionCount, float nullRate)
        {
            return new TestParameter(positionCount, nullRate);
        }
    }

    @Test
    public void testConvertRandomPage()
    {
        try (BufferAllocator allocator = new RootAllocator()) {
            List<Type> types = List.of(
                    INTEGER,
                    BIGINT,
                    VARCHAR,
                    VARBINARY,
                    TIMESTAMP_MICROS,
                    UUID,
                    // createDecimalType(18, 10),
                    createDecimalType(28, 10),
                    createDecimalType(),
                    RowType.from(List.of(
                            new RowType.Field(Optional.of("varchar"), VARCHAR),
                            new RowType.Field(Optional.of("int"), INTEGER),
                            new RowType.Field(Optional.of("uuid"), UUID))),
                    RowType.from(List.of(
                            new RowType.Field(Optional.of("varchar"), VARCHAR),
                            new RowType.Field(Optional.of("varchar_array"), new ArrayType(VARCHAR)))),
                    RowType.from(List.of(
                            new RowType.Field(Optional.of("varchar"), VARCHAR),
                            new RowType.Field(Optional.of("int_varchar_map"), new MapType(INTEGER, VARCHAR, new TypeOperators())))),
                    RowType.from(List.of(
                            new RowType.Field(Optional.of("varchar"), VARCHAR),
                            new RowType.Field(Optional.of("varchar_array"), new ArrayType(VARCHAR)),
                            new RowType.Field(Optional.of("int_varchar_map"), new MapType(INTEGER, VARCHAR, new TypeOperators())))),
                    new MapType(VARCHAR, VARCHAR, new TypeOperators()),
                    new MapType(VARCHAR, RowType.from(List.of(
                            new RowType.Field(Optional.of("uuid"), UUID),
                            new RowType.Field(Optional.of("int"), INTEGER))), new TypeOperators()),
                    new MapType(VARCHAR, new ArrayType(VARCHAR), new TypeOperators()),
                    new MapType(VARCHAR, RowType.from(List.of(
                            new RowType.Field(Optional.of("varchar"), VARCHAR),
                            new RowType.Field(Optional.of("varchar_array"), new ArrayType(VARCHAR)),
                            new RowType.Field(Optional.of("int_varchar_map"), new MapType(INTEGER, VARCHAR, new TypeOperators())))), new TypeOperators()),
                    new MapType(VARCHAR, new MapType(VARCHAR, VARCHAR, new TypeOperators()), new TypeOperators()),
                    new ArrayType(VARCHAR),
                    new ArrayType(UUID),
                    new ArrayType(new ArrayType(VARCHAR)),
                    new ArrayType(new ArrayType(new ArrayType(VARCHAR))),
                    new ArrayType(RowType.from(List.of(new RowType.Field(Optional.empty(), VARCHAR), new RowType.Field(Optional.empty(), new ArrayType(VARCHAR))))),
                    new ArrayType(new ArrayType(RowType.from(List.of(new RowType.Field(Optional.empty(), VARCHAR), new RowType.Field(Optional.empty(), new ArrayType(VARCHAR)))))),
                    new ArrayType(new MapType(VARCHAR, RowType.from(List.of(
                            new RowType.Field(Optional.of("varchar"), VARCHAR),
                            new RowType.Field(Optional.of("varchar_array"), new ArrayType(VARCHAR)),
                            new RowType.Field(Optional.of("int_varchar_map"), new MapType(INTEGER, VARCHAR, new TypeOperators())))), new TypeOperators())),
                    REAL,
                    DOUBLE);

            List<String> columnNames = IntStream.range(0, types.size()).mapToObj(idx -> types.get(idx).getDisplayName() + "_" + idx).toList();

            List<TestParameter> cases = List.of(
                    TestParameter.of(0, 0.0f),
                    TestParameter.of(0, 0.5f),
                    TestParameter.of(0, 1.0f),
                    TestParameter.of(1, 0.0f),
                    TestParameter.of(1, 1.0f),
                    TestParameter.of(2, 0.0f),
                    TestParameter.of(2, 0.5f),
                    TestParameter.of(2, 1.0f),
                    TestParameter.of(3, 0.0f),
                    TestParameter.of(3, 0.5f),
                    TestParameter.of(3, 1.0f),
                    TestParameter.of(1000, 0.0f),
                    TestParameter.of(1000, 0.5f),
                    TestParameter.of(1000, 1.0f));
            testHelper(allocator, columnNames, types, cases);
        }
    }

    @Test
    public void testConvertTimeDate()
    {
        try (BufferAllocator allocator = new RootAllocator()) {
            List<Type> types = List.of(TIME_MICROS, DATE);

            ImmutableList.Builder<Block> blocksBuilder = ImmutableList.builder();

            {
                BlockBuilder builder = TIME_MICROS.createFixedSizeBlockBuilder(3);

                builder.appendNull();
                TIME_MICROS.writeLong(builder, DateTimeTestingUtils.sqlTimeOf(6, 12, 23, 56, 123456000).getPicos());
                TIME_MICROS.writeLong(builder, DateTimeTestingUtils.sqlTimeOf(6, 22, 13, 41, 456789000).getPicos());

                Block block = builder.build();
                blocksBuilder.add(block);
            }

            {
                BlockBuilder builder = DATE.createFixedSizeBlockBuilder(3);

                builder.appendNull();
                DATE.writeLong(builder, DateTimeTestingUtils.sqlDateOf(2024, 1, 1).getDays());
                DATE.writeLong(builder, DateTimeTestingUtils.sqlDateOf(2000, 12, 12).getDays());

                Block block = builder.build();
                blocksBuilder.add(block);
            }

            List<Block> blocks = blocksBuilder.build();
            Page page = new Page(3, blocks.toArray(Block[]::new));

            List<String> columnNames = IntStream.range(0, types.size()).mapToObj(idx -> types.get(idx).getDisplayName() + "_" + idx).toList();

            PageToArrowConverter pageToArrowConverter = new PageToArrowConverter(allocator, columnNames, types, null);
            ArrowToPageConverter arrowToPageConverter = new ArrowToPageConverter(allocator, pageToArrowConverter.getSchema());

            try (VectorSchemaRoot root = pageToArrowConverter.convert(page)) {
                Page convertedPage = arrowToPageConverter.convert(root);
                PageAssertions.assertPageEquals(types, convertedPage, page);
            }
        }
    }

    @Test
    public void testConvertTimestampWithTimeZone()
    {
        try (BufferAllocator allocator = new RootAllocator()) {
            List<Type> types = List.of(TIMESTAMP_TZ_MICROS);

            ImmutableList.Builder<Block> blocksBuilder = ImmutableList.builder();
            {
                Instant instant = ZonedDateTime.of(2001, 8, 22, 12, 34, 56, 123456789, ZoneId.of("UTC")).toInstant();
                SqlTimestampWithTimeZone ts = SqlTimestampWithTimeZone.fromInstant(6, instant, ZoneId.of("-0500"));
                Instant instant2 = ZonedDateTime.of(2024, 1, 22, 22, 31, 16, 123456789, ZoneId.of("UTC")).toInstant();
                SqlTimestampWithTimeZone ts2 = SqlTimestampWithTimeZone.fromInstant(6, instant2, ZoneId.of("+0800"));

                BlockBuilder tsTzBuilder = TIMESTAMP_TZ_MICROS.createFixedSizeBlockBuilder(3);
                tsTzBuilder.appendNull();
                TIMESTAMP_TZ_MICROS.writeObject(tsTzBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(ts.getEpochMillis(), ts.getPicosOfMilli(), ts.getTimeZoneKey()));
                TIMESTAMP_TZ_MICROS.writeObject(tsTzBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(ts2.getEpochMillis(), ts2.getPicosOfMilli(), ts2.getTimeZoneKey()));
                Block block = tsTzBuilder.build();
                blocksBuilder.add(block);
            }

            Page page = new Page(3, blocksBuilder.build().toArray(Block[]::new));

            ImmutableList.Builder<Block> expectedBlocksBuilder = ImmutableList.builder();
            {
                Instant instant = ZonedDateTime.of(2001, 8, 22, 12, 34, 56, 123456789, ZoneId.of("UTC")).toInstant();
                SqlTimestampWithTimeZone ts = SqlTimestampWithTimeZone.fromInstant(6, instant, ZoneId.of("-0500"));
                Instant instant2 = ZonedDateTime.of(2024, 1, 22, 22, 31, 16, 123456789, ZoneId.of("UTC")).toInstant();
                SqlTimestampWithTimeZone ts2 = SqlTimestampWithTimeZone.fromInstant(6, instant2, ZoneId.of("+0800"));

                BlockBuilder tsTzBuilder = TIMESTAMP_TZ_MICROS.createFixedSizeBlockBuilder(3);
                tsTzBuilder.appendNull();
                TIMESTAMP_TZ_MICROS.writeObject(tsTzBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(ts.getEpochMillis(), ts.getPicosOfMilli(), TimeZoneKey.UTC_KEY));
                TIMESTAMP_TZ_MICROS.writeObject(tsTzBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(ts2.getEpochMillis(), ts2.getPicosOfMilli(), TimeZoneKey.UTC_KEY));
                Block block = tsTzBuilder.build();
                expectedBlocksBuilder.add(block);
            }

            Page expectedPage = new Page(3, expectedBlocksBuilder.build().toArray(Block[]::new));

            List<String> columnNames = IntStream.range(0, types.size()).mapToObj(idx -> types.get(idx).getDisplayName() + "_" + idx).toList();

            PageToArrowConverter pageToArrowConverter = new PageToArrowConverter(allocator, columnNames, types, null);
            ArrowToPageConverter arrowToPageConverter = new ArrowToPageConverter(allocator, pageToArrowConverter.getSchema());

            try (VectorSchemaRoot root = pageToArrowConverter.convert(page)) {
                Page convertedPage = arrowToPageConverter.convert(root);
                PageAssertions.assertPageEquals(types, convertedPage, expectedPage);
            }
        }
    }
}
