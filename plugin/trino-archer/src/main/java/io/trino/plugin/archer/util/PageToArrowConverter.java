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
package io.trino.plugin.archer.util;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateDayWriter;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.complex.writer.FixedSizeBinaryWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMicroWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMicroWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.holders.FixedSizeBinaryHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.archer.ArcherUtil.checkMapType;
import static io.trino.plugin.archer.util.ConverterUtils.trinoTypeToArrowField;
import static io.trino.plugin.archer.util.Timestamps.getTimestampTz;
import static io.trino.plugin.archer.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PageToArrowConverter
{
    private static final Logger log = Logger.get(PageToArrowConverter.class);
    private final BufferAllocator allocator;
    private final Schema schema;
    private final List<FieldVector> tmpFieldVector;
    private final List<Function<Block, FieldVector>> converters;

    public PageToArrowConverter(BufferAllocator allocator, List<String> columnNames, List<Type> columnTypes, List<Boolean> nullables)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        checkState(columnNames.size() == columnTypes.size(), "column names list size should equal to column types list size");
        if (nullables != null) {
            checkState(columnNames.size() == nullables.size(), "column names list size should equal to nullables list size");
        }
        int length = columnNames.size();
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        ImmutableList.Builder<Function<Block, FieldVector>> converters = ImmutableList.builder();
        for (int i = 0; i < length; i++) {
            boolean nullable = nullables != null ? nullables.get(i) : true;
            fields.add(trinoTypeToArrowField(columnNames.get(i), columnTypes.get(i), nullable));
            Function<Block, FieldVector> converter = convertBlock(columnNames.get(i), columnTypes.get(i), nullable);
            converters.add(converter);
        }
        this.schema = new Schema(fields.build());
        this.converters = converters.build();
        this.tmpFieldVector = new ArrayList<>(columnNames.size());
    }

    public VectorSchemaRoot convert(Page page)
    {
        try {
            int fieldSize = this.schema.getFields().size();
            ImmutableList.Builder<FieldVector> fieldVectors = ImmutableList.builder();
            checkState(page.getChannelCount() == fieldSize);
            for (int i = 0; i < fieldSize; i++) {
                fieldVectors.add(this.converters.get(i).apply(page.getBlock(i)));
            }
            VectorSchemaRoot root = new VectorSchemaRoot(this.schema, fieldVectors.build(), page.getPositionCount());
            tmpFieldVector.clear();
            return root;
        }
        finally {
            try {
                AutoCloseables.close(tmpFieldVector);
            }
            catch (Exception e) {
                log.warn("failed to close field vector", e);
            }
            tmpFieldVector.clear();
        }
    }

    public Schema getSchema()
    {
        return schema;
    }

    private Function<Block, FieldVector> convertBlock(String columnName, Type type, boolean nullable)
    {
        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                return block -> writeBitVector(block, columnName, type, nullable);
            }
            else if (type instanceof DecimalType decimalType) {
                return block -> writeDecimalVector(block, columnName, decimalType, nullable);
            }
            else if (javaType == long.class) {
                if (type.equals(INTEGER)) {
                    return block -> writeIntVector(block, columnName, type, nullable);
                }
                else if (type.equals(BIGINT)) {
                    return block -> writeBigIntVector(block, columnName, type, nullable);
                }
                else if (type.equals(DATE)) {
                    return block -> writeDateDayVector(block, columnName, type, nullable);
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    return block -> writeTimeStampMicroVector(block, columnName, type, nullable);
                }
                else if (type.equals(TIME_MICROS)) {
                    return block -> writeTimeMicroVector(block, columnName, type, nullable);
                }
                else if (type.equals(REAL)) {
                    return block -> writeFloat4Vector(block, columnName, type, nullable);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                return block -> writeFloat8Vector(block, columnName, type, nullable);
            }
            else if (javaType == Slice.class) {
                if (type instanceof VarcharType) {
                    return block -> writeVarCharVector(block, columnName, type, nullable);
                }
                else if (type instanceof VarbinaryType) {
                    return block -> writeVarBinaryCharVector(block, columnName, type, nullable);
                }
                else if (type instanceof UuidType) {
                    return block -> writeUuidVector(block, columnName, type, nullable);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
                }
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                return block -> writeTimeStampTZMicroVector(block, columnName, type, nullable);
            }
            else if (type instanceof ArrayType arrayType) {
                return block -> writeListVector(block, columnName, arrayType, nullable);
            }
            else if (type instanceof RowType rowType) {
                return block -> writeStructVector(block, columnName, rowType, nullable);
            }
            else if (type instanceof MapType mapType) {
                return block -> writeMapVector(block, columnName, mapType, nullable);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type), ex);
        }
    }

    private FieldVector writeBitVector(Block block, String columnName, Type type, boolean nullable)
    {
        BitVector vector = new BitVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, type.getBoolean(block, i) ? 1 : 0);
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeIntVector(Block block, String columnName, Type type, boolean nullable)
    {
        IntVector vector = new IntVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, toIntExact(type.getLong(block, i)));
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeBigIntVector(Block block, String columnName, Type type, boolean nullable)
    {
        BigIntVector vector = new BigIntVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, type.getLong(block, i));
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeDecimalVector(Block block, String columnName, DecimalType decimalType, boolean nullable)
    {
        DecimalVector vector = new DecimalVector(trinoTypeToArrowField(columnName, decimalType, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, Decimals.readBigDecimal(decimalType, block, i));
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeDateDayVector(Block block, String columnName, Type type, boolean nullable)
    {
        DateDayVector vector = new DateDayVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, DATE.getInt(block, i));
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeTimeStampMicroVector(Block block, String columnName, Type type, boolean nullable)
    {
        TimeStampMicroTZVector vector = new TimeStampMicroTZVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, TIMESTAMP_MICROS.getLong(block, i));
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeTimeStampTZMicroVector(Block block, String columnName, Type type, boolean nullable)
    {
        TimeStampMicroVector vector = new TimeStampMicroVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                LongTimestampWithTimeZone ts = getTimestampTz(block, i);
                vector.set(i, timestampTzToMicros(ts));
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeTimeMicroVector(Block block, String columnName, Type type, boolean nullable)
    {
        TimeMicroVector vector = new TimeMicroVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, TIME_MICROS.getLong(block, i) / PICOSECONDS_PER_MICROSECOND);
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeFloat4Vector(Block block, String columnName, Type type, boolean nullable)
    {
        Float4Vector vector = new Float4Vector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, REAL.getFloat(block, i));
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeFloat8Vector(Block block, String columnName, Type type, boolean nullable)
    {
        Float8Vector vector = new Float8Vector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.set(i, DOUBLE.getDouble(block, i));
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeVarCharVector(Block block, String columnName, Type type, boolean nullable)
    {
        VarCharVector vector = new VarCharVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                Slice slice = VARCHAR.getSlice(block, i);
                // Invalid UTF-8 sequences are replaced with the Unicode replacement character U+FFFD (0xEF 0xBF 0xBD).
                slice = SliceUtf8.fixInvalidUtf8(slice);
                vector.setSafe(i, slice.getBytes());
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeVarBinaryCharVector(Block block, String columnName, Type type, boolean nullable)
    {
        VarBinaryVector vector = new VarBinaryVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.setSafe(i, VARBINARY.getSlice(block, i).getBytes());
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeUuidVector(Block block, String columnName, Type type, boolean nullable)
    {
        FixedSizeBinaryVector vector = new FixedSizeBinaryVector(trinoTypeToArrowField(columnName, type, nullable), allocator);
        this.tmpFieldVector.add(vector);
        int length = block.getPositionCount();
        vector.allocateNew(block.getPositionCount());
        for (int i = 0; i < length; i++) {
            if (!block.isNull(i)) {
                vector.setSafe(i, UUID.getSlice(block, i).getBytes());
            }
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeListVector(Block block, String columnName, ArrayType arrayType, boolean nullable)
    {
        ListVector vector = new ListVector(trinoTypeToArrowField(columnName, arrayType, nullable), allocator, null);
        this.tmpFieldVector.add(vector);
        UnionListWriter listWriter = vector.getWriter();
        listWriter.allocate();
        BiConsumer<Block, Integer> consumer = genListWriter(listWriter, arrayType);
        int length = block.getPositionCount();
        for (int i = 0; i < length; i++) {
            listWriter.setPosition(i);
            consumer.accept(block, i);
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeMapVector(Block block, String columnName, MapType mapType, boolean nullable)
    {
        MapVector vector = new MapVector(trinoTypeToArrowField(columnName, mapType, nullable), allocator, null);
        this.tmpFieldVector.add(vector);
        UnionMapWriter unionMapWriter = vector.getWriter();
        unionMapWriter.allocate();
        BiConsumer<Block, Integer> consumer = genMapWriter(unionMapWriter, mapType);
        int length = block.getPositionCount();
        for (int i = 0; i < length; i++) {
            unionMapWriter.setPosition(i);
            consumer.accept(block, i);
        }
        vector.setValueCount(length);
        return vector;
    }

    private FieldVector writeStructVector(Block rowBlock, String columnName, RowType rowType, boolean nullable)
    {
        StructVector vector = new StructVector(trinoTypeToArrowField(columnName, rowType, nullable), allocator, null);
        this.tmpFieldVector.add(vector);
        NullableStructWriter structWriter = vector.getWriter();
        structWriter.allocate();
        BiConsumer<Block, Integer> consumer = genStructWriter(structWriter, rowType);
        int length = rowBlock.getPositionCount();
        for (int i = 0; i < length; i++) {
            structWriter.setPosition(i);
            consumer.accept(rowBlock, i);
        }
        vector.setValueCount(length);
        return vector;
    }

    private BiConsumer<Block, Integer> genBitWriter(BitWriter writer, Type type)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeBit(type.getBoolean(block, position) ? 1 : 0);
            }
        };
    }

    private BiConsumer<Block, Integer> genIntWriter(IntWriter writer, Type type)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeInt(toIntExact(type.getLong(block, position)));
            }
        };
    }

    private BiConsumer<Block, Integer> genBigIntWriter(BigIntWriter writer, Type type)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeBigInt(type.getLong(block, position));
            }
        };
    }

    private BiConsumer<Block, Integer> genDateDayWriter(DateDayWriter writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeDateDay(DATE.getInt(block, position));
            }
        };
    }

    private BiConsumer<Block, Integer> genTimeStampMicroWriter(TimeStampMicroWriter writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeTimeStampMicro((TIMESTAMP_MICROS.getLong(block, position)));
            }
        };
    }

    private BiConsumer<Block, Integer> genTimeMicroWriter(TimeMicroWriter writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeTimeMicro(TIME_MICROS.getLong(block, position) / PICOSECONDS_PER_MICROSECOND);
            }
        };
    }

    private BiConsumer<Block, Integer> genFloat4Writer(Float4Writer writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeFloat4(REAL.getFloat(block, position));
            }
        };
    }

    private BiConsumer<Block, Integer> genFloat8Writer(Float8Writer writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeFloat8(DOUBLE.getDouble(block, position));
            }
        };
    }

    private BiConsumer<Block, Integer> genDecimalWriter(DecimalWriter writer, DecimalType decimalType)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeDecimal(Decimals.readBigDecimal(decimalType, block, position));
            }
        };
    }

    private BiConsumer<Block, Integer> genVarCharWriter(VarCharWriter writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                Slice slice = VARCHAR.getSlice(block, position);
                // Invalid UTF-8 sequences are replaced with the Unicode replacement character U+FFFD (0xEF 0xBF 0xBD).
                String value = slice.toStringUtf8();
                writer.writeVarChar(value);
            }
        };
    }

    private BiConsumer<Block, Integer> genVarBinaryWriter(VarBinaryWriter writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                writer.writeVarBinary(VARBINARY.getSlice(block, position).getBytes());
            }
        };
    }

    private BiConsumer<Block, Integer> genUuidWriter(FixedSizeBinaryWriter writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                try (ArrowBuf buf = allocator.buffer(16)) {
                    buf.writeBytes(UUID.getSlice(block, position).getBytes());
                    FixedSizeBinaryHolder holder = new FixedSizeBinaryHolder();
                    holder.byteWidth = 16;
                    holder.buffer = buf;
                    writer.write(holder);
                }
            }
        };
    }

    private BiConsumer<Block, Integer> genTimeStampMicroTZWriter(TimeStampMicroWriter writer)
    {
        return (block, position) -> {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                LongTimestampWithTimeZone ts = getTimestampTz(block, position);
                writer.writeTimeStampMicro(timestampTzToMicros(ts));
            }
        };
    }

    private BiConsumer<Block, Integer> genElementWriter(BaseWriter.ListWriter listWriter, Type elemType)
    {
        Class<?> javaType = elemType.getJavaType();
        try {
            if (javaType == boolean.class) {
                BitWriter writer = listWriter.bit();
                return genBitWriter(writer, elemType);
            }
            else if (elemType instanceof DecimalType decimalType) {
                DecimalWriter writer = listWriter.decimal();
                return genDecimalWriter(writer, decimalType);
            }
            else if (javaType == long.class) {
                if (elemType.equals(INTEGER)) {
                    IntWriter writer = listWriter.integer();
                    return genIntWriter(writer, elemType);
                }
                else if (elemType.equals(BIGINT)) {
                    BigIntWriter writer = listWriter.bigInt();
                    return genBigIntWriter(writer, elemType);
                }
                else if (elemType.equals(DATE)) {
                    DateDayWriter writer = listWriter.dateDay();
                    return genDateDayWriter(writer);
                }
                else if (elemType.equals(TIMESTAMP_MICROS)) {
                    TimeStampMicroWriter writer = listWriter.timeStampMicro();
                    return genTimeStampMicroWriter(writer);
                }
                else if (elemType.equals(TIME_MICROS)) {
                    TimeMicroWriter writer = listWriter.timeMicro();
                    return genTimeMicroWriter(writer);
                }
                else if (elemType.equals(REAL)) {
                    Float4Writer writer = listWriter.float4();
                    return genFloat4Writer(writer);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), elemType));
                }
            }
            else if (javaType == double.class) {
                Float8Writer writer = listWriter.float8();
                return genFloat8Writer(writer);
            }
            else if (javaType == Slice.class) {
                if (elemType instanceof VarcharType) {
                    VarCharWriter writer = listWriter.varChar();
                    return genVarCharWriter(writer);
                }
                else if (elemType instanceof VarbinaryType) {
                    VarBinaryWriter writer = listWriter.varBinary();
                    return genVarBinaryWriter(writer);
                }
                else if (elemType instanceof UuidType) {
                    FixedSizeBinaryWriter writer = listWriter.fixedSizeBinary();
                    return genUuidWriter(writer);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + elemType.getTypeSignature());
                }
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                TimeStampMicroWriter writer = listWriter.timeStampMicro();
                return genTimeStampMicroTZWriter(writer);
            }
            else if (elemType instanceof ArrayType arrayType) {
                BaseWriter.ListWriter writer = listWriter.list();
                return genListWriter(writer, arrayType);
            }
            else if (elemType instanceof RowType rowType) {
                BaseWriter.StructWriter writer = listWriter.struct();
                return genStructWriter(writer, rowType);
            }
            else if (elemType instanceof MapType mapType) {
                BaseWriter.MapWriter writer = listWriter.map(false);
                return genMapWriter(writer, mapType);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), elemType));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), elemType), ex);
        }
    }

    private BiConsumer<Block, Integer> genListWriter(BaseWriter.ListWriter listWriter, ArrayType type)
    {
        BiConsumer<Block, Integer> consumer = genElementWriter(listWriter, type.getElementType());
        return (block, position) -> {
            if (block.isNull(position)) {
                listWriter.writeNull();
            }
            else {
                Block elemBlock = type.getObject(block, position);
                int length = elemBlock.getPositionCount();
                listWriter.startList();
                for (int i = 0; i < length; i++) {
                    consumer.accept(elemBlock, i);
                }
                listWriter.endList();
            }
        };
    }

    private BiConsumer<Block, Integer> genMapFieldWriter(BaseWriter.MapWriter mapWriter, MapType mapType)
    {
        checkMapType(mapType);
        BiConsumer<Block, Integer> keyConsumer = genElementWriter(mapWriter.key(), mapType.getKeyType());
        BiConsumer<Block, Integer> valueConsumer = genElementWriter(mapWriter.value(), mapType.getValueType());

        return new BiConsumer<>()
        {
            private Block block;
            private ColumnarMap columnarMap;

            @Override
            public void accept(Block block, Integer position)
            {
                if (this.block == null || this.block != block) {
                    this.block = block;
                    this.columnarMap = toColumnarMap(block);
                }
                ColumnarMap columnarMap = this.columnarMap;
                int offset = columnarMap.getOffset(position);
                Block keyBlock = columnarMap.getKeysBlock();
                Block valueBlock = columnarMap.getValuesBlock();
                int length = columnarMap.getEntryCount(position);

                for (int i = 0; i < length; i++) {
                    int kvPos = offset + i;
                    mapWriter.startEntry();
                    keyConsumer.accept(keyBlock, kvPos);
                    valueConsumer.accept(valueBlock, kvPos);
                    mapWriter.endEntry();
                }
            }
        };
    }

    private BiConsumer<Block, Integer> genMapWriter(BaseWriter.MapWriter mapWriter, MapType type)
    {
        BiConsumer<Block, Integer> consumer = genMapFieldWriter(mapWriter, type);
        return (block, position) -> {
            if (block.isNull(position)) {
                mapWriter.writeNull();
            }
            else {
                mapWriter.startMap();
                consumer.accept(block, position);
                mapWriter.endMap();
            }
        };
    }

    private BiConsumer<Block, Integer> genStructFieldWriter(BaseWriter.StructWriter structWriter, String fieldName, Type fieldType)
    {
        Class<?> javaType = fieldType.getJavaType();
        try {
            if (javaType == boolean.class) {
                BitWriter writer = structWriter.bit(fieldName);
                return genBitWriter(writer, fieldType);
            }
            else if (fieldType instanceof DecimalType decimalType) {
                DecimalWriter writer = structWriter.decimal(fieldName);
                return genDecimalWriter(writer, decimalType);
            }
            else if (javaType == long.class) {
                if (fieldType.equals(INTEGER)) {
                    IntWriter writer = structWriter.integer(fieldName);
                    return genIntWriter(writer, fieldType);
                }
                else if (fieldType.equals(BIGINT)) {
                    BigIntWriter writer = structWriter.bigInt(fieldName);
                    return genBigIntWriter(writer, fieldType);
                }
                else if (fieldType.equals(DATE)) {
                    DateDayWriter writer = structWriter.dateDay(fieldName);
                    return genDateDayWriter(writer);
                }
                else if (fieldType.equals(TIMESTAMP_MICROS)) {
                    TimeStampMicroWriter writer = structWriter.timeStampMicro(fieldName);
                    return genTimeStampMicroWriter(writer);
                }
                else if (fieldType.equals(TIME_MICROS)) {
                    TimeMicroWriter writer = structWriter.timeMicro(fieldName);
                    return genTimeMicroWriter(writer);
                }
                else if (fieldType.equals(REAL)) {
                    Float4Writer writer = structWriter.float4(fieldName);
                    return genFloat4Writer(writer);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), fieldType));
                }
            }
            else if (javaType == double.class) {
                Float8Writer writer = structWriter.float8(fieldName);
                return genFloat8Writer(writer);
            }
            else if (javaType == Slice.class) {
                switch (fieldType) {
                    case VarcharType _ -> {
                        VarCharWriter writer = structWriter.varChar(fieldName);
                        return genVarCharWriter(writer);
                    }
                    case VarbinaryType _ -> {
                        VarBinaryWriter writer = structWriter.varBinary(fieldName);
                        return genVarBinaryWriter(writer);
                    }
                    case UuidType _ -> {
                        FixedSizeBinaryWriter writer = structWriter.fixedSizeBinary(fieldName, 16);
                        return genUuidWriter(writer);
                    }
                    default -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + fieldType.getTypeSignature());
                }
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                TimeStampMicroWriter writer = structWriter.timeStampMicro(fieldName);
                return genTimeStampMicroTZWriter(writer);
            }
            else if (fieldType instanceof ArrayType arrayType) {
                BaseWriter.ListWriter writer = structWriter.list(fieldName);
                return genListWriter(writer, arrayType);
            }
            else if (fieldType instanceof RowType rowType) {
                BaseWriter.StructWriter writer = structWriter.struct(fieldName);
                return genStructWriter(writer, rowType);
            }
            else if (fieldType instanceof MapType mapType) {
                BaseWriter.MapWriter writer = structWriter.map(fieldName, false);
                return genMapWriter(writer, mapType);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), fieldType));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), fieldType), ex);
        }
    }

    private BiConsumer<Block, Integer> genStructWriter(BaseWriter.StructWriter structWriter, RowType rowType)
    {
        List<RowType.Field> fields = rowType.getFields();

        ImmutableList.Builder<BiConsumer<Block, Integer>> fieldWritersBuilder = ImmutableList.builder();
        for (int i = 0; i < rowType.getTypeSignature().getParameters().size(); i++) {
            TypeSignatureParameter parameter = rowType.getTypeSignature().getParameters().get(i);
            String fieldName = parameter.getNamedTypeSignature().getName().orElse("field" + i);
            Type type = fields.get(i).getType();
            fieldWritersBuilder.add(genStructFieldWriter(structWriter, fieldName, type));
        }

        List<BiConsumer<Block, Integer>> fieldWriters = fieldWritersBuilder.build();

        return (block, position) -> {
            if (block.isNull(position)) {
                structWriter.writeNull();
            }
            else {
                List<Block> fieldBlocks = ((RowBlock) block).getFieldBlocks();
                structWriter.start();
                int fieldSize = fieldWriters.size();
                for (int i = 0; i < fieldSize; i++) {
                    fieldWriters.get(i).accept(fieldBlocks.get(i), position);
                }
                structWriter.end();
            }
        };
    }
}
