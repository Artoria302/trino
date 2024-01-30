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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.archer.ArcherErrorCode.ARCHER_MISSING_METADATA;
import static io.trino.plugin.archer.ArcherUtil.checkMapType;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static net.qihoo.archer.arrow.util.ArrowSchemaUtil.ORIGINAL_TYPE;
import static net.qihoo.archer.arrow.util.ArrowSchemaUtil.TIMESTAMP_TYPE;
import static net.qihoo.archer.arrow.util.ArrowSchemaUtil.TIMESTAMP_TZ_TYPE;
import static net.qihoo.archer.arrow.util.ArrowSchemaUtil.UUID_TYPE;

public class ConverterUtils
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private ConverterUtils() {}

    public static Type arrowFieldToTrinoType(Field field)
    {
        ArrowType type = field.getType();
        switch (type.getTypeID()) {
            case Int -> {
                int width = ((ArrowType.Int) type).getBitWidth();
                if (!((ArrowType.Int) type).getIsSigned()) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Only support signed arrow int type");
                }
                if (width == 32) {
                    return INTEGER;
                }
                else if (width == 64) {
                    return BIGINT;
                }
                else {
                    throw new TrinoException(INVALID_ARGUMENTS, format("Invalid bit width %d for arrow int type", width));
                }
            }
            case Bool -> {
                return BOOLEAN;
            }
            case FloatingPoint -> {
                FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
                switch (precision) {
                    case HALF -> throw new TrinoException(INVALID_ARGUMENTS, "Not support arrow half precision float");
                    case SINGLE -> {
                        return REAL;
                    }
                    case DOUBLE -> {
                        return DoubleType.DOUBLE;
                    }
                }
            }
            case Decimal -> {
                return DecimalType.createDecimalType(((ArrowType.Decimal) type).getPrecision(), ((ArrowType.Decimal) type).getScale());
            }
            case Date -> {
                return DATE;
            }
            case Time -> {
                return TIME_MICROS;
            }
            case Timestamp -> {
                String originalType = field.getMetadata().get(ORIGINAL_TYPE);
                if (TIMESTAMP_TZ_TYPE.equals(originalType)) {
                    return TIMESTAMP_TZ_MICROS;
                }
                else if (TIMESTAMP_TYPE.equals(originalType)) {
                    return TIMESTAMP_MICROS;
                }
                else {
                    throw new TrinoException(ARCHER_MISSING_METADATA, "Detail information of timestamp type is not found in arrow field metadata");
                }
            }
            case Utf8, LargeUtf8 -> {
                return VARCHAR;
            }
            case Binary, LargeBinary -> {
                return VARBINARY;
            }
            case FixedSizeBinary -> {
                if (UUID_TYPE.equals(field.getMetadata().get(ORIGINAL_TYPE))) {
                    return UUID;
                }
                return VARBINARY;
            }
            case List, LargeList, FixedSizeList -> {
                Type elementType = arrowFieldToTrinoType(field.getChildren().get(0));
                return new ArrayType(elementType);
            }
            case Struct -> {
                List<RowType.Field> types = field.getChildren().stream().map(f ->
                        new RowType.Field(Optional.of(f.getName()), arrowFieldToTrinoType(f))
                ).collect(toImmutableList());
                return RowType.from(types);
            }
            case Map -> {
                return new MapType(
                        arrowFieldToTrinoType(field.getChildren().get(0).getChildren().get(0)),
                        arrowFieldToTrinoType(field.getChildren().get(0).getChildren().get(1)), TYPE_OPERATORS);
            }
        }
        throw new TrinoException(INVALID_ARGUMENTS, format("Unsupported arrow field %s", field));
    }

    public static Field trinoTypeToArrowField(String columnName, Type type, boolean nullable)
    {
        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                ArrowType arrowType = Types.MinorType.BIT.getType();
                return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
            }
            else if (type instanceof DecimalType decimalType) {
                ArrowType arrowType = new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), /*bitWidth=*/DecimalVector.TYPE_WIDTH * 8);
                return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
            }
            else if (javaType == long.class) {
                if (type.equals(INTEGER)) {
                    ArrowType arrowType = Types.MinorType.INT.getType();
                    return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
                }
                else if (type.equals(BIGINT)) {
                    ArrowType arrowType = Types.MinorType.BIGINT.getType();
                    return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
                }
                else if (type.equals(DATE)) {
                    ArrowType arrowType = Types.MinorType.DATEDAY.getType();
                    return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    ArrowType arrowType = Types.MinorType.TIMESTAMPMICRO.getType();
                    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                    builder.put(ORIGINAL_TYPE, TIMESTAMP_TYPE);
                    FieldType fieldType = new FieldType(nullable, arrowType, null, builder.buildOrThrow());
                    return new Field(columnName, fieldType, null);
                }
                else if (type.equals(TIME_MICROS)) {
                    ArrowType arrowType = Types.MinorType.TIMEMICRO.getType();
                    return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
                }
                else if (type.equals(REAL)) {
                    ArrowType arrowType = Types.MinorType.FLOAT4.getType();
                    return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                ArrowType arrowType = Types.MinorType.FLOAT8.getType();
                return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
            }
            else if (javaType == Slice.class) {
                if (type instanceof VarcharType) {
                    ArrowType arrowType = Types.MinorType.VARCHAR.getType();
                    return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
                }
                else if (type instanceof VarbinaryType) {
                    ArrowType arrowType = Types.MinorType.VARBINARY.getType();
                    return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), null);
                }
                else if (type instanceof UuidType) {
                    ArrowType arrowType = new ArrowType.FixedSizeBinary(16);
                    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                    builder.put(ORIGINAL_TYPE, UUID_TYPE);
                    FieldType fieldType = new FieldType(nullable, arrowType, null, builder.buildOrThrow());
                    return new Field(columnName, fieldType, null);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
                }
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                ArrowType arrowType = Types.MinorType.TIMESTAMPMICRO.getType();
                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                builder.put(ORIGINAL_TYPE, TIMESTAMP_TZ_TYPE);
                FieldType fieldType = new FieldType(nullable, arrowType, null, builder.buildOrThrow());
                return new Field(columnName, fieldType, null);
            }
            else if (type instanceof ArrayType arrayType) {
                List<Field> children = List.of(trinoTypeToArrowField(ListVector.DATA_VECTOR_NAME, arrayType.getElementType(), true));
                return new Field(columnName, nullable ? FieldType.nullable(ArrowType.List.INSTANCE) : FieldType.notNullable(ArrowType.List.INSTANCE), children);
            }
            else if (type instanceof RowType rowType) {
                List<RowType.Field> fields = rowType.getFields();
                ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
                for (int i = 0; i < rowType.getTypeSignature().getParameters().size(); i++) {
                    TypeSignatureParameter parameter = rowType.getTypeSignature().getParameters().get(i);
                    String fieldName = parameter.getNamedTypeSignature().getName().orElse("field" + i);
                    Type subType = fields.get(i).getType();
                    fieldsBuilder.add(trinoTypeToArrowField(fieldName, subType, true));
                }
                List<Field> children = fieldsBuilder.build();
                return new Field(columnName, nullable ? FieldType.nullable(ArrowType.Struct.INSTANCE) : FieldType.notNullable(ArrowType.Struct.INSTANCE), children);
            }
            else if (type instanceof MapType mapType) {
                checkMapType(mapType);
                Field kv = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                        trinoTypeToArrowField(MapVector.KEY_NAME, mapType.getKeyType(), false),
                        trinoTypeToArrowField(MapVector.VALUE_NAME, mapType.getValueType(), true)));
                ArrowType arrowType = new ArrowType.Map(false);
                List<Field> children = List.of(kv);
                return new Field(columnName, nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType), children);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type), ex);
        }
    }
}
