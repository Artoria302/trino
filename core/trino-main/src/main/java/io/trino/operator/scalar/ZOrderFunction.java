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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;

public class ZOrderFunction
        extends SqlScalarFunction
{
    public static final String FUNCTION_NAME = "zorder";
    public static final ZOrderFunction Z_ORDER_FUNCTION = new ZOrderFunction();
    private static final MethodHandle METHOD_HANDLE = methodHandle(ZOrderFunction.class, "zorder", List.class, Block.class);

    private static final long BIT_8_MASK = 1L << 7;
    private static final long BIT_16_MASK = 1L << 15;
    private static final long BIT_32_MASK = 1L << 31;
    private static final long BIT_64_MASK = 1L << 63;

    private ZOrderFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(FUNCTION_NAME)
                        .variadicTypeParameter("T", "row")
                        .argumentType(new TypeSignature("T"))
                        .returnType(VARBINARY.getTypeSignature())
                        .build())
                .description("convert columns to a zorder value column")
                .build());
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        if (boundSignature.getArity() < 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "There must be one or more arguments to " + FUNCTION_NAME);
        }

        Type rowType = boundSignature.getArgumentType(0);

        List<Function<Block, byte[]>> converters = mapWithIndex(
                rowType.getTypeParameters().stream(),
                (type, index) -> converter(type, toIntExact(index)))
                .collect(toImmutableList());

        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                METHOD_HANDLE.bindTo(converters));
    }

    @UsedByGeneratedCode
    public static Slice zorder(List<Function<Block, byte[]>> converters, Block row)
    {
        byte[][] args = new byte[converters.size()][];
        for (int i = 0; i < args.length; i++) {
            args[i] = converters.get(i).apply(row);
        }
        return zorder(args);
    }

    private static Slice zorder(byte[][] arrays)
    {
        int totalLength = 0;
        int maxLength = 0;
        for (byte[] arr : arrays) {
            totalLength += arr.length;
            maxLength = Math.max(maxLength, arr.length * 8);
        }
        byte[] result = new byte[totalLength];
        int resultBit = 0;
        for (int bit = 0; bit < maxLength; bit++) {
            int bytePos = bit / 8;
            int bitPos = bit % 8;

            for (byte[] arr : arrays) {
                int len = arr.length;
                if (bytePos < len) {
                    int resultBytePos = totalLength - 1 - resultBit / 8;
                    int resultBitPos = resultBit % 8;
                    result[resultBitPos] = updatePos(result[resultBytePos], resultBitPos, arr[len - 1 - bytePos], bitPos);
                    resultBit += 1;
                }
            }
        }

        Slice slice = Slices.allocate(totalLength);
        slice.setBytes(0, result);
        return slice;
    }

    private static byte updatePos(byte a, int apos, byte b, int bpos)
    {
        byte tmp = (byte) (b & (1 << bpos));
        if (apos > bpos) {
            tmp = (byte) (tmp << (apos - bpos));
        }
        else if (apos < bpos) {
            tmp = (byte) (tmp >> (bpos - apos));
        }
        byte atmp = (byte) (a & (1 << apos));
        if (atmp == tmp) {
            return a;
        }
        return (byte) (a ^ (1 << apos));
    }

    private static Function<Block, byte[]> converter(Type type, int position)
    {
        Function<Block, byte[]> converter = valueConverter(type, position);
        return (block) -> block.isNull(position) ? null : converter.apply(block);
    }

    private static Function<Block, byte[]> valueConverter(Type type, int position)
    {
        if (type.equals(BOOLEAN)) {
            return block -> (block.isNull(position) || type.getBoolean(block, position)) ? (new byte[] {(byte) 1}) : (new byte[] {(byte) 0});
        }
        if (type.equals(TINYINT)) {
            return block -> new byte[block.isNull(position) ? Byte.MAX_VALUE : (byte) (type.getLong(block, position) ^ BIT_8_MASK)];
        }
        if (type.equals(SMALLINT)) {
            return block -> Shorts.toByteArray(block.isNull(position) ? Short.MAX_VALUE : (short) (type.getLong(block, position) ^ BIT_16_MASK));
        }
        if (type.equals(INTEGER)) {
            return block -> Ints.toByteArray(block.isNull(position) ? Integer.MAX_VALUE : (int) (type.getLong(block, position) ^ BIT_32_MASK));
        }
        if (type.equals(BIGINT)) {
            return block -> Longs.toByteArray(block.isNull(position) ? Long.MAX_VALUE : type.getLong(block, position) ^ BIT_64_MASK);
        }
        if (type.equals(REAL)) {
            return block -> Longs.toByteArray(block.isNull(position) ? Long.MAX_VALUE : type.getLong(block, position) ^ BIT_64_MASK);
        }
        if (type.equals(DOUBLE)) {
            return block -> Longs.toByteArray(block.isNull(position) ? Long.MAX_VALUE : Double.doubleToRawLongBits(type.getDouble(block, position)) ^ BIT_64_MASK);
        }
        if (isShortDecimal(type)) {
            return block -> Longs.toByteArray(block.isNull(position) ? Long.MAX_VALUE : type.getLong(block, position) ^ BIT_64_MASK);
        }
        if (isLongDecimal(type)) {
            return block -> Longs.toByteArray(block.isNull(position) ? Long.MAX_VALUE : ((Int128) type.getObject(block, position)).toLong() ^ BIT_64_MASK);
        }
        if (type instanceof VarcharType) {
            return block -> block.isNull(position) ? Longs.toByteArray(Long.MAX_VALUE) : paddingTo8Byte(type.getSlice(block, position).toStringUtf8().getBytes(StandardCharsets.UTF_8));
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            return block -> block.isNull(position) ? Longs.toByteArray(Long.MAX_VALUE) : paddingTo8Byte(padSpaces(type.getSlice(block, position), charType).toStringUtf8().getBytes(StandardCharsets.UTF_8));
        }

        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid argument type '" + type + "' to " + FUNCTION_NAME);
    }

    private static byte[] paddingTo8Byte(byte[] a)
    {
        int len = a.length;
        if (len == 8) {
            return a;
        }
        byte[] result = new byte[8];
        if (len > 8) {
            System.arraycopy(a, 0, result, 0, 8);
        }
        else {
            System.arraycopy(a, 0, result, 8 - len, len);
        }
        return result;
    }
}
