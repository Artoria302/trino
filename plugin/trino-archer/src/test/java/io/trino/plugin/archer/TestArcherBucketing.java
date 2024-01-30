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

import com.google.common.primitives.Primitives;
import io.trino.plugin.archer.PartitionTransforms.ColumnTransform;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.UuidType;
import net.qihoo.archer.transforms.Transform;
import net.qihoo.archer.transforms.Transforms;
import net.qihoo.archer.types.Type;
import net.qihoo.archer.types.Types;
import net.qihoo.archer.types.Types.BooleanType;
import net.qihoo.archer.types.Types.DecimalType;
import net.qihoo.archer.types.Types.DoubleType;
import net.qihoo.archer.types.Types.FloatType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.archer.TypeConverter.toTrinoType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.DateTimes.MICROSECONDS_PER_DAY;
import static io.trino.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.MICROSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.UuidOperators.castFromVarcharToUuid;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static net.qihoo.archer.types.Type.TypeID.DECIMAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArcherBucketing
{
    private static final TypeManager TYPE_MANAGER = new TestingTypeManager();

    @Test
    public void testBucketNumberCompare()
    {
        assertBucketAndHashEquals("int", null, null);
        assertBucketAndHashEquals("int", 0, 1669671676);
        assertBucketAndHashEquals("int", 300_000, 1798339266);
        assertBucketAndHashEquals("int", Integer.MIN_VALUE, 74448856);
        assertBucketAndHashEquals("int", Integer.MAX_VALUE, 1819228606);

        assertBucketAndHashEquals("long", null, null);
        assertBucketAndHashEquals("long", 0L, 1669671676);
        assertBucketAndHashEquals("long", 300_000_000_000L, 371234369);
        assertBucketAndHashEquals("long", Long.MIN_VALUE, 1366273829);
        assertBucketAndHashEquals("long", Long.MAX_VALUE, 40977599);

        assertBucketAndHashEquals("decimal(1, 1)", null, null);
        assertBucketAndHashEquals("decimal(1, 0)", "0", 1364076727);
        assertBucketAndHashEquals("decimal(1, 0)", "1", 1683673515);
        assertBucketAndHashEquals("decimal(1, 0)", "9", 1771774483);
        assertBucketAndHashEquals("decimal(1, 0)", "-9", 156162024);
        assertBucketAndHashEquals("decimal(3, 1)", "0.1", 1683673515);
        assertBucketAndHashEquals("decimal(3, 1)", "1.0", 307159211);
        assertBucketAndHashEquals("decimal(3, 1)", "12.3", 1308316337);
        assertBucketAndHashEquals("decimal(3, 1)", "-12.3", 1847027525);
        assertBucketAndHashEquals("decimal(18, 10)", "0", 1364076727);
        assertBucketAndHashEquals("decimal(38, 10)", null, null);
        assertBucketAndHashEquals("decimal(38, 10)", "999999.9999999999", 1053577599);
        assertBucketAndHashEquals("decimal(38, 10)", "-999999.9999999999", 1054888790);
        assertBucketAndHashEquals("decimal(38, 0)", "99999999999999999999999999999999999999", 1067515814);
        assertBucketAndHashEquals("decimal(38, 0)", "-99999999999999999999999999999999999999", 193266010);
        assertBucketAndHashEquals("decimal(38, 10)", "9999999999999999999999999999.9999999999", 1067515814);
        assertBucketAndHashEquals("decimal(38, 10)", "-9999999999999999999999999999.9999999999", 193266010);
        assertBucketAndHashEquals("decimal(38, 10)", "123456789012345.0", 93815101);
        assertBucketAndHashEquals("decimal(38, 10)", "-123456789012345.0", 522439017);

        assertBucketAndHashEquals("string", null, null);
        assertBucketAndHashEquals("string", "", 0);
        assertBucketAndHashEquals("string", "test string", 671244848);
        assertBucketAndHashEquals("string", "Trino rocks", 2131833594);
        assertBucketAndHashEquals("string", "\u5f3a\u5927\u7684Trino\u5f15\u64ce", 822296301); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        assertBucketAndHashEquals("string", "\uD83D\uDCB0", 661122892); // 4-byte UTF-8 codepoint (non-BMP)
        assertBucketAndHashEquals("string", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF", 2094039023); // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2

        assertBucketAndHashEquals("binary", null, null);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap(new byte[] {}), 0);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap("hello trino".getBytes(StandardCharsets.UTF_8)), 493441885);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap("\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF".getBytes(StandardCharsets.UTF_16)), 1291558121);

        assertBucketAndHashEquals("uuid", null, null);
        assertBucketAndHashEquals("uuid", UUID.fromString("00000000-0000-0000-0000-000000000000"), 20237816);
        assertBucketAndHashEquals("uuid", UUID.fromString("1-2-3-4-5"), 1802237169);
        assertBucketAndHashEquals("uuid", UUID.fromString("406caec7-68b9-4778-81b2-a12ece70c8b1"), 1231261529);

        assertBucketAndHashEquals("fixed[3]", null, null);
        assertBucketAndHashEquals("fixed[3]", ByteBuffer.wrap(new byte[] {0, 0, 0}), 99660839);
        assertBucketAndHashEquals("fixed[3]", ByteBuffer.wrap(new byte[] {1, 2, 3}), 13750788);
        assertBucketAndHashEquals("fixed[3]", ByteBuffer.wrap(new byte[] {127, -128, 1}), 107475887);
        assertBucketAndHashEquals("fixed[3]", ByteBuffer.wrap(new byte[] {-1, -1, -1}), 1058185254);
        assertBucketAndHashEquals("fixed[3]", ByteBuffer.wrap(new byte[] {Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE}), 533318325);
        assertBucketAndHashEquals("fixed[3]", ByteBuffer.wrap(new byte[] {Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE}), 1945840528);

        assertBucketAndHashEquals("date", null, null);
        assertBucketAndHashEquals("date", 0, 1669671676);
        assertBucketAndHashEquals("date", 1, 1392991556);
        assertBucketAndHashEquals("date", toIntExact(LocalDate.of(2005, 9, 10).toEpochDay()), 1958311396);
        assertBucketAndHashEquals("date", toIntExact(LocalDate.of(1965, 1, 2).toEpochDay()), 1149697962); // before epoch

        assertBucketAndHashEquals("time", null, null);
        assertBucketAndHashEquals("time", 0L, 1669671676);
        assertBucketAndHashEquals("time", 1L, 1392991556);
        assertBucketAndHashEquals("time", LocalTime.of(17, 13, 15, 123_000_000).toNanoOfDay() / NANOSECONDS_PER_MICROSECOND, 539121226);
        assertBucketAndHashEquals("time", MICROSECONDS_PER_DAY - 1, 1641029256); // max value

        assertBucketAndHashEquals("timestamp", null, null);
        assertBucketAndHashEquals("timestamp", 0L, 1669671676);
        assertBucketAndHashEquals("timestamp", 1L, 1392991556);
        assertBucketAndHashEquals("timestamp", -1L, 1651860712);
        assertBucketAndHashEquals("timestamp", -13L, 1222449245);
        assertBucketAndHashEquals("timestamp", LocalDateTime.of(2005, 9, 10, 13, 30, 15).toEpochSecond(UTC) * MICROSECONDS_PER_SECOND + 123_456, 1162062113);
        assertBucketAndHashEquals("timestamp", LocalDateTime.of(1965, 1, 2, 13, 30, 15).toEpochSecond(UTC) * MICROSECONDS_PER_SECOND + 123_456, 236109233);  // before epoch

        assertBucketAndHashEquals("timestamptz", null, null);
        assertBucketAndHashEquals("timestamptz", 0L, 1669671676);
        assertBucketAndHashEquals("timestamptz", 1L, 1392991556);
        assertBucketAndHashEquals("timestamptz", -1L, 1651860712);
        assertBucketAndHashEquals("timestamptz", -13L, 1222449245);
        assertBucketAndHashEquals("timestamptz", LocalDateTime.of(2005, 9, 10, 13, 30, 15).toEpochSecond(UTC) * MICROSECONDS_PER_SECOND + 123_456, 1162062113);
        assertBucketAndHashEquals("timestamptz", LocalDateTime.of(1965, 1, 2, 13, 30, 15).toEpochSecond(UTC) * MICROSECONDS_PER_SECOND + 123_456, 236109233);  // before epoch
    }

    /**
     * Test example values from https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
     */
    @Test
    public void testBucketingSpecValues()
    {
        assertBucketAndHashEquals("int", 34, 2017239379);
        assertBucketAndHashEquals("long", 34L, 2017239379);
        assertBucketAndHashEquals("decimal(4, 2)", "14.20", -500754589 & Integer.MAX_VALUE);
        assertBucketAndHashEquals("decimal(10, 2)", "14.20", -500754589 & Integer.MAX_VALUE);
        assertBucketAndHashEquals("decimal(22, 2)", "14.20", -500754589 & Integer.MAX_VALUE);
        assertBucketAndHashEquals("date", toIntExact(LocalDate.of(2017, 11, 16).toEpochDay()), -653330422 & Integer.MAX_VALUE);
        assertBucketAndHashEquals("time", LocalTime.of(22, 31, 8).toNanoOfDay() / NANOSECONDS_PER_MICROSECOND, -662762989 & Integer.MAX_VALUE);
        assertBucketAndHashEquals("timestamp", LocalDateTime.of(2017, 11, 16, 22, 31, 8).toEpochSecond(UTC) * MICROSECONDS_PER_SECOND, -2047944441 & Integer.MAX_VALUE);
        assertBucketAndHashEquals("timestamptz", LocalDateTime.of(2017, 11, 16, 14, 31, 8).toEpochSecond(ZoneOffset.ofHours(-8)) * MICROSECONDS_PER_SECOND, -2047944441 & Integer.MAX_VALUE);
        assertBucketAndHashEquals("string", "iceberg", 1210000089);
        assertBucketAndHashEquals("uuid", UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7"), 1488055340);
        assertBucketAndHashEquals("fixed[4]", ByteBuffer.wrap(new byte[] {0x00, 0x01, 0x02, 0x03}), -188683207 & Integer.MAX_VALUE);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap(new byte[] {0x00, 0x01, 0x02, 0x03}), -188683207 & Integer.MAX_VALUE);
    }

    @ParameterizedTest
    @MethodSource("unsupportedBucketingTypes")
    public void testUnsupportedTypes(Type type)
    {
        assertThatThrownBy(() -> computearcherBucket(type, null, 1))
                .hasMessage("Cannot bucket by type: %s", type);

        assertThatThrownBy(() -> computeTrinoBucket(type, null, 1))
                .hasMessage("Unsupported type for 'bucket': %s", toTrinoType(type, TYPE_MANAGER));
    }

    public static Object[][] unsupportedBucketingTypes()
    {
        return new Object[][] {
                {BooleanType.get()},
                {FloatType.get()},
                {DoubleType.get()},
        };
    }

    private void assertBucketAndHashEquals(String archerTypeName, Object archerValue, Integer expectedHash)
    {
        Type archerType = Types.fromPrimitiveString(archerTypeName);
        if (archerValue != null && archerType.typeId() == DECIMAL) {
            archerValue = new BigDecimal((String) archerValue).setScale(((DecimalType) archerType).scale());
        }

        assertBucketEquals(archerType, archerValue);
        assertHashEquals(archerType, archerValue, expectedHash);
    }

    private void assertBucketEquals(Type archerType, Object archerValue)
    {
        assertBucketNumberEquals(archerType, archerValue, Integer.MAX_VALUE);

        assertBucketNumberEquals(archerType, archerValue, 2);
        assertBucketNumberEquals(archerType, archerValue, 7);
        assertBucketNumberEquals(archerType, archerValue, 31);
        assertBucketNumberEquals(archerType, archerValue, 32);
        assertBucketNumberEquals(archerType, archerValue, 100);
        assertBucketNumberEquals(archerType, archerValue, 10000);
        assertBucketNumberEquals(archerType, archerValue, 524287); // prime number
        assertBucketNumberEquals(archerType, archerValue, 1 << 30);
    }

    private void assertBucketNumberEquals(Type archerType, Object archerValue, int bucketCount)
    {
        Integer archerBucket = computearcherBucket(archerType, archerValue, bucketCount);
        Integer trinoBucket = computeTrinoBucket(archerType, archerValue, bucketCount);

        assertThat(trinoBucket)
                .describedAs(format("archerType=%s, bucketCount=%s, archerBucket=%d, trinoBucket=%d;", archerType, bucketCount, archerBucket, trinoBucket))
                .isEqualTo(archerBucket);
    }

    private void assertHashEquals(Type archerType, Object archerValue, Integer expectedHash)
    {
        // In Iceberg, hash is 31-bit number (no sign), so computing bucket number for Integer.MAX_VALUE gives as back actual
        // hash value (except when hash equals Integer.MAX_VALUE).

        Integer archerBucketHash = computearcherBucket(archerType, archerValue, Integer.MAX_VALUE);
        Integer trinoBucketHash = computeTrinoBucket(archerType, archerValue, Integer.MAX_VALUE);

        // Ensure hash is stable and does not change
        assertThat(archerBucketHash)
                .describedAs(format("expected Iceberg %s(%s) bucket with %sd buckets to be %d, got %d", archerType, archerValue, Integer.MAX_VALUE, expectedHash, archerBucketHash))
                .isEqualTo(expectedHash);

        // Ensure hash is stable and does not change
        assertThat(trinoBucketHash)
                .describedAs(format("expected Trino %s(%s) bucket with %sd buckets to be %d, got %d", archerType, archerValue, Integer.MAX_VALUE, expectedHash, trinoBucketHash))
                .isEqualTo(expectedHash);
    }

    private Integer computearcherBucket(Type type, Object archerValue, int bucketCount)
    {
        Transform<Object, Integer> bucketTransform = Transforms.bucket(bucketCount);
        return bucketTransform.bind(type).apply(archerValue);
    }

    private Integer computeTrinoBucket(Type archerType, Object archerValue, int bucketCount)
    {
        io.trino.spi.type.Type trinoType = toTrinoType(archerType, TYPE_MANAGER);
        ColumnTransform transform = PartitionTransforms.bucket(trinoType, bucketCount);
        Function<Block, Block> blockTransform = transform.blockTransform();

        BlockBuilder blockBuilder = trinoType.createBlockBuilder(null, 1);

        Object trinoValue = toTrinoValue(archerType, archerValue);
        verify(trinoValue == null || Primitives.wrap(trinoType.getJavaType()).isInstance(trinoValue), "Unexpected value for %s: %s", trinoType, trinoValue != null ? trinoValue.getClass() : null);
        writeNativeValue(trinoType, blockBuilder, trinoValue);
        Block block = blockBuilder.build();

        Block bucketBlock = blockTransform.apply(block);
        verify(bucketBlock.getPositionCount() == 1);
        Integer trinoBucketWithBlock = bucketBlock.isNull(0) ? null : INTEGER.getInt(bucketBlock, 0);

        Long trinoBucketWithValue = (Long) transform.valueTransform().apply(block, 0);
        Integer trinoBucketWithValueAsInteger = trinoBucketWithValue == null ? null : toIntExact(trinoBucketWithValue);
        assertThat(trinoBucketWithValueAsInteger).isEqualTo(trinoBucketWithBlock);

        return trinoBucketWithBlock;
    }

    private static Object toTrinoValue(Type archerType, Object archerValue)
    {
        io.trino.spi.type.Type trinoType = toTrinoType(archerType, TYPE_MANAGER);

        if (archerValue == null) {
            return null;
        }

        if (trinoType == INTEGER) {
            return (long) (int) archerValue;
        }

        if (trinoType == BIGINT) {
            //noinspection RedundantCast
            return (long) archerValue;
        }

        if (trinoType instanceof io.trino.spi.type.DecimalType trinoDecimalType) {
            if (trinoDecimalType.isShort()) {
                return Decimals.encodeShortScaledValue((BigDecimal) archerValue, trinoDecimalType.getScale());
            }
            return Decimals.encodeScaledValue((BigDecimal) archerValue, trinoDecimalType.getScale());
        }

        if (trinoType == VARCHAR) {
            return utf8Slice((String) archerValue);
        }

        if (trinoType == VARBINARY) {
            return wrappedBuffer(((ByteBuffer) archerValue).array());
        }

        if (trinoType == UuidType.UUID) {
            UUID uuidValue = (UUID) archerValue;
            return castFromVarcharToUuid(utf8Slice(uuidValue.toString()));
        }

        if (trinoType == DATE) {
            return (long) (int) archerValue;
        }

        if (trinoType == TIME_MICROS) {
            return (long) archerValue * PICOSECONDS_PER_MICROSECOND;
        }

        if (trinoType == TIMESTAMP_MICROS) {
            //noinspection RedundantCast
            return (long) archerValue;
        }

        if (trinoType == TIMESTAMP_TZ_MICROS) {
            long epochMicros = (long) archerValue;
            return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                    floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND),
                    floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND,
                    UTC_KEY.getKey());
        }

        throw new UnsupportedOperationException("Unsupported type: " + trinoType);
    }
}
