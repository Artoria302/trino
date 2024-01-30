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

import com.google.common.base.VerifyException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import net.qihoo.archer.expressions.Expression;
import net.qihoo.archer.expressions.Expressions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiFunction;

import static io.trino.plugin.archer.ArcherTypes.convertTrinoValueToArcher;
import static io.trino.plugin.hive.util.HiveUtil.isStructuralType;
import static java.lang.String.format;
import static net.qihoo.archer.expressions.Expressions.alwaysFalse;
import static net.qihoo.archer.expressions.Expressions.alwaysTrue;
import static net.qihoo.archer.expressions.Expressions.equal;
import static net.qihoo.archer.expressions.Expressions.greaterThan;
import static net.qihoo.archer.expressions.Expressions.greaterThanOrEqual;
import static net.qihoo.archer.expressions.Expressions.in;
import static net.qihoo.archer.expressions.Expressions.isNull;
import static net.qihoo.archer.expressions.Expressions.lessThan;
import static net.qihoo.archer.expressions.Expressions.lessThanOrEqual;
import static net.qihoo.archer.expressions.Expressions.not;

public final class ExpressionConverter
{
    private ExpressionConverter() {}

    public static boolean isConvertableToArcherExpression(Domain domain)
    {
        // structural types cannot be used to filter a table scan in Archer library.
        return !isStructuralType(domain.getType());
    }

    public static Expression toArcherExpression(TupleDomain<ArcherColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return alwaysTrue();
        }
        if (tupleDomain.getDomains().isEmpty()) {
            return alwaysFalse();
        }
        Map<ArcherColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Expression> conjuncts = new ArrayList<>();
        for (Map.Entry<ArcherColumnHandle, Domain> entry : domainMap.entrySet()) {
            ArcherColumnHandle columnHandle = entry.getKey();
            //checkArgument(!isMetadataColumnId(columnHandle.getId()), "Constraint on an unexpected column %s", columnHandle);
            Domain domain = entry.getValue();
            conjuncts.add(toArcherExpression(columnHandle.getQualifiedName(), columnHandle.getType(), domain));
        }
        return and(conjuncts);
    }

    private static Expression toArcherExpression(String columnName, Type type, Domain domain)
    {
        if (domain.isAll()) {
            return alwaysTrue();
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? alwaysTrue() : not(isNull(columnName));
        }

        // Skip structural types. TODO (https://github.com/trinodb/trino/issues/8759) Evaluate Apache Archer's support for predicate on structural types
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            // Fail fast. Ignoring expression could lead to data loss in case of deletions.
            throw new UnsupportedOperationException("Unsupported type for expression: " + type);
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> archerValues = new ArrayList<>();
            List<Expression> rangeExpressions = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    archerValues.add(convertTrinoValueToArcher(type, range.getLowBoundedValue()));
                }
                else {
                    rangeExpressions.add(toArcherExpression(columnName, range));
                }
            }
            Expression ranges = or(rangeExpressions);
            Expression values = archerValues.isEmpty() ? alwaysFalse() : in(columnName, archerValues);
            Expression nullExpression = domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();

            return or(nullExpression, or(values, ranges));
        }

        throw new VerifyException(format("Unsupported type %s with domain values %s", type, domain));
    }

    public static Expression toArcherExpression(String columnName, Range range)
    {
        Type type = range.getType();

        if (range.isSingleValue()) {
            Object archerValue = convertTrinoValueToArcher(type, range.getSingleValue());
            return equal(columnName, archerValue);
        }

        List<Expression> conjuncts = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            Object archerLow = convertTrinoValueToArcher(type, range.getLowBoundedValue());
            Expression lowBound;
            if (range.isLowInclusive()) {
                lowBound = greaterThanOrEqual(columnName, archerLow);
            }
            else {
                lowBound = greaterThan(columnName, archerLow);
            }
            conjuncts.add(lowBound);
        }

        if (!range.isHighUnbounded()) {
            Object archerHigh = convertTrinoValueToArcher(type, range.getHighBoundedValue());
            Expression highBound;
            if (range.isHighInclusive()) {
                highBound = lessThanOrEqual(columnName, archerHigh);
            }
            else {
                highBound = lessThan(columnName, archerHigh);
            }
            conjuncts.add(highBound);
        }

        return and(conjuncts);
    }

    private static Expression and(List<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return alwaysTrue();
        }
        return combine(expressions, Expressions::and);
    }

    private static Expression or(Expression left, Expression right)
    {
        return Expressions.or(left, right);
    }

    private static Expression or(List<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return alwaysFalse();
        }
        return combine(expressions, Expressions::or);
    }

    private static Expression combine(List<Expression> expressions, BiFunction<Expression, Expression, Expression> combiner)
    {
        // Build balanced tree that preserves the evaluation order of the input expressions.
        //
        // The tree is built bottom up by combining pairs of elements into binary expressions.
        //
        // Example:
        //
        // Initial state:
        //  a b c d e
        //
        // First iteration:
        //
        //  /\    /\   e
        // a  b  c  d
        //
        // Second iteration:
        //
        //    / \    e
        //  /\   /\
        // a  b c  d
        //
        //
        // Last iteration:
        //
        //      / \
        //    / \  e
        //  /\   /\
        // a  b c  d

        Queue<Expression> queue = new ArrayDeque<>(expressions);
        while (queue.size() > 1) {
            Queue<Expression> buffer = new ArrayDeque<>();

            // combine pairs of elements
            while (queue.size() >= 2) {
                buffer.add(combiner.apply(queue.remove(), queue.remove()));
            }

            // if there's and odd number of elements, just append the last one
            if (!queue.isEmpty()) {
                buffer.add(queue.remove());
            }

            // continue processing the pairs that were just built
            queue = buffer;
        }

        return queue.remove();
    }
}
