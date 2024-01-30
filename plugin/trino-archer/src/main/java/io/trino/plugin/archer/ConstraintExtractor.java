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
import com.google.common.math.LongMath;
import io.airlift.slice.Slice;
import io.trino.plugin.archer.util.CIDRUtils;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import net.qihoo.archer.InvertedIndex;
import net.qihoo.archer.InvertedIndexType;
import net.qihoo.archer.index.InvertedIndexQuery;
import net.qihoo.archer.index.Literal;
import net.qihoo.archer.index.Set;
import net.qihoo.archer.index.UserInputAst;
import net.qihoo.archer.index.UserInputClause;
import net.qihoo.archer.index.UserInputLeaf;
import net.qihoo.archer.types.Types;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.expression.ConnectorExpressions.and;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractConjuncts;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ConstraintExtractor
{
    private ConstraintExtractor() {}

    public static ExtractionResult extractTupleDomain(Constraint constraint, Optional<InvertedIndex> invertedIndex)
    {
        TupleDomain<ColumnHandle> result = constraint.getSummary();
        ImmutableList.Builder<ConnectorExpression> remainingExpressions = ImmutableList.builder();
        ImmutableList.Builder<UserInputAst> userInputAstBuilder = ImmutableList.builder();
        for (ConnectorExpression conjunct : extractConjuncts(constraint.getExpression())) {
            // inverted index function
            AtomicBoolean containsAnyInvFunction = new AtomicBoolean(false);
            if (validateInvertedIndexFunction(conjunct, containsAnyInvFunction)) {
                checkArgument(invertedIndex.isPresent(), "Try to use inverted index function on none inverted indexed table");
                UserInputAst userInputAst = extractUserInputAst((Call) conjunct, invertedIndex.get());
                userInputAstBuilder.add(userInputAst);
                continue;
            }
            else if (containsAnyInvFunction.get()) {
                throw new IllegalArgumentException("Cannot pushdown all inverted index function together, inverted index function cannot combined with none inverted function predicate with 'or'");
            }

            Optional<TupleDomain<ColumnHandle>> converted = toTupleDomain(conjunct, constraint.getAssignments());
            if (converted.isEmpty()) {
                remainingExpressions.add(conjunct);
            }
            else {
                result = result.intersect(converted.get());
                if (result.isNone()) {
                    return new ExtractionResult(TupleDomain.none(), Constant.TRUE, Optional.empty());
                }
            }
        }

        List<UserInputAst> userInputAstList = userInputAstBuilder.build();
        if (userInputAstList.isEmpty()) {
            return new ExtractionResult(result, and(remainingExpressions.build()), Optional.empty());
        }
        else {
            Optional<InvertedIndexQuery> query;
            if (userInputAstList.size() == 1) {
                query = Optional.of(new InvertedIndexQuery(userInputAstList.get(0), true));
            }
            else {
                UserInputAst combined = andUserInputAstList(userInputAstList);
                query = Optional.of(new InvertedIndexQuery(combined, true));
            }
            return new ExtractionResult(result, and(remainingExpressions.build()), query);
        }
    }

    private static UserInputAst extractUserInputAst(Call call, InvertedIndex invertedIndex)
    {
        String functionName = call.getFunctionName().getName();
        if (functionName.startsWith("inv_match")) {
            return extractMatchFunction(call, invertedIndex);
        }
        else {
            if (functionName.equals(NOT_FUNCTION_NAME.getName())) {
                return extractNotFunction(call.getArguments(), invertedIndex);
            }
            else if (functionName.equals(AND_FUNCTION_NAME.getName())) {
                return extractAndFunction(call.getArguments(), invertedIndex);
            }
            else if (functionName.equals(OR_FUNCTION_NAME.getName())) {
                return extractOrFunction(call.getArguments(), invertedIndex);
            }
            else {
                throw new IllegalArgumentException("Unknown inverted index logic function: " + functionName);
            }
        }
    }

    // ALL but not
    private static UserInputAst extractNotFunction(List<ConnectorExpression> arguments, InvertedIndex invertedIndex)
    {
        ImmutableList.Builder<UserInputClause.Entry> entries = ImmutableList.builder();
        entries.add(UserInputClause.Entry.of(UserInputClause.Occur.MUST, UserInputAst.leaf(UserInputLeaf.all())));
        for (ConnectorExpression argument : arguments) {
            if (argument instanceof Call call) {
                entries.add(UserInputClause.Entry.of(UserInputClause.Occur.MUST_NOT, extractUserInputAst(call, invertedIndex)));
            }
            else {
                throw new IllegalArgumentException("Inverted index logic function argument should be inverted index function, but is " + argument);
            }
        }
        return UserInputAst.clause(UserInputClause.of(entries.build()));
    }

    private static List<UserInputClause.Entry> trySimplifyAnd(List<UserInputClause.Entry> entries)
    {
        int allMust = 0;
        int notAllMust = 0;
        for (UserInputClause.Entry entry : entries) {
            if (entry.occur().isMust()) {
                if (entry.userInputAst().isLeaf() && entry.userInputAst().leaf().isAll()) {
                    allMust++;
                }
                else {
                    notAllMust++;
                }
            }
        }
        if (allMust == 0 || (allMust == 1 && notAllMust == 0)) {
            return entries;
        }

        ImmutableList.Builder<UserInputClause.Entry> builder = ImmutableList.builder();

        // keep only '1' MUST_ALL if there is no MUST_NOT_ALL
        // keep '0' MUST_ALL if there is MUST_NOT_ALL
        int removeCount = notAllMust > 0 ? allMust : allMust - 1;
        for (UserInputClause.Entry entry : entries) {
            if (removeCount > 0 && entry.userInputAst().isLeaf() && entry.userInputAst().leaf().isAll()) {
                removeCount--;
                continue;
            }
            builder.add(entry);
        }

        return builder.build();
    }

    private static boolean canMerge(UserInputAst userInputAst)
    {
        return userInputAst.isClause() && userInputAst.clause().entries().stream().noneMatch(entry -> entry.occur().isShould());
    }

    public static UserInputAst andUserInputAstList(List<UserInputAst> inputs)
    {
        ImmutableList.Builder<UserInputClause.Entry> builder = ImmutableList.builder();
        boolean merge = false;
        for (UserInputAst ast : inputs) {
            if (canMerge(ast)) {
                merge = true;
                for (UserInputClause.Entry entry : ast.clause().entries()) {
                    builder.add(entry);
                }
            }
            else {
                builder.add(UserInputClause.Entry.of(UserInputClause.Occur.MUST, ast));
            }
        }
        List<UserInputClause.Entry> entries = builder.build();
        if (merge) {
            entries = trySimplifyAnd(entries);
        }
        return UserInputAst.clause(UserInputClause.of(entries));
    }

    private static UserInputAst extractAndFunction(List<ConnectorExpression> arguments, InvertedIndex invertedIndex)
    {
        ImmutableList.Builder<UserInputAst> builder = ImmutableList.builder();
        for (ConnectorExpression argument : arguments) {
            if (argument instanceof Call call) {
                UserInputAst userInputAst = extractUserInputAst(call, invertedIndex);
                builder.add(userInputAst);
            }
            else {
                throw new IllegalArgumentException("Inverted index logic function argument should be inverted index function, but is " + argument);
            }
        }
        return andUserInputAstList(builder.build());
    }

    private static UserInputAst extractOrFunction(List<ConnectorExpression> arguments, InvertedIndex invertedIndex)
    {
        ImmutableList.Builder<UserInputClause.Entry> entries = ImmutableList.builder();
        for (ConnectorExpression argument : arguments) {
            if (argument instanceof Call call) {
                entries.add(UserInputClause.Entry.of(UserInputClause.Occur.SHOULD, extractUserInputAst(call, invertedIndex)));
            }
            else {
                throw new IllegalArgumentException("Inverted index logic function argument should be inverted index function, but is " + argument);
            }
        }
        return UserInputAst.clause(UserInputClause.of(entries.build()));
    }

    private static UserInputAst extractMatchFunction(Call call, InvertedIndex invertedIndex)
    {
        String functionName = call.getFunctionName().getName();
        checkArgument(functionName.startsWith("inv_match"), "Not inverted index function");
        List<ConnectorExpression> arguments = call.getArguments();

        switch (functionName) {
            case "inv_match_index" -> {
                return match(arguments, invertedIndex, true);
            }
            case "inv_match" -> {
                return match(arguments, invertedIndex, false);
            }
            case "inv_match_index_text_slop" -> {
                return matchTextSlop(arguments, invertedIndex, true);
            }
            case "inv_match_text_slop" -> {
                return matchTextSlop(arguments, invertedIndex, false);
            }
            case "inv_match_index_text_prefix" -> {
                return matchTextPrefix(arguments, invertedIndex, true);
            }
            case "inv_match_text_prefix" -> {
                return matchTextPrefix(arguments, invertedIndex, false);
            }
            case "inv_match_index_json" -> {
                return matchJson(arguments, invertedIndex, true);
            }
            case "inv_match_json" -> {
                return matchJson(arguments, invertedIndex, false);
            }
            case "inv_match_index_json_slop" -> {
                return matchJsonSlop(arguments, invertedIndex, true);
            }
            case "inv_match_json_slop" -> {
                return matchJsonSlop(arguments, invertedIndex, false);
            }
            case "inv_match_index_json_prefix" -> {
                return matchJsonPrefix(arguments, invertedIndex, true);
            }
            case "inv_match_json_prefix" -> {
                return matchJsonPrefix(arguments, invertedIndex, false);
            }
            case "inv_match_index_in_set" -> {
                return matchInSet(arguments, invertedIndex, true);
            }
            case "inv_match_in_set" -> {
                return matchInSet(arguments, invertedIndex, false);
            }
            case "inv_match_index_json_in_set" -> {
                return matchJsonInSet(arguments, invertedIndex, true);
            }
            case "inv_match_json_in_set" -> {
                return matchJsonInSet(arguments, invertedIndex, false);
            }
            case "inv_match_index_gt" -> {
                return matchCompare(arguments, invertedIndex, true, false, false);
            }
            case "inv_match_gt" -> {
                return matchCompare(arguments, invertedIndex, false, false, false);
            }
            case "inv_match_index_gte" -> {
                return matchCompare(arguments, invertedIndex, true, false, true);
            }
            case "inv_match_gte" -> {
                return matchCompare(arguments, invertedIndex, false, false, true);
            }
            case "inv_match_index_lt" -> {
                return matchCompare(arguments, invertedIndex, true, true, false);
            }
            case "inv_match_lt" -> {
                return matchCompare(arguments, invertedIndex, false, true, false);
            }
            case "inv_match_index_lte" -> {
                return matchCompare(arguments, invertedIndex, true, true, true);
            }
            case "inv_match_lte" -> {
                return matchCompare(arguments, invertedIndex, false, true, true);
            }
            case "inv_match_index_between" -> {
                return matchBetween(arguments, invertedIndex, true);
            }
            case "inv_match_between" -> {
                return matchBetween(arguments, invertedIndex, false);
            }
            case "inv_match_index_network" -> {
                return matchNetwork(arguments, invertedIndex, true);
            }
            case "inv_match_network" -> {
                return matchNetwork(arguments, invertedIndex, false);
            }
            default -> {
                throw new IllegalArgumentException("Unknown inverted index match function: " + functionName);
            }
        }
    }

    private static net.qihoo.archer.InvertedIndexField getInvertedIndexField(List<ConnectorExpression> arguments, boolean specifyIndex, InvertedIndex invertedIndex)
    {
        ConnectorExpression columnNameExpr = arguments.get(0);
        ConnectorExpression indexNameExpr = specifyIndex ? arguments.get(1) : null;
        if (!(columnNameExpr instanceof Variable variable)) {
            throw new IllegalArgumentException("Inverted index match function first argument should be a column");
        }
        String columnName = variable.getName();
        Types.NestedField field = invertedIndex.schema().findField(columnName);
        checkArgument(field != null, "Not find column: " + columnName);

        String indexName;
        if (indexNameExpr == null) {
            indexName = columnName;
        }
        else {
            if (!(indexNameExpr instanceof Constant constant)) {
                throw new IllegalArgumentException("Inverted index match function second argument is index name, and should be a constant varchar");
            }
            checkArgument(constant.getType().getDisplayName().equals("varchar") && constant.getValue() != null,
                    "Inverted index name should be a none null constant varchar");
            indexName = ((Slice) constant.getValue()).toStringUtf8();
        }
        indexName = indexName.toLowerCase(ENGLISH);
        net.qihoo.archer.InvertedIndexField indexField = invertedIndex.findField(field.fieldId(), indexName);
        checkArgument(indexField != null, "Not find index '" + indexName + "' for column: " + columnName);
        return indexField;
    }

    private static String getVarcharArgument(List<ConnectorExpression> arguments, int idx)
    {
        checkArgument(arguments != null, "Arguments is null");
        checkArgument(arguments.size() > idx, "Argument index " + idx + " out range of arguments: " + arguments.size());
        ConnectorExpression arg = arguments.get(idx);
        if (!(arg instanceof Constant constant)) {
            throw new IllegalArgumentException("Argument is not a constant value");
        }
        checkArgument(constant.getValue() != null, "Constant value is null");
        checkArgument(constant.getType().getDisplayName().equals("varchar"), "Constant value is not varchar type");
        return ((Slice) constant.getValue()).toStringUtf8();
    }

    private static boolean getBooleanArgument(List<ConnectorExpression> arguments, int idx)
    {
        checkArgument(arguments != null, "Arguments is null");
        checkArgument(arguments.size() > idx, "Argument index " + idx + " out range of arguments: " + arguments.size());
        ConnectorExpression arg = arguments.get(idx);
        if (!(arg instanceof Constant constant)) {
            throw new IllegalArgumentException("Argument is not a constant value");
        }
        checkArgument(constant.getValue() != null, "Constant value is null");
        checkArgument(constant.getType().getDisplayName().equals(StandardTypes.BOOLEAN), "Constant value is not boolean type");
        return (boolean) constant.getValue();
    }

    private static int getIntegerArgument(List<ConnectorExpression> arguments, int idx)
    {
        checkArgument(arguments != null, "Arguments is null");
        checkArgument(arguments.size() > idx, "Argument index " + idx + " out range of arguments: " + arguments.size());
        ConnectorExpression arg = arguments.get(idx);
        if (!(arg instanceof Constant constant)) {
            throw new IllegalArgumentException("Argument is not a constant value");
        }
        checkArgument(constant.getValue() != null, "Constant value is null");
        checkArgument(constant.getType().getDisplayName().equals("integer"), "Constant value is not integer type");
        return toIntExact((long) constant.getValue());
    }

    private static List<String> getVarcharArrayArgument(List<ConnectorExpression> arguments, int idx)
    {
        checkArgument(arguments != null, "Arguments is null");
        checkArgument(arguments.size() > idx, "Argument index " + idx + " out range of arguments: " + arguments.size());
        ConnectorExpression arg = arguments.get(idx);
        if (!(arg instanceof Constant constant)) {
            throw new IllegalArgumentException("Argument is not a constant value");
        }
        checkArgument(constant.getValue() != null, "Constant value is null");
        checkArgument(constant.getType().getDisplayName().equals("array(varchar)"), "Constant value is not array(varchar) type");

        Block block = (Block) constant.getValue();
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        int count = block.getPositionCount();
        for (int i = 0; i < count; i++) {
            checkArgument(!block.isNull(i), "array element is null");
            builder.add(VARCHAR.getSlice(block, i).toStringUtf8());
        }
        return builder.build();
    }

    private static UserInputAst match(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int idx = arguments.size() - 1;
        String phrase = getVarcharArgument(arguments, idx);
        Literal literal = new Literal(fieldId, phrase);
        return UserInputAst.leaf(UserInputLeaf.literal(literal));
    }

    private static UserInputAst matchTextPrefix(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int idx = arguments.size() - 1;
        String phrase = getVarcharArgument(arguments, idx);
        Literal literal = new Literal(fieldId, phrase, true);
        return UserInputAst.leaf(UserInputLeaf.literal(literal));
    }

    private static UserInputAst matchTextSlop(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int phraseIdx = arguments.size() - 2;
        int slopIdx = arguments.size() - 1;
        String phrase = getVarcharArgument(arguments, phraseIdx);
        int slop = getIntegerArgument(arguments, slopIdx);
        Literal literal = new Literal(fieldId, phrase, slop);
        return UserInputAst.leaf(UserInputLeaf.literal(literal));
    }

    private static UserInputAst matchJson(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int jsonPathIdx = arguments.size() - 2;
        int phraseIdx = arguments.size() - 1;
        String jsonPath = getVarcharArgument(arguments, jsonPathIdx);
        checkArgument(!jsonPath.isEmpty(), "Json path is empty");
        String phrase = getVarcharArgument(arguments, phraseIdx);
        Literal literal = new Literal(fieldId, jsonPath, phrase);
        return UserInputAst.leaf(UserInputLeaf.literal(literal));
    }

    private static UserInputAst matchJsonSlop(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int jsonPathIdx = arguments.size() - 3;
        int phraseIdx = arguments.size() - 2;
        int slopIdx = arguments.size() - 1;
        String jsonPath = getVarcharArgument(arguments, jsonPathIdx);
        checkArgument(!jsonPath.isEmpty(), "Json path is empty");
        String phrase = getVarcharArgument(arguments, phraseIdx);
        int slop = getIntegerArgument(arguments, slopIdx);
        Literal literal = new Literal(fieldId, jsonPath, phrase, slop);
        return UserInputAst.leaf(UserInputLeaf.literal(literal));
    }

    private static UserInputAst matchJsonPrefix(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int jsonPathIdx = arguments.size() - 2;
        int phraseIdx = arguments.size() - 1;
        String jsonPath = getVarcharArgument(arguments, jsonPathIdx);
        checkArgument(!jsonPath.isEmpty(), "Json path is empty");
        String phrase = getVarcharArgument(arguments, phraseIdx);
        Literal literal = new Literal(fieldId, jsonPath, phrase, true);
        return UserInputAst.leaf(UserInputLeaf.literal(literal));
    }

    private static UserInputAst matchInSet(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int setIdx = arguments.size() - 1;
        List<String> elements = getVarcharArrayArgument(arguments, setIdx);
        Set set = Set.of(fieldId, elements);
        return UserInputAst.leaf(UserInputLeaf.set(set));
    }

    private static UserInputAst matchJsonInSet(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int jsonPathIdx = arguments.size() - 2;
        int setIdx = arguments.size() - 1;
        String jsonPath = getVarcharArgument(arguments, jsonPathIdx);
        checkArgument(!jsonPath.isEmpty(), "Json path is empty");
        List<String> elements = getVarcharArrayArgument(arguments, setIdx);
        List<UserInputClause.Entry> entries = elements.stream().map(token -> {
            Literal literal = new Literal(fieldId, jsonPath, token);
            return UserInputClause.Entry.of(UserInputClause.Occur.SHOULD, UserInputAst.leaf(UserInputLeaf.literal(literal)));
        }).toList();
        return UserInputAst.clause(UserInputClause.of(entries));
    }

    private static UserInputAst matchCompare(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex, boolean less, boolean inclusive)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int valueIdx = arguments.size() - 1;
        String value = getVarcharArgument(arguments, valueIdx);
        checkArgument(!value.isEmpty(), "compare value is empty");

        net.qihoo.archer.index.Range range;
        net.qihoo.archer.index.Range.UserInputBound unbounded = net.qihoo.archer.index.Range.UserInputBound.unbounded();
        if (less) {
            if (inclusive) {
                range = new net.qihoo.archer.index.Range(fieldId, unbounded, net.qihoo.archer.index.Range.UserInputBound.inclusive(value));
            }
            else {
                range = new net.qihoo.archer.index.Range(fieldId, unbounded, net.qihoo.archer.index.Range.UserInputBound.exclusive(value));
            }
        }
        else {
            if (inclusive) {
                range = new net.qihoo.archer.index.Range(fieldId, net.qihoo.archer.index.Range.UserInputBound.inclusive(value), unbounded);
            }
            else {
                range = new net.qihoo.archer.index.Range(fieldId, net.qihoo.archer.index.Range.UserInputBound.exclusive(value), unbounded);
            }
        }
        return UserInputAst.leaf(UserInputLeaf.range(range));
    }

    private static UserInputAst matchBetween(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        int fieldId = getInvertedIndexField(arguments, specifyIndex, invertedIndex).fieldId();
        int size = arguments.size();
        String lowerValue = getVarcharArgument(arguments, size - 4);
        checkArgument(!lowerValue.isEmpty(), "lower value is empty");
        boolean lowerInclusive = getBooleanArgument(arguments, size - 3);
        String upperValue = getVarcharArgument(arguments, size - 2);
        checkArgument(!upperValue.isEmpty(), "upper value is empty");
        boolean upperInclusive = getBooleanArgument(arguments, size - 1);

        net.qihoo.archer.index.Range.UserInputBound lowerBound = lowerInclusive ?
                net.qihoo.archer.index.Range.UserInputBound.inclusive(lowerValue) :
                net.qihoo.archer.index.Range.UserInputBound.exclusive(lowerValue);

        net.qihoo.archer.index.Range.UserInputBound upperBound = upperInclusive ?
                net.qihoo.archer.index.Range.UserInputBound.inclusive(upperValue) :
                net.qihoo.archer.index.Range.UserInputBound.exclusive(upperValue);

        net.qihoo.archer.index.Range range = new net.qihoo.archer.index.Range(fieldId, lowerBound, upperBound);
        return UserInputAst.leaf(UserInputLeaf.range(range));
    }

    private static UserInputAst matchNetwork(List<ConnectorExpression> arguments, InvertedIndex invertedIndex, boolean specifyIndex)
    {
        net.qihoo.archer.InvertedIndexField indexField = getInvertedIndexField(arguments, specifyIndex, invertedIndex);
        checkArgument(indexField.type() == InvertedIndexType.IP, "Only Ip index support inv_match_network function");
        int fieldId = indexField.fieldId();
        int size = arguments.size();

        String network = getVarcharArgument(arguments, size - 2);
        boolean inclusiveHostCount = getBooleanArgument(arguments, size - 1);

        CIDRUtils utils = new CIDRUtils(network);
        String lowerValue = utils.getNetworkAddress();
        String upperValue = utils.getBroadcastAddress();

        net.qihoo.archer.index.Range.UserInputBound lowerBound = inclusiveHostCount ?
                net.qihoo.archer.index.Range.UserInputBound.inclusive(lowerValue) :
                net.qihoo.archer.index.Range.UserInputBound.exclusive(lowerValue);

        net.qihoo.archer.index.Range.UserInputBound upperBound = inclusiveHostCount ?
                net.qihoo.archer.index.Range.UserInputBound.inclusive(upperValue) :
                net.qihoo.archer.index.Range.UserInputBound.exclusive(upperValue);

        net.qihoo.archer.index.Range range = new net.qihoo.archer.index.Range(fieldId, lowerBound, upperBound);
        return UserInputAst.leaf(UserInputLeaf.range(range));
    }

    private static boolean validateInvertedIndexFunction(ConnectorExpression expression, AtomicBoolean containsAnyInvFunction)
    {
        if (expression instanceof Call call) {
            String name = call.getFunctionName().getName();
            if (NOT_FUNCTION_NAME.getName().equals(name) || AND_FUNCTION_NAME.getName().equals(name) || OR_FUNCTION_NAME.getName().equals(name)) {
                boolean all = true;
                for (ConnectorExpression connectorExpression : call.getArguments()) {
                    boolean thisAll = validateInvertedIndexFunction(connectorExpression, containsAnyInvFunction);
                    if (!thisAll && containsAnyInvFunction.get()) {
                        return false;
                    }
                    all &= thisAll;
                }
                return all;
            }
            else if (name.startsWith("inv_match")) {
                containsAnyInvFunction.set(true);
                return true;
            }
            else {
                for (ConnectorExpression connectorExpression : call.getArguments()) {
                    validateInvertedIndexFunction(connectorExpression, containsAnyInvFunction);
                }
                return false;
            }
        }
        else {
            return false;
        }
    }

    private static Optional<TupleDomain<ColumnHandle>> toTupleDomain(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (expression instanceof Call call) {
            return toTupleDomain(call, assignments);
        }
        return Optional.empty();
    }

    private static Optional<TupleDomain<ColumnHandle>> toTupleDomain(Call call, Map<String, ColumnHandle> assignments)
    {
        if (call.getArguments().size() == 2) {
            ConnectorExpression firstArgument = call.getArguments().get(0);
            ConnectorExpression secondArgument = call.getArguments().get(1);

            // Note: CanonicalizeExpressionRewriter ensures that constants are the second comparison argument.

            if (firstArgument instanceof Call firstAsCall && firstAsCall.getFunctionName().equals(CAST_FUNCTION_NAME) &&
                    secondArgument instanceof Constant constant &&
                    // if type do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapCastInComparison(
                        call.getFunctionName(),
                        getOnlyElement(firstAsCall.getArguments()),
                        constant,
                        assignments);
            }

            if (firstArgument instanceof Call firstAsCall &&
                    firstAsCall.getFunctionName().equals(new FunctionName("date_trunc")) && firstAsCall.getArguments().size() == 2 &&
                    firstAsCall.getArguments().get(0) instanceof Constant unit &&
                    secondArgument instanceof Constant constant &&
                    // if type do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapDateTruncInComparison(
                        call.getFunctionName(),
                        unit,
                        firstAsCall.getArguments().get(1),
                        constant,
                        assignments);
            }

            if (firstArgument instanceof Call firstAsCall &&
                    firstAsCall.getFunctionName().equals(new FunctionName("year")) &&
                    firstAsCall.getArguments().size() == 1 &&
                    getOnlyElement(firstAsCall.getArguments()).getType() instanceof TimestampWithTimeZoneType &&
                    secondArgument instanceof Constant constant &&
                    // if types do no match, this cannot be a comparison function
                    firstArgument.getType().equals(secondArgument.getType())) {
                return unwrapYearInTimestampTzComparison(
                        call.getFunctionName(),
                        getOnlyElement(firstAsCall.getArguments()),
                        constant,
                        assignments);
            }
        }

        return Optional.empty();
    }

    private static Optional<TupleDomain<ColumnHandle>> unwrapCastInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression castSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(castSource instanceof Variable sourceVariable)) {
            // Engine unwraps casts in comparisons in UnwrapCastInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        ColumnHandle column = resolve(sourceVariable, assignments);
        if (sourceVariable.getType() instanceof TimestampWithTimeZoneType columnType) {
            if (constant.getType() == DateType.DATE) {
                return unwrapTimestampTzToDateCast(columnType, functionName, (long) constant.getValue())
                        .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
            }
            // TODO support timestamp constant
        }

        return Optional.empty();
    }

    private static Optional<Domain> unwrapTimestampTzToDateCast(Type columnType, FunctionName functionName, long date)
    {
        // Verify no overflow. Date values must be in integer range.
        verify(date <= Integer.MAX_VALUE, "Date value out of range: %s", date);

        Object startOfDate;
        Object startOfNextDate;
        int precision = ((TimestampWithTimeZoneType) columnType).getPrecision();
        if (precision <= MAX_SHORT_PRECISION) {
            startOfDate = packDateTimeWithZone(date * MILLISECONDS_PER_DAY, UTC_KEY);
            startOfNextDate = packDateTimeWithZone((date + 1) * MILLISECONDS_PER_DAY, UTC_KEY);
        }
        else {
            startOfDate = LongTimestampWithTimeZone.fromEpochMillisAndFraction(date * MILLISECONDS_PER_DAY, 0, UTC_KEY);
            startOfNextDate = LongTimestampWithTimeZone.fromEpochMillisAndFraction((date + 1) * MILLISECONDS_PER_DAY, 0, UTC_KEY);
        }

        return createDomain(functionName, columnType, startOfDate, startOfNextDate);
    }

    private static Optional<Domain> unwrapYearInTimestampTzComparison(FunctionName functionName, Type type, Constant constant)
    {
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);

        int year = toIntExact((Long) constant.getValue());
        ZonedDateTime periodStart = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, UTC);
        ZonedDateTime periodEnd = periodStart.plusYears(1);

        Object start;
        Object end;
        int precision = ((TimestampWithTimeZoneType) type).getPrecision();
        if (precision <= MAX_SHORT_PRECISION) {
            start = packDateTimeWithZone(periodStart.toEpochSecond() * MILLISECONDS_PER_SECOND, UTC_KEY);
            end = packDateTimeWithZone(periodEnd.toEpochSecond() * MILLISECONDS_PER_SECOND, UTC_KEY);
        }
        else {
            start = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodStart.toEpochSecond(), 0, UTC_KEY);
            end = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodEnd.toEpochSecond(), 0, UTC_KEY);
        }

        return createDomain(functionName, type, start, end);
    }

    private static Optional<Domain> createDomain(FunctionName functionName, Type type, Object startOfDate, Object startOfNextDate)
    {
        if (functionName.equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, startOfDate, true, startOfNextDate, false)), false));
        }
        if (functionName.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, startOfDate), Range.greaterThanOrEqual(type, startOfNextDate)), false));
        }
        if (functionName.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, startOfDate)), false));
        }
        if (functionName.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, startOfNextDate)), false));
        }
        if (functionName.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, startOfNextDate)), false));
        }
        if (functionName.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, startOfDate)), false));
        }
        if (functionName.equals(IDENTICAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, startOfDate, true, startOfNextDate, false)), true));
        }

        return Optional.empty();
    }

    private static Optional<TupleDomain<ColumnHandle>> unwrapDateTruncInComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            Constant unit,
            ConnectorExpression dateTruncSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(dateTruncSource instanceof Variable sourceVariable)) {
            // Engine unwraps date_trunc in comparisons in UnwrapDateTruncInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (unit.getValue() == null) {
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        ColumnHandle column = resolve(sourceVariable, assignments);
        if (sourceVariable.getType() instanceof TimestampWithTimeZoneType type) {
            verify(constant.getType().equals(type), "This method should not be invoked when type mismatch (i.e. surely not a comparison)");

            return unwrapDateTruncInComparison(((Slice) unit.getValue()).toStringUtf8(), functionName, constant)
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        }

        return Optional.empty();
    }

    private static Optional<Domain> unwrapDateTruncInComparison(String unit, FunctionName functionName, Constant constant)
    {
        Type type = constant.getType();
        checkArgument(constant.getValue() != null, "Unexpected constant: %s", constant);

        ZonedDateTime dateTime;
        int precision = ((TimestampWithTimeZoneType) type).getPrecision();
        if (precision <= MAX_SHORT_PRECISION) {
            // Normalized to UTC because for comparisons the zone is irrelevant
            dateTime = Instant.ofEpochMilli(unpackMillisUtc((long) constant.getValue()))
                    .atZone(UTC);
        }
        else {
            if (precision > 9) {
                return Optional.empty();
            }
            // Normalized to UTC because for comparisons the zone is irrelevant
            dateTime = Instant.ofEpochMilli(((LongTimestampWithTimeZone) constant.getValue()).getEpochMillis())
                    .plusNanos(LongMath.divide(((LongTimestampWithTimeZone) constant.getValue()).getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND, UNNECESSARY))
                    .atZone(UTC);
        }

        ZonedDateTime periodStart;
        ZonedDateTime nextPeriodStart;
        switch (unit.toLowerCase(ENGLISH)) {
            case "hour" -> {
                periodStart = ZonedDateTime.of(dateTime.toLocalDate(), LocalTime.of(dateTime.getHour(), 0), UTC);
                nextPeriodStart = periodStart.plusHours(1);
            }
            case "day" -> {
                periodStart = dateTime.toLocalDate().atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusDays(1);
            }
            case "month" -> {
                periodStart = dateTime.toLocalDate().withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusMonths(1);
            }
            case "year" -> {
                periodStart = dateTime.toLocalDate().withMonth(1).withDayOfMonth(1).atStartOfDay().atZone(UTC);
                nextPeriodStart = periodStart.plusYears(1);
            }
            default -> {
                return Optional.empty();
            }
        }
        boolean constantAtPeriodStart = dateTime.equals(periodStart);

        Object start;
        Object end;
        if (precision <= MAX_SHORT_PRECISION) {
            start = packDateTimeWithZone(periodStart.toEpochSecond() * MILLISECONDS_PER_SECOND, UTC_KEY);
            end = packDateTimeWithZone(nextPeriodStart.toEpochSecond() * MILLISECONDS_PER_SECOND, UTC_KEY);
        }
        else {
            start = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(periodStart.toEpochSecond(), 0, UTC_KEY);
            end = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(nextPeriodStart.toEpochSecond(), 0, UTC_KEY);
        }

        if (functionName.equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.none(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, start, true, end, false)), false));
        }
        if (functionName.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.notNull(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, start), Range.greaterThanOrEqual(type, end)), false));
        }
        if (functionName.equals(IDENTICAL_OPERATOR_FUNCTION_NAME)) {
            if (!constantAtPeriodStart) {
                return Optional.of(Domain.none(type));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, start, true, end, false)), true));
        }
        if (functionName.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            if (constantAtPeriodStart) {
                return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, start)), false));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, end)), false));
        }
        if (functionName.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, end)), false));
        }
        if (functionName.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, end)), false));
        }
        if (functionName.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            if (constantAtPeriodStart) {
                return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, start)), false));
            }
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, end)), false));
        }
        return Optional.empty();
    }

    private static Optional<TupleDomain<ColumnHandle>> unwrapYearInTimestampTzComparison(
            // upon invocation, we don't know if this really is a comparison
            FunctionName functionName,
            ConnectorExpression yearSource,
            Constant constant,
            Map<String, ColumnHandle> assignments)
    {
        if (!(yearSource instanceof Variable sourceVariable)) {
            // Engine unwraps year in comparisons in UnwrapYearInComparison. Within a connector we can do more than
            // engine only for source columns. We cannot draw many conclusions for intermediate expressions without
            // knowing them well.
            return Optional.empty();
        }

        if (constant.getValue() == null) {
            // Comparisons with NULL should be simplified by the engine
            return Optional.empty();
        }

        ColumnHandle column = resolve(sourceVariable, assignments);
        if (sourceVariable.getType() instanceof TimestampWithTimeZoneType type) {
            return unwrapYearInTimestampTzComparison(functionName, type, constant)
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        }

        return Optional.empty();
    }

    private static ColumnHandle resolve(Variable variable, Map<String, ColumnHandle> assignments)
    {
        ColumnHandle columnHandle = assignments.get(variable.getName());
        checkArgument(columnHandle != null, "No assignment for %s", variable);
        return columnHandle;
    }

    public record ExtractionResult(TupleDomain<ColumnHandle> tupleDomain, ConnectorExpression remainingExpression, Optional<InvertedIndexQuery> query)
    {
        public ExtractionResult
        {
            requireNonNull(tupleDomain, "tupleDomain is null");
            requireNonNull(remainingExpression, "remainingExpression is null");
            requireNonNull(query, "query is null");
        }
    }
}
