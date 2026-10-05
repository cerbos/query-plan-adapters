/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.exceptUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.ternaryUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unmapped;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsafeExplicitNullComparison;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates leaf comparisons: one document field against one literal.
 */
final class LeafTranslator {

    /** Operators whose literal must be a scalar: term and range queries take one value. */
    private static final Set<String> SCALAR_OPERAND_OPERATORS = Set.of(
            "eq", "ne", "lt", "gt", "le", "ge", "contains", "startsWith", "endsWith", "matches");

    private static final Set<String> STRING_OPERATORS =
            Set.of("contains", "startsWith", "endsWith", "matches");

    /** The negation of each ordering operator. */
    private static final Map<String, String> NEGATED_RANGE = Map.of(
            "lt", "ge", "le", "gt", "gt", "le", "ge", "lt");

    private sealed interface ResolvedOperand {
        record Field(String variable) implements ResolvedOperand {}
        record Literal(Object value) implements ResolvedOperand {}
    }

    private record Comparison(String variable, Object value, boolean variableFirst) {}

    private final Options options;

    LeafTranslator(Options options) {
        this.options = options;
    }

    Map<String, Object> applyResolvedLeaf(
            String operator, List<Operand> operands, Scope scope, Polarity polarity) {
        boolean whenTrue = polarity.holds();
        if (operands.size() != 2) {
            throw malformed(operator + " requires exactly 2 operands, got " + operands.size());
        }
        Map<String, Object> rewritten = rewrittenLeaf(operator, operands, scope, polarity);
        if (rewritten != null) {
            return rewritten;
        }
        Comparison comparison = resolveComparison(operands.get(0), operands.get(1));
        String field = scope.field(comparison.variable());
        Object value = comparison.value();
        boolean variableFirst = comparison.variableFirst();

        String normalizedOperator = normalizeLeafOperator(operator, variableFirst);
        if ("in".equals(normalizedOperator) && !variableFirst && isObjectPath(field)) {
            // CEL's `in` over a map tests its keys. An object field's key set is not indexed:
            // Elasticsearch indexes no JSON null, so a key held with a null value is
            // indistinguishable from an absent key, and no query answers the membership.
            throw unsupported("in over the object field '" + field + "' tests its keys, as CEL"
                    + " does over a map, and Elasticsearch indexes no key whose value is null,"
                    + " so a query cannot tell a key held with a null from an absent one");
        }
        if (!whenTrue && "in".equals(normalizedOperator) && !variableFirst) {
            throw unsupported(
                    "Negated membership in a document collection cannot distinguish a missing "
                            + "collection from an empty collection in Elasticsearch");
        }
        rejectNonScalarOperand(normalizedOperator, value, variableFirst);
        if (value == null) {
            return nullLeafQuery(normalizedOperator, field, variableFirst, whenTrue);
        }
        Map<String, Object> typeMismatch =
                typeMismatchQuery(normalizedOperator, field, value, operands, whenTrue);
        if (typeMismatch != null) {
            return typeMismatch;
        }
        if ("in".equals(normalizedOperator) && variableFirst && value instanceof List<?> values) {
            List<?> members = typedMembers(field, values, operands);
            if (members.isEmpty()) {
                // No element has the field's type, so CEL's `in` is false wherever the field is
                // present.
                return whenTrue ? Queries.matchNone() : Queries.exists(field);
            }
            value = members;
        }
        if (!variableFirst && "in".equals(normalizedOperator)
                && !options.operatorOverrides().containsKey("in")
                && !inhabits(declaredElementType(field, "in", operands), value)) {
            // A value of the wrong type is never an element. The negated form was refused above.
            return Queries.matchNone();
        }
        if ("hasIntersection".equals(normalizedOperator) && value instanceof List<?> values) {
            rejectNullIntersection(values);
            if (!options.operatorOverrides().containsKey("hasIntersection")) {
                List<?> members = typedIntersection(field, values, operands);
                if (members.isEmpty()) {
                    return Queries.matchNone();
                }
                value = members;
            }
        }
        if ("in".equals(normalizedOperator) && value instanceof List<?> values
                && values.stream().anyMatch(Objects::isNull)) {
            return nullAwareMembershipQuery(field, values, whenTrue);
        }

        Map<String, Object> positive = positiveQuery(normalizedOperator, field, value);
        return whenTrue ? positive : negatedQuery(normalizedOperator, field, value, positive);
    }

    /**
     * A comparison whose computed operand reduces to a plain field leaf, or {@code null} when
     * neither operand is one of those shapes:
     * <ul>
     *   <li>a map literal indexed by a field, {@code {"k": v}[f] == x}: membership of {@code f}
     *       in the keys whose value matches;</li>
     *   <li>{@code upperAscii(f)}/{@code lowerAscii(f)} compared with a literal: membership of
     *       {@code f} in the literal's ASCII case variants;</li>
     *   <li>{@code timestamp(f) ± duration(d)} compared with a timestamp literal: the field
     *       compared with the literal shifted the other way.</li>
     * </ul>
     */
    private Map<String, Object> rewrittenLeaf(
            String operator, List<Operand> operands, Scope scope, Polarity polarity) {
        for (int side = 0; side < 2; side++) {
            Operand computed = operands.get(side);
            if (computed.getNodeCase() != Operand.NodeCase.EXPRESSION) {
                continue;
            }
            Expression expression = computed.getExpression();
            Operand other = operands.get(1 - side);
            switch (expression.getOperator()) {
                case "index" -> {
                    if (expression.getOperandsCount() == 2
                            && isStructLiteral(expression.getOperands(0))) {
                        return mapLiteralLookup(operator, expression, other, scope, polarity);
                    }
                }
                case "upperAscii", "lowerAscii" -> {
                    return asciiCaseFold(operator, expression, other, scope, polarity);
                }
                case "add", "sub" -> {
                    Map<String, Object> shifted =
                            shiftedTimestampComparison(operator, side, expression, other, scope, polarity);
                    if (shifted != null) {
                        return shifted;
                    }
                }
                default -> {
                    // Not a rewritable shape; the comparison resolves operands as usual.
                }
            }
        }
        return null;
    }

    private static boolean isStructLiteral(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "struct".equals(operand.getExpression().getOperator());
    }

    /**
     * {@code {k1: v1, ...}[f] == x}. CEL errors, and so denies, when {@code f} is not a key, in
     * either polarity, so both polarities are a positive membership of {@code f}: in the keys
     * whose value equals {@code x}, or in the keys whose value does not.
     */
    private Map<String, Object> mapLiteralLookup(String operator, Expression index, Operand other,
                                                 Scope scope, Polarity polarity) {
        if (!"eq".equals(operator) && !"ne".equals(operator)) {
            throw unsupported(operator + " against a map literal indexed by a document field cannot"
                    + " be expressed: only == and != reduce to membership of the field in the"
                    + " map's keys");
        }
        Operand key = index.getOperands(1);
        if (key.getNodeCase() != Operand.NodeCase.VARIABLE
                || other.getNodeCase() != Operand.NodeCase.VALUE) {
            throw unsupported("A map literal lookup reduces to a terms query only when one document"
                    + " field is the key and the lookup is compared with a literal");
        }
        Object target = PlanValues.protoValueToJava(other.getValue());
        PlanValues.rejectNonFinite(target);
        if (target instanceof List<?> || target instanceof Map<?, ?>) {
            throw unsupported("A map literal lookup compared with a " + kindOf(target)
                    + " literal cannot be expressed: the Query DSL has no whole-" + kindOf(target)
                    + " comparison");
        }
        boolean wantMatch = "eq".equals(operator) == polarity.holds();
        ListValue.Builder keys = ListValue.newBuilder();
        for (Operand entry : index.getOperands(0).getExpression().getOperandsList()) {
            Expression setField = entry.getExpression();
            if (!"set-field".equals(setField.getOperator()) || setField.getOperandsCount() != 2
                    || setField.getOperands(0).getNodeCase() != Operand.NodeCase.VALUE
                    || setField.getOperands(1).getNodeCase() != Operand.NodeCase.VALUE) {
                throw unsupported("A map literal with a computed key or value cannot be looked up"
                        + " by a document field without scripts");
            }
            Object value = PlanValues.protoValueToJava(setField.getOperands(1).getValue());
            if (value instanceof List<?> || value instanceof Map<?, ?>) {
                throw unsupported("A map literal holding a " + kindOf(value) + " value cannot be"
                        + " looked up by a document field: the Query DSL has no whole-"
                        + kindOf(value) + " comparison");
            }
            if (celEquals(value, target) == wantMatch) {
                keys.addValues(setField.getOperands(0).getValue());
            }
        }
        Operand keyList = Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(keys)).build();
        return applyResolvedLeaf("in", List.of(key, keyList), scope, Polarity.TRUE);
    }

    /** CEL equality of two scalar literals: numbers compare by value across int and double. */
    private static boolean celEquals(Object left, Object right) {
        if (left == null || right == null) {
            return left == right;
        }
        if (left instanceof Number a && right instanceof Number b) {
            return a.doubleValue() == b.doubleValue();
        }
        return left.equals(right);
    }

    /** More case variants than this and the {@code terms} list is refused rather than built. */
    private static final int MAX_CASE_VARIANT_LETTERS = 10;

    /**
     * {@code upperAscii(f) == "ONE"}. {@code upperAscii} folds ASCII letters only, so the strings
     * it maps onto the literal are the literal's ASCII case variants; Elasticsearch compares a
     * keyword byte for byte, so membership in them is exact.
     */
    private Map<String, Object> asciiCaseFold(String operator, Expression fold, Operand other,
                                              Scope scope, Polarity polarity) {
        String function = fold.getOperator();
        if (!"eq".equals(operator) && !"ne".equals(operator)) {
            throw unsupported(operator + " over " + function + "() cannot be expressed: only =="
                    + " and != reduce to membership of the field in the literal's case variants,"
                    + " and computing " + function + " over a field needs a script");
        }
        if (fold.getOperandsCount() != 1
                || fold.getOperands(0).getNodeCase() != Operand.NodeCase.VARIABLE
                || other.getNodeCase() != Operand.NodeCase.VALUE
                || other.getValue().getKindCase() != Value.KindCase.STRING_VALUE) {
            throw unsupported(function + "() reduces to a terms query only over one document field"
                    + " compared with a string literal");
        }
        Operand variable = fold.getOperands(0);
        String field = scope.field(variable.getVariable());
        if (declaredType(field, operator) != ScalarType.STRING) {
            throw unsupported(function + "() over the non-string field '" + field + "' is a CEL"
                    + " error, which a terms query cannot reproduce");
        }
        List<String> variants =
                asciiCaseVariants(other.getValue().getStringValue(), "upperAscii".equals(function));
        ListValue.Builder list = ListValue.newBuilder();
        variants.forEach(v -> list.addValues(Value.newBuilder().setStringValue(v)));
        Operand variantList = Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(list)).build();
        boolean holds = "eq".equals(operator) == polarity.holds();
        return applyResolvedLeaf("in", List.of(variable, variantList), scope,
                holds ? Polarity.TRUE : Polarity.FALSE);
    }

    /**
     * Every string {@code upperAscii} (or {@code lowerAscii}) maps onto {@code literal}: none when
     * the literal holds an ASCII letter of the other case, else each ASCII letter in either case.
     */
    static List<String> asciiCaseVariants(String literal, boolean upper) {
        List<Integer> letters = new ArrayList<>();
        for (int i = 0; i < literal.length(); i++) {
            char c = literal.charAt(i);
            boolean folded = upper ? (c >= 'A' && c <= 'Z') : (c >= 'a' && c <= 'z');
            boolean unreachable = upper ? (c >= 'a' && c <= 'z') : (c >= 'A' && c <= 'Z');
            if (unreachable) {
                return List.of();
            }
            if (folded) {
                letters.add(i);
            }
        }
        if (letters.size() > MAX_CASE_VARIANT_LETTERS) {
            throw unsupported((upper ? "upperAscii" : "lowerAscii") + "() compared with a literal of"
                    + " more than " + MAX_CASE_VARIANT_LETTERS + " ASCII letters is refused: the"
                    + " terms query would list every case variant, 2^" + letters.size()
                    + " of them, and computing the fold over a field needs a script");
        }
        List<String> variants = new ArrayList<>();
        for (int mask = 0; mask < (1 << letters.size()); mask++) {
            char[] chars = literal.toCharArray();
            for (int bit = 0; bit < letters.size(); bit++) {
                if ((mask & (1 << bit)) != 0) {
                    int at = letters.get(bit);
                    chars[at] = upper
                            ? Character.toLowerCase(chars[at]) : Character.toUpperCase(chars[at]);
                }
            }
            variants.add(new String(chars));
        }
        return variants;
    }

    private static final Pattern DURATION = Pattern.compile("^(-)?([0-9]+)(?:\\.([0-9]{1,9}))?s$");
    private static final Set<String> TIMESTAMP_COMPARISONS = Set.of("eq", "ne", "lt", "le", "gt", "ge");
    private static final Instant CEL_TIMESTAMP_MIN = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant CEL_TIMESTAMP_MAX = Instant.parse("9999-12-31T23:59:59.999999999Z");

    /**
     * {@code timestamp(f) + duration(d) < timestamp(L)} becomes {@code timestamp(f) < timestamp(L - d)},
     * and {@code -} likewise. CEL errors when {@code f + d} leaves the timestamp range, so the
     * field is also bounded to where the sum exists. Returns {@code null} for any other
     * arithmetic, which the leaf resolver refuses.
     */
    private Map<String, Object> shiftedTimestampComparison(String operator, int side,
                                                           Expression arithmetic, Operand other,
                                                           Scope scope, Polarity polarity) {
        if (arithmetic.getOperandsCount() != 2) {
            return null;
        }
        boolean add = "add".equals(arithmetic.getOperator());
        Operand first = arithmetic.getOperands(0);
        Operand second = arithmetic.getOperands(1);
        Operand timestamp;
        Operand duration;
        if (isTimestampOfField(first) && isDurationLiteral(second)) {
            timestamp = first;
            duration = second;
        } else if (add && isDurationLiteral(first) && isTimestampOfField(second)) {
            timestamp = second;
            duration = first;
        } else {
            return null;
        }
        if (!TIMESTAMP_COMPARISONS.contains(operator) || !isTimestampLiteral(other)) {
            throw unsupported("Timestamp arithmetic over a document field reduces to a range query"
                    + " only when compared with a timestamp() literal; computing it over a field"
                    + " needs a script");
        }
        Duration shift = parseDuration(
                duration.getExpression().getOperands(0).getValue().getStringValue());
        if (!add) {
            shift = shift.negated();
        }
        Object literal = PlanValues.protoValueToJava(
                other.getExpression().getOperands(0).getValue());
        PlanValues.validateTimestampLiteral(literal);
        Instant shifted = OffsetDateTime.parse((String) literal, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .toInstant().minus(shift);
        if (shifted.isBefore(CEL_TIMESTAMP_MIN) || shifted.isAfter(CEL_TIMESTAMP_MAX)) {
            throw unsupported("Shifting the timestamp literal by the duration leaves CEL's timestamp"
                    + " range, so the comparison has no range-query form");
        }
        Operand shiftedLiteral = Operand.newBuilder().setExpression(Expression.newBuilder()
                .setOperator("timestamp")
                .addOperands(Operand.newBuilder().setValue(
                        Value.newBuilder().setStringValue(DateTimeFormatter.ISO_INSTANT.format(shifted)))))
                .build();
        List<Operand> rewritten = side == 0
                ? List.of(timestamp, shiftedLiteral) : List.of(shiftedLiteral, timestamp);
        Map<String, Object> comparison = applyResolvedLeaf(operator, rewritten, scope, polarity);
        if (shift.isZero()) {
            return comparison;
        }
        String field = scope.field(timestamp.getExpression().getOperands(0).getVariable());
        // Elasticsearch stores a date to the millisecond, so a millisecond bound is exact.
        Map<String, Object> inRange = shift.isNegative()
                ? Queries.range(field, "gte", ceilToMillis(CEL_TIMESTAMP_MIN.minus(shift)))
                : Queries.range(field, "lte", floorToMillis(CEL_TIMESTAMP_MAX.minus(shift)));
        return Queries.boolMust(List.of(comparison, inRange));
    }

    private static boolean isTimestampOfField(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "timestamp".equals(operand.getExpression().getOperator())
                && operand.getExpression().getOperandsCount() == 1
                && operand.getExpression().getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE;
    }

    private static boolean isTimestampLiteral(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "timestamp".equals(operand.getExpression().getOperator())
                && operand.getExpression().getOperandsCount() == 1
                && operand.getExpression().getOperands(0).getNodeCase() == Operand.NodeCase.VALUE;
    }

    private static boolean isDurationLiteral(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "duration".equals(operand.getExpression().getOperator())
                && operand.getExpression().getOperandsCount() == 1
                && operand.getExpression().getOperands(0).getNodeCase() == Operand.NodeCase.VALUE
                && operand.getExpression().getOperands(0).getValue().getKindCase()
                        == Value.KindCase.STRING_VALUE;
    }

    /** A protobuf JSON duration, as the planner spells {@code duration("24h")}: {@code "86400s"}. */
    private static Duration parseDuration(String spelling) {
        Matcher matcher = DURATION.matcher(spelling);
        if (!matcher.matches()) {
            throw malformed("duration() requires a protobuf duration literal such as \"86400s\": "
                    + spelling);
        }
        String fraction = matcher.group(3) == null ? "" : matcher.group(3);
        long nanos = fraction.isEmpty() ? 0 : Long.parseLong((fraction + "000000000").substring(0, 9));
        Duration duration = Duration.ofSeconds(Long.parseLong(matcher.group(2)), nanos);
        return matcher.group(1) == null ? duration : duration.negated();
    }

    private static String floorToMillis(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.format(instant.truncatedTo(ChronoUnit.MILLIS));
    }

    private static String ceilToMillis(Instant instant) {
        Instant floor = instant.truncatedTo(ChronoUnit.MILLIS);
        return DateTimeFormatter.ISO_INSTANT.format(floor.equals(instant) ? floor : floor.plusMillis(1));
    }

    /** Whether the field map names a sub-field of {@code field}, so it is an object in the index. */
    private boolean isObjectPath(String field) {
        String prefix = field + ".";
        return options.fieldMap().values().stream().anyMatch(f -> f.startsWith(prefix));
    }

    private static Comparison resolveComparison(Operand leftOperand, Operand rightOperand) {
        ResolvedOperand left = resolveLeafOperand(leftOperand);
        ResolvedOperand right = resolveLeafOperand(rightOperand);
        if (left instanceof ResolvedOperand.Field field
                && right instanceof ResolvedOperand.Literal literal) {
            return new Comparison(field.variable(), literal.value(), true);
        }
        if (left instanceof ResolvedOperand.Literal literal
                && right instanceof ResolvedOperand.Field field) {
            return new Comparison(field.variable(), literal.value(), false);
        }
        if (left instanceof ResolvedOperand.Field) {
            throw unsupported(
                    "Elasticsearch Query DSL cannot compare two document fields without scripts");
        }
        throw malformed("Leaf expression must contain exactly one document field");
    }

    /**
     * The query for a literal whose type does not match the field's declared type, or
     * {@code null} when the types match or an override handles the operator.
     *
     * <p>A cross-type comparison is a CEL error, so it matches nothing in either polarity. The
     * exception is {@code eq}/{@code ne}: cross-type equality is false, so {@code ne} matches
     * wherever the field exists.
     */
    private Map<String, Object> typeMismatchQuery(String operator, String field, Object value,
                                                  List<Operand> operands, boolean whenTrue) {
        if (options.operatorOverrides().containsKey(operator)
                || !SCALAR_OPERAND_OPERATORS.contains(operator)) {
            return null;
        }
        ScalarType type = declaredType(field, operator);
        if (type == ScalarType.TIMESTAMP && !hasTimestampWrapper(operands)) {
            throw bareTemporalComparison();
        }
        boolean compatible = inhabits(type, value);
        if (compatible && !(STRING_OPERATORS.contains(operator) && type != ScalarType.STRING)) {
            return null;
        }
        if ("eq".equals(operator) || "ne".equals(operator)) {
            boolean matches = "ne".equals(operator) == whenTrue;
            return matches ? Queries.exists(field) : Queries.matchNone();
        }
        return Queries.matchNone();
    }

    /**
     * The elements of {@code field in [...]} that have the field's type. Elasticsearch would
     * coerce the others and match them, so they are dropped. Null elements are kept for the
     * null-aware path. Unchanged when an {@code in} override is set.
     */
    private List<?> typedMembers(String field, List<?> values, List<Operand> operands) {
        if (options.operatorOverrides().containsKey("in")) {
            return values;
        }
        ScalarType type = declaredType(field, "in");
        if (type == ScalarType.TIMESTAMP && !hasTimestampWrapper(operands)) {
            throw bareTemporalComparison();
        }
        return values.stream().filter(element -> element == null || inhabits(type, element)).toList();
    }

    /**
     * The {@code hasIntersection} literals that have the collection's declared element type. An
     * empty result means the intersection is false.
     */
    List<?> typedIntersection(String field, List<?> values, List<Operand> operands) {
        ScalarType type = declaredElementType(field, "hasIntersection", operands);
        return values.stream().filter(element -> inhabits(type, element)).toList();
    }

    private ScalarType declaredElementType(String field, String operator, List<Operand> operands) {
        ScalarType type = declaredType(field, operator);
        if (type == ScalarType.TIMESTAMP && !hasTimestampWrapper(operands)) {
            throw bareTemporalComparison();
        }
        return type;
    }

    /**
     * The declared {@link ScalarType} for {@code field}. Required because Elasticsearch coerces a
     * query term to the mapped type ({@code "5"} matches the number {@code 5}) while CEL's
     * cross-type equality is false, and the adapter cannot see the mapping.
     */
    private ScalarType declaredType(String field, String operator) {
        ScalarType type = options.scalarTypes().get(field);
        if (type == null) {
            throw unmapped("Field '" + field + "' has no declared scalar type: " + operator
                    + " lowers to a term or range query, and Elasticsearch coerces the query term "
                    + "onto the field's mapped type where CEL's cross-type equality is false. "
                    + "Declare it in Options.withScalarTypes");
        }
        return type;
    }

    private static boolean inhabits(ScalarType type, Object value) {
        return switch (type) {
            case STRING, TIMESTAMP -> value instanceof String;
            case NUMBER -> value instanceof Number;
            case BOOLEAN -> value instanceof Boolean;
        };
    }

    private static boolean hasTimestampWrapper(List<Operand> operands) {
        return operands.stream().anyMatch(operand ->
                operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                        && "timestamp".equals(operand.getExpression().getOperator()));
    }

    private static UnsupportedPlanShapeException bareTemporalComparison() {
        return unsupported("Bare temporal comparison cannot preserve CEL string equality; use timestamp() explicitly");
    }

    /** Without an {@code ne} override, {@code ne} is {@code exists AND NOT eq}. */
    private Map<String, Object> positiveQuery(String operator, String field, Object value) {
        if ("ne".equals(operator) && !options.operatorOverrides().containsKey("ne")) {
            return Queries.definedAndNot(field, operatorOrDefault("eq").apply(field, value));
        }
        OperatorFunction function = operatorOrDefault(operator);
        if (function == null) {
            throw unsupported("Unknown operator: " + operator);
        }
        return function.apply(field, value);
    }

    /**
     * The query for the operator not holding. Requires the field to exist: CEL errors on a missing
     * field, but a bare {@code bool.must_not} would match it.
     */
    private Map<String, Object> negatedQuery(
            String operator, String field, Object value, Map<String, Object> positive) {
        return switch (operator) {
            case "eq", "in", "contains", "startsWith", "endsWith", "matches" ->
                    Queries.definedAndNot(field, positive);
            case "ne" -> operatorOrDefault("eq").apply(field, value);
            // Uses the override of the negated operator, if any.
            case "lt", "le", "gt", "ge" ->
                    operatorOrDefault(NEGATED_RANGE.get(operator)).apply(field, value);
            default -> throw unsupported(
                    "Cannot safely negate operator without scripts: " + operator);
        };
    }

    /**
     * Refuses a list or map literal where a scalar is needed. CEL gives them meanings (whole-list
     * equality, key membership) that the Query DSL cannot express.
     */
    private static void rejectNonScalarOperand(String operator, Object value, boolean variableFirst) {
        boolean nonScalar = value instanceof List<?> || value instanceof Map<?, ?>;
        if (SCALAR_OPERAND_OPERATORS.contains(operator) && nonScalar) {
            throw unsupported(operator + " against a " + kindOf(value) + " literal cannot be "
                    + "expressed: a term or range query compares a field against a scalar, and "
                    + "the Query DSL has no whole-" + kindOf(value) + " comparison");
        }
        if ("in".equals(operator) && variableFirst && value instanceof Map<?, ?>) {
            throw unsupported("in over a map literal cannot be expressed: CEL map membership "
                    + "tests the map's keys, and the Query DSL has no such operand");
        }
        if ("in".equals(operator) && !variableFirst && nonScalar) {
            throw unsupported("in with a " + kindOf(value) + " element cannot be expressed: a "
                    + "terms query matches scalar terms only");
        }
        if (("in".equals(operator) || "hasIntersection".equals(operator))
                && value instanceof List<?> values) {
            rejectNonScalarElements(operator, values);
        }
    }

    static void rejectNonScalarElements(String operator, List<?> values) {
        for (Object element : values) {
            if (element instanceof List<?> || element instanceof Map<?, ?>) {
                throw unsupported(operator + " over a list holding a " + kindOf(element)
                        + " element cannot be expressed: a terms query matches scalar terms only");
            }
        }
    }

    private static String kindOf(Object value) {
        return value instanceof Map<?, ?> ? "map" : "list";
    }

    private static ResolvedOperand resolveLeafOperand(Operand operand) {
        return switch (operand.getNodeCase()) {
            case VARIABLE -> new ResolvedOperand.Field(operand.getVariable());
            case VALUE -> {
                Object value = PlanValues.protoValueToJava(operand.getValue());
                PlanValues.rejectNonFinite(value);
                yield new ResolvedOperand.Literal(value);
            }
            case EXPRESSION -> {
                Expression expression = operand.getExpression();
                if ("except".equals(expression.getOperator())) {
                    throw exceptUnsupported();
                }
                if ("if".equals(expression.getOperator())) {
                    throw ternaryUnsupported();
                }
                if (!"timestamp".equals(expression.getOperator())
                        || expression.getOperandsCount() != 1) {
                    throw unsupported("Unexpected " + expression.getOperator()
                            + " expression in leaf operand: a Query DSL leaf compares one document"
                            + " field against a literal, and computing " + expression.getOperator()
                            + " over a field needs a script");
                }
                ResolvedOperand resolved = resolveLeafOperand(expression.getOperands(0));
                if (resolved instanceof ResolvedOperand.Literal literal) {
                    PlanValues.validateTimestampLiteral(literal.value());
                }
                yield resolved;
            }
            default -> throw malformed(
                    "Unexpected operand type in leaf expression: " + operand.getNodeCase());
        };
    }

    /**
     * The operator rewritten with the field on the left: {@code 5 < x} becomes {@code x > 5}. A
     * string operator whose receiver is the literal has no mirror and is refused.
     */
    static String normalizeLeafOperator(String operator, boolean variableFirst) {
        if (variableFirst) {
            return operator;
        }
        return switch (operator) {
            case "eq", "ne", "in" -> operator;
            case "lt" -> "gt";
            case "le" -> "ge";
            case "gt" -> "lt";
            case "ge" -> "le";
            case "contains", "startsWith", "endsWith", "matches" ->
                    throw unsupported(
                            operator + " with a document field as the receiver argument "
                                    + "cannot be expressed without scripts");
            default -> operator;
        };
    }

    private static Map<String, Object> nullLeafQuery(
            String operator,
            String field,
            boolean variableFirst,
            boolean whenTrue) {
        return switch (operator) {
            case "eq" -> {
                if (whenTrue) {
                    throw unsafeExplicitNullComparison();
                }
                yield Queries.exists(field);
            }
            case "ne" -> {
                if (!whenTrue) {
                    throw unsafeExplicitNullComparison();
                }
                yield Queries.exists(field);
            }
            case "in" -> {
                if (!variableFirst) {
                    throw unsupported(
                            "null membership in a document array requires an explicit null-value mapping");
                }
                throw unsafeExplicitNullComparison();
            }
            default -> throw unsupported(
                    "Null values are only supported with eq, ne, and scalar in operators");
        };
    }

    private Map<String, Object> nullAwareMembershipQuery(
            String field, List<?> values, boolean whenTrue) {
        if (whenTrue) {
            throw unsafeExplicitNullComparison();
        }
        List<?> nonNull = values.stream().filter(Objects::nonNull).toList();
        if (nonNull.isEmpty()) {
            return Queries.exists(field);
        }
        return Queries.definedAndNot(field, operatorOrDefault("in").apply(field, nonNull));
    }

    /** Elasticsearch does not index null, so a null intersection element cannot match. */
    static void rejectNullIntersection(List<?> values) {
        if (values.stream().anyMatch(Objects::isNull)) {
            throw unsupported("hasIntersection with null requires an explicit null-value mapping");
        }
    }

    OperatorFunction operatorOrDefault(String operator) {
        return options.operatorOverrides().getOrDefault(operator, Queries.DEFAULT_OPERATORS.get(operator));
    }
}
