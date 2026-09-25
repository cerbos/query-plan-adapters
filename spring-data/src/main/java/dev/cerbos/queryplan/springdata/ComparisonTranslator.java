/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Translates every leaf comparison: field against value, field against field, constant against
 * constant, constant-receiver string matches, arithmetic, ternary and {@code size()}
 * comparisons.
 *
 * <p>{@link #resolve} classifies each operand into a {@link Resolved} shape without converting
 * it, and {@link #dispatch} translates the pair. Conversion is lazy because the error a
 * malformed operand raises depends on the whole comparison, and those messages are pinned.
 *
 * <p>To add an operand type, add a {@code Resolved} case, classify it in {@link #resolve} and
 * handle its pairings in {@link #dispatch}, as the {@code timestamp()} cases do. Unhandled
 * pairings fall through to {@link #leafOperandError}.
 */
final class ComparisonTranslator {

    static final Set<String> COMPARISON_OPS =
            Set.of("eq", "ne", "lt", "gt", "le", "ge");

    private static final Set<String> ORDERING_OPS = Set.of("lt", "gt", "le", "ge");

    /** Whether {@code text} holds a UTF-16 code unit at or above 0xD800. */
    private static boolean hasCodeUnitFromSurrogates(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) >= '\uD800') {
                return true;
            }
        }
        return false;
    }

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final TernaryTranslator ternary;
    private final SizeTranslator sizes;
    private final ArithmeticTranslator arithmetic;

    ComparisonTranslator(CriteriaBuilder cb, TriPredicate tri, LeafTranslator leaf,
                         TernaryTranslator ternary, SizeTranslator sizes) {
        this.cb = cb;
        this.tri = tri;
        this.leaf = leaf;
        this.ternary = ternary;
        this.sizes = sizes;
        this.arithmetic = new ArithmeticTranslator(cb, tri, leaf, this);
    }

    /**
     * Translates an operator that {@link PlanWalker#traverseExpression} does not handle by
     * name. The ternary rewrite runs first, on the raw operands, because it needs source order.
     * Then the operands are normalized field-first ({@link NormalizedBinary}), {@code size()}
     * comparisons are tried, and everything else is resolved and dispatched.
     */
    Predicate translate(String op, List<Operand> operands, Scope scope) {
        Predicate ternaryPred = ternary.tryTernaryComparison(op, operands, scope);
        if (ternaryPred != null) {
            return ternaryPred;
        }
        NormalizedBinary nb = NormalizedBinary.of(op, operands);
        // Check arity before the size() probe: its operand scan keeps the last match and would
        // silently drop an extra operand.
        if (nb.operands().size() != 2) {
            throw Refusals.malformed(
                    nb.op() + " requires exactly 2 operands, got " + nb.operands().size());
        }
        Predicate sizePred = sizes.trySizeComparison(nb.op(), nb.operands(), scope);
        if (sizePred != null) {
            return sizePred;
        }
        return dispatch(nb.op(),
                resolve(nb.operands().get(0)),
                resolve(nb.operands().get(1)),
                nb.operands(), scope);
    }

    // -- operand resolution --

    /** A comparison operand classified by shape. Values convert on demand in dispatch. */
    private sealed interface Resolved {
        /** A plan constant. */
        record Constant(Operand operand) implements Resolved {
            Object value() {
                return PlanValues.protoValueToJava(operand.getValue());
            }
        }

        /** A mapped attribute. */
        record Field(String variable) implements Resolved {}

        /** {@code add(value, value)}, folded by {@link PlanValues#foldAdd}. */
        record ConstantAdd(Operand left, Operand right) implements Resolved {
            Object fold() {
                return PlanValues.foldAdd(
                        PlanValues.protoValueToJava(left.getValue()),
                        PlanValues.protoValueToJava(right.getValue()));
            }
        }

        /**
         * {@code add(field, value)} or {@code add(value, field)}. Under eq/ne against a
         * constant it is solved for the field when the solve is exact; otherwise it lowers to
         * SQL arithmetic.
         */
        record FieldPlusConstant(String fieldVariable, Operand constant, boolean fieldIsLeft)
                implements Resolved {}

        /** Any other arithmetic expression, lowered to SQL by {@link ArithmeticTranslator}. */
        record Arithmetic(String operator) implements Resolved {}

        /** {@code timestamp(variable)}. */
        record TimestampField(String variable) implements Resolved {}

        /**
         * {@code timestamp(value)}. The planner folds {@code now() - duration(...)} into this
         * shape too. {@link #instant()} accepts any RFC-3339 offset and normalizes to the
         * absolute instant, as CEL timestamp equality does.
         */
        record TimestampConstant(Operand operand) implements Resolved {
            Instant instant() {
                Object raw = PlanValues.protoValueToJava(operand.getValue());
                // CEL's timestamp() rejects a non-string or unparseable literal, so the planner
                // cannot emit one.
                if (!(raw instanceof String s)) {
                    throw Refusals.malformed(
                            "timestamp() constant must be an RFC-3339 string, got "
                                    + (raw == null ? "null" : raw.getClass().getSimpleName()));
                }
                try {
                    return Instant.parse(s);
                } catch (DateTimeParseException e) {
                    try {
                        return OffsetDateTime.parse(s).toInstant();
                    } catch (DateTimeParseException e2) {
                        throw Refusals.malformed(
                                "timestamp() constant could not be parsed as an RFC-3339 instant", e2);
                    }
                }
            }
        }

        /**
         * {@code string(variable)}. String, Boolean and numeric columns are translated
         * ({@link #stringOfFieldComparison}).
         */
        record StringOfField(String variable) implements Resolved {}

        /**
         * {@code int(variable)}. Only a Double or Integer column compared with a number is
         * translated ({@link #intOfFieldComparison}).
         */
        record IntOfField(String variable) implements Resolved {}

        /** An operand no leaf case handles, reported by {@link #leafOperandError}. */
        record Opaque() implements Resolved {}
    }

    private Resolved resolve(Operand o) {
        return switch (o.getNodeCase()) {
            case VALUE -> new Resolved.Constant(o);
            case VARIABLE -> new Resolved.Field(o.getVariable());
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = o.getExpression();
                String exprOp = e.getOperator();
                // The planner emits temporal comparisons as timestamp(variable) or
                // timestamp(value). A nested expression has no translation.
                if ("timestamp".equals(exprOp) && e.getOperandsCount() == 1) {
                    Operand arg = e.getOperands(0);
                    if (arg.getNodeCase() == Operand.NodeCase.VARIABLE) {
                        yield new Resolved.TimestampField(arg.getVariable());
                    }
                    if (arg.getNodeCase() == Operand.NodeCase.VALUE) {
                        yield new Resolved.TimestampConstant(arg);
                    }
                    yield new Resolved.Opaque();
                }
                if ("string".equals(exprOp) && e.getOperandsCount() == 1
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE) {
                    yield new Resolved.StringOfField(e.getOperands(0).getVariable());
                }
                if ("int".equals(exprOp) && e.getOperandsCount() == 1
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE) {
                    yield new Resolved.IntOfField(e.getOperands(0).getVariable());
                }
                if (!ArithmeticTranslator.ARITHMETIC_OPS.contains(exprOp)) {
                    yield new Resolved.Opaque();
                }
                if ("add".equals(exprOp) && e.getOperandsCount() == 2) {
                    Operand l = e.getOperands(0);
                    Operand r = e.getOperands(1);
                    boolean lValue = l.getNodeCase() == Operand.NodeCase.VALUE;
                    boolean rValue = r.getNodeCase() == Operand.NodeCase.VALUE;
                    if (lValue && rValue) {
                        yield new Resolved.ConstantAdd(l, r);
                    }
                    if (l.getNodeCase() == Operand.NodeCase.VARIABLE && rValue) {
                        yield new Resolved.FieldPlusConstant(l.getVariable(), r, true);
                    }
                    if (lValue && r.getNodeCase() == Operand.NodeCase.VARIABLE) {
                        yield new Resolved.FieldPlusConstant(r.getVariable(), l, false);
                    }
                }
                yield new Resolved.Arithmetic(exprOp);
            }
            default -> new Resolved.Opaque();
        };
    }

    private static boolean isAddRooted(Resolved r) {
        return r instanceof Resolved.ConstantAdd
                || r instanceof Resolved.FieldPlusConstant
                || (r instanceof Resolved.Arithmetic a && "add".equals(a.operator()));
    }

    private static boolean isArithmeticRooted(Resolved r) {
        return r instanceof Resolved.ConstantAdd
                || r instanceof Resolved.FieldPlusConstant
                || r instanceof Resolved.Arithmetic;
    }

    // -- dispatch --

    /**
     * Translates one comparison from its resolved pair. {@code operands} are the normalized
     * raw operands, needed by arithmetic lowering and by error reporting.
     */
    private Predicate dispatch(String op, Resolved left, Resolved right,
                               List<Operand> operands, Scope scope) {
        // The planner never emits two constants, but ternary substitution does: the else
        // branch of `(aBool ? aNumber : 0) > 0` becomes gt(value(0), value(0)).
        if (COMPARISON_OPS.contains(op)
                && left instanceof Resolved.Constant lc
                && right instanceof Resolved.Constant rc) {
            return constantComparison(op, lc.value(), rc.value());
        }

        // `"a,b".contains(R.attr.x)`: the constant is the haystack and the column the needle.
        // A concat receiver is folded here too; the add solve would swap haystack and needle.
        StringMatch match = StringMatch.of(op);
        if (match != null && right instanceof Resolved.Field needleField) {
            Object receiver = left instanceof Resolved.Constant c ? c.value()
                    : left instanceof Resolved.ConstantAdd ca ? ca.fold()
                    : null;
            if (receiver != null) {
                if (!(receiver instanceof String haystack)) {
                    // CEL has no contains() on a number, so the planner cannot emit this.
                    throw Refusals.malformed(
                            op + " requires a string receiver, got "
                                    + PlanValues.typeName(receiver));
                }
                Path<?> needle = scope.path(needleField.variable());
                return fieldToFieldLike(cb.literal(haystack), needle, match);
            }
            // A null receiver falls through to the null-value error below.
        }

        if (COMPARISON_OPS.contains(op)) {
            // Both operands are expressions, so NormalizedBinary cannot reorder them; the
            // value-first form is mirrored here.
            if (left instanceof Resolved.TimestampField tsField
                    && right instanceof Resolved.TimestampConstant tsConst) {
                return timestampLeaf(op, tsField, tsConst, scope);
            }
            if (left instanceof Resolved.TimestampConstant tsConst
                    && right instanceof Resolved.TimestampField tsField) {
                return timestampLeaf(NormalizedBinary.mirror(op), tsField, tsConst, scope);
            }
            // Reachable through ternary substitution.
            if (left instanceof Resolved.TimestampConstant lts
                    && right instanceof Resolved.TimestampConstant rts) {
                return timestampConstantComparison(op, lts.instant(), rts.instant());
            }
            // NormalizedBinary puts string() first, so both spellings arrive here. Other
            // operators and constants reach leafOperandError.
            if (("eq".equals(op) || "ne".equals(op))
                    && left instanceof Resolved.StringOfField sf
                    && right instanceof Resolved.Constant c
                    && c.value() instanceof String text) {
                return stringOfFieldComparison(op, sf, text, operands, scope);
            }
            if (left instanceof Resolved.IntOfField intField
                    && right instanceof Resolved.Constant c
                    && c.value() instanceof Number n) {
                return intOfFieldComparison(op, intField, n.doubleValue(), operands, scope);
            }
            // `field op add(value, value)`: strings concatenate, as in CEL.
            if (left instanceof Resolved.Field f && right instanceof Resolved.ConstantAdd ca) {
                return leaf.applyLeaf(op, scope.path(f.variable()), ca.fold());
            }
            // `add(field, c) eq/ne value` is solved in Java only when the solve is exact
            // (strings, in-range integers). Floating-point subtraction does not invert
            // addition, so other numbers are compared in SQL.
            if (("eq".equals(op) || "ne".equals(op))
                    && left instanceof Resolved.FieldPlusConstant fpc
                    && right instanceof Resolved.Constant other
                    && !PlanValues.requiresSqlLowering(
                            other.value(),
                            PlanValues.protoValueToJava(fpc.constant().getValue()))) {
                return solveAddComparison(op, fpc, other, scope);
            }
            if (isAddRooted(left) || isAddRooted(right)) {
                Predicate concat = tryConcatComparison(op, operands, scope);
                if (concat != null) {
                    return concat;
                }
            }
            if (isArithmeticRooted(left) || isArithmeticRooted(right)) {
                return arithmetic.numericComparison(op, operands, scope);
            }
        } else if (isAddRooted(left) || isAddRooted(right)) {
            return addFoldOrError(op, operands, scope);
        }

        if (left instanceof Resolved.Field a && right instanceof Resolved.Field b) {
            return fieldToFieldComparison(op, a.variable(), b.variable(), scope);
        }

        // Either order: string-match operators are not normalized, so a null receiver arrives
        // value-first and must still reach the null-value error.
        Resolved.Field field = left instanceof Resolved.Field lf ? lf
                : right instanceof Resolved.Field rf ? rf : null;
        Resolved.Constant constant = left instanceof Resolved.Constant lc2 ? lc2
                : right instanceof Resolved.Constant rc2 ? rc2 : null;
        if (field != null && constant != null) {
            return leafFieldValue(op, field, constant, scope);
        }

        throw leafOperandError(op, operands);
    }

    private Predicate leafFieldValue(String op, Resolved.Field field,
                                     Resolved.Constant constant, Scope scope) {
        Object value = constant.value();

        // A list or map constant has no scalar-column comparison, and Hibernate would fail
        // with a raw coercion error. Checked before path resolution so a Relation-mapped
        // attribute gets this message too. The message never includes element values.
        if ((value instanceof List<?> || value instanceof Map<?, ?>)
                && holdsScalars(field.variable(), scope)) {
            return scalarAgainstAggregate(op, field, scope);
        }
        if (value instanceof List<?> || value instanceof Map<?, ?>) {
            throw Refusals.unsupported(
                    op + " comparison against a " + constantShape(value)
                            + " constant is not supported for attribute "
                            + field.variable() + ". Whole-" + kindWord(value)
                            + " equality is not translatable to a scalar column"
                            + " comparison; map the attribute as a Relation and use"
                            + " in/hasIntersection, or compare elements individually.");
        }

        Path<?> path = scope.path(field.variable());

        if (value == null) {
            // A NULL column sends no attribute, so `x == null` is never true: a present value is
            // unequal and a missing one is a CEL error, which denies. The upfront OMITTED scan
            // lets this shape through only without an override for op.
            if (("eq".equals(op) || "ne".equals(op)) && !leaf.overridden(op)
                    && leaf.isDeclaredOmitted(field.variable(), scope)) {
                return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                        () -> cb.isNull(path));
            }
            return leaf.withOverride(op, path, null, () -> switch (op) {
                case "eq" -> cb.isNull(path);
                case "ne" -> cb.isNotNull(path);
                // `x < null` is a CEL error, which denies. No ordering predicate matches that.
                default -> throw Refusals.unsupported(
                        "Null values are only supported with eq and ne operators (got " + op + ")");
            });
        }

        // An explicit-null attribute holds a null value in CEL, so eq/ne is definite. An
        // overridden operator is left to the override.
        if (("eq".equals(op) || "ne".equals(op))
                && !leaf.overridden(op)
                && leaf.isExplicitNull(field.variable(), scope)) {
            return leaf.definiteEquality(op, path, cb.literal(value), true, false);
        }

        // CEL orders strings by code point. A JPA store may compare by UTF-16 code unit instead
        // (H2 uses Java's String.compareTo), which puts an astral character's high surrogate
        // (from 0xD800) before U+E000–U+FFFF. The two orders can only disagree at a position
        // where one side holds a code unit at or above 0xD800, so a literal holding none cannot
        // be ordered differently; one that does is refused.
        if (ORDERING_OPS.contains(op) && !leaf.overridden(op)
                && value instanceof String text && hasCodeUnitFromSurrogates(text)) {
            throw Refusals.unsupported(op + " orders a string attribute against a literal holding"
                    + " a character at or above U+D800 (an astral character or U+E000–U+FFFF):"
                    + " CEL orders strings by code point, and a store that compares UTF-16 code"
                    + " units (H2, Java's String.compareTo) puts a surrogate pair before"
                    + " U+E000–U+FFFF");
        }

        return leaf.applyLeaf(op, path, value);
    }

    /**
     * Whether {@code variable} holds a scalar: a Field, or the bare element of a relation whose
     * elements are values rather than objects. An unmapped variable answers false and is
     * refused by the caller.
     */
    private static boolean holdsScalars(String variable, Scope scope) {
        Scope.Resolution resolved;
        try {
            resolved = scope.resolve(variable);
        } catch (IllegalArgumentException unmapped) {
            return false;
        }
        return resolved instanceof Scope.ResolvedScalar scalar
                && (scalar.mapping() instanceof AttributeMapping.Field
                        || scalar.mapping() instanceof AttributeMapping.Relation r
                                && (r.defaultMemberField() != null || r.fields().isEmpty()));
    }

    /**
     * A scalar against a list or map literal. CEL equality across types is false, not an error,
     * so {@code ==} matches nothing and {@code !=} everything, a NULL column staying UNKNOWN
     * unless it is sent as an explicit null; any other operator has no overload and errors.
     */
    private Predicate scalarAgainstAggregate(String op, Resolved.Field field, Scope scope) {
        if (!"eq".equals(op) && !"ne".equals(op)) {
            return tri.unknown();
        }
        if (leaf.isExplicitNull(field.variable(), scope)) {
            return constant("ne".equals(op));
        }
        Path<?> path = scope.path(field.variable());
        return matchesNothing(op, path);
    }

    /**
     * {@code timestamp(field) op timestamp(constant)}, with {@code op} already field-first.
     *
     * <p>Only {@link Instant} and {@link OffsetDateTime} columns are translated, because both
     * denote an absolute instant. {@code LocalDateTime}, {@code java.util.Date} and
     * {@code String} do not, and guessing a zone could return rows the PDP denies, so they
     * throw. A registered {@link OperatorFunction} override is tried first, with the parsed
     * {@link Instant}. A NULL column makes the comparison UNKNOWN, matching CEL's deny.
     */
    private Predicate timestampLeaf(String op, Resolved.TimestampField field,
                                    Resolved.TimestampConstant constant, Scope scope) {
        Instant instant = constant.instant();
        Path<?> path = scope.path(field.variable());
        return leaf.withOverride(op, path, instant, () -> {
            Class<?> javaType = path.getJavaType();
            Object bound;
            if (Instant.class.equals(javaType)) {
                bound = instant;
            } else if (OffsetDateTime.class.equals(javaType)) {
                bound = instant.atOffset(ZoneOffset.UTC);
            } else {
                // Unmapped, not unsupported: the caller fixes it by remapping the column or
                // registering an override.
                throw Refusals.unmapped(
                        "timestamp() comparison requires a column mapped to java.time.Instant "
                                + "or java.time.OffsetDateTime, but '" + field.variable()
                                + "' maps to " + javaType.getSimpleName()
                                + ". Other temporal representations (LocalDateTime, "
                                + "java.util.Date, String) are ambiguous about the absolute "
                                + "instant they store; remap the column or register an "
                                + "OperatorFunction override for '" + op + "'.");
            }
            return leaf.defaultLeaf(op, path, bound);
        });
    }

    private Predicate timestampConstantComparison(String op, Instant left, Instant right) {
        return constant(holds(op, left.compareTo(right)));
    }

    /** {@code 2^63}: CEL's int() errors at or beyond it in either direction. */
    private static final double INT64_LIMIT = 0x1p63;

    /**
     * {@code int(column) op c}, over a Double or Integer column. CEL's {@code int()} truncates a
     * double toward zero, which PostgreSQL and MySQL {@code CAST} do not (they round), so the
     * comparison is solved for the column instead: {@code int(d) >= m} is {@code d >= m} for a
     * positive {@code m} and {@code d > m - 1} otherwise, and {@code int(d) <= m} is
     * {@code d <= m} for a negative {@code m} and {@code d < m + 1} otherwise. A NULL column, or
     * one outside the int64 range (where CEL errors, NaN included), is UNKNOWN under both
     * polarities.
     */
    private Predicate intOfFieldComparison(String op, Resolved.IntOfField field, double c,
                                           List<Operand> operands, Scope scope) {
        Path<?> path = scope.path(field.variable());
        Class<?> type = path.getJavaType();
        if (!Double.class.equals(type) && !Integer.class.equals(type)
                && !String.class.equals(type)) {
            throw leafOperandError(op, operands);
        }
        if (Double.isNaN(c) || Math.abs(c) >= 0x1p53) {
            throw Refusals.unsupported("int() compared with " + c + " is not supported: the"
                    + " bound is not an exactly representable integer");
        }
        if (String.class.equals(type)) {
            @SuppressWarnings("unchecked")
            IntText text = new IntText((Expression<String>) path);
            return tri.baseUnlessUnknown(intBounds(op, c, text::atLeast, text::atMost),
                    text::unparsable);
        }
        Expression<Double> d = path.as(Double.class);
        return tri.baseUnlessUnknown(
                intBounds(op, c, m -> atLeast(d, m), m -> atMost(d, m)),
                () -> cb.or(cb.isNull(path), cb.le(d, -INT64_LIMIT), cb.ge(d, INT64_LIMIT)));
    }

    /** {@code int(x) op c} from the integral bounds {@code int(x) >= m} and {@code <= m}. */
    private Predicate intBounds(String op, double c,
                                java.util.function.DoubleFunction<Predicate> atLeast,
                                java.util.function.DoubleFunction<Predicate> atMost) {
        boolean integral = c == Math.rint(c);
        return switch (op) {
            case "gt" -> atLeast.apply(Math.floor(c) + 1);
            case "ge" -> atLeast.apply(Math.ceil(c));
            case "lt" -> atMost.apply(Math.ceil(c) - 1);
            case "le" -> atMost.apply(Math.floor(c));
            case "eq" -> integral ? cb.and(atLeast.apply(c), atMost.apply(c)) : cb.disjunction();
            case "ne" -> integral ? tri.not(cb.and(atLeast.apply(c), atMost.apply(c)))
                    : cb.conjunction();
            default -> throw Refusals.internal("Unsupported int() comparison operator: " + op);
        };
    }

    /**
     * {@code int(s)} over a String column, which CEL parses with Go's
     * {@code strconv.ParseInt(s, 10, 64)}: an optional {@code +} or {@code -}, then one or more
     * ASCII digits, within int64. SQL {@code CAST} accepts other spellings or fails the query,
     * so the value is compared without one: its sign, then its digits with leading zeros
     * trimmed, which order by length and then lexicographically. Every expression is built
     * fresh, so none is shared between polarities.
     */
    private final class IntText {
        private static final String INT64_MAX = "9223372036854775807";
        private static final String INT64_MIN_MAGNITUDE = "9223372036854775808";

        private final Expression<String> s;

        IntText(Expression<String> s) {
            this.s = s;
        }

        private Predicate negative() {
            return cb.like(s, "-%");
        }

        /** The string without its sign. */
        private Expression<String> body() {
            return cb.<String>selectCase()
                    .when(cb.or(cb.like(s, "+%"), cb.like(s, "-%")), cb.substring(s, 2))
                    .otherwise(s);
        }

        /** The magnitude's digits, without leading zeros: {@code ''} for zero. */
        private Expression<String> digits() {
            return cb.trim(CriteriaBuilder.Trimspec.LEADING, '0', body());
        }

        /** {@code |value| op k} for a magnitude {@code k} spelled without leading zeros. */
        private Predicate magnitude(String op, String k) {
            Expression<Integer> length = cb.length(digits());
            Predicate longer = "ge".equals(op) || "gt".equals(op)
                    ? cb.gt(length, k.length()) : cb.lt(length, k.length());
            Predicate sameLength = cb.equal(cb.length(digits()), k.length());
            Predicate lexical = switch (op) {
                case "ge" -> cb.greaterThanOrEqualTo(digits(), k);
                case "gt" -> cb.greaterThan(digits(), k);
                case "le" -> cb.lessThanOrEqualTo(digits(), k);
                default -> throw Refusals.internal("Unsupported magnitude comparison: " + op);
            };
            return cb.or(longer, cb.and(sameLength, lexical));
        }

        private static String spelled(double magnitude) {
            long m = (long) Math.abs(magnitude);
            return m == 0 ? "" : Long.toString(m);
        }

        /** {@code int(s) >= m}. */
        Predicate atLeast(double m) {
            if (m <= 0) {
                return cb.or(tri.not(negative()), magnitude("le", spelled(m)));
            }
            return cb.and(tri.not(negative()), magnitude("ge", spelled(m)));
        }

        /** {@code int(s) <= m}. */
        Predicate atMost(double m) {
            if (m >= 0) {
                return cb.or(negative(), magnitude("le", spelled(m)));
            }
            return cb.and(negative(), magnitude("ge", spelled(m)));
        }

        /** NULL, not a base-10 integer, or outside int64: CEL's int() errors. */
        Predicate unparsable() {
            Expression<String> rest = body();
            for (char digit = '0'; digit <= '9'; digit++) {
                rest = cb.function("replace", String.class, rest,
                        cb.literal(String.valueOf(digit)), cb.literal(""));
            }
            Predicate malformed = cb.or(cb.equal(body(), ""), cb.notEqual(rest, ""));
            Predicate outOfRange = cb.or(
                    cb.and(tri.not(negative()), magnitude("gt", INT64_MAX)),
                    cb.and(negative(), magnitude("gt", INT64_MIN_MAGNITUDE)));
            return cb.or(cb.isNull(s), malformed, outOfRange);
        }
    }

    /** {@code trunc(d) >= m} for an integral {@code m}. */
    private Predicate atLeast(Expression<Double> d, double m) {
        return m > 0 ? cb.ge(d, m) : cb.gt(d, m - 1);
    }

    /** {@code trunc(d) <= m} for an integral {@code m}. */
    private Predicate atMost(Expression<Double> d, double m) {
        return m < 0 ? cb.le(d, m) : cb.lt(d, m + 1);
    }

    /** The numeric column types whose values CEL receives as doubles, rendered by %g. */
    private static final Set<Class<?>> DOUBLE_RENDERED =
            Set.of(Double.class, Integer.class, Long.class);

    /**
     * {@code string(column) eq/ne "text"}, decided per column type. SQL has no portable spelling
     * of CEL's conversion ({@code CAST} gives {@code '1'} for a MySQL boolean and its own number
     * format everywhere), so the constant is inverted in Java and only the column reaches SQL:
     *
     * <ul>
     *   <li>{@link String}: {@code string()} is the identity, so the column is compared as it
     *       stands, through any override for {@code op}.</li>
     *   <li>{@link Boolean}: CEL renders exactly {@code "true"} or {@code "false"}. A
     *       text-producing {@code CASE} would compare in the connection collation, which is
     *       case-insensitive under MySQL Connector/J.</li>
     *   <li>{@link Double}, {@link Integer}, {@link Long}: the one double CEL renders as the
     *       constant ({@link CelDoubleText}), compared as a double.</li>
     * </ul>
     *
     * <p>A constant no value renders as matches nothing. A NULL column stays UNKNOWN under both
     * polarities, even one declared EXPLICIT, because CEL's {@code string()} errors on null and
     * the PDP denies. Other column types, primitive ones included, are refused.
     */
    private Predicate stringOfFieldComparison(String op, Resolved.StringOfField field,
                                              String value, List<Operand> operands, Scope scope) {
        Path<?> path = scope.path(field.variable());
        Class<?> type = path.getJavaType();
        if (String.class.equals(type)) {
            return leaf.applyLeaf(op, path, value);
        }
        if (Boolean.class.equals(type)) {
            if ("true".equals(value) || "false".equals(value)) {
                return leaf.applyLeaf(op, path, Boolean.valueOf(value));
            }
            return matchesNothing(op, path);
        }
        if (DOUBLE_RENDERED.contains(type)) {
            if (CelDoubleText.solve(value) instanceof CelDoubleText.Solution.Exactly exactly) {
                return leaf.defaultLeaf(op, path, exactly.value());
            }
            return matchesNothing(op, path);
        }
        throw leafOperandError(op, operands);
    }

    /** eq FALSE and ne TRUE, UNKNOWN for a NULL column. */
    private Predicate matchesNothing(String op, Path<?> path) {
        return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                () -> cb.isNull(path));
    }

    // Size and kind only: element values never go into an error message.
    private static String constantShape(Object value) {
        if (value instanceof List<?> l) {
            return "list of " + l.size() + " element" + (l.size() == 1 ? "" : "s");
        }
        Map<?, ?> m = (Map<?, ?>) value;
        return "map of " + m.size() + " entr" + (m.size() == 1 ? "y" : "ies");
    }

    private static String kindWord(Object value) {
        return value instanceof List<?> ? "list" : "map";
    }

    /**
     * A comparison whose {@code add} is CEL string concatenation, or {@code null} when no string
     * is involved and the comparison is numeric. The plan does not say which {@code +} it is, so
     * the operand types decide: a string constant or a {@link String} column anywhere under the
     * {@code add} makes it concatenation, and every other leaf must then be a string too.
     *
     * <p>{@code "a" + null} is a CEL error, so a NULL column under the concatenation keeps the
     * comparison UNKNOWN under both polarities; {@code CONCAT} alone would not, since some
     * dialects skip NULL arguments. A column compared with the concatenation keeps its own null
     * convention: declared EXPLICIT, {@code ==} and {@code !=} are definite on a NULL.
     */
    private Predicate tryConcatComparison(String op, List<Operand> operands, Scope scope) {
        if (!isStringTyped(operands.get(0), scope) && !isStringTyped(operands.get(1), scope)) {
            return null;
        }
        List<Path<?>> nullable = new ArrayList<>();
        // At most one side is a bare column; the other holds the add.
        int explicitSide = -1;
        for (int side = 0; side < 2; side++) {
            Operand o = operands.get(side);
            if (("eq".equals(op) || "ne".equals(op))
                    && o.getNodeCase() == Operand.NodeCase.VARIABLE
                    && leaf.isExplicitNull(o.getVariable(), scope)) {
                explicitSide = side;
            }
        }
        List<Expression<String>> sides = new ArrayList<>(2);
        for (int side = 0; side < 2; side++) {
            Operand o = operands.get(side);
            sides.add(side == explicitSide
                    ? stringPath(o.getVariable(), op, operands, scope)
                    : concatOperand(o, op, operands, scope, nullable));
        }
        Predicate base = explicitSide >= 0
                ? leaf.definiteEquality(op, sides.get(0), sides.get(1),
                        explicitSide == 0, explicitSide == 1)
                : comparePredicate(op, sides.get(0), sides.get(1));
        return nullable.isEmpty() ? base : tri.baseUnlessUnknown(base,
                () -> cb.or(nullable.stream().map(cb::isNull).toArray(Predicate[]::new)));
    }

    /** Whether a string constant or {@link String} column is anywhere under {@code o}. */
    private boolean isStringTyped(Operand o, Scope scope) {
        return switch (o.getNodeCase()) {
            case VALUE -> o.getValue().getKindCase() == Value.KindCase.STRING_VALUE;
            case VARIABLE -> String.class.equals(scope.path(o.getVariable()).getJavaType());
            case EXPRESSION -> "add".equals(o.getExpression().getOperator())
                    && o.getExpression().getOperandsList().stream()
                            .anyMatch(child -> isStringTyped(child, scope));
            default -> false;
        };
    }

    /**
     * Lowers {@code o} to a string expression, adding each column it reads to {@code nullable}.
     * A leaf that is not a string makes the {@code +} a type error CEL has no overload for; it is
     * refused rather than guessed.
     */
    private Expression<String> concatOperand(Operand o, String op, List<Operand> operands,
                                             Scope scope, List<Path<?>> nullable) {
        switch (o.getNodeCase()) {
            case VALUE -> {
                if (PlanValues.protoValueToJava(o.getValue()) instanceof String text) {
                    return cb.literal(text);
                }
            }
            case VARIABLE -> {
                Expression<String> path = stringPath(o.getVariable(), op, operands, scope);
                nullable.add((Path<?>) path);
                return path;
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = o.getExpression();
                if ("add".equals(e.getOperator()) && e.getOperandsCount() == 2) {
                    return cb.concat(
                            concatOperand(e.getOperands(0), op, operands, scope, nullable),
                            concatOperand(e.getOperands(1), op, operands, scope, nullable));
                }
            }
            default -> { }
        }
        throw Refusals.unsupported("String concatenation under " + op + " requires every operand"
                + " to be a string constant or a String column: got "
                + Refusals.describeOperand(operands.get(0)) + " and "
                + Refusals.describeOperand(operands.get(1)));
    }

    @SuppressWarnings("unchecked")
    private Expression<String> stringPath(String variable, String op, List<Operand> operands,
                                          Scope scope) {
        Path<?> path = scope.path(variable);
        if (!String.class.equals(path.getJavaType())) {
            throw Refusals.unsupported("String concatenation under " + op + " requires every"
                    + " operand to be a string constant or a String column: '" + variable
                    + "' is " + path.getJavaType().getSimpleName() + " (operands "
                    + Refusals.describeOperand(operands.get(0)) + " and "
                    + Refusals.describeOperand(operands.get(1)) + ")");
        }
        return (Path<String>) path;
    }

    /**
     * Solves {@code add(field, c) eq/ne value} for the field. With no solution (e.g.
     * {@code "projects:123" == "users:" + R.id}) eq is always false and ne always true, except
     * that a NULL field stays UNKNOWN: {@code "users:" + null} is a CEL error, which denies.
     */
    private Predicate solveAddComparison(String op, Resolved.FieldPlusConstant fpc,
                                         Resolved.Constant other, Scope scope) {
        Object otherValue = other.value();
        Object addConst = PlanValues.protoValueToJava(fpc.constant().getValue());
        Object solved = PlanValues.solveAdd(otherValue, addConst, fpc.fieldIsLeft());
        if (solved == null) {
            return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                    () -> cb.isNull(scope.path(fpc.fieldVariable())));
        }
        return leaf.applyLeaf(op, scope.path(fpc.fieldVariable()), solved);
    }

    /**
     * {@code add} under a string match or unknown operator. Only a folded
     * {@code add(value, value)} against a field translates; everything else throws.
     */
    private Predicate addFoldOrError(String op, List<Operand> operands, Scope scope) {
        Operand addExprOperand = null;
        Operand otherOperand = null;
        for (Operand o : operands) {
            if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "add".equals(o.getExpression().getOperator())) {
                addExprOperand = o;
            } else {
                otherOperand = o;
            }
        }
        if (otherOperand == null) {
            throw Refusals.unsupported(
                    op + " between two add() expressions is not supported: got "
                            + Refusals.describeOperand(operands.get(0)) + " and "
                            + Refusals.describeOperand(operands.get(1))
                            + "; one operand must be a mapped attribute or constant");
        }
        List<Operand> addOperands = addExprOperand.getExpression().getOperandsList();
        if (addOperands.size() != 2) {
            throw Refusals.malformed("add requires exactly 2 operands");
        }
        Operand addLeft = addOperands.get(0);
        Operand addRight = addOperands.get(1);
        if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
            Object folded = PlanValues.foldAdd(
                    PlanValues.protoValueToJava(addLeft.getValue()),
                    PlanValues.protoValueToJava(addRight.getValue()));
            if (otherOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw Refusals.unsupported(
                        "add(const, const) compared to a non-field operand is not supported");
            }
            return leaf.applyLeaf(op, scope.path(otherOperand.getVariable()), folded);
        }
        throw Refusals.unsupported(
                "add comparison with a field reference only supports eq/ne (got " + op + ")");
    }

    /**
     * Reports an operand shape no case accepts. Reads the raw operands left to right so each
     * shape keeps its pinned message.
     */
    private IllegalArgumentException leafOperandError(String op, List<Operand> operands) {
        String variable = null;
        for (Operand o : operands) {
            switch (o.getNodeCase()) {
                case VARIABLE -> variable = o.getVariable();
                // Converting a malformed VALUE throws here, in operand order.
                case VALUE -> PlanValues.protoValueToJava(o.getValue());
                case EXPRESSION -> {
                    String innerOp = o.getExpression().getOperator();
                    // map() is only supported inside hasIntersection.
                    if ("map".equals(innerOp)) {
                        throw Refusals.unsupported(
                                "Direct comparison of map(...) to a value is not supported "
                                        + "(operator: " + op + "). Wrap the map() expression in "
                                        + "hasIntersection(map(...), [...]) instead.");
                    }
                    // eq(except(variable, list), list): the comparison form of except().
                    if ("except".equals(innerOp)) {
                        throw Refusals.exceptUnsupported();
                    }
                    // A cast, index, lambda or nested timestamp(): legal CEL with no column
                    // shape.
                    throw Refusals.unsupported(
                            "Unexpected " + innerOp + "() expression in leaf operand of " + op);
                }
                default -> throw Refusals.malformed(
                        "Unexpected operand type in leaf expression: " + o.getNodeCase());
            }
        }
        // Two constants under an operator the constant fold does not cover. The planner
        // evaluates these itself.
        if (variable == null) {
            return Refusals.malformed("Missing variable operand for " + op);
        }
        return Refusals.malformed("Missing value operand for " + op);
    }

    /**
     * Evaluates a comparison of two constants to always-true ({@code 1=1}) or always-false
     * ({@code 1=0}). Numbers compare as doubles and strings lexicographically; other types
     * support only eq/ne, and ordering them throws.
     *
     * <p>Uses the primitive operators, not {@link Double#compare}, which ranks NaN above every
     * number. A NaN constant (e.g. an unfolded {@code 0.0/0.0}) must compare false, as in CEL.
     */
    Predicate constantComparison(String op, Object left, Object right) {
        if ("eq".equals(op) || "ne".equals(op)) {
            boolean equal = (left instanceof Number ln && right instanceof Number rn)
                    ? ln.doubleValue() == rn.doubleValue()
                    : Objects.equals(left, right);
            return constant("eq".equals(op) == equal);
        }
        if (left instanceof Number ln && right instanceof Number rn) {
            double l = ln.doubleValue();
            double r = rn.doubleValue();
            return constant(switch (op) {
                case "lt" -> l < r;
                case "gt" -> l > r;
                case "le" -> l <= r;
                case "ge" -> l >= r;
                default -> throw Refusals.internal(
                        "Unsupported constant comparison operator: " + op);
            });
        }
        if (left instanceof String ls && right instanceof String rs) {
            return constant(holds(op, ls.compareTo(rs)));
        }
        // CEL cannot order two booleans or a string against a number, so the planner would
        // have rejected the policy.
        throw Refusals.malformed(
                "Cannot order constant operands of " + op + ": "
                        + PlanValues.typeName(left) + " vs " + PlanValues.typeName(right));
    }

    /** Applies {@code op} to a {@code compareTo} result. Not used for numbers. */
    private static boolean holds(String op, int cmp) {
        return switch (op) {
            case "eq" -> cmp == 0;
            case "ne" -> cmp != 0;
            case "lt" -> cmp < 0;
            case "gt" -> cmp > 0;
            case "le" -> cmp <= 0;
            case "ge" -> cmp >= 0;
            default -> throw Refusals.internal(
                    "Unsupported constant comparison operator: " + op);
        };
    }

    private Predicate constant(boolean result) {
        return result ? cb.conjunction() : cb.disjunction();
    }

    /**
     * Compares or pattern-matches two columns. Source order is kept: {@link NormalizedBinary}
     * never swaps two variables.
     */
    private Predicate fieldToFieldComparison(String op, String leftVar, String rightVar,
                                             Scope scope) {
        Expression<?> left = scope.path(leftVar);
        Expression<?> right = scope.path(rightVar);
        if (Temporal.class.isAssignableFrom(left.getJavaType())
                || Temporal.class.isAssignableFrom(right.getJavaType())) {
            throw Refusals.unsupported("Bare temporal comparison cannot preserve CEL string "
                    + "equality; use timestamp() explicitly");
        }
        boolean leftExplicit = leaf.isExplicitNull(leftVar, scope);
        boolean rightExplicit = leaf.isExplicitNull(rightVar, scope);
        if (!LeafTranslator.compatibleTypes(left.getJavaType(), right.getJavaType())) {
            return "eq".equals(op) || "ne".equals(op)
                    ? leaf.definiteEquality(op, left, right, leftExplicit, rightExplicit)
                    : tri.unknown();
        }
        // A NULL on the explicit-null side needs a definite answer and a NULL on the other
        // side needs UNKNOWN: the definite equality, made UNKNOWN wherever the omitted side is
        // NULL, whatever the explicit side holds.
        if (("eq".equals(op) || "ne".equals(op)) && leftExplicit != rightExplicit) {
            Expression<?> omitted = leftExplicit ? right : left;
            Predicate equality = tri.baseUnlessUnknown(
                    leaf.definiteEquality("eq", left, right, leftExplicit, rightExplicit),
                    () -> cb.isNull(omitted));
            return "ne".equals(op) ? tri.not(equality) : equality;
        }
        if (("eq".equals(op) || "ne".equals(op)) && leftExplicit && rightExplicit) {
            return leaf.definiteEquality(op, left, right, leftExplicit, rightExplicit);
        }
        if (COMPARISON_OPS.contains(op)) {
            return comparePredicate(op, left, right);
        }
        StringMatch match = StringMatch.of(op);
        if (match != null) {
            return fieldToFieldLike(left, right, match);
        }
        throw Refusals.unsupported(
                "Field-to-field comparison is not supported for operator '" + op + "': "
                        + leftVar + " vs " + rightVar);
    }

    /**
     * Compares two SQL expressions. A constant right-hand side is bound as a value instead
     * (see {@link ArithmeticTranslator#numericComparison}).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    Predicate comparePredicate(String op,
                               Expression left,
                               Expression right) {
        return switch (op) {
            case "eq" -> cb.equal(left, right);
            case "ne" -> cb.notEqual(left, right);
            case "lt" -> cb.lessThan(left, right);
            case "gt" -> cb.greaterThan(left, right);
            case "le" -> cb.lessThanOrEqualTo(left, right);
            case "ge" -> cb.greaterThanOrEqualTo(left, right);
            default -> throw Refusals.internal(
                    "Unsupported arithmetic comparison operator: " + op);
        };
    }

    // Escaped in this order: the escape character itself must go first.
    private static final List<String> LIKE_METACHARACTERS = List.of("\\", "%", "_", "[");

    /**
     * {@code haystack LIKE pattern(needle)} for a column needle. The needle's LIKE
     * metacharacters are escaped with nested {@code REPLACE}, as {@link PlanValues#escapeLike}
     * does for constants. {@code [} is escaped because SQL Server treats it as a character
     * class.
     *
     * <p>A NULL needle gives a NULL pattern, so the LIKE is UNKNOWN under both polarities, as
     * CEL's missing-attribute error denies under both. It also stops a dialect whose
     * {@code CONCAT} treats NULL as {@code ''} from building a match-all {@code '%%'}.
     */
    private Predicate fieldToFieldLike(Expression<?> haystack, Expression<?> needle,
                                       StringMatch match) {
        Expression<String> pattern = needle.as(String.class);
        for (String metacharacter : LIKE_METACHARACTERS) {
            pattern = cb.function("replace", String.class,
                    pattern, cb.literal(metacharacter), cb.literal("\\" + metacharacter));
        }
        if (match.leadingWildcard) {
            pattern = cb.concat(cb.literal("%"), pattern);
        }
        if (match.trailingWildcard) {
            pattern = cb.concat(pattern, cb.literal("%"));
        }
        Expression<String> guardedPattern =
                cb.<String>selectCase()
                        .when(cb.isNull(needle), cb.nullLiteral(String.class))
                        .otherwise(pattern);
        return cb.like(haystack.as(String.class), guardedPattern, '\\');
    }
}
