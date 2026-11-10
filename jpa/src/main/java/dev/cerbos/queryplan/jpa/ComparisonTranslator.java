/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.time.Duration;
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
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Translates every leaf comparison. {@link #resolve} classifies each operand into a {@link Resolved}
 * shape without converting it, and {@link #dispatch} translates the pair; conversion is lazy because
 * which error a malformed operand raises depends on the whole comparison.
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
    private final PlanWalker walker;
    private final ArithmeticTranslator arithmetic;

    ComparisonTranslator(CriteriaBuilder cb, TriPredicate tri, LeafTranslator leaf,
                         TernaryTranslator ternary, SizeTranslator sizes, PlanWalker walker) {
        this.cb = cb;
        this.tri = tri;
        this.leaf = leaf;
        this.ternary = ternary;
        this.sizes = sizes;
        this.walker = walker;
        this.arithmetic = new ArithmeticTranslator(cb, tri, leaf, this);
    }

    /** Translates an operator {@link PlanWalker#traverseExpression} does not handle by name: ternary first, then field-first. */
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

        /** {@code string(int(variable))} ({@link #stringOfIntOfFieldComparison}). */
        record StringOfIntOfField(String variable) implements Resolved {}

        /**
         * {@code string(expression)} over a boolean-valued expression
         * ({@link #stringOfBooleanComparison}).
         */
        record StringOfBoolean(Operand inner) implements Resolved {}

        /** A constant CEL error, such as {@code int()} of a literal beyond int64. */
        record AlwaysError() implements Resolved {}

        /**
         * {@code timestamp(variable) + duration(value)}, {@code duration(value) +
         * timestamp(variable)} or {@code timestamp(variable) - duration(value)}, with the
         * signed shift ({@link #shiftedTimestampLeaf}).
         */
        record ShiftedTimestampField(String variable, Duration shift) implements Resolved {}

        /** {@code timestamp(variable).timeSince()} ({@link #timeSinceLeaf}). */
        record TimeSinceField(String variable) implements Resolved {}

        /** {@code duration(value)}. */
        record DurationConstant(Duration duration) implements Resolved {}

        /**
         * {@code variable.upperAscii()}, compared with a string constant
         * ({@link #upperAsciiComparison}).
         */
        record UpperAsciiField(String variable) implements Resolved {}

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
                if ("string".equals(exprOp) && e.getOperandsCount() == 1) {
                    yield resolveStringOf(e.getOperands(0));
                }
                if (("int".equals(exprOp) || "double".equals(exprOp)) && e.getOperandsCount() == 1
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VALUE) {
                    // A literal a ternary's other branch typed (TernaryTranslator).
                    yield convertedLiteral(exprOp, e.getOperands(0));
                }
                if ("duration".equals(exprOp) && e.getOperandsCount() == 1
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VALUE) {
                    yield new Resolved.DurationConstant(
                            CelDuration.parse(e.getOperands(0).getValue()));
                }
                if ("timeSince".equals(exprOp) && e.getOperandsCount() == 1) {
                    String variable = timestampOfVariable(e.getOperands(0));
                    yield variable != null
                            ? new Resolved.TimeSinceField(variable) : new Resolved.Opaque();
                }
                if (("add".equals(exprOp) || "sub".equals(exprOp)) && e.getOperandsCount() == 2) {
                    Resolved.ShiftedTimestampField shifted = shiftedTimestamp(exprOp,
                            e.getOperands(0), e.getOperands(1));
                    if (shifted != null) {
                        yield shifted;
                    }
                }
                if ("upperAscii".equals(exprOp) && e.getOperandsCount() == 1
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE) {
                    yield new Resolved.UpperAsciiField(e.getOperands(0).getVariable());
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

    /** The operators whose expression is boolean-valued, so {@code string()} of it renders it. */
    private static final Set<String> BOOLEAN_OPS = Set.of(
            "eq", "ne", "lt", "gt", "le", "ge", "and", "or", "not", "in", "has",
            "exists", "exists_one", "all", "matches", "hasIntersection", "has_intersection",
            "isSubset", "overlaps", "ancestorOf", "descendentOf",
            "contains", "startsWith", "endsWith");

    /**
     * {@code string(arg)}: of a column, of a literal (folded), of {@code int(column)}, or of a
     * boolean-valued expression. A ternary under it is substituted away before this
     * ({@link TernaryTranslator}).
     */
    private Resolved resolveStringOf(Operand arg) {
        switch (arg.getNodeCase()) {
            case VARIABLE -> {
                return new Resolved.StringOfField(arg.getVariable());
            }
            case VALUE -> {
                return new Resolved.Constant(stringOfConstant(null, arg.getValue()));
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression inner = arg.getExpression();
                String op = inner.getOperator();
                if (("int".equals(op) || "double".equals(op)) && inner.getOperandsCount() == 1) {
                    Operand converted = inner.getOperands(0);
                    if (converted.getNodeCase() == Operand.NodeCase.VALUE
                            && converted.getValue().getKindCase() == Value.KindCase.NUMBER_VALUE) {
                        Operand text = stringOfConstant(op, converted.getValue());
                        return text == null ? new Resolved.AlwaysError()
                                : new Resolved.Constant(text);
                    }
                    if ("int".equals(op) && converted.getNodeCase() == Operand.NodeCase.VARIABLE) {
                        return new Resolved.StringOfIntOfField(converted.getVariable());
                    }
                }
                if (BOOLEAN_OPS.contains(op)) {
                    return new Resolved.StringOfBoolean(arg);
                }
                return new Resolved.Opaque();
            }
            default -> {
                return new Resolved.Opaque();
            }
        }
    }

    /**
     * CEL's {@code string()} of a literal, under {@code conversion} ({@code int} or
     * {@code double}) when there is one, as a string operand; {@code null} when the conversion
     * is a CEL error. A bare whole number is refused: the plan does not say whether the policy
     * wrote the int {@code 1000000} or the double {@code 1e+06}.
     */
    private static Operand stringOfConstant(String conversion, Value value) {
        String text;
        switch (value.getKindCase()) {
            case STRING_VALUE -> text = value.getStringValue();
            case BOOL_VALUE -> text = Boolean.toString(value.getBoolValue());
            case NUMBER_VALUE -> {
                double d = value.getNumberValue();
                if ("int".equals(conversion)) {
                    if (Double.isNaN(d) || d <= -0x1p63 || d >= 0x1p63) {
                        return null;
                    }
                    text = Long.toString((long) d);
                } else if (conversion == null && d == Math.rint(d) && !Double.isInfinite(d)) {
                    throw Refusals.unsupported("string() of a whole number literal is not"
                            + " supported: the plan does not say whether the policy wrote an"
                            + " int or a double, which CEL renders differently (\"1000000\""
                            + " against \"1e+06\")");
                } else {
                    text = Double.isNaN(d) ? "NaN"
                            : Double.isInfinite(d) ? (d > 0 ? "+Inf" : "-Inf")
                            : d == 0 ? (1 / d < 0 ? "-0" : "0")
                            : CelDoubleText.render(d);
                }
            }
            default -> throw Refusals.unsupported("string() of a "
                    + PlanValues.typeName(PlanValues.protoValueToJava(value))
                    + " literal is not supported");
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setStringValue(text)).build();
    }

    /** {@code int(literal)} or {@code double(literal)} as the constant it folds to. */
    private static Resolved convertedLiteral(String conversion, Operand literal) {
        Value v = literal.getValue();
        if (v.getKindCase() != Value.KindCase.NUMBER_VALUE) {
            return new Resolved.Opaque();
        }
        double d = v.getNumberValue();
        if ("int".equals(conversion)) {
            if (Double.isNaN(d) || d <= -0x1p63 || d >= 0x1p63) {
                return new Resolved.AlwaysError();
            }
            d = (long) d;
        }
        return new Resolved.Constant(
                Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(d)).build());
    }

    /** The variable of {@code timestamp(variable)}, else {@code null}. */
    private static String timestampOfVariable(Operand o) {
        if (o.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"timestamp".equals(o.getExpression().getOperator())
                || o.getExpression().getOperandsCount() != 1
                || o.getExpression().getOperands(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
            return null;
        }
        return o.getExpression().getOperands(0).getVariable();
    }

    /** The duration of {@code duration(value)}, else {@code null}. */
    private static Duration durationConstant(Operand o) {
        if (o.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"duration".equals(o.getExpression().getOperator())
                || o.getExpression().getOperandsCount() != 1
                || o.getExpression().getOperands(0).getNodeCase() != Operand.NodeCase.VALUE) {
            return null;
        }
        return CelDuration.parse(o.getExpression().getOperands(0).getValue());
    }

    private static Resolved.ShiftedTimestampField shiftedTimestamp(String op, Operand l,
                                                                   Operand r) {
        String variable = timestampOfVariable(l);
        Duration shift = durationConstant(r);
        if (variable != null && shift != null) {
            return new Resolved.ShiftedTimestampField(variable,
                    "sub".equals(op) ? shift.negated() : shift);
        }
        variable = timestampOfVariable(r);
        shift = durationConstant(l);
        // duration - timestamp has no CEL overload.
        if ("add".equals(op) && variable != null && shift != null) {
            return new Resolved.ShiftedTimestampField(variable, shift);
        }
        return null;
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
        if (left instanceof Resolved.AlwaysError || right instanceof Resolved.AlwaysError) {
            return tri.unknown();
        }
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
            if (left instanceof Resolved.ShiftedTimestampField shifted
                    && right instanceof Resolved.TimestampConstant tsConst) {
                return shiftedTimestampLeaf(op, shifted, tsConst.instant(), scope);
            }
            if (left instanceof Resolved.TimestampConstant tsConst
                    && right instanceof Resolved.ShiftedTimestampField shifted) {
                return shiftedTimestampLeaf(NormalizedBinary.mirror(op), shifted,
                        tsConst.instant(), scope);
            }
            if (left instanceof Resolved.TimeSinceField since
                    && right instanceof Resolved.DurationConstant d) {
                return timeSinceLeaf(op, since, d.duration(), scope);
            }
            if (left instanceof Resolved.DurationConstant d
                    && right instanceof Resolved.TimeSinceField since) {
                return timeSinceLeaf(NormalizedBinary.mirror(op), since, d.duration(), scope);
            }
            // An attribute is never a CEL timestamp: check() receives JSON values, so a string,
            // number, bool, list or map ordered against a timestamp has no overload and errors.
            if (ORDERING_OPS.contains(op)
                    && ((left instanceof Resolved.Field && right instanceof Resolved.TimestampConstant)
                    || (left instanceof Resolved.TimestampConstant
                            && right instanceof Resolved.Field))) {
                return tri.unknown();
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
            if (("eq".equals(op) || "ne".equals(op))
                    && left instanceof Resolved.StringOfIntOfField sif
                    && right instanceof Resolved.Constant c
                    && c.value() instanceof String text) {
                return stringOfIntOfFieldComparison(op, sif, text, operands, scope);
            }
            if (("eq".equals(op) || "ne".equals(op))
                    && left instanceof Resolved.StringOfBoolean sb
                    && right instanceof Resolved.Constant c
                    && c.value() instanceof String text) {
                return stringOfBooleanComparison(op, sb, text, scope);
            }
            if (left instanceof Resolved.UpperAsciiField upper
                    && right instanceof Resolved.Constant c) {
                return upperAsciiComparison(op, upper, c.value(), scope);
            }
            if (left instanceof Resolved.Constant c
                    && right instanceof Resolved.UpperAsciiField upper) {
                return upperAsciiComparison(NormalizedBinary.mirror(op), upper, c.value(), scope);
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
                if (ORDERING_OPS.contains(op) && ordersStringAgainstNumericAdd(operands, scope)) {
                    return tri.unknown();
                }
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
     * {@code timestamp(field) op timestamp(constant)}. Only {@link Instant} and {@link OffsetDateTime}
     * columns translate; a zoneless type would need a guessed zone. A NULL column is UNKNOWN.
     */
    private Predicate timestampLeaf(String op, Resolved.TimestampField field,
                                    Resolved.TimestampConstant constant, Scope scope) {
        return timestampLeaf(op, field.variable(), constant.instant(), scope);
    }

    private Predicate timestampLeaf(String op, String variable, Instant instant, Scope scope) {
        Path<?> path = scope.path(variable);
        return leaf.withOverride(op, path, instant,
                () -> leaf.defaultLeaf(op, path, temporalBound(variable, path, op, instant)));
    }

    /** {@code instant} as the column's Java type; see {@link #timestampLeaf}. */
    private static Object temporalBound(String variable, Path<?> path, String op,
                                        Instant instant) {
        Class<?> javaType = path.getJavaType();
        if (Instant.class.equals(javaType)) {
            return instant;
        }
        if (OffsetDateTime.class.equals(javaType)) {
            return instant.atOffset(ZoneOffset.UTC);
        }
        // Unmapped, not unsupported: the caller fixes it by remapping the column or
        // registering an override.
        throw Refusals.unmapped(
                "timestamp() comparison requires a column mapped to java.time.Instant "
                        + "or java.time.OffsetDateTime, but '" + variable
                        + "' maps to " + javaType.getSimpleName()
                        + ". Other temporal representations (LocalDateTime, "
                        + "java.util.Date, String) are ambiguous about the absolute "
                        + "instant they store; remap the column or register an "
                        + "OperatorFunction override for '" + op + "'.");
    }

    /** CEL's timestamp range: years 1 to 9999, in UTC. */
    private static final Instant MIN_TIMESTAMP = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant MAX_TIMESTAMP = Instant.parse("9999-12-31T23:59:59.999999999Z");

    /**
     * {@code timestamp(field) + shift op c}, solved as {@code timestamp(field) op c - shift}. A column
     * within {@code shift} of CEL's year range is UNKNOWN; a solved constant outside it is refused.
     */
    private Predicate shiftedTimestampLeaf(String op, Resolved.ShiftedTimestampField shifted,
                                           Instant c, Scope scope) {
        Duration shift = shifted.shift();
        Instant solved = c.minus(shift);
        if (solved.isBefore(MIN_TIMESTAMP) || solved.isAfter(MAX_TIMESTAMP)) {
            throw Refusals.unsupported("timestamp() + duration() compared with a timestamp whose"
                    + " solved bound leaves CEL's timestamp range (years 1 to 9999)");
        }
        Predicate base = timestampLeaf(op, shifted.variable(), solved, scope);
        if (shift.isZero()) {
            return base;
        }
        Path<?> path = scope.path(shifted.variable());
        // The column values whose sum overflows: past MAX - shift, or before MIN - shift.
        String overflowOp = shift.isNegative() ? "lt" : "gt";
        Instant edge = shift.isNegative() ? MIN_TIMESTAMP.minus(shift) : MAX_TIMESTAMP.minus(shift);
        return tri.baseUnlessUnknown(base, () -> leaf.defaultLeaf(overflowOp, path,
                temporalBound(shifted.variable(), path, overflowOp, edge)));
    }

    /** {@code timestamp(field).timeSince() op d}, solved as {@code field op' now - d}; {@code now} is read at build time. */
    private Predicate timeSinceLeaf(String op, Resolved.TimeSinceField since, Duration d,
                                    Scope scope) {
        Instant bound = Instant.now().minus(d);
        if (bound.isBefore(MIN_TIMESTAMP) || bound.isAfter(MAX_TIMESTAMP)) {
            throw Refusals.unsupported("timeSince() compared with a duration whose solved bound"
                    + " leaves CEL's timestamp range (years 1 to 9999)");
        }
        return timestampLeaf(NormalizedBinary.mirror(op), since.variable(), bound, scope);
    }

    private Predicate timestampConstantComparison(String op, Instant left, Instant right) {
        return constant(holds(op, left.compareTo(right)));
    }

    /** {@code 2^63}: CEL's int() errors at or beyond it in either direction. */
    private static final double INT64_LIMIT = 0x1p63;

    /**
     * {@code int(column) op c} over a Double or Integer column, solved for the column because CEL's
     * {@code int()} truncates where SQL {@code CAST} rounds. NULL or out-of-int64 is UNKNOWN.
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
     * {@code int(s)} over a String column, parsed as Go's {@code strconv.ParseInt}: compared by sign and
     * then by trimmed digits, since SQL {@code CAST} accepts other spellings.
     */
    private final class IntText {
        private static final String INT64_MAX = "9223372036854775807";
        private static final String INT64_MIN_MAGNITUDE = "9223372036854775808";

        private final Expression<String> s;

        IntText(Expression<String> s) {
            this.s = ExactStrings.exact(cb, s);
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
     * {@code string(column) eq/ne "text"}, solved per column type: a String compares as is, a Boolean
     * against {@code true}/{@code false}, a number against the one double CEL renders as the text. A
     * NULL column is UNKNOWN; other column types are refused.
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

    /** CEL's rendering of an int: no sign on zero, no leading zeros, no exponent. */
    private static final Pattern INT_RENDERING = Pattern.compile("0|-?[1-9][0-9]*");

    /** The int64 whose CEL rendering is {@code text}, else {@code null}. */
    private static Long canonicalInt64(String text) {
        if (!INT_RENDERING.matcher(text).matches()) {
            return null;
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException beyondInt64) {
            return null;
        }
    }

    /** {@code string(int(column)) eq/ne "text"}: holds where the column converts to that int; UNKNOWN where {@code int()} errors. */
    @SuppressWarnings("unchecked")
    private Predicate stringOfIntOfFieldComparison(String op, Resolved.StringOfIntOfField field,
                                                   String text, List<Operand> operands,
                                                   Scope scope) {
        Path<?> path = scope.path(field.variable());
        Class<?> type = path.getJavaType();
        Long k = canonicalInt64(text);
        boolean ne = "ne".equals(op);
        if (String.class.equals(type)) {
            IntText parsed = new IntText((Expression<String>) path);
            Predicate equal;
            if (k == null) {
                equal = cb.disjunction();
            } else if (k == 0) {
                equal = cb.equal(parsed.digits(), "");
            } else {
                equal = cb.and(k < 0 ? parsed.negative() : tri.not(parsed.negative()),
                        cb.equal(parsed.digits(), Long.toString(k).replace("-", "")));
            }
            return tri.baseUnlessUnknown(ne ? tri.not(equal) : equal, parsed::unparsable);
        }
        if (Double.class.equals(type) || Integer.class.equals(type)) {
            if (k != null && Math.abs(k) < 0x1p53) {
                return intOfFieldComparison(op, new Resolved.IntOfField(field.variable()), k,
                        operands, scope);
            }
            Expression<Double> d = path.as(Double.class);
            // Beyond 2^53 every double is whole, so int() is the identity where it holds.
            Predicate equal = k != null && Double.class.equals(type) && (long) (double) k == k
                    ? cb.equal(d, (double) k) : cb.disjunction();
            return tri.baseUnlessUnknown(ne ? tri.not(equal) : equal,
                    () -> Double.class.equals(type)
                            ? cb.or(cb.isNull(path), cb.le(d, -INT64_LIMIT), cb.ge(d, INT64_LIMIT))
                            : cb.isNull(path));
        }
        if (Long.class.equals(type)) {
            Predicate equal = k == null ? cb.disjunction() : cb.equal(path, k);
            return tri.baseUnlessUnknown(ne ? tri.not(equal) : equal, () -> cb.isNull(path));
        }
        throw leafOperandError(op, operands);
    }

    /**
     * {@code string(p) eq/ne "text"} over a boolean-valued {@code p}, which CEL renders as
     * exactly {@code "true"} or {@code "false"}: the predicate itself, its negation, or for any
     * other text a constant that stays UNKNOWN where {@code p} is, since CEL's error there
     * denies under both polarities.
     */
    private Predicate stringOfBooleanComparison(String op, Resolved.StringOfBoolean field,
                                                String text, Scope scope) {
        Supplier<Predicate> inner = () -> walker.traverse(field.inner(), scope);
        boolean eq = "eq".equals(op);
        return switch (text) {
            case "true" -> eq ? inner.get() : tri.not(inner.get());
            case "false" -> eq ? tri.not(inner.get()) : inner.get();
            default -> eq ? tri.not(tri.determined(inner)) : tri.determined(inner);
        };
    }

    private static final String ASCII_LOWER = "abcdefghijklmnopqrstuvwxyz";

    /**
     * {@code column.upperAscii() op "text"}, as 26 nested case-sensitive {@code REPLACE}s because SQL
     * {@code UPPER} folds beyond ASCII. A NULL column is UNKNOWN.
     */
    @SuppressWarnings("unchecked")
    private Predicate upperAsciiComparison(String op, Resolved.UpperAsciiField field,
                                           Object constant, Scope scope) {
        Path<?> path = scope.path(field.variable());
        if (!String.class.equals(path.getJavaType())) {
            throw Refusals.unsupported("upperAscii() requires a String column: '"
                    + field.variable() + "' is " + path.getJavaType().getSimpleName());
        }
        if (!(constant instanceof String text)) {
            if (ORDERING_OPS.contains(op)) {
                return tri.unknown();
            }
            return matchesNothing(op, path);
        }
        Expression<String> folded = (Path<String>) path;
        for (char lower : ASCII_LOWER.toCharArray()) {
            folded = cb.function("replace", String.class, folded,
                    cb.literal(String.valueOf(lower)),
                    cb.literal(String.valueOf(Character.toUpperCase(lower))));
        }
        return comparePredicate(op, folded, cb.literal(text));
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
     * A comparison whose {@code add} is string concatenation, or {@code null} when it is numeric: any
     * string leaf makes it concatenation. A NULL column keeps it UNKNOWN.
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

    /** Whether a string is ordered against a numeric {@code add}, which CEL errors on (#575). */
    private boolean ordersStringAgainstNumericAdd(List<Operand> operands, Scope scope) {
        for (int side = 0; side < 2; side++) {
            Operand add = operands.get(side);
            Operand other = operands.get(1 - side);
            if (add.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "add".equals(add.getExpression().getOperator())
                    && !isStringTyped(add, scope)
                    && hasNumberLeaf(add, scope)
                    && isStringTyped(other, scope)) {
                return true;
            }
        }
        return false;
    }

    /** Whether a number constant or numeric column is anywhere under the {@code add} {@code o}. */
    private boolean hasNumberLeaf(Operand o, Scope scope) {
        return switch (o.getNodeCase()) {
            case VALUE -> o.getValue().getKindCase() == Value.KindCase.NUMBER_VALUE;
            case VARIABLE -> isNumericType(scope.path(o.getVariable()).getJavaType());
            case EXPRESSION -> "add".equals(o.getExpression().getOperator())
                    && o.getExpression().getOperandsList().stream()
                            .anyMatch(child -> hasNumberLeaf(child, scope));
            default -> false;
        };
    }

    private static boolean isNumericType(Class<?> type) {
        return Number.class.isAssignableFrom(type)
                || (type.isPrimitive() && type != boolean.class && type != char.class);
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
     * Evaluates a comparison of two constants to {@code 1=1} or {@code 1=0}, with primitive operators so a
     * NaN compares false as in CEL.
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
        left = ExactStrings.forOperator(cb, op, left);
        right = ExactStrings.forOperator(cb, op, right);
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
     * {@code haystack LIKE pattern(needle)} for a column needle, escaped with nested {@code REPLACE}. A
     * NULL needle gives a NULL pattern, keeping the LIKE UNKNOWN.
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
        return cb.like(ExactStrings.exact(cb, haystack.as(String.class)), guardedPattern, '\\');
    }
}
