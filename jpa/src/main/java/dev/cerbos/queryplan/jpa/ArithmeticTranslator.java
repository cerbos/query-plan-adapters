/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Lowers arithmetic in a comparison operand to SQL, in IEEE double space: columns cast to a real
 * double, constant subtrees fold in Java, constants bind as doubles. {@code int()}-typed subtrees go
 * to {@link IntArithmetic}.
 */
final class ArithmeticTranslator {

    static final Set<String> ARITHMETIC_OPS = Set.of("add", "sub", "mult", "div", "mod");

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final ComparisonTranslator comparisons;
    private final IntArithmetic ints;

    /**
     * The division the enclosing rewrite has already split on a zero divisor, while its non-zero
     * arm is built. Its {@code NULLIF} guard never fires there, so arithmetic around it lowers.
     */
    private Operand guardedDivision;

    ArithmeticTranslator(CriteriaBuilder cb, TriPredicate tri, LeafTranslator leaf,
                         ComparisonTranslator comparisons) {
        this.cb = cb;
        this.tri = tri;
        this.leaf = leaf;
        this.comparisons = comparisons;
        this.ints = new IntArithmetic(cb, tri, leaf, this, comparisons);
    }

    /**
     * Translates {@code cmp(arith(...), other)} in double space, since attribute values are CEL doubles.
     * Mixing int and double, or {@code mod} on a double, is UNKNOWN. An {@link OperatorFunction} override
     * is consulted only when one side is a constant.
     */
    Predicate numericComparison(String op, List<Operand> operands, Scope scope) {
        IntArithmetic.CelType left = IntArithmetic.typeOf(operands.get(0), scope);
        IntArithmetic.CelType right = IntArithmetic.typeOf(operands.get(1), scope);
        if (left == IntArithmetic.CelType.ERROR || right == IntArithmetic.CelType.ERROR) {
            return tri.unknown();
        }
        if (left != null && right != null
                && (left == IntArithmetic.CelType.INT || right == IntArithmetic.CelType.INT)) {
            return ints.comparison(op, operands, scope, left, right);
        }
        // `NaN != x` is TRUE in CEL, but `NULL != x` is UNKNOWN, so a possibly-zero division
        // is folded against the comparison rather than lowered to NULL.
        Predicate folded = tryDivisionByZeroComparison(op, operands, scope);
        if (folded != null) {
            return folded;
        }
        return numericComparisonWithoutZeroGuard(op, operands, scope);
    }

    /**
     * Rewrites a comparison against a possibly-zero division into nested ternaries over the divisor and
     * numerator, so NaN and infinities compare as in CEL; {@code null} when the shape does not apply.
     */
    private Predicate tryDivisionByZeroComparison(
            String op, List<Operand> operands, Scope scope) {
        if (hasZeroCapableDivision(operands.get(0), scope)
                && hasZeroCapableDivision(operands.get(1), scope)) {
            // Only one side can be rewritten; the other would still lower to NULL.
            throw Refusals.unsupported(
                    "a comparison with a zero-capable division on BOTH sides is not "
                            + "supported: only one side can be folded into IEEE arms and "
                            + "the other would lower to SQL NULL");
        }
        for (int side = 0; side < 2; side++) {
            Operand candidate = operands.get(side);
            // The division is the operand itself, or nested in arithmetic whose other leaves are
            // constants, e.g. `a / a + 1.0`.
            Operand divisionOperand = isZeroCapableDivisionOperand(candidate, scope)
                    ? candidate : nestedZeroCapableDivision(candidate, scope);
            if (divisionOperand == null) {
                continue;
            }
            PlanResourcesFilter.Expression division = divisionOperand.getExpression();
            // A non-zero constant divisor cannot divide by zero. A zero constant still needs
            // the rewrite.
            NumericOperand divisor = resolveNumericOperand(division.getOperands(1), scope);
            if (divisor instanceof NumericOperand.Constant dc && dc.value() != 0.0) {
                continue;
            }
            NumericOperand dividend =
                    resolveNumericOperand(division.getOperands(0), scope);
            // A fully constant division folds in Java instead.
            if (divisor instanceof NumericOperand.Constant
                    && dividend instanceof NumericOperand.Constant) {
                continue;
            }
            // `n / -0.0` is the opposite infinity from `n / 0.0`. A constant divisor keeps
            // its sign bit. SQL cannot read the sign of a stored zero (-0.0 = 0.0 holds), so
            // a SQL divisor is taken as positive only where the sign cannot change the result
            // (cerbos/query-plan-adapters#312).
            boolean negativeZeroDivisor = divisor instanceof NumericOperand.Constant zc
                    && Double.doubleToRawLongBits(zc.value()) != 0L;
            double positiveDividendResult = negativeZeroDivisor
                    ? Double.NEGATIVE_INFINITY
                    : Double.POSITIVE_INFINITY;
            double negativeDividendResult = negativeZeroDivisor
                    ? Double.POSITIVE_INFINITY
                    : Double.NEGATIVE_INFINITY;

            boolean divisionIsLeft = side == 0;
            Operand other = operands.get(divisionIsLeft ? 1 : 0);

            if (divisor instanceof NumericOperand.Sql
                    && !zeroDivisorSignIsMoot(division, scope)
                    && signOfZeroDecides(op, candidate, divisionOperand, other,
                            divisionIsLeft, scope)) {
                throw Refusals.unsupported(
                        "a comparison with a division by a floating-point expression that may "
                                + "be zero is not supported here: CEL divides by a signed zero "
                                + "into an infinity of either sign, and the comparison's "
                                + "result depends on that sign, which SQL cannot read from a "
                                + "stored zero (-0.0 = 0.0)");
            }

            // A non-finite compares the same way against every present value, so against a
            // column the arm is folded in Java and only a NULL column makes it UNKNOWN. Around
            // a nested division the arm is the whole side folded with the non-finite in place,
            // which IEEE arithmetic in Java carries as CEL does.
            Function<Double, Predicate> arm = divisionResult -> {
                double nonFinite = divisionOperand == candidate ? divisionResult
                        : foldAround(candidate, divisionOperand, divisionResult, scope);
                NumericOperand o = resolveNumericOperand(other, scope);
                if (o instanceof NumericOperand.Constant oc) {
                    return divisionIsLeft
                            ? comparisons.constantComparison(op, nonFinite, oc.value())
                            : comparisons.constantComparison(op, oc.value(), nonFinite);
                }
                if (Double.isFinite(nonFinite)) {
                    // e.g. `1.0 / (1.0 / a)`: finite, so compared in SQL like any constant.
                    List<Operand> substituted = divisionIsLeft
                            ? List.of(numberOperand(nonFinite), other)
                            : List.of(other, numberOperand(nonFinite));
                    return numericComparisonWithoutZeroGuard(op, substituted, scope);
                }
                Predicate folded = divisionIsLeft
                        ? comparisons.constantComparison(op, nonFinite, 0.0)
                        : comparisons.constantComparison(op, 0.0, nonFinite);
                return tri.baseUnlessUnknown(folded, () -> cb.isNull(sqlOf(o)));
            };

            Supplier<Predicate> zeroDivisor = divisor instanceof NumericOperand.Constant
                    ? () -> cb.conjunction()
                    : () -> cb.equal(sqlOf(divisor), 0.0);
            Supplier<Predicate> zeroDividend = () -> cb.equal(sqlOf(dividend), 0.0);
            Supplier<Predicate> positiveDividend = () -> cb.gt(sqlOf(dividend), 0.0);

            return tri.ternary(
                    zeroDivisor,
                    () -> tri.ternary(
                            zeroDividend,
                            () -> arm.apply(Double.NaN),
                            () -> tri.ternary(
                                    positiveDividend,
                                    () -> arm.apply(positiveDividendResult),
                                    () -> arm.apply(negativeDividendResult))),
                    () -> withGuardedDivision(divisionOperand,
                            () -> numericComparisonWithoutZeroGuard(op, operands, scope)));
        }
        return null;
    }

    /**
     * Whether the sign of a zero divisor cannot matter: an integral column never holds -0.0,
     * and in {@code a / a} a zero divisor makes the dividend zero too, so the result is NaN.
     */
    private boolean zeroDivisorSignIsMoot(PlanResourcesFilter.Expression division, Scope scope) {
        Operand divisor = division.getOperands(1);
        if (divisor.equals(division.getOperands(0))) {
            return true;
        }
        if (divisor.getNodeCase() != Operand.NodeCase.VARIABLE) {
            return false;
        }
        Class<?> type = scope.path(divisor.getVariable()).getJavaType();
        return Integer.class.equals(type) || Long.class.equals(type)
                || Short.class.equals(type) || Byte.class.equals(type);
    }

    /**
     * Whether the comparison holds differently for a division result of {@code +Inf} and of
     * {@code -Inf}, the two values dividing a non-zero dividend by a signed zero gives.
     */
    private boolean signOfZeroDecides(String op, Operand candidate, Operand divisionOperand,
            Operand other, boolean divisionIsLeft, Scope scope) {
        double pos = divisionOperand == candidate ? Double.POSITIVE_INFINITY
                : foldAround(candidate, divisionOperand, Double.POSITIVE_INFINITY, scope);
        double neg = divisionOperand == candidate ? Double.NEGATIVE_INFINITY
                : foldAround(candidate, divisionOperand, Double.NEGATIVE_INFINITY, scope);
        if (Double.isNaN(pos) && Double.isNaN(neg)) {
            return false;
        }
        NumericOperand o = resolveNumericOperand(other, scope);
        if (o instanceof NumericOperand.Constant oc) {
            return ieeeHolds(op, pos, oc.value(), divisionIsLeft)
                    != ieeeHolds(op, neg, oc.value(), divisionIsLeft);
        }
        if (Double.isInfinite(pos) && Double.isInfinite(neg)) {
            // Folded against any present value, as the arm does.
            return ieeeHolds(op, pos, 0.0, divisionIsLeft)
                    != ieeeHolds(op, neg, 0.0, divisionIsLeft);
        }
        // Finite results are compared in SQL, where 0.0 and -0.0 are the same value.
        return pos != neg;
    }

    static boolean ieeeHolds(String op, double side, double other, boolean sideIsLeft) {
        double l = sideIsLeft ? side : other;
        double r = sideIsLeft ? other : side;
        return switch (op) {
            case "eq" -> l == r;
            case "ne" -> l != r;
            case "lt" -> l < r;
            case "gt" -> l > r;
            case "le" -> l <= r;
            case "ge" -> l >= r;
            default -> throw Refusals.internal("Unsupported comparison operator: " + op);
        };
    }

    private Predicate withGuardedDivision(Operand division, Supplier<Predicate> body) {
        Operand previous = guardedDivision;
        guardedDivision = division;
        try {
            return body.get();
        } finally {
            guardedDivision = previous;
        }
    }

    /** Whether {@code operand} is, or has nested in its arithmetic, a zero-capable division. */
    boolean hasZeroCapableDivision(Operand operand, Scope scope) {
        return isZeroCapableDivisionOperand(operand, scope)
                || operand.getNodeCase() == Operand.NodeCase.EXPRESSION
                && ARITHMETIC_OPS.contains(operand.getExpression().getOperator())
                && containsZeroCapableDivision(operand.getExpression(), scope);
    }

    /**
     * The one zero-capable division nested under the arithmetic {@code operand}, or
     * {@code null} when there is none. More than one cannot be split into arms and is refused
     * when the operand is lowered.
     */
    private Operand nestedZeroCapableDivision(Operand operand, Scope scope) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !ARITHMETIC_OPS.contains(operand.getExpression().getOperator())) {
            return null;
        }
        List<Operand> found = new ArrayList<>();
        collectZeroCapableDivisions(operand.getExpression(), scope, found);
        return found.size() == 1 ? found.get(0) : null;
    }

    private void collectZeroCapableDivisions(PlanResourcesFilter.Expression expr, Scope scope,
                                             List<Operand> found) {
        for (Operand child : expr.getOperandsList()) {
            if (isZeroCapableDivisionOperand(child, scope)) {
                found.add(child);
            } else if (child.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && ARITHMETIC_OPS.contains(child.getExpression().getOperator())) {
                collectZeroCapableDivisions(child.getExpression(), scope, found);
            }
        }
    }

    /**
     * {@code side} evaluated in Java with {@code division} replaced by {@code value}. Only a
     * side whose other leaves are constants folds; one that still reads a column is refused,
     * since SQL has no NaN or infinity to carry through it.
     */
    private double foldAround(Operand side, Operand division, double value, Scope scope) {
        if (resolveNumericOperand(substitute(side, division, numberOperand(value)), scope)
                instanceof NumericOperand.Constant c) {
            return c.value();
        }
        throw nestedDivisionUnsupported();
    }

    private static Operand substitute(Operand node, Operand target, Operand replacement) {
        if (node == target) {
            return replacement;
        }
        if (node.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return node;
        }
        PlanResourcesFilter.Expression.Builder e = node.getExpression().toBuilder().clearOperands();
        node.getExpression().getOperandsList()
                .forEach(child -> e.addOperands(substitute(child, target, replacement)));
        return Operand.newBuilder().setExpression(e).build();
    }

    private static Operand numberOperand(double value) {
        return Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(value)).build();
    }

    private static UnsupportedPlanShapeException nestedDivisionUnsupported() {
        return Refusals.unsupported(
                "arithmetic composed on a division whose denominator may be "
                        + "zero is not supported: CEL carries the resulting NaN "
                        + "or infinity through the surrounding arithmetic and "
                        + "SQL has no value that does");
    }

    private Predicate numericComparisonWithoutZeroGuard(
            String op, List<Operand> operands, Scope scope) {
        for (int i = 0; i < 2; i++) {
            if (operands.get(1 - i).getNodeCase() == Operand.NodeCase.VARIABLE
                    && resolveNumericOperand(operands.get(i), scope)
                            instanceof NumericOperand.Constant c
                    && Double.isNaN(c.value())) {
                return comparisons.constantComparison(op, Double.NaN, 0.0);
            }
        }
        NumericOperand left = resolveNumericOperand(operands.get(0), scope);
        NumericOperand right = resolveNumericOperand(operands.get(1), scope);
        if (left instanceof NumericOperand.Constant lc
                && right instanceof NumericOperand.Constant rc) {
            return comparisons.constantComparison(op, lc.value(), rc.value());
        }
        Operand fieldOperand = operands.get(0);
        if (left instanceof NumericOperand.Constant) {
            fieldOperand = operands.get(1);
            NumericOperand tmp = left;
            left = right;
            right = tmp;
            op = NormalizedBinary.mirror(op);
        }
        Expression<Double> lhs = sqlOf(left);
        if (right instanceof NumericOperand.Constant rc) {
            // Bind a double parameter: cb.literal would inline an exact NUMERIC literal on
            // H2 and PostgreSQL.
            String cmpOp = op;
            double v = rc.value();
            if (Double.isNaN(v) && !"eq".equals(cmpOp) && !"ne".equals(cmpOp)) {
                // An ordered comparison with NaN is false for any present value. Check the
                // original column for NULL: casting a string column to double could fail in
                // SQL.
                Expression<?> presence =
                        fieldOperand.getNodeCase() == Operand.NodeCase.VARIABLE
                                ? scope.path(fieldOperand.getVariable()) : lhs;
                return leaf.withOverride(cmpOp, lhs, v, () -> tri.baseUnlessUnknown(
                        cb.disjunction(), () -> cb.isNull(presence)));
            }
            return leaf.withOverride(cmpOp, lhs, rc.value(), () -> switch (cmpOp) {
                case "eq" -> cb.equal(lhs, v);
                case "ne" -> cb.notEqual(lhs, v);
                case "lt" -> cb.lt(lhs, v);
                case "gt" -> cb.gt(lhs, v);
                case "le" -> cb.le(lhs, v);
                case "ge" -> cb.ge(lhs, v);
                default -> throw Refusals.internal(
                        "Unsupported arithmetic comparison operator: " + cmpOp);
            });
        }
        return comparisons.comparePredicate(op, lhs, sqlOf(right));
    }

    private static Expression<Double> sqlOf(NumericOperand o) {
        return ((NumericOperand.Sql) o).expr();
    }

    /** A constant subtree folded in Java with IEEE semantics, or a double SQL expression. */
    sealed interface NumericOperand {
        record Constant(double value) implements NumericOperand {}
        record Sql(Expression<Double> expr) implements NumericOperand {}
    }

    private boolean isZeroCapableDivisionOperand(Operand operand, Scope scope) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return false;
        }
        PlanResourcesFilter.Expression expr = operand.getExpression();
        if (!"div".equals(expr.getOperator()) || expr.getOperandsCount() != 2) {
            return false;
        }
        NumericOperand divisor = resolveNumericOperand(expr.getOperands(1), scope);
        NumericOperand dividend = resolveNumericOperand(expr.getOperands(0), scope);
        if (divisor instanceof NumericOperand.Constant dc && dc.value() != 0.0) {
            return false;
        }
        return !(divisor instanceof NumericOperand.Constant
                && dividend instanceof NumericOperand.Constant);
    }

    private boolean containsZeroCapableDivision(
            PlanResourcesFilter.Expression expr, Scope scope) {
        String op = expr.getOperator();
        if (!ARITHMETIC_OPS.contains(op)) {
            return false;
        }
        for (Operand child : expr.getOperandsList()) {
            if (child != guardedDivision && isZeroCapableDivisionOperand(child, scope)) {
                return true;
            }
            if (child.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && containsZeroCapableDivision(child.getExpression(), scope)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolves an operand to double space ({@code 0.1} is NUMERIC on H2 and PostgreSQL). A column divisor
     * becomes {@code NULLIF(d, 0)} and a constant zero divisor a NULL literal; see
     * {@link #tryDivisionByZeroComparison}.
     */
    NumericOperand resolveNumericOperand(Operand operand, Scope scope) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                Expression<?> raw = scope.path(operand.getVariable());
                Class<?> javaType = raw.getJavaType();
                if (javaType != null && !Number.class.isAssignableFrom(javaType)
                        && !javaType.isPrimitive()) {
                    throw Refusals.unsupported(
                            "Arithmetic over a " + javaType.getSimpleName() + " attribute is a"
                                    + " CEL no-overload error, which SQL would coerce");
                }
                if (javaType == boolean.class) {
                    throw Refusals.unsupported(
                            "Arithmetic over a boolean attribute is a CEL no-overload error,"
                                    + " which SQL would coerce");
                }
                @SuppressWarnings("unchecked")
                Expression<? extends Number> path = (Expression<? extends Number>) raw;
                return new NumericOperand.Sql(toIeeeDouble(path));
            }
            case VALUE -> {
                // Read the raw double: protoValueToJava narrows integral values to Long and
                // would lose the sign of -0.0.
                if (operand.getValue().getKindCase()
                        == Value.KindCase.NUMBER_VALUE) {
                    return new NumericOperand.Constant(operand.getValue().getNumberValue());
                }
                Object v = PlanValues.protoValueToJava(operand.getValue());
                if (!(v instanceof Number n)) {
                    // e.g. `R.attr.aString + "x" < "y"`: legal CEL, but this path is numeric only.
                    throw Refusals.unsupported(
                            "Arithmetic comparison requires numeric operands, got "
                                    + PlanValues.typeName(v));
                }
                return new NumericOperand.Constant(n.doubleValue());
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression expr = operand.getExpression();
                String op = expr.getOperator();
                if ("mod".equals(op)) {
                    // Typed int or error by IntArithmetic; reached only beside a non-numeric
                    // operand, e.g. `aString % 2`.
                    throw Refusals.unsupported(
                            "mod is not supported in comparisons unless each operand is int()"
                                    + " of a numeric column or an integral constant: CEL % is"
                                    + " integer-only while attribute values are always doubles"
                                    + " at check time, so a satisfiable policy must cast with"
                                    + " int() first");
                }
                if (ARITHMETIC_OPS.contains(op) && !"div".equals(op)
                        && containsZeroCapableDivision(expr, scope)) {
                    // CEL carries NaN or infinity through the surrounding arithmetic, while
                    // NULLIF makes it NULL, so `NaN + 1.0 != 2.0` would drop a row the PDP
                    // allows. The rewrite folds such arithmetic only when its other leaves are
                    // constants, and lowers it only in the arm where the divisor is non-zero.
                    throw nestedDivisionUnsupported();
                }
                if (!ARITHMETIC_OPS.contains(op)) {
                    // A cast or a size() inside arithmetic: legal CEL, no lowering.
                    throw Refusals.unsupported(
                            "Unexpected " + op + "() expression inside an arithmetic "
                                    + "comparison operand");
                }
                if (expr.getOperandsCount() != 2) {
                    throw Refusals.malformed(op + " requires exactly 2 operands");
                }
                NumericOperand l = resolveNumericOperand(expr.getOperands(0), scope);
                NumericOperand r = resolveNumericOperand(expr.getOperands(1), scope);
                if (l instanceof NumericOperand.Constant lc
                        && r instanceof NumericOperand.Constant rc) {
                    return new NumericOperand.Constant(switch (op) {
                        case "add" -> lc.value() + rc.value();
                        case "sub" -> lc.value() - rc.value();
                        case "mult" -> lc.value() * rc.value();
                        case "div" -> lc.value() / rc.value(); // IEEE: ±Infinity, 0/0 = NaN
                        default -> throw Refusals.internal(
                                "Unsupported arithmetic operator: " + op);
                    });
                }
                return new NumericOperand.Sql(arithmeticSql(op, l, r));
            }
            default -> throw Refusals.malformed(
                    "Unexpected operand type in arithmetic comparison: "
                            + operand.getNodeCase());
        }
    }

    /**
     * Casts a column to IEEE double. Uses {@code cb.toDouble}, except when
     * {@link MySqlDoubleCastFunctionContributor} has registered {@code cerbos_ieee_double}:
     * {@code MySQLDialect} renders {@code cb.toDouble} as an exact {@code decimal(53,20)}.
     */
    Expression<Double> toIeeeDouble(
            Expression<? extends Number> path) {
        if (RegisteredFunctions.isRegistered(cb, MySqlDoubleCastFunctionContributor.FUNCTION_NAME)) {
            return cb.function(
                    MySqlDoubleCastFunctionContributor.FUNCTION_NAME, Double.class, path);
        }
        return cb.toDouble(path);
    }

    // At least one side is SQL. Constants use the Number overloads, which bind doubles.
    private Expression<Double> arithmeticSql(
            String op, NumericOperand l, NumericOperand r) {
        Expression<Double> le =
                l instanceof NumericOperand.Sql s ? s.expr() : null;
        Expression<Double> re =
                r instanceof NumericOperand.Sql s ? s.expr() : null;
        Double lc = l instanceof NumericOperand.Constant c ? c.value() : null;
        Double rc = r instanceof NumericOperand.Constant c ? c.value() : null;
        return switch (op) {
            case "add" -> le == null ? cb.sum(lc, re)
                    : re == null ? cb.sum(le, rc) : cb.sum(le, re);
            case "sub" -> le == null ? cb.diff(lc, re)
                    : re == null ? cb.diff(le, rc) : cb.diff(le, re);
            case "mult" -> le == null ? cb.prod(lc, re)
                    : re == null ? cb.prod(le, rc) : cb.prod(le, re);
            case "div" -> divisionSql(le, lc, re, rc);
            default -> throw Refusals.internal(
                    "Unsupported arithmetic operator: " + op);
        };
    }

    private Expression<Double> divisionSql(
            Expression<Double> le, Double lc,
            Expression<Double> re, Double rc) {
        if (rc != null) {
            // A zero constant divisor is UNKNOWN for every row.
            if (rc == 0.0) {
                return cb.nullLiteral(Double.class);
            }
            return cb.quot(le, rc).as(Double.class);
        }
        Expression<Double> guarded = cb.nullif(re, 0.0);
        return (lc != null ? cb.quot(lc, guarded) : cb.quot(le, guarded)).as(Double.class);
    }
}
