/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Lowers arithmetic ({@code add}/{@code sub}/{@code mult}/{@code div}) in a comparison operand
 * to SQL, in IEEE double space.
 *
 * <p>Columns are cast to a real double ({@link #toIeeeDouble}), constant subtrees fold in Java,
 * and constants mixed into SQL bind as double parameters. A zero divisor is guarded with
 * {@code NULLIF}, and a comparison against a possibly-zero division is rewritten
 * ({@link #tryDivisionByZeroComparison}) so NaN and infinities compare as they do in CEL.
 */
final class ArithmeticTranslator {

    static final Set<String> ARITHMETIC_OPS = Set.of("add", "sub", "mult", "div", "mod");

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final ComparisonTranslator comparisons;

    ArithmeticTranslator(CriteriaBuilder cb, TriPredicate tri, LeafTranslator leaf,
                         ComparisonTranslator comparisons) {
        this.cb = cb;
        this.tri = tri;
        this.leaf = leaf;
        this.comparisons = comparisons;
    }

    /**
     * Translates {@code cmp(arith(...), other)}, e.g. {@code R.attr.aNumber + 1.0 > 2.0}, by
     * emitting the arithmetic in SQL and comparing.
     *
     * <p>Everything is computed in double space because attribute values are always CEL
     * doubles at check time, and the wire plan does not distinguish int from double.
     *
     * <p>{@code mod} is refused: a bare {@code attr % n} is a CEL error, and
     * {@code int(attr) % n} needs {@code int()}, which has no faithful SQL lowering.
     *
     * <p>An {@link OperatorFunction} override is consulted when one side is a constant; it
     * receives the arithmetic expression and the constant as a {@link Double}. Expression
     * against expression comparisons do not consult overrides.
     */
    Predicate numericComparison(String op, List<Operand> operands, Scope scope) {
        // `NaN != x` is TRUE in CEL, but `NULL != x` is UNKNOWN, so a possibly-zero division
        // is folded against the comparison rather than lowered to NULL.
        Predicate folded = tryDivisionByZeroComparison(op, operands, scope);
        if (folded != null) {
            return folded;
        }
        return numericComparisonWithoutZeroGuard(op, operands, scope);
    }

    /**
     * Rewrites a comparison against a division that may divide by zero, or returns
     * {@code null} when the shape does not apply.
     *
     * <p>In CEL {@code 0/0} is NaN and {@code x/0} is a signed infinity. The {@code NULLIF}
     * guard alone would make every comparison UNKNOWN, which is wrong for an inequality:
     * {@code NaN != 1.0} is TRUE. So the comparison becomes nested ternaries:
     *
     * <pre>{@code
     * if (d == 0) { if (n == 0) NaN op v else if (n > 0) +Inf op v else -Inf op v }
     * else { n / d op v }
     * }</pre>
     *
     * <p>Each non-finite arm folds in Java, so no NaN or infinity is bound. A NULL numerator or
     * denominator makes every condition UNKNOWN, so the row stays excluded under both
     * polarities.
     */
    private Predicate tryDivisionByZeroComparison(
            String op, List<Operand> operands, Scope scope) {
        if (isZeroCapableDivisionOperand(operands.get(0), scope)
                && isZeroCapableDivisionOperand(operands.get(1), scope)) {
            // Only one side can be rewritten; the other would still lower to NULL.
            throw Refusals.unsupported(
                    "a comparison with a zero-capable division on BOTH sides is not "
                            + "supported: only one side can be folded into IEEE arms and "
                            + "the other would lower to SQL NULL");
        }
        for (int side = 0; side < 2; side++) {
            Operand candidate = operands.get(side);
            if (candidate.getNodeCase() != Operand.NodeCase.EXPRESSION) {
                continue;
            }
            PlanResourcesFilter.Expression division = candidate.getExpression();
            if (!"div".equals(division.getOperator())
                    || division.getOperandsCount() != 2) {
                continue;
            }
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
            // its sign bit. SQL cannot read the sign of a stored zero, so a column divisor is
            // assumed positive (cerbos/query-plan-adapters#312).
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

            // A non-finite compares the same way against every present value, so against a
            // column the arm is folded in Java and only a NULL column makes it UNKNOWN.
            Function<Double, Predicate> arm = nonFinite -> {
                NumericOperand o = resolveNumericOperand(other, scope);
                if (o instanceof NumericOperand.Constant oc) {
                    return divisionIsLeft
                            ? comparisons.constantComparison(op, nonFinite, oc.value())
                            : comparisons.constantComparison(op, oc.value(), nonFinite);
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
                    () -> numericComparisonWithoutZeroGuard(op, operands, scope));
        }
        return null;
    }

    private Predicate numericComparisonWithoutZeroGuard(
            String op, List<Operand> operands, Scope scope) {
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
    private sealed interface NumericOperand {
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
            if (isZeroCapableDivisionOperand(child, scope)) {
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
     * Resolves an operand to double space. Database decimal arithmetic is not IEEE: H2 and
     * PostgreSQL type {@code 0.1} as exact NUMERIC, so {@code aNumber * 0.1 == 0.3} would match
     * rows CEL denies. So:
     * <ul>
     *   <li>columns go through {@link #toIeeeDouble};</li>
     *   <li>constant subtrees fold in Java;</li>
     *   <li>constants mixed into SQL bind as double parameters.</li>
     * </ul>
     *
     * <p>A column divisor is wrapped in {@code NULLIF(d, 0)}, because SQL fails the whole query
     * on a zero divisor. A constant zero divisor becomes a NULL literal. This is only correct
     * for ordered comparisons; see {@link #tryDivisionByZeroComparison}.
     */
    private NumericOperand resolveNumericOperand(Operand operand, Scope scope) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                @SuppressWarnings("unchecked")
                Expression<? extends Number> path =
                        (Expression<? extends Number>) scope.path(operand.getVariable());
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
                    // A bare `attr % n` is a CEL error; `int(attr) % n` needs int(), which
                    // has no faithful SQL lowering.
                    throw Refusals.unsupported(
                            "mod is not supported in comparisons: CEL % is integer-only "
                                    + "while attribute values are always doubles at check "
                                    + "time, so a satisfiable policy must cast with int() "
                                    + "first — and int() has no faithful SQL lowering, "
                                    + "because CAST rounds where CEL truncates toward zero");
                }
                if (ARITHMETIC_OPS.contains(op) && !"div".equals(op)
                        && containsZeroCapableDivision(expr, scope)) {
                    // CEL carries NaN or infinity through the surrounding arithmetic, while
                    // NULLIF makes it NULL, so `NaN + 1.0 != 2.0` would drop a row the PDP
                    // allows. The rewrite only handles a division that is the operand itself.
                    throw Refusals.unsupported(
                            "arithmetic composed on a division whose denominator may be "
                                    + "zero is not supported: CEL carries the resulting NaN "
                                    + "or infinity through the surrounding arithmetic and "
                                    + "SQL has no value that does");
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
    private Expression<Double> toIeeeDouble(
            Expression<? extends Number> path) {
        if (IeeeDoubleCast.isRegistered(cb)) {
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
