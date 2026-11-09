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

import java.math.BigInteger;
import java.util.List;
import java.util.function.Supplier;

/**
 * Lowers CEL int-typed arithmetic to SQL integer arithmetic.
 *
 * <p>Every attribute number reaches CEL as a double, so an arithmetic subtree is int-typed only
 * through {@code int()} (or {@code size()}), and the wire plan does not type its literals. The
 * type is inferred ({@link #typeOf}): a literal takes the type of its sibling, and an int beside
 * a double is a CEL no-overload error, which denies the row under both polarities.
 *
 * <p>An int subtree is computed as SQL {@code bigint}, where {@code +}, {@code -} and {@code *}
 * are exact. CEL errors on an int64 overflow, a zero divisor and {@code MIN / -1}: each lowered
 * node carries its value bounds, an overflow a bound rules out is not guarded, one a constant
 * operand can cause is turned into NULL, and any other is refused. Division truncates toward zero
 * as CEL's does, spelled {@code (a - mod(a, b)) / b} so a dialect whose {@code /} is decimal
 * agrees too, with a zero divisor made NULL by {@code NULLIF}.
 */
final class IntArithmetic {

    /** The CEL type of a numeric operand, as far as the plan lets the adapter tell. */
    enum CelType {
        INT,
        DOUBLE,
        /** An integral number literal, which CEL types from its sibling. */
        LITERAL,
        /** A no-overload error on every row. */
        ERROR
    }

    private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    /** 2^53: within it a double holds every integer, so a bigint-to-double cast is exact. */
    private static final BigInteger EXACT_DOUBLE = BigInteger.ONE.shiftLeft(53);
    private static final double INT64_LIMIT = 0x1p63;

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final ArithmeticTranslator doubles;
    private final ComparisonTranslator comparisons;

    IntArithmetic(CriteriaBuilder cb, TriPredicate tri, LeafTranslator leaf,
                  ArithmeticTranslator doubles, ComparisonTranslator comparisons) {
        this.cb = cb;
        this.tri = tri;
        this.leaf = leaf;
        this.doubles = doubles;
        this.comparisons = comparisons;
    }

    /**
     * The CEL type of a numeric operand, or {@code null} for one that is not numeric (a string
     * or boolean column, a list), which the double lowering reports in its own terms.
     */
    static CelType typeOf(Operand o, Scope scope) {
        switch (o.getNodeCase()) {
            case VALUE -> {
                if (o.getValue().getKindCase() != Value.KindCase.NUMBER_VALUE) {
                    return null;
                }
                return isIntegralLiteral(o.getValue().getNumberValue())
                        ? CelType.LITERAL : CelType.DOUBLE;
            }
            case VARIABLE -> {
                Class<?> type = scalarType(o.getVariable(), scope);
                return type != null && Number.class.isAssignableFrom(type) ? CelType.DOUBLE : null;
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = o.getExpression();
                return switch (e.getOperator()) {
                    case "int", "size" -> CelType.INT;
                    case "double" -> CelType.DOUBLE;
                    case "mod" -> {
                        if (e.getOperandsCount() != 2) {
                            yield null;
                        }
                        CelType l = typeOf(e.getOperands(0), scope);
                        CelType r = typeOf(e.getOperands(1), scope);
                        if (l == null || r == null) {
                            yield null;
                        }
                        yield l == CelType.DOUBLE || r == CelType.DOUBLE
                                || l == CelType.ERROR || r == CelType.ERROR
                                ? CelType.ERROR : CelType.INT;
                    }
                    case "add", "sub", "mult", "div" -> e.getOperandsCount() == 2
                            ? combine(typeOf(e.getOperands(0), scope),
                                    typeOf(e.getOperands(1), scope))
                            : null;
                    default -> null;
                };
            }
            default -> {
                return null;
            }
        }
    }

    private static CelType combine(CelType l, CelType r) {
        if (l == null || r == null) {
            return null;
        }
        if (l == CelType.ERROR || r == CelType.ERROR) {
            return CelType.ERROR;
        }
        if (l == CelType.LITERAL) {
            return r;
        }
        if (r == CelType.LITERAL) {
            return l;
        }
        return l == r ? l : CelType.ERROR;
    }

    /** A number literal CEL could have typed as an int: whole, and within int64. */
    private static boolean isIntegralLiteral(double d) {
        return d == Math.rint(d) && d >= -INT64_LIMIT && d < INT64_LIMIT;
    }

    private static Class<?> scalarType(String variable, Scope scope) {
        try {
            return scope.resolve(variable) instanceof Scope.ResolvedScalar s && s.path() != null
                    ? s.path().getJavaType() : null;
        } catch (IllegalArgumentException unmapped) {
            return null;
        }
    }

    // -- lowering --

    /** A lowered int-typed operand. */
    private sealed interface IntNode {
        record Constant(long value) implements IntNode {}

        /** A bigint SQL expression, built fresh on each call, with inclusive value bounds. */
        record Sql(Supplier<Expression<Long>> build, BigInteger lo, BigInteger hi)
                implements IntNode {}

        /** A CEL error on every row: a zero constant divisor, or a constant overflow. */
        record Error() implements IntNode {}
    }

    /**
     * Translates {@code cmp(l, r)} where at least one side is int-typed and neither is an
     * error. A double-typed other side compares across types, as CEL does.
     */
    Predicate comparison(String op, List<Operand> operands, Scope scope,
                         CelType left, CelType right) {
        if (left == CelType.DOUBLE || right == CelType.DOUBLE) {
            return crossTypeComparison(op, operands, scope, left == CelType.INT ? 0 : 1);
        }
        for (int side = 0; side < 2; side++) {
            CelType type = side == 0 ? left : right;
            if (type == CelType.LITERAL && containsDivision(operands.get(side))) {
                // `int(x) > 1 / 0`: an int division errors where a double one gives an infinity,
                // and the plan does not say which the policy wrote.
                throw Refusals.unsupported("a constant division or modulo compared with an int"
                        + " expression is not supported: the plan does not say whether its"
                        + " literals are ints or doubles, which divide differently");
            }
        }
        IntNode l = lower(operands.get(0), scope);
        IntNode r = lower(operands.get(1), scope);
        if (l instanceof IntNode.Error || r instanceof IntNode.Error) {
            return tri.unknown();
        }
        if (l instanceof IntNode.Constant lc && r instanceof IntNode.Constant rc) {
            return constant(holds(op, Long.compare(lc.value(), rc.value())));
        }
        if (r instanceof IntNode.Constant rc) {
            return againstConstant(op, (IntNode.Sql) l, rc.value());
        }
        if (l instanceof IntNode.Constant lc) {
            return againstConstant(NormalizedBinary.mirror(op), (IntNode.Sql) r, lc.value());
        }
        return compare(op, ((IntNode.Sql) l).build().get(), ((IntNode.Sql) r).build().get());
    }

    private static boolean containsDivision(Operand o) {
        if (o.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return false;
        }
        PlanResourcesFilter.Expression e = o.getExpression();
        return "div".equals(e.getOperator()) || "mod".equals(e.getOperator())
                || e.getOperandsList().stream().anyMatch(IntArithmetic::containsDivision);
    }

    /**
     * {@code int op c} for a whole {@code c}, through any override for {@code op}, which receives
     * the bigint expression and the constant as a {@link Long}.
     */
    private Predicate againstConstant(String op, IntNode.Sql e, long c) {
        return leaf.withOverride(op, e.build().get(), c,
                () -> compare(op, e.build().get(), c));
    }

    private Predicate compare(String op, Expression<Long> e, long c) {
        return switch (op) {
            case "eq" -> cb.equal(e, c);
            case "ne" -> cb.notEqual(e, c);
            case "lt" -> cb.lt(e, c);
            case "gt" -> cb.gt(e, c);
            case "le" -> cb.le(e, c);
            case "ge" -> cb.ge(e, c);
            default -> throw Refusals.internal("Unsupported int comparison operator: " + op);
        };
    }

    private Predicate compare(String op, Expression<Long> l, Expression<Long> r) {
        return switch (op) {
            case "eq" -> cb.equal(l, r);
            case "ne" -> cb.notEqual(l, r);
            case "lt" -> cb.lessThan(l, r);
            case "gt" -> cb.greaterThan(l, r);
            case "le" -> cb.lessThanOrEqualTo(l, r);
            case "ge" -> cb.greaterThanOrEqualTo(l, r);
            default -> throw Refusals.internal("Unsupported int comparison operator: " + op);
        };
    }

    /**
     * An int expression against a double: a fractional constant, {@code double()}, or an
     * attribute. CEL compares the two exactly, so the int side is cast to double only where its
     * bounds keep that cast exact.
     */
    private Predicate crossTypeComparison(String op, List<Operand> operands, Scope scope,
                                          int intSide) {
        IntNode node = lower(operands.get(intSide), scope);
        if (node instanceof IntNode.Error) {
            return tri.unknown();
        }
        Operand other = operands.get(1 - intSide);
        boolean intIsLeft = intSide == 0;
        String intFirst = intIsLeft ? op : NormalizedBinary.mirror(op);
        if (node instanceof IntNode.Constant k) {
            // Unreachable from the planner, which folds a constant side; translated the way
            // the double lowering compares a constant.
            List<Operand> substituted = intIsLeft
                    ? List.of(numberOperand(k.value()), other)
                    : List.of(other, numberOperand(k.value()));
            return doubles.numericComparison(op, substituted, scope);
        }
        IntNode.Sql e = (IntNode.Sql) node;
        if (doubles.hasZeroCapableDivision(other, scope)) {
            throw Refusals.unsupported("an int expression compared with a double division whose"
                    + " divisor may be zero is not supported");
        }
        ArithmeticTranslator.NumericOperand o = doubles.resolveNumericOperand(other, scope);
        if (o instanceof ArithmeticTranslator.NumericOperand.Constant oc) {
            return againstDouble(intFirst, e, oc.value());
        }
        if (e.lo().compareTo(EXACT_DOUBLE.negate()) < 0 || e.hi().compareTo(EXACT_DOUBLE) > 0) {
            throw Refusals.unsupported("an int expression that may exceed 2^53 compared with a"
                    + " double attribute is not supported: SQL compares the two through a cast"
                    + " that is no longer exact there, while CEL compares them exactly");
        }
        Expression<Double> i = doubles.toIeeeDouble(e.build().get());
        return comparisons.comparePredicate(intFirst, i,
                ((ArithmeticTranslator.NumericOperand.Sql) o).expr());
    }

    private static Operand numberOperand(double value) {
        return Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(value)).build();
    }

    /** {@code int op c} for a double {@code c} that is not a whole int64, NaN included. */
    private Predicate againstDouble(String op, IntNode.Sql e, double c) {
        if (isIntegralLiteral(c)) {
            return againstConstant(op, e, (long) c);
        }
        if (Double.isNaN(c)) {
            // Unordered against every int; `!=` alone holds. A NULL expression stays UNKNOWN.
            return nullUnless(e, "ne".equals(op));
        }
        if (Double.isInfinite(c) || Math.abs(c) >= INT64_LIMIT) {
            // Every int64 sits on one side of c.
            return nullUnless(e, ArithmeticTranslator.ieeeHolds(op, 0.0, c, true));
        }
        // A fraction: solve for the integral bound.
        return switch (op) {
            case "gt" -> compare("ge", e.build().get(), (long) Math.floor(c) + 1);
            case "ge" -> compare("ge", e.build().get(), (long) Math.ceil(c));
            case "lt" -> compare("le", e.build().get(), (long) Math.ceil(c) - 1);
            case "le" -> compare("le", e.build().get(), (long) Math.floor(c));
            case "eq" -> nullUnless(e, false);
            case "ne" -> nullUnless(e, true);
            default -> throw Refusals.internal("Unsupported int comparison operator: " + op);
        };
    }

    private Predicate nullUnless(IntNode.Sql e, boolean result) {
        return tri.baseUnlessUnknown(constant(result), () -> cb.isNull(e.build().get()));
    }

    private Predicate constant(boolean result) {
        return result ? cb.conjunction() : cb.disjunction();
    }

    private static boolean holds(String op, int cmp) {
        return switch (op) {
            case "eq" -> cmp == 0;
            case "ne" -> cmp != 0;
            case "lt" -> cmp < 0;
            case "gt" -> cmp > 0;
            case "le" -> cmp <= 0;
            case "ge" -> cmp >= 0;
            default -> throw Refusals.internal("Unsupported int comparison operator: " + op);
        };
    }

    private IntNode lower(Operand o, Scope scope) {
        return switch (o.getNodeCase()) {
            case VALUE -> {
                double d = o.getValue().getNumberValue();
                if (!isIntegralLiteral(d)) {
                    throw Refusals.internal("a fractional literal in an int-typed expression");
                }
                yield new IntNode.Constant((long) d);
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = o.getExpression();
                String op = e.getOperator();
                yield switch (op) {
                    case "int" -> conversion(e, scope);
                    case "add", "sub", "mult", "div", "mod" -> {
                        if (e.getOperandsCount() != 2) {
                            throw Refusals.malformed(op + " requires exactly 2 operands");
                        }
                        yield binary(op, lower(e.getOperands(0), scope),
                                lower(e.getOperands(1), scope));
                    }
                    default -> throw Refusals.unsupported("Unexpected " + op
                            + "() expression inside an arithmetic comparison operand");
                };
            }
            default -> throw Refusals.malformed(
                    "Unexpected operand type in arithmetic comparison: " + o.getNodeCase());
        };
    }

    /**
     * {@code int(x)}: the identity over an integral column, truncation toward zero over a double
     * one, with CEL's error (NULL) at or beyond ±2^63. A literal arrives here only from a ternary
     * whose other branch typed it.
     */
    private IntNode conversion(PlanResourcesFilter.Expression e, Scope scope) {
        if (e.getOperandsCount() != 1) {
            throw Refusals.malformed("int requires exactly 1 operand");
        }
        Operand arg = e.getOperands(0);
        if (arg.getNodeCase() == Operand.NodeCase.VALUE
                && arg.getValue().getKindCase() == Value.KindCase.NUMBER_VALUE) {
            double d = arg.getValue().getNumberValue();
            return Double.isNaN(d) || d <= -INT64_LIMIT || d >= INT64_LIMIT
                    ? new IntNode.Error() : new IntNode.Constant((long) d);
        }
        if (arg.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw Refusals.unsupported("int() inside arithmetic is lowered only over a mapped"
                    + " Integer, Long or Double column");
        }
        String variable = arg.getVariable();
        Class<?> type = scope.path(variable).getJavaType();
        if (Integer.class.equals(type) || Short.class.equals(type) || Byte.class.equals(type)) {
            long lo = Integer.class.equals(type) ? Integer.MIN_VALUE
                    : Short.class.equals(type) ? Short.MIN_VALUE : Byte.MIN_VALUE;
            long hi = Integer.class.equals(type) ? Integer.MAX_VALUE
                    : Short.class.equals(type) ? Short.MAX_VALUE : Byte.MAX_VALUE;
            return new IntNode.Sql(() -> cb.toLong(numericPath(variable, scope)),
                    BigInteger.valueOf(lo), BigInteger.valueOf(hi));
        }
        if (Long.class.equals(type)) {
            return new IntNode.Sql(() -> longPath(variable, scope), INT64_MIN, INT64_MAX);
        }
        if (Double.class.equals(type)) {
            return new IntNode.Sql(() -> truncated(variable, scope), INT64_MIN, INT64_MAX);
        }
        throw Refusals.unsupported("int() inside arithmetic is lowered only over an Integer,"
                + " Long or Double column: '" + variable + "' is " + type.getSimpleName());
    }

    @SuppressWarnings("unchecked")
    private static Expression<? extends Number> numericPath(String variable, Scope scope) {
        return (Expression<? extends Number>) scope.path(variable);
    }

    @SuppressWarnings("unchecked")
    private static Expression<Long> longPath(String variable, Scope scope) {
        return (Expression<Long>) scope.path(variable);
    }

    @SuppressWarnings("unchecked")
    private Expression<Long> truncated(String variable, Scope scope) {
        Path<Double> d = (Path<Double>) scope.path(variable);
        Expression<Double> whole = cb.<Double>selectCase()
                .when(cb.ge(d, 0.0), cb.floor(d))
                .otherwise(cb.ceiling(d));
        return cb.<Long>selectCase()
                .when(cb.or(cb.le(d, -INT64_LIMIT), cb.ge(d, INT64_LIMIT)),
                        cb.nullLiteral(Long.class))
                .otherwise(cb.toLong(whole));
    }

    private IntNode binary(String op, IntNode l, IntNode r) {
        if (l instanceof IntNode.Error || r instanceof IntNode.Error) {
            return new IntNode.Error();
        }
        if (l instanceof IntNode.Constant lc && r instanceof IntNode.Constant rc) {
            return fold(op, lc.value(), rc.value());
        }
        return switch (op) {
            case "add", "sub", "mult" -> linear(op, l, r);
            case "div" -> division(l, r);
            case "mod" -> modulo(l, r);
            default -> throw Refusals.internal("Unsupported int arithmetic operator: " + op);
        };
    }

    private static IntNode fold(String op, long l, long r) {
        try {
            return new IntNode.Constant(switch (op) {
                case "add" -> Math.addExact(l, r);
                case "sub" -> Math.subtractExact(l, r);
                case "mult" -> Math.multiplyExact(l, r);
                case "div" -> {
                    if (r == 0 || (l == Long.MIN_VALUE && r == -1)) {
                        throw new ArithmeticException();
                    }
                    yield l / r;
                }
                case "mod" -> {
                    if (r == 0 || (l == Long.MIN_VALUE && r == -1)) {
                        throw new ArithmeticException();
                    }
                    yield l % r;
                }
                default -> throw Refusals.internal("Unsupported int arithmetic operator: " + op);
            });
        } catch (ArithmeticException overflow) {
            return new IntNode.Error();
        }
    }

    private static BigInteger[] bounds(IntNode n) {
        return n instanceof IntNode.Constant c
                ? new BigInteger[] {BigInteger.valueOf(c.value()), BigInteger.valueOf(c.value())}
                : new BigInteger[] {((IntNode.Sql) n).lo(), ((IntNode.Sql) n).hi()};
    }

    private static boolean fitsInt64(BigInteger lo, BigInteger hi) {
        return lo.compareTo(INT64_MIN) >= 0 && hi.compareTo(INT64_MAX) <= 0;
    }

    /**
     * {@code +}, {@code -} and {@code *}. The result's bounds follow from the operands'; where
     * they leave int64 and one operand is a constant, the other operand's values that would
     * overflow are made NULL first, since CEL errors on them.
     */
    private IntNode linear(String op, IntNode l, IntNode r) {
        BigInteger[] lb = bounds(l);
        BigInteger[] rb = bounds(r);
        BigInteger lo;
        BigInteger hi;
        switch (op) {
            case "add" -> {
                lo = lb[0].add(rb[0]);
                hi = lb[1].add(rb[1]);
            }
            case "sub" -> {
                lo = lb[0].subtract(rb[1]);
                hi = lb[1].subtract(rb[0]);
            }
            default -> {
                BigInteger[] products = {lb[0].multiply(rb[0]), lb[0].multiply(rb[1]),
                        lb[1].multiply(rb[0]), lb[1].multiply(rb[1])};
                lo = products[0];
                hi = products[0];
                for (BigInteger p : products) {
                    lo = lo.min(p);
                    hi = hi.max(p);
                }
            }
        }
        if (fitsInt64(lo, hi)) {
            return new IntNode.Sql(() -> apply(op, l, r), lo, hi);
        }
        if (!(l instanceof IntNode.Constant) && !(r instanceof IntNode.Constant)) {
            throw Refusals.unsupported("int " + op + " between two expressions whose result may"
                    + " overflow int64 is not supported: CEL errors on the overflow and SQL"
                    + " would wrap or fail the query");
        }
        long c = l instanceof IntNode.Constant lc ? lc.value() : ((IntNode.Constant) r).value();
        IntNode.Sql e = (IntNode.Sql) (l instanceof IntNode.Constant ? r : l);
        boolean constantIsLeft = l instanceof IntNode.Constant;
        BigInteger[] safe = safeRange(op, c, constantIsLeft);
        if (safe == null) {
            throw Refusals.unsupported("int " + op + " whose result may overflow int64 is not"
                    + " supported: CEL errors on the overflow");
        }
        Supplier<Expression<Long>> guarded = () -> cb.<Long>selectCase()
                .when(cb.or(cb.lt(e.build().get(), safe[0].longValue()),
                                cb.gt(e.build().get(), safe[1].longValue())),
                        cb.nullLiteral(Long.class))
                .otherwise(apply(op, l, r));
        return new IntNode.Sql(guarded, lo.max(INT64_MIN), hi.min(INT64_MAX));
    }

    /**
     * The values {@code v} for which {@code v op c} (or {@code c op v}) stays within int64, or
     * {@code null} when no such range exists.
     */
    private static BigInteger[] safeRange(String op, long c, boolean constantIsLeft) {
        BigInteger k = BigInteger.valueOf(c);
        return switch (op) {
            case "add" -> new BigInteger[] {INT64_MIN.subtract(k), INT64_MAX.subtract(k)};
            case "sub" -> constantIsLeft
                    // c - v: v in [c - MAX, c - MIN]
                    ? new BigInteger[] {k.subtract(INT64_MAX), k.subtract(INT64_MIN)}
                    // v - c: v in [MIN + c, MAX + c]
                    : new BigInteger[] {INT64_MIN.add(k), INT64_MAX.add(k)};
            default -> {
                if (c == 0) {
                    yield new BigInteger[] {INT64_MIN, INT64_MAX};
                }
                BigInteger[] ends = {ceilDiv(INT64_MIN, k), floorDiv(INT64_MAX, k)};
                yield c > 0 ? ends : new BigInteger[] {ceilDiv(INT64_MAX, k), floorDiv(INT64_MIN, k)};
            }
        };
    }

    private static BigInteger floorDiv(BigInteger a, BigInteger b) {
        BigInteger[] qr = a.divideAndRemainder(b);
        return qr[1].signum() != 0 && (qr[1].signum() != b.signum())
                ? qr[0].subtract(BigInteger.ONE) : qr[0];
    }

    private static BigInteger ceilDiv(BigInteger a, BigInteger b) {
        BigInteger[] qr = a.divideAndRemainder(b);
        return qr[1].signum() != 0 && (qr[1].signum() == b.signum())
                ? qr[0].add(BigInteger.ONE) : qr[0];
    }

    private Expression<Long> apply(String op, IntNode l, IntNode r) {
        Expression<Long> le = l instanceof IntNode.Sql s ? s.build().get() : null;
        Expression<Long> re = r instanceof IntNode.Sql s ? s.build().get() : null;
        Long lc = l instanceof IntNode.Constant c ? c.value() : null;
        Long rc = r instanceof IntNode.Constant c ? c.value() : null;
        return switch (op) {
            case "add" -> le == null ? cb.sum(lc, re) : re == null ? cb.sum(le, rc) : cb.sum(le, re);
            case "sub" -> le == null ? cb.diff(lc, re) : re == null ? cb.diff(le, rc) : cb.diff(le, re);
            case "mult" -> le == null ? cb.prod(lc, re) : re == null ? cb.prod(le, rc) : cb.prod(le, re);
            default -> throw Refusals.internal("Unsupported int arithmetic operator: " + op);
        };
    }

    /**
     * {@code a / b}, truncating toward zero: {@code (a - mod(a, b)) / b}, whose numerator is a
     * multiple of {@code b}, so a decimal {@code /} gives the same whole number. A zero divisor
     * is NULL, CEL's error.
     */
    private IntNode division(IntNode l, IntNode r) {
        BigInteger[] lb = bounds(l);
        BigInteger[] rb = bounds(r);
        if (r instanceof IntNode.Constant rc && rc.value() == 0) {
            return new IntNode.Error();
        }
        if (lb[0].equals(INT64_MIN) && rb[0].compareTo(BigInteger.ONE.negate()) <= 0
                && rb[1].compareTo(BigInteger.ONE.negate()) >= 0) {
            throw Refusals.unsupported("int division of a value that may be -2^63 by one that"
                    + " may be -1 is not supported: CEL errors on that overflow");
        }
        BigInteger magnitude = lb[0].abs().max(lb[1].abs());
        Supplier<Expression<Long>> build = () -> {
            Expression<Long> numerator = cb.diff(
                    build(l), remainder(build(l), divisor(r)));
            Expression<? extends Number> quotient = r instanceof IntNode.Constant rc
                    ? cb.quot(numerator, rc.value())
                    : cb.quot(numerator, divisor(r));
            return quotient.as(Long.class);
        };
        return new IntNode.Sql(build, magnitude.negate(), magnitude);
    }

    /** {@code a % b}, with the dividend's sign as CEL's; a zero divisor is NULL. */
    private IntNode modulo(IntNode l, IntNode r) {
        BigInteger[] lb = bounds(l);
        BigInteger[] rb = bounds(r);
        if (r instanceof IntNode.Constant rc && rc.value() == 0) {
            return new IntNode.Error();
        }
        if (lb[0].equals(INT64_MIN) && rb[0].compareTo(BigInteger.ONE.negate()) <= 0
                && rb[1].compareTo(BigInteger.ONE.negate()) >= 0) {
            throw Refusals.unsupported("int modulo of a value that may be -2^63 by one that"
                    + " may be -1 is not supported: CEL errors on that overflow");
        }
        BigInteger below = rb[0].abs().max(rb[1].abs()).subtract(BigInteger.ONE);
        BigInteger lo = lb[0].max(below.negate());
        BigInteger hi = lb[1].min(below);
        return new IntNode.Sql(() -> remainder(build(l), divisor(r)), lo, hi.max(lo));
    }

    private Expression<Long> build(IntNode n) {
        return n instanceof IntNode.Constant c ? cb.literal(c.value()) : ((IntNode.Sql) n).build().get();
    }

    /** The divisor, with a zero made NULL where it is not a constant. */
    private Expression<Long> divisor(IntNode r) {
        return r instanceof IntNode.Constant c
                ? cb.literal(c.value())
                : cb.nullif(((IntNode.Sql) r).build().get(), 0L);
    }

    @SuppressWarnings("unchecked")
    private Expression<Long> remainder(Expression<Long> a, Expression<Long> b) {
        // JPA types mod() over Integer; the SQL is the dialect's mod over whatever it is given.
        Expression<?> mod = cb.mod((Expression<Integer>) (Expression<?>) a,
                (Expression<Integer>) (Expression<?>) b);
        return (Expression<Long>) mod;
    }
}
