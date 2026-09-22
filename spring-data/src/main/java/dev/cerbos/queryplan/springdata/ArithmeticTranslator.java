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
 * Arithmetic ({@code add}/{@code sub}/{@code mult}/{@code div}) as a comparison operand,
 * lowered to SQL in IEEE double space.
 *
 * <p>Owns the Double space: every column is forced through a real double cast
 * ({@link #toIeeeDouble}), every constant subtree folds in Java with IEEE semantics
 * ({@link NumericOperand.Constant}), and every constant mixed into SQL arithmetic binds as a
 * double parameter rather than a decimal literal. It also owns the zero-divisor story —
 * the {@code NULLIF} guard that keeps a SQL query from failing on a zero row, and the
 * IEEE-arm rewrite ({@link #tryDivisionByZeroComparison}) that makes an inequality against
 * NaN or a signed infinity agree with CEL where the guard alone would not. Only
 * {@link ComparisonTranslator#dispatch} routes here, and only for arithmetic-rooted shapes it
 * did not consume as the {@code add} fold or the eq/ne solve — those never enter double space.
 *
 * <p>The constant fold ({@link ComparisonTranslator#constantComparison}) and the raw
 * expression comparison ({@link ComparisonTranslator#comparePredicate}) stay with the
 * comparison seam and are reached through it: they are how a comparison is decided, not how
 * a number is computed.
 */
final class ArithmeticTranslator {

    /** CEL arithmetic operators that can appear as an operand of a comparison. */
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
     * Translate {@code cmp(arith(...), other)} — e.g. {@code R.attr.aNumber + 1.0 > 2.0}
     * arriving as {@code gt(add(variable, value(1)), value(2))} — by emitting the arithmetic
     * on the SQL side ({@code cb.sum}/{@code diff}/{@code prod}/{@code quot}) and comparing.
     *
     * <p>Everything is computed and compared in DOUBLE space. This is not a convenience:
     * Cerbos attribute values are protobuf {@code Value} numbers, i.e. ALWAYS CEL doubles at
     * check time, so the only arithmetic that can evaluate without a no-overload error is
     * double-typed — verified against a live PDP: {@code R.attr.n + 1} (int literal) denies
     * every row, {@code + 1.0} works, and {@code / 2.0} is true double division
     * ({@code 5 / 2.0 == 2.5}). Integer truncation is therefore never observable through the
     * check API, and the wire plan erases the int/double distinction anyway (both arrive as
     * {@code number_value}). Emitting the arithmetic (rather than solving algebraically)
     * also means multiplication/division by negative constants needs no inequality flipping.
     *
     * <p>DOUBLE space must be enforced explicitly, because DB decimal arithmetic is not
     * IEEE double arithmetic (see {@link #resolveNumericOperand}): columns are CAST, plan
     * constants are folded in Java or bound as double parameters, and pure-constant
     * comparisons are evaluated statically in Java (full CEL fidelity, Infinity/NaN
     * included).
     *
     * <p>{@code mod} stays unsupported: CEL {@code %} has no double overload, so on
     * attribute values it always errors (deny) — translating it to SQL {@code MOD} would
     * fabricate rows the PDP denies.
     *
     * <p>{@link OperatorFunction} overrides win here like on every other scalar path when
     * the comparison has a plan constant on one side: the arithmetic SQL expression is
     * passed as the field argument and the folded constant (always a {@link Double} — the
     * arithmetic path is double-space end to end) as the value. Expression-vs-expression
     * comparisons (arithmetic against arithmetic or against another column) have no
     * (field, value) pair and are not consulted — the same exclusion as field-to-field
     * comparisons.
     *
     * <p>Only {@link ComparisonTranslator#dispatch} routes here, and only for
     * arithmetic-rooted shapes it did not consume as the {@code add} fold
     * ({@code field op add(value, value)}) or the eq/ne concat solve — those never enter
     * double space.
     */
    Predicate numericComparison(String op, List<Operand> operands, Scope scope) {
        // A zero-divisor division must be folded against the comparison, not lowered to
        // NULL: `NaN != x` is TRUE in CEL while `NULL != x` is UNKNOWN (see
        // #divisionByZeroComparison).
        Predicate folded = tryDivisionByZeroComparison(op, operands, scope);
        if (folded != null) {
            return folded;
        }
        return numericComparisonWithoutZeroGuard(op, operands, scope);
    }

    /**
     * Fold a comparison whose operand is a division that can divide by zero, returning
     * {@code null} when the shape does not apply so the caller falls through.
     *
     * <p>CEL attribute arithmetic is double-typed, so {@code 0/0} is NaN and {@code x/0}
     * is a signed infinity. Lowering the division to SQL NULL (the {@code NULLIF} guard in
     * {@link #divisionSql}) makes every comparison UNKNOWN, which agrees with CEL for
     * ORDERED comparisons — NaN and NULL both exclude the row, which is why
     * {@code cr-div-zero} passed — but diverges for an INEQUALITY: {@code NaN != 1.0} is
     * TRUE and the PDP allows the row, while {@code NULL != 1.0} is UNKNOWN and the
     * adapter denies it. Under-inclusive rather than a bypass, but it still breaks the
     * oracle equality (corpus actions {@code cr-div-zero-ne}, {@code cr-div-zero-eq-neg}).
     *
     * <p>Rewritten as nested ternaries rather than {@code CASE WHEN}, matching this
     * translator's predicate-only design (see {@link TernaryTranslator#tryTernaryComparison}):
     *
     * <pre>{@code
     * if (d == 0) { if (n == 0) NaN op v else if (n > 0) +Inf op v else -Inf op v }
     * else { n / d op v }
     * }</pre>
     *
     * <p>Each non-finite arm folds statically through
     * {@link ComparisonTranslator#constantComparison}, so no NaN or Infinity is ever bound as
     * a parameter. {@link TriPredicate#ternary} owns the UNKNOWN arms: a NULL numerator or
     * denominator drives every condition UNKNOWN, so the row stays excluded under BOTH
     * polarities — the CEL missing-attribute deny.
     */
    private Predicate tryDivisionByZeroComparison(
            String op, List<Operand> operands, Scope scope) {
        if (isZeroCapableDivisionOperand(operands.get(0), scope)
                && isZeroCapableDivisionOperand(operands.get(1), scope)) {
            // Only one side can be folded into IEEE arms; the other would still lower to
            // NULL, turning `NaN != NaN` (TRUE in CEL) into UNKNOWN. Fail closed.
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
            // A NON-ZERO constant divisor is already decided statically, and a fully
            // constant subtree folds to an exact IEEE value — neither needs the rewrite.
            // A constant ZERO does: NULLIF(0, 0) is NULL, which makes every comparison
            // UNKNOWN, while CEL produces a signed infinity for a non-zero numerator.
            NumericOperand divisor = resolveNumericOperand(division.getOperands(1), scope);
            if (divisor instanceof NumericOperand.Constant dc && dc.value() != 0.0) {
                continue;
            }
            NumericOperand dividend =
                    resolveNumericOperand(division.getOperands(0), scope);
            // A fully constant subtree folds to an exact IEEE value elsewhere; only a
            // COLUMN dividend needs the per-row rewrite.
            if (divisor instanceof NumericOperand.Constant
                    && dividend instanceof NumericOperand.Constant) {
                continue;
            }
            // IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity
            // from `n / 0.0`. A constant divisor carries its sign all the way here — the
            // planner ships `-0` and protobuf doubles preserve the sign bit — so it must
            // be applied. A COLUMN divisor cannot: SQL has no portable way to read the
            // sign bit of a stored zero, so the positive reading is assumed and
            // documented (cerbos/query-plan-adapters#312).
            boolean negativeZeroDivisor = divisor instanceof NumericOperand.Constant zc
                    && Double.doubleToRawLongBits(zc.value()) != 0L;
            double positiveDividendResult = negativeZeroDivisor
                    ? Double.NEGATIVE_INFINITY
                    : Double.POSITIVE_INFINITY;
            double negativeDividendResult = negativeZeroDivisor
                    ? Double.POSITIVE_INFINITY
                    : Double.NEGATIVE_INFINITY;

            // The comparison as written, with the division on the side it appeared.
            boolean divisionIsLeft = side == 0;
            Operand other = operands.get(divisionIsLeft ? 1 : 0);

            // Fold `nonFinite op other` (or the mirrored order) in Java. A non-finite
            // compares the same way against every PRESENT value, so a column operand only
            // needs its NULL-ness preserved — baseUnlessUnknown drives the arm to UNKNOWN
            // when the operand is NULL, keeping the row excluded under both polarities.
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

    /** The plain arithmetic comparison, bypassing the zero-divisor rewrite. */
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
            // Bind a genuine double parameter: cb.literal would inline an exact NUMERIC
            // literal on H2/Postgres and pull the comparison out of IEEE space.
            String cmpOp = op;
            double v = rc.value();
            if (Double.isNaN(v) && !"eq".equals(cmpOp) && !"ne".equals(cmpOp)) {
                // Cerbos 0.55 compares NaN as unordered even against a present non-number.
                // Inspect the original column for presence: casting a string to double would
                // fail in SQL before the constant false result can be negated.
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

    /**
     * A resolved arithmetic operand: either a pure-constant subtree folded in Java —
     * genuine IEEE double semantics, exactly matching CEL, including division by zero
     * yielding ±Infinity/NaN — or a SQL expression forced into double space.
     */
    private sealed interface NumericOperand {
        record Constant(double value) implements NumericOperand {}
        record Sql(Expression<Double> expr) implements NumericOperand {}
    }

    /** Whether an operand IS a division that can divide by zero. */
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

    /** Whether an arithmetic subtree holds a division that can divide by zero. */
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
     * Resolve a comparison operand to double space. DB decimal arithmetic is NOT IEEE
     * double arithmetic: H2 (and Postgres) type a bare {@code 0.1} literal as exact
     * NUMERIC and evaluate {@code intCol * 0.1} decimally, so {@code aNumber * 0.1 == 0.3}
     * matched rows the PDP (IEEE: {@code 0.30000000000000004}) denies. Verified against
     * H2 2.3: only {@code CAST(col AS DOUBLE) * CAST(0.1 AS DOUBLE)} diverges from
     * {@code 0.3}; {@code Expression.as(Double.class)} renders NO SQL cast (it is a type
     * marker only) and {@code cb.toDouble(literal)} elides the cast on a node already
     * Double-typed, both leaving the arithmetic decimal. Therefore:
     * <ul>
     *   <li>columns go through {@link #toIeeeDouble}: {@code cb.toDouble} (renders
     *       {@code cast(col as float(53))}) — except on MySQL, where Hibernate's
     *       {@code MySQLDialect} renders that cast as exact {@code decimal(53,20)} and
     *       the adapter instead emits the {@code cerbos_ieee_double} function
     *       ({@code cast(col as double)}) registered by
     *       {@link MySqlDoubleCastFunctionContributor};</li>
     *   <li>constant subtrees fold in Java ({@link NumericOperand.Constant});</li>
     *   <li>constants mixed into SQL arithmetic bind through the plain-{@code Number}
     *       CriteriaBuilder overloads, which emit genuine double-typed bind parameters
     *       instead of decimal literals. (MySQL Connector/J's default client-side
     *       prepared statements still interpolate those binds as DECIMAL literals in
     *       the statement text — harmless once every column cast is a true DOUBLE,
     *       because MySQL promotes arithmetic and comparisons with an approximate
     *       operand to double space; see {@link MySqlDoubleCastFunctionContributor}.)</li>
     * </ul>
     *
     * <p>Division guard: SQL raises an error on a zero divisor — a data-dependent runtime
     * failure of the WHOLE query — while CEL double division is defined (±Infinity, or NaN
     * for 0/0). A column divisor is wrapped in {@code NULLIF(d, 0)} so the query survives.
     * Constant divisors are decided statically: non-zero skips the guard, zero collapses
     * the division to a NULL literal (UNKNOWN for every row).
     *
     * <p>That guard alone is NOT semantically faithful, and this method is not the whole
     * story: mapping a zero divisor to UNKNOWN agrees with CEL only for ORDERED
     * comparisons, where NaN and NULL both exclude the row. It diverges for equality —
     * {@code NaN != 1.0} is TRUE in CEL but {@code NULL != 1.0} is UNKNOWN. Comparisons
     * over a possibly-zero divisor are therefore intercepted before they reach here and
     * rewritten into IEEE-exact branches; see
     * {@link #tryDivisionByZeroComparison}. The guard below survives only as the finite
     * arm of that rewrite, and for value positions no comparison folds.
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
                // Read the raw double rather than going through protoValueToJava, which
                // narrows an integral value to Long and would discard the sign bit of
                // -0.0 — the one thing that decides which infinity `n / -0.0` is
                // (cerbos/query-plan-adapters#312).
                if (operand.getValue().getKindCase()
                        == Value.KindCase.NUMBER_VALUE) {
                    return new NumericOperand.Constant(operand.getValue().getNumberValue());
                }
                Object v = PlanValues.protoValueToJava(operand.getValue());
                if (!(v instanceof Number n)) {
                    // `R.attr.aString + "x" < "y"` is legal CEL (concatenation, then a
                    // string ordering); this path lowers to double arithmetic only.
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
                    // The original wording here claimed the condition "can never be
                    // satisfied by the PDP". That holds only for a BARE `attr % n`, which
                    // is a CEL no-overload error; `int(attr) % n` is satisfiable, and the
                    // corpus's arith-mod action allows 9 of its 22 seeds
                    // (cerbos/query-plan-adapters#387). The rejection stands either way,
                    // because the cast that makes it satisfiable is itself unlowerable —
                    // the same limitation that refuses cast-int-double.
                    throw Refusals.unsupported(
                            "mod is not supported in comparisons: CEL % is integer-only "
                                    + "while attribute values are always doubles at check "
                                    + "time, so a satisfiable policy must cast with int() "
                                    + "first — and int() has no faithful SQL lowering, "
                                    + "because CAST rounds where CEL truncates toward zero");
                }
                if (ARITHMETIC_OPS.contains(op) && !"div".equals(op)
                        && containsZeroCapableDivision(expr, scope)) {
                    // CEL propagates a NaN or signed infinity through the surrounding
                    // arithmetic; SQL has neither, and the NULLIF guard turns the whole
                    // sum into NULL. `NaN + 1.0 != 2.0` is TRUE for the zero row while
                    // `NULL + 1 <> 2` is UNKNOWN, so the row the PDP allows would be
                    // dropped. The rewrite in tryDivisionByZeroComparison only reaches a
                    // division that IS the comparison operand, so fail closed rather than
                    // emit the under-granting filter (cerbos/query-plan-adapters#312).
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
     * Force a column into IEEE double space for arithmetic (see
     * {@link #resolveNumericOperand}). Renders {@code cb.toDouble} everywhere except
     * when {@link MySqlDoubleCastFunctionContributor} has registered the
     * {@code cerbos_ieee_double} function (Hibernate on MySQL 8.0.17+), which renders
     * {@code cast(col as double)} instead of the {@code MySQLDialect}'s exact-decimal
     * {@code decimal(53,20)} cast. Keyed off the ACTUAL function registration — not
     * dialect name sniffing — so H2/PostgreSQL SQL stays byte-identical and a missing
     * registration (non-Hibernate provider, old MySQL, contributor not discovered)
     * degrades to the previous behavior, never to an unknown-function SQL error.
     */
    private Expression<Double> toIeeeDouble(
            Expression<? extends Number> path) {
        if (IeeeDoubleCast.isRegistered(cb)) {
            return cb.function(
                    MySqlDoubleCastFunctionContributor.FUNCTION_NAME, Double.class, path);
        }
        return cb.toDouble(path);
    }

    /**
     * Emit one SQL arithmetic node; at least one side is a SQL expression. Constants go
     * through the plain-{@code Number} overloads (double bind parameters — see
     * {@link #resolveNumericOperand}).
     */
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

    /** Division with the NULLIF zero-divisor guard (see {@link #resolveNumericOperand}). */
    private Expression<Double> divisionSql(
            Expression<Double> le, Double lc,
            Expression<Double> re, Double rc) {
        if (rc != null) {
            // Constant divisor, numerator is a SQL expression (both-constant subtrees
            // fold before reaching here). Zero → UNKNOWN for every row; non-zero → no
            // guard needed.
            if (rc == 0.0) {
                return cb.nullLiteral(Double.class);
            }
            return cb.quot(le, rc).as(Double.class);
        }
        Expression<Double> guarded = cb.nullif(re, 0.0);
        return (lc != null ? cb.quot(lc, guarded) : cb.quot(le, guarded)).as(Double.class);
    }
}
