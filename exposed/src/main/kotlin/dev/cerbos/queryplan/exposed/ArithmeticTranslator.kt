package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.IeeeDoubleCast
import dev.cerbos.queryplan.exposed.sql.NullLiteral
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import com.google.protobuf.Value
import org.jetbrains.exposed.v1.core.CustomFunction
import org.jetbrains.exposed.v1.core.CustomOperator
import org.jetbrains.exposed.v1.core.DoubleColumnType
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.doubleParam

/**
 * `add` / `sub` / `mult` / `div` as a comparison operand, lowered to SQL in IEEE double space.
 *
 * Owns the double space: every column goes through a real cast ([IeeeDoubleCast]), every constant
 * subtree folds here with IEEE semantics, and every constant mixed into SQL arithmetic binds as a
 * double parameter rather than a decimal literal. This is not a convenience. Cerbos transports
 * every attribute number as a protobuf double, so the only arithmetic that evaluates without a
 * CEL no-overload error at check time is double-typed — while H2 and PostgreSQL type a bare `0.1`
 * as exact NUMERIC and evaluate `intCol * 0.1 == 0.3` to TRUE where CEL says
 * `0.30000000000000004`.
 *
 * It also owns the zero-divisor story: the `NULLIF` guard that keeps a query from failing on a
 * zero row, and the IEEE-arm rewrite ([tryDivisionByZeroComparison]) that makes an INEQUALITY
 * against NaN or a signed infinity agree with CEL, where the guard alone would not — `NaN != 1.0`
 * is TRUE and the PDP allows the row, while `NULL <> 1.0` is UNKNOWN and would deny it.
 *
 * Only [ComparisonTranslator.dispatch] routes here, and only for arithmetic-rooted shapes it did
 * not consume as the `add` fold, the eq/ne solve, or a string concatenation — none of those enter
 * double space.
 */
internal class ArithmeticTranslator(private val comparisons: ComparisonTranslator) {

    /** A resolved arithmetic operand: a constant folded here, or a SQL expression in double space. */
    private sealed interface NumericOperand {
        class Constant(val value: Double) : NumericOperand

        class Sql(val expr: Expression<*>) : NumericOperand
    }

    /**
     * Translates `cmp(arith(...), other)` by emitting the arithmetic on the SQL side and comparing
     * there. Emitting it rather than solving algebraically is also what makes multiplication or
     * division by a negative constant need no inequality flipping.
     */
    fun numericComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> {
        // A zero-divisor division must be folded against the comparison, not lowered to NULL.
        tryDivisionByZeroComparison(operator, operands, scope)?.let { return it }
        return comparisonWithoutZeroGuard(operator, operands, scope)
    }

    /** The plain arithmetic comparison, bypassing the zero-divisor rewrite. */
    private fun comparisonWithoutZeroGuard(
        operator: String,
        operands: List<Operand>,
        scope: Scope,
    ): Op<Boolean> {
        var left = resolveNumericOperand(operands[0], scope)
        var right = resolveNumericOperand(operands[1], scope)
        var op = operator

        // Both sides folded to constants — reachable through ternary substitution, e.g.
        // gt(add(1.0, 2.0), 4.0). Evaluate statically, with IEEE semantics.
        if (left is NumericOperand.Constant && right is NumericOperand.Constant) {
            return comparisons.constantComparison(op, left.value, right.value)
        }
        // Keep the SQL side on the left, mirroring the operator. Normalisation usually guarantees
        // it already, but an expression that FOLDS to a constant ranks as an expression and can
        // still arrive first.
        if (left is NumericOperand.Constant) {
            val swapped = left
            left = right
            right = swapped
            op = NormalizedBinary.mirror(op)
        }
        val lhs = (left as NumericOperand.Sql).expr
        val rhs = when (right) {
            is NumericOperand.Constant -> doubleParam(right.value)
            is NumericOperand.Sql -> right.expr
        }
        return comparisons.compare(op, lhs, rhs)
    }

    /**
     * Folds a comparison whose operand is a division that can divide by zero, or returns `null`
     * when the shape does not apply.
     *
     * CEL attribute arithmetic is double-typed, so `0/0` is NaN and `x/0` is a signed infinity.
     * Lowering the division to SQL NULL (the `NULLIF` guard in [divisionSql]) agrees with CEL for
     * ORDERED comparisons — NaN and NULL both exclude the row — and diverges for an INEQUALITY.
     * The rewrite is nested ternaries rather than `CASE WHEN`, matching this translator's
     * predicate-only design:
     *
     * ```
     * if (d == 0) { if (n == 0) NaN op v else if (n > 0) +Inf op v else -Inf op v }
     * else { n / d op v }
     * ```
     *
     * Each non-finite arm folds statically, so no NaN or infinity is ever bound — SQL has no
     * literal for either. [TriLogic.ternary] owns the UNKNOWN arms: a NULL numerator or
     * denominator drives every condition UNKNOWN, so the row stays excluded under BOTH polarities,
     * which is the CEL missing-attribute deny.
     */
    private fun tryDivisionByZeroComparison(
        operator: String,
        operands: List<Operand>,
        scope: Scope,
    ): Op<Boolean>? {
        if (isZeroCapableDivision(operands[0], scope) && isZeroCapableDivision(operands[1], scope)) {
            // Only one side can be folded into IEEE arms; the other would still lower to NULL,
            // turning `NaN != NaN` (TRUE in CEL) into UNKNOWN. Fail closed.
            throw Refusals.unsupported(
                "a comparison with a zero-capable division on BOTH sides is not supported: only " +
                    "one side can be folded into IEEE arms and the other would lower to SQL NULL",
            )
        }
        for (side in 0..1) {
            val candidate = operands[side]
            if (candidate.nodeCase != Operand.NodeCase.EXPRESSION) continue
            val division = candidate.expression
            if (division.operator != "div" || division.operandsCount != 2) continue

            // A NON-ZERO constant divisor is already decided statically, and a fully constant
            // subtree folds to an exact IEEE value — neither needs the rewrite. A constant ZERO
            // does: NULLIF(0, 0) is NULL, which makes every comparison UNKNOWN, while CEL
            // produces a signed infinity for a non-zero numerator.
            val divisor = resolveNumericOperand(division.getOperands(1), scope)
            if (divisor is NumericOperand.Constant && divisor.value != 0.0) continue
            val dividend = resolveNumericOperand(division.getOperands(0), scope)
            if (divisor is NumericOperand.Constant && dividend is NumericOperand.Constant) continue

            // IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from
            // `n / 0.0`. A constant divisor carries its sign here — the planner ships `-0` and
            // protobuf doubles preserve the sign bit — so it must be applied. A COLUMN divisor
            // cannot: SQL has no portable way to read the sign bit of a stored zero, so the
            // positive reading is assumed and documented (#312).
            val negativeZeroDivisor =
                divisor is NumericOperand.Constant && java.lang.Double.doubleToRawLongBits(divisor.value) != 0L
            val positiveDividendResult =
                if (negativeZeroDivisor) Double.NEGATIVE_INFINITY else Double.POSITIVE_INFINITY
            val negativeDividendResult =
                if (negativeZeroDivisor) Double.POSITIVE_INFINITY else Double.NEGATIVE_INFINITY

            val divisionIsLeft = side == 0
            val other = operands[if (divisionIsLeft) 1 else 0]

            // Fold `nonFinite op other` (or the mirrored order) here. A non-finite compares the
            // same way against every PRESENT value, so a column operand only needs its NULL-ness
            // preserved: baseUnlessUnknown drives the arm to UNKNOWN when it is NULL, keeping the
            // row excluded under both polarities.
            fun arm(nonFinite: Double): Op<Boolean> {
                val resolved = resolveNumericOperand(other, scope)
                if (resolved is NumericOperand.Constant) {
                    return if (divisionIsLeft) {
                        comparisons.constantComparison(operator, nonFinite, resolved.value)
                    } else {
                        comparisons.constantComparison(operator, resolved.value, nonFinite)
                    }
                }
                val folded = if (divisionIsLeft) {
                    comparisons.constantComparison(operator, nonFinite, 0.0)
                } else {
                    comparisons.constantComparison(operator, 0.0, nonFinite)
                }
                return TriLogic.baseUnlessUnknown(folded, IsNullOp((resolved as NumericOperand.Sql).expr))
            }

            val zeroDivisor: Op<Boolean> = if (divisor is NumericOperand.Constant) {
                Op.TRUE
            } else {
                EqOp((divisor as NumericOperand.Sql).expr, doubleParam(0.0))
            }
            val dividendSql = sqlOf(dividend)
            return TriLogic.ternary(
                zeroDivisor,
                TriLogic.ternary(
                    EqOp(dividendSql, doubleParam(0.0)),
                    arm(Double.NaN),
                    TriLogic.ternary(
                        GreaterOp(dividendSql, doubleParam(0.0)),
                        arm(positiveDividendResult),
                        arm(negativeDividendResult),
                    ),
                ),
                comparisonWithoutZeroGuard(operator, operands, scope),
            )
        }
        return null
    }

    /** Whether an operand IS a division that can divide by zero. */
    private fun isZeroCapableDivision(operand: Operand, scope: Scope): Boolean {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION) return false
        val expression = operand.expression
        if (expression.operator != "div" || expression.operandsCount != 2) return false
        val divisor = resolveNumericOperand(expression.getOperands(1), scope)
        val dividend = resolveNumericOperand(expression.getOperands(0), scope)
        if (divisor is NumericOperand.Constant && divisor.value != 0.0) return false
        return !(divisor is NumericOperand.Constant && dividend is NumericOperand.Constant)
    }

    /** Whether an arithmetic subtree holds a division that can divide by zero. */
    private fun containsZeroCapableDivision(expression: PlanResourcesFilter.Expression, scope: Scope): Boolean {
        if (expression.operator !in ARITHMETIC_OPS) return false
        return expression.operandsList.any { child ->
            isZeroCapableDivision(child, scope) ||
                (child.nodeCase == Operand.NodeCase.EXPRESSION && containsZeroCapableDivision(child.expression, scope))
        }
    }

    private fun sqlOf(operand: NumericOperand): Expression<*> = when (operand) {
        is NumericOperand.Sql -> operand.expr
        is NumericOperand.Constant -> doubleParam(operand.value)
    }

    private fun resolveNumericOperand(operand: Operand, scope: Scope): NumericOperand = when (operand.nodeCase) {
        Operand.NodeCase.VARIABLE -> {
            val target = scope.scalar(operand.variable)
            if (!ScalarColumnTypes.isNumeric(target.column)) {
                // Emitting the cast anyway would abort the whole query on PostgreSQL for a text
                // column, and read a numeric prefix on SQLite — a wrong answer rather than an
                // error. CEL raises a no-overload error here, which denies.
                throw Refusals.unmapped(
                    "Arithmetic over '${operand.variable}' requires a numeric column, but it maps " +
                        "to a ${ScalarColumnTypes.describe(target.column)} column",
                )
            }
            NumericOperand.Sql(IeeeDoubleCast(target.expression))
        }
        Operand.NodeCase.VALUE -> {
            // The raw double, not PlanValues.toKotlin, which narrows an integral value to a Long
            // and would discard the sign bit of -0.0 — the one thing that decides which infinity
            // `n / -0.0` is (#312).
            if (operand.value.kindCase == Value.KindCase.NUMBER_VALUE) {
                NumericOperand.Constant(operand.value.numberValue)
            } else {
                val converted = PlanValues.toKotlin(operand.value)
                if (converted !is Number) {
                    // `R.attr.aString + "x" < "y"` is legal CEL (concatenation, then a string
                    // ordering); this path lowers to double arithmetic only.
                    throw Refusals.unsupported(
                        "Arithmetic comparison requires numeric operands, got " +
                            PlanValues.typeName(converted),
                    )
                }
                NumericOperand.Constant(converted.toDouble())
            }
        }
        Operand.NodeCase.EXPRESSION -> resolveNumericExpression(operand.expression, scope)
        else -> throw Refusals.malformed(
            "Unexpected operand type in arithmetic comparison: ${operand.nodeCase}",
        )
    }

    private fun resolveNumericExpression(
        expression: PlanResourcesFilter.Expression,
        scope: Scope,
    ): NumericOperand {
        val operator = expression.operator
        if (operator == "mod") throw ScalarRefusals.modUnsupported()
        if (operator in ARITHMETIC_OPS && operator != "div" && containsZeroCapableDivision(expression, scope)) {
            // CEL propagates a NaN or signed infinity through the surrounding arithmetic; SQL has
            // neither, and the NULLIF guard turns the whole sum into NULL. `NaN + 1.0 != 2.0` is
            // TRUE for the zero row while `NULL + 1 <> 2` is UNKNOWN, so the row the PDP allows
            // would be dropped. The rewrite above only reaches a division that IS the comparison
            // operand, so fail closed rather than emit the under-granting filter (#312).
            throw Refusals.unsupported(
                "arithmetic composed on a division whose denominator may be zero is not " +
                    "supported: CEL carries the resulting NaN or infinity through the surrounding " +
                    "arithmetic and SQL has no value that does",
            )
        }
        if (operator !in ARITHMETIC_OPS) {
            throw ScalarRefusals.unexpectedInsideArithmetic(operator)
        }
        if (expression.operandsCount != 2) {
            throw Refusals.malformed("$operator requires exactly 2 operands")
        }
        val left = resolveNumericOperand(expression.getOperands(0), scope)
        val right = resolveNumericOperand(expression.getOperands(1), scope)
        if (left is NumericOperand.Constant && right is NumericOperand.Constant) {
            return NumericOperand.Constant(
                when (operator) {
                    "add" -> left.value + right.value
                    "sub" -> left.value - right.value
                    "mult" -> left.value * right.value
                    // IEEE: ±Infinity, and NaN for 0/0.
                    "div" -> left.value / right.value
                    else -> throw Refusals.internal("Unsupported arithmetic operator: $operator")
                },
            )
        }
        return NumericOperand.Sql(arithmeticSql(operator, left, right))
    }

    private fun arithmeticSql(operator: String, left: NumericOperand, right: NumericOperand): Expression<*> =
        when (operator) {
            "add" -> binary("+", left, right)
            "sub" -> binary("-", left, right)
            "mult" -> binary("*", left, right)
            "div" -> divisionSql(left, right)
            else -> throw Refusals.internal("Unsupported arithmetic operator: $operator")
        }

    private fun binary(symbol: String, left: NumericOperand, right: NumericOperand): Expression<*> =
        CustomOperator<Double>(symbol, DoubleColumnType(), sqlOf(left), sqlOf(right))

    /**
     * Division with the zero-divisor guard. SQL raises on a zero divisor — a data-dependent
     * failure of the WHOLE query — while CEL double division is defined. A column divisor is
     * wrapped in `NULLIF(d, 0)` so the query survives; a constant divisor is decided here, since
     * [tryDivisionByZeroComparison] has already rewritten every shape where the UNKNOWN a zero
     * divisor produces would diverge from CEL.
     */
    private fun divisionSql(dividend: NumericOperand, divisor: NumericOperand): Expression<*> {
        if (divisor is NumericOperand.Constant) {
            if (divisor.value == 0.0) return NullLiteral
            return CustomOperator<Double>("/", DoubleColumnType(), sqlOf(dividend), doubleParam(divisor.value))
        }
        val guarded = CustomFunction<Double>("NULLIF", DoubleColumnType(), sqlOf(divisor), doubleParam(0.0))
        return CustomOperator<Double>("/", DoubleColumnType(), sqlOf(dividend), guarded)
    }

    companion object {
        /** CEL arithmetic operators that can appear as an operand of a comparison. */
        val ARITHMETIC_OPS: Set<String> = setOf("add", "sub", "mult", "div", "mod")
    }
}
