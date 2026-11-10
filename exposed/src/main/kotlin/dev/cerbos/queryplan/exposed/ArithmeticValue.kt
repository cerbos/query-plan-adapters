package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.IeeeDoubleCast
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
 * One arithmetic operand. SQL has no NaN or infinity, so a division whose denominator may be zero
 * stays a [Branch] over the IEEE outcomes, which the enclosing operations distribute over.
 */
internal sealed interface ArithmeticValue {
    /** A constant, possibly non-finite: a division arm is NaN or a signed infinity. */
    class Constant(val value: Double) : ArithmeticValue

    /** A SQL expression in double space. [unsignedZero] marks an integral or decimal column read, whose zero is `+0.0`. */
    class Sql(val expr: Expression<*>, val unsignedZero: Boolean = false) : ArithmeticValue

    /**
     * A retained ternary. [condition] is two-valued for a present row and UNKNOWN for a NULL
     * operand, which is what carries a CEL missing-attribute error through to the deny that
     * [TriLogic.ternary] emits for it.
     */
    class Branch(
        val condition: Op<Boolean>,
        val whenTrue: ArithmeticValue,
        val whenFalse: ArithmeticValue,
    ) : ArithmeticValue
}

/**
 * CEL's arithmetic and comparison over [ArithmeticValue], in IEEE double space. Every operation
 * distributes over a [ArithmeticValue.Branch] first, so non-finite arms fold to constants.
 */
internal class ArithmeticValues(private val comparisons: ComparisonTranslator) {

    /** `add` / `sub` / `mult` / `div` over two resolved operands. */
    fun apply(operator: String, left: ArithmeticValue, right: ArithmeticValue): ArithmeticValue {
        if (left is ArithmeticValue.Branch) {
            return ArithmeticValue.Branch(
                left.condition,
                apply(operator, left.whenTrue, right),
                apply(operator, left.whenFalse, right),
            )
        }
        if (right is ArithmeticValue.Branch) {
            return ArithmeticValue.Branch(
                right.condition,
                apply(operator, left, right.whenTrue),
                apply(operator, left, right.whenFalse),
            )
        }
        if (operator == "div") return divide(left, right)
        if (left is ArithmeticValue.Constant && right is ArithmeticValue.Constant) {
            return ArithmeticValue.Constant(fold(operator, left.value, right.value))
        }
        return ArithmeticValue.Sql(
            CustomOperator(symbolFor(operator), DoubleColumnType(), sqlOf(left), sqlOf(right)),
        )
    }

    /** The comparison of two resolved operands, as a three-valued predicate. */
    fun compare(operator: String, left: ArithmeticValue, right: ArithmeticValue): Op<Boolean> {
        if (left is ArithmeticValue.Branch) {
            return TriLogic.ternary(
                left.condition,
                compare(operator, left.whenTrue, right),
                compare(operator, left.whenFalse, right),
            )
        }
        if (right is ArithmeticValue.Branch) {
            return TriLogic.ternary(
                right.condition,
                compare(operator, left, right.whenTrue),
                compare(operator, left, right.whenFalse),
            )
        }
        if (left is ArithmeticValue.Constant && right is ArithmeticValue.Constant) {
            return comparisons.constantComparison(operator, left.value, right.value)
        }
        if (left is ArithmeticValue.Constant && !left.value.isFinite()) {
            return nonFiniteComparison(operator, left.value, sqlOf(right), nonFiniteIsLeft = true)
        }
        if (right is ArithmeticValue.Constant && !right.value.isFinite()) {
            return nonFiniteComparison(operator, right.value, sqlOf(left), nonFiniteIsLeft = false)
        }
        // Keep the SQL side on the left, mirroring the operator. Normalisation usually guarantees
        // it already, but an expression that FOLDS to a constant ranks as an expression and can
        // still arrive first.
        if (left is ArithmeticValue.Constant) {
            return comparisons.compare(NormalizedBinary.mirror(operator), sqlOf(right), sqlOf(left))
        }
        return comparisons.compare(operator, sqlOf(left), sqlOf(right))
    }

    /** CEL division, kept symbolic where a zero denominator is possible; the finite arm keeps `NULLIF` since SQL may evaluate both. */
    private fun divide(dividend: ArithmeticValue, divisor: ArithmeticValue): ArithmeticValue {
        if (divisor is ArithmeticValue.Constant) {
            if (divisor.value != 0.0) {
                if (dividend is ArithmeticValue.Constant) {
                    return ArithmeticValue.Constant(dividend.value / divisor.value)
                }
                // Through [sqlOf], which refuses a non-finite divisor rather than binding it.
                return ArithmeticValue.Sql(
                    CustomOperator("/", DoubleColumnType(), sqlOf(dividend), sqlOf(divisor)),
                )
            }
            // IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from
            // `n / 0.0`. A CONSTANT denominator carries its sign here — the planner ships `-0` and
            // protobuf doubles preserve the sign bit — so it decides the arms.
            return nonFiniteQuotient(dividend, divisor.value)
        }
        // A SQL denominator cannot carry a sign: SQL compares -0.0 equal to 0.0 and has no portable
        // way to read the sign bit, while CEL divides a positive number by -0.0 into -Infinity.
        // Only a denominator whose zero is certainly positive is lowered (#312).
        if (divisor !is ArithmeticValue.Sql || !divisor.unsignedZero) {
            throw ScalarRefusals.signedZeroDivisor()
        }
        val denominator = sqlOf(divisor)
        return ArithmeticValue.Branch(
            EqOp(denominator, doubleParam(0.0)),
            nonFiniteQuotient(dividend, 0.0),
            ArithmeticValue.Sql(
                CustomOperator(
                    "/",
                    DoubleColumnType(),
                    sqlOf(dividend),
                    CustomFunction<Double>("NULLIF", DoubleColumnType(), denominator, doubleParam(0.0)),
                ),
            ),
        )
    }

    /** The quotient of [dividend] by a signed zero: NaN for a zero numerator, else an infinity. */
    private fun nonFiniteQuotient(dividend: ArithmeticValue, zeroDenominator: Double): ArithmeticValue {
        if (dividend is ArithmeticValue.Constant) {
            return ArithmeticValue.Constant(dividend.value / zeroDenominator)
        }
        val numerator = sqlOf(dividend)
        return ArithmeticValue.Branch(
            EqOp(numerator, doubleParam(0.0)),
            ArithmeticValue.Constant(Double.NaN),
            ArithmeticValue.Branch(
                GreaterOp(numerator, doubleParam(0.0)),
                ArithmeticValue.Constant(1.0 / zeroDenominator),
                ArithmeticValue.Constant(-1.0 / zeroDenominator),
            ),
        )
    }

    /** A NaN or infinity against a SQL expression: `0.0` stands in, keeping only the NULL-ness. */
    private fun nonFiniteComparison(
        operator: String,
        nonFinite: Double,
        other: Expression<*>,
        nonFiniteIsLeft: Boolean,
    ): Op<Boolean> {
        val folded = if (nonFiniteIsLeft) {
            comparisons.constantComparison(operator, nonFinite, 0.0)
        } else {
            comparisons.constantComparison(operator, 0.0, nonFinite)
        }
        return TriLogic.baseUnlessUnknown(folded, IsNullOp(other))
    }

    private fun sqlOf(value: ArithmeticValue): Expression<*> = when (value) {
        is ArithmeticValue.Sql -> value.expr
        is ArithmeticValue.Constant ->
            if (value.value.isFinite()) doubleParam(value.value) else throw ScalarRefusals.nonFiniteInArithmetic()
        // Every operation above distributes before it asks for a SQL form, so this is an adapter
        // bug rather than a plan the walk cannot express.
        is ArithmeticValue.Branch ->
            throw Refusals.internal("A retained division ternary has no SQL form; distribute over it first")
    }

    private fun fold(operator: String, left: Double, right: Double): Double = when (operator) {
        "add" -> left + right
        "sub" -> left - right
        "mult" -> left * right
        else -> throw Refusals.internal("Unsupported arithmetic operator: $operator")
    }

    private fun symbolFor(operator: String): String = when (operator) {
        "add" -> "+"
        "sub" -> "-"
        "mult" -> "*"
        else -> throw Refusals.internal("Unsupported arithmetic operator: $operator")
    }
}
