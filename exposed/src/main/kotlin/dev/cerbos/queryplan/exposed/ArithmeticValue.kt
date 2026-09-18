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
 * One arithmetic operand, resolved as far as SQL can take it.
 *
 * SQL has no NaN and no infinity, and CEL produces both: attribute arithmetic is double-typed
 * (Cerbos transports every number as a protobuf double), so `0.0/0.0` is NaN and `x/0.0` is a
 * signed infinity, and each of those goes on to flow through `+`, `-` and `*`. A division is
 * therefore NOT lowered to SQL where its denominator may be zero. It stays a [Branch] — a ternary
 * over the denominator, then the numerator — whose arms are IEEE constants, and the enclosing
 * arithmetic and comparison distribute over it until each arm is something SQL can state.
 */
internal sealed interface ArithmeticValue {
    /** A constant, possibly non-finite: a division arm is NaN or a signed infinity. */
    class Constant(val value: Double) : ArithmeticValue

    /** A SQL expression in double space, already cast by [IeeeDoubleCast] where it is a column. */
    class Sql(val expr: Expression<*>) : ArithmeticValue

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
 * CEL's arithmetic and comparison over [ArithmeticValue], in IEEE double space.
 *
 * Every operation distributes over a [ArithmeticValue.Branch] before it does anything else, so a
 * non-finite division arm reaches the leaf as a constant and is FOLDED there rather than asked for
 * a SQL form it does not have. That is the whole mechanism: `R.attr.aNumber / R.attr.aNumber + 1.0`
 * becomes four arms — `NaN + 1.0`, `+Infinity + 1.0`, `-Infinity + 1.0`, and the guarded quotient —
 * of which only the last reaches the database. Lowering the division to SQL instead turns the sum
 * into `NULL + 1`, and `NULL <> 2.0` is UNKNOWN where CEL's `NaN != 2.0` is TRUE: the row the PDP
 * allows would be dropped.
 *
 * [ComparisonTranslator] owns the leaf comparisons themselves, so the IEEE constant folding here is
 * the same one every other constant comparison in the adapter goes through.
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

    /**
     * CEL division, kept symbolic wherever a zero denominator is possible.
     *
     * The finite arm still carries a `NULLIF` guard even though it can never be SELECTED for a
     * zero denominator: [TriLogic.ternary] emits a predicate, not a `CASE`, so SQL is free to
     * evaluate both arms and an unguarded division would abort the whole query on the zero row —
     * a data-dependent failure rather than a filter.
     */
    private fun divide(dividend: ArithmeticValue, divisor: ArithmeticValue): ArithmeticValue {
        if (divisor is ArithmeticValue.Constant) {
            if (divisor.value != 0.0) {
                if (dividend is ArithmeticValue.Constant) {
                    return ArithmeticValue.Constant(dividend.value / divisor.value)
                }
                return ArithmeticValue.Sql(
                    CustomOperator("/", DoubleColumnType(), sqlOf(dividend), doubleParam(divisor.value)),
                )
            }
            // IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from
            // `n / 0.0`. A CONSTANT denominator carries its sign here — the planner ships `-0` and
            // protobuf doubles preserve the sign bit — so it decides the arms.
            return nonFiniteQuotient(dividend, divisor.value)
        }
        val denominator = sqlOf(divisor)
        return ArithmeticValue.Branch(
            EqOp(denominator, doubleParam(0.0)),
            // A COLUMN denominator cannot carry a sign: SQL has no portable way to read the sign
            // bit of a stored zero, so the positive-zero reading is assumed and documented (#312).
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

    /**
     * The quotient of [dividend] by a zero of [zeroDenominator]'s sign: NaN for a zero numerator,
     * and otherwise the infinity the two signs give.
     *
     * A NULL numerator leaves both conditions UNKNOWN, which is the CEL missing-attribute deny.
     */
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

    /**
     * A NaN or an infinity against a SQL expression.
     *
     * Both compare the same way against every FINITE number, so `0.0` stands in for the value and
     * only its NULL-ness has to survive: [TriLogic.baseUnlessUnknown] drives the arm to UNKNOWN for
     * a NULL, which keeps the row excluded under BOTH polarities. A stored value that is itself an
     * infinity is outside what the stand-in states; no engine here produces one from a translated
     * expression, since every division that could is folded above rather than executed.
     */
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
