package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.IeeeDoubleCast
import dev.cerbos.queryplan.exposed.sql.ScalarColumnKind
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import com.google.protobuf.Value
import org.jetbrains.exposed.v1.core.Op

/**
 * `add` / `sub` / `mult` / `div` as a comparison operand, lowered to SQL in IEEE double space.
 *
 * Owns the double space: every column goes through a real cast ([IeeeDoubleCast]), every constant
 * subtree folds with IEEE semantics, and every constant mixed into SQL arithmetic binds as a double
 * parameter rather than a decimal literal. This is not a convenience. Cerbos transports every
 * attribute number as a protobuf double, so the only arithmetic that evaluates without a CEL
 * no-overload error at check time is double-typed — while H2 and PostgreSQL type a bare `0.1` as
 * exact NUMERIC and evaluate `intCol * 0.1 == 0.3` to TRUE where CEL says `0.30000000000000004`.
 *
 * This class is the PLAN side of that: it turns one operand subtree into an [ArithmeticValue] and
 * owns the refusals a plan shape earns. [ArithmeticValues] owns the algebra, including the whole
 * zero-denominator story — why a division stays symbolic, and how its NaN and infinity arms reach
 * the comparison instead of SQL NULL.
 *
 * Only [ComparisonTranslator.dispatch] routes here, and only for arithmetic-rooted shapes it did
 * not consume as the `add` fold, the eq/ne solve, or a string concatenation — none of those enter
 * double space.
 */
internal class ArithmeticTranslator(comparisons: ComparisonTranslator) {

    private val values = ArithmeticValues(comparisons)

    /**
     * Translates `cmp(arith(...), other)`. Emitting the arithmetic rather than solving it
     * algebraically is what makes multiplication or division by a negative constant need no
     * inequality flipping.
     */
    fun numericComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> = values.compare(
        operator,
        resolveNumericOperand(operands[0], scope),
        resolveNumericOperand(operands[1], scope),
    )

    private fun resolveNumericOperand(operand: Operand, scope: Scope): ArithmeticValue = when (operand.nodeCase) {
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
            ArithmeticValue.Sql(
                IeeeDoubleCast(target.expression),
                unsignedZero = ScalarColumnTypes.kindOf(target.column).let {
                    it == ScalarColumnKind.INTEGRAL || it == ScalarColumnKind.DECIMAL
                },
            )
        }
        Operand.NodeCase.VALUE -> {
            // The raw double, not PlanValues.toKotlin, which narrows an integral value to a Long
            // and would discard the sign bit of -0.0 — the one thing that decides which infinity
            // `n / -0.0` is (#312).
            if (operand.value.kindCase == Value.KindCase.NUMBER_VALUE) {
                ArithmeticValue.Constant(operand.value.numberValue)
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
                ArithmeticValue.Constant(converted.toDouble())
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
    ): ArithmeticValue {
        val operator = expression.operator
        if (operator == "mod") throw ScalarRefusals.modUnsupported()
        if (operator !in ARITHMETIC_OPS) {
            throw ScalarRefusals.unexpectedInsideArithmetic(operator)
        }
        if (expression.operandsCount != 2) {
            throw Refusals.malformed("$operator requires exactly 2 operands")
        }
        return values.apply(
            operator,
            resolveNumericOperand(expression.getOperands(0), scope),
            resolveNumericOperand(expression.getOperands(1), scope),
        )
    }

    companion object {
        /** CEL arithmetic operators that can appear as an operand of a comparison. */
        val ARITHMETIC_OPS: Set<String> = setOf("add", "sub", "mult", "div", "mod")
    }
}
