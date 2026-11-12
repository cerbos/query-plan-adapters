package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.IeeeDoubleCast
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import dev.cerbos.queryplan.exposed.sql.ScalarValueFamily
import dev.cerbos.queryplan.exposed.sql.StringIntCast
import org.jetbrains.exposed.v1.core.GreaterEqOp
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.LessEqOp
import org.jetbrains.exposed.v1.core.LessOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.doubleParam
import org.jetbrains.exposed.v1.core.longParam
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode

/**
 * CEL int arithmetic over one `int(column)` and int constants, compared with a constant:
 * `int(x) / 2 == 1`, `int(x) * 2 > 0`, `7 - int(s) <= 3`. Each step is monotone in `v = int(x)`, so
 * the rows where no step overflows (or divides by zero) are one interval of `v`, and the rows that
 * satisfy the comparison are an interval inside it. Both are found by exact search and emitted as
 * bounds on the column; a row outside the first is CEL's error, UNKNOWN.
 */
internal class IntChain private constructor(
    private val variable: String,
    private val steps: List<Step>,
) {
    private class Step(val operator: String, val constant: BigInteger, val constantFirst: Boolean)

    /** `f(v)`, or `null` where CEL raises. */
    private fun evaluate(v: BigInteger): BigInteger? {
        var value = v
        for (step in steps) {
            val (a, b) = if (step.constantFirst) step.constant to value else value to step.constant
            value = when (step.operator) {
                "add" -> a + b
                "sub" -> a - b
                "mult" -> a * b
                "div" -> if (b.signum() == 0) return null else a.divide(b)
                else -> return null
            }
            if (value < MIN || value > MAX) return null
        }
        return value
    }

    fun compare(operator: String, bound: BigDecimal, scope: Scope): Op<Boolean> {
        val target = scope.scalar(variable)
        val text = ScalarColumnTypes.familyOf(target.column) == ScalarValueFamily.TEXT
        if (!text && !ScalarColumnTypes.isNumeric(target.column)) throw ScalarRefusals.numericCastUnsupported("int")
        if (evaluate(BigInteger.ZERO) == null) throw ScalarRefusals.numericCastUnsupported("int")
        val low = edge(MIN, BigInteger.ZERO) { evaluate(it) != null }
        val high = edge(MAX, BigInteger.ZERO) { evaluate(it) != null }
        val rising = evaluate(low)!! <= evaluate(high)!!
        fun holds(v: BigInteger, op: String): Boolean {
            val c = BigDecimal(evaluate(v)!!).compareTo(bound)
            return when (op) {
                "ge" -> c >= 0
                "gt" -> c > 0
                "le" -> c <= 0
                "lt" -> c < 0
                else -> throw Refusals.internal("Unsupported int comparison operator: $op")
            }
        }
        val bounds = Bounds(target.expression, text)
        fun side(op: String): Op<Boolean> {
            val atHighEnd = (op == "ge" || op == "gt") == rising
            val (end, other) = if (atHighEnd) high to low else low to high
            if (!holds(end, op)) return Op.FALSE
            if (holds(other, op)) return Op.TRUE
            val first = edge(other, end) { holds(it, op) }
            return if (atHighEnd) bounds.atLeast(first) else bounds.atMost(first)
        }
        val base = when (operator) {
            "eq" -> TriLogic.and(side("ge"), side("le"))
            "ne" -> TriLogic.not(TriLogic.and(side("ge"), side("le")))
            "gt", "ge", "lt", "le" -> side(operator)
            else -> throw Refusals.internal("Unsupported int comparison operator: $operator")
        }
        val valid = TriLogic.and(bounds.cast(), bounds.atLeast(low), bounds.atMost(high))
        return TriLogic.baseUnlessUnknown(base, TriLogic.not(valid))
    }

    /** The point nearest [from] in the direction of [inside], where [test] holds, given it holds at [inside]. */
    private fun edge(from: BigInteger, inside: BigInteger, test: (BigInteger) -> Boolean): BigInteger {
        if (test(from)) return from
        var out = from
        var good = inside
        while ((good - out).abs() > BigInteger.ONE) {
            val mid = (good + out).shiftRight(1).let { if (it == out) out + (good - out).signum().toBigInteger() else it }
            if (test(mid)) good = mid else out = mid
        }
        return good
    }

    /** `int(x) >= m` and `int(x) <= m` as bounds on the column itself. */
    private class Bounds(private val expression: org.jetbrains.exposed.v1.core.Expression<*>, private val text: Boolean) {
        private val asInt = StringIntCast(expression)
        private val asDouble = IeeeDoubleCast(expression)

        /** Where `int(x)` exists at all. */
        fun cast(): Op<Boolean> = if (text) {
            TriLogic.not(IsNullOp(asInt))
        } else {
            TriLogic.and(GreaterOp(asDouble, doubleParam(-TWO_63)), LessOp(asDouble, doubleParam(TWO_63)))
        }

        fun atLeast(m: BigInteger): Op<Boolean> = when {
            m <= MIN -> Op.TRUE
            text -> GreaterEqOp(asInt, longParam(m.toLong()))
            // int() truncates toward zero: int(x) >= 1 is x >= 1, int(x) >= 0 is x > -1.
            m.signum() > 0 -> GreaterEqOp(asDouble, doubleParam(roundUp(BigDecimal(m))))
            else -> GreaterOp(asDouble, doubleParam(roundDown(BigDecimal(m - BigInteger.ONE))))
        }

        fun atMost(m: BigInteger): Op<Boolean> = when {
            m >= MAX -> Op.TRUE
            text -> LessEqOp(asInt, longParam(m.toLong()))
            m.signum() < 0 -> LessEqOp(asDouble, doubleParam(roundDown(BigDecimal(m))))
            else -> LessOp(asDouble, doubleParam(roundUp(BigDecimal(m + BigInteger.ONE))))
        }

        /** The least double not below [m]: `x >= m` is `x >= roundUp(m)` for a double x. */
        private fun roundUp(m: BigDecimal): Double {
            val d = m.toDouble()
            return if (BigDecimal(d) < m) Math.nextUp(d) else d
        }

        private fun roundDown(m: BigDecimal): Double {
            val d = m.toDouble()
            return if (BigDecimal(d) > m) Math.nextDown(d) else d
        }
    }

    companion object {
        private val MIN: BigInteger = BigInteger.valueOf(Long.MIN_VALUE)
        private val MAX: BigInteger = BigInteger.valueOf(Long.MAX_VALUE)
        private const val TWO_63 = 9.223372036854775808E18
        private val OPERATORS = setOf("add", "sub", "mult", "div")

        /** The chain [operand] spells, or `null` when it is not one `int(variable)` under int constants. */
        fun of(operand: Operand): IntChain? {
            if (operand.nodeCase != Operand.NodeCase.EXPRESSION) return null
            val expression = operand.expression
            if (expression.operator == "int" && expression.operandsCount == 1 &&
                expression.getOperands(0).nodeCase == Operand.NodeCase.VARIABLE
            ) {
                return IntChain(expression.getOperands(0).variable, emptyList())
            }
            if (expression.operator !in OPERATORS || expression.operandsCount != 2) return null
            val (a, b) = expression.operandsList
            val constantFirst = a.nodeCase == Operand.NodeCase.VALUE
            val (inner, constant) = if (constantFirst) b to a else a to b
            // A constant divided by the column is not monotone.
            if (constantFirst && expression.operator == "div") return null
            val value = integer(constant) ?: return null
            val chain = of(inner) ?: return null
            return IntChain(chain.variable, chain.steps + Step(expression.operator, value, constantFirst))
        }

        private fun integer(operand: Operand): BigInteger? {
            if (operand.nodeCase != Operand.NodeCase.VALUE || operand.value.kindCase != Value.KindCase.NUMBER_VALUE) {
                return null
            }
            val n = operand.value.numberValue
            if (!n.isFinite() || n != Math.rint(n)) return null
            return BigDecimal(n).setScale(0, RoundingMode.UNNECESSARY).toBigIntegerExact()
        }
    }
}
