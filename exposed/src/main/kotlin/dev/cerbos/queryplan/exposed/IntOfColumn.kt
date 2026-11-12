package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.IeeeDoubleCast
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import dev.cerbos.queryplan.exposed.sql.ScalarValueFamily
import dev.cerbos.queryplan.exposed.sql.StringIntCast
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.LessOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.doubleParam
import org.jetbrains.exposed.v1.core.longParam
import java.math.BigDecimal
import java.math.BigInteger

/** CEL's `int()` of a numeric or text column, compared with an exact integer; UNKNOWN where `int()` raises. */
internal class IntOfColumn(target: Resolution.Scalar) {
    private val text = ScalarColumnTypes.familyOf(target.column) == ScalarValueFamily.TEXT
    private val asInt = StringIntCast(target.expression)
    private val asDouble = IeeeDoubleCast(target.expression)

    init {
        if (!text && !ScalarColumnTypes.isNumeric(target.column)) throw ScalarRefusals.numericCastUnsupported("int")
    }

    private fun exists(): Op<Boolean> = if (text) {
        TriLogic.not(IsNullOp(asInt))
    } else {
        TriLogic.and(GreaterOp(asDouble, doubleParam(-TWO_63)), LessOp(asDouble, doubleParam(TWO_63)))
    }

    /** `int(x) == n`. A double truncates to `n` on `(n - 1, n + 1)` for `n == 0`, else on the half-open side away from zero. */
    fun equalTo(n: BigInteger): Op<Boolean> {
        val base = if (text) {
            EqOp(asInt, longParam(n.toLong()))
        } else {
            val low = if (n.signum() > 0) BigDecimal(n) else BigDecimal(n - BigInteger.ONE)
            val high = if (n.signum() < 0) BigDecimal(n) else BigDecimal(n + BigInteger.ONE)
            val lowOp = if (n.signum() > 0) {
                org.jetbrains.exposed.v1.core.GreaterEqOp(asDouble, doubleParam(roundUp(low)))
            } else {
                GreaterOp(asDouble, doubleParam(roundDown(low)))
            }
            val highOp = if (n.signum() < 0) {
                org.jetbrains.exposed.v1.core.LessEqOp(asDouble, doubleParam(roundDown(high)))
            } else {
                LessOp(asDouble, doubleParam(roundUp(high)))
            }
            TriLogic.and(lowOp, highOp)
        }
        return TriLogic.baseUnlessUnknown(base, TriLogic.not(exists()))
    }

    /** FALSE wherever `int(x)` exists. */
    fun never(): Op<Boolean> = TriLogic.baseUnlessUnknown(Op.FALSE, TriLogic.not(exists()))

    private fun roundUp(m: BigDecimal): Double = m.toDouble().let { if (BigDecimal(it) < m) Math.nextUp(it) else it }

    private fun roundDown(m: BigDecimal): Double = m.toDouble().let { if (BigDecimal(it) > m) Math.nextDown(it) else it }

    private companion object {
        const val TWO_63 = 9.223372036854775808E18
    }
}
