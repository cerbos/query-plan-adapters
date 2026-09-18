package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.Params
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.GreaterEqOp
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.LessEqOp
import org.jetbrains.exposed.v1.core.LessOp
import org.jetbrains.exposed.v1.core.NeqOp
import org.jetbrains.exposed.v1.core.Op

/**
 * The scalar leaf: one resolved value against one plan constant.
 *
 * THE SINGLE ROUTING POINT. Every comparison of one mapped column against one constant goes
 * through [applyLeaf], under the NORMALISED operator name, whichever path reached it. Nothing hooks
 * into it today; it is what makes operator overrides a one-place change if they are ever added.
 *
 * OWNED BY THE SCALAR SIDE. This minimal form covers the six comparison operators against a
 * non-null constant. Still to come: string matching with escaped LIKE, the null operand, definite
 * equality under the EXPLICIT null convention, and double-space comparison for fractional
 * constants.
 */
internal class LeafTranslator(@Suppress("unused") private val translation: Translation) {
    fun applyLeaf(operator: String, target: Resolution.Scalar, value: Any?): Op<Boolean> {
        if (value == null) throw Refusals.notYetImplemented("$operator against a null constant")
        val constant = Params.of(value)
        return when (operator) {
            "eq" -> EqOp(target.expression, constant)
            "ne" -> NeqOp(target.expression, constant)
            "lt" -> LessOp(target.expression, constant)
            "le" -> LessEqOp(target.expression, constant)
            "gt" -> GreaterOp(target.expression, constant)
            "ge" -> GreaterEqOp(target.expression, constant)
            else -> throw Refusals.notYetImplemented("the $operator operator")
        }
    }
}
