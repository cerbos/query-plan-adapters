package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * `in` and `hasIntersection`, over a scalar column, a relation chain, or a `map()` projection.
 * Owns CEL's null-element semantics: a null list element is an `IS NULL` disjunct, never an
 * `IN (…, NULL)` that SQL silently drops.
 *
 * OWNED BY THE RELATION SIDE. Stub. The scalar path goes through [LeafTranslator].
 */
internal class MembershipTranslator(@Suppress("unused") private val translation: Translation) {
    fun translateIn(operands: List<Operand>, scope: Scope): Op<Boolean> =
        throw Refusals.notYetImplemented("the in operator")

    fun translateHasIntersection(operands: List<Operand>, scope: Scope): Op<Boolean> =
        throw Refusals.notYetImplemented("the hasIntersection operator")
}
