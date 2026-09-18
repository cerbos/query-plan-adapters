package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * `overlaps`, `ancestorOf` and `descendentOf` over Cerbos hierarchies (delimited paths).
 *
 * OWNED BY THE SCALAR SIDE. Stub.
 */
internal class HierarchyTranslator(@Suppress("unused") private val translation: Translation) {
    fun translate(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> =
        throw Refusals.notYetImplemented("the $operator hierarchy operator")
}
