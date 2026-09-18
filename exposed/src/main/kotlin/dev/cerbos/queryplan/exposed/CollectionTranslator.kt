package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * The collection macros `exists`, `all` and `exists_one`, lowered to ONE scoring subquery each,
 * never a plain `EXISTS`: an element whose body is UNKNOWN silently fails to match under `EXISTS`,
 * which collapses CEL's error case to FALSE, and an enclosing NOT then readmits the row.
 *
 * OWNED BY THE RELATION SIDE. Stub.
 */
internal class CollectionTranslator(@Suppress("unused") private val translation: Translation) {
    fun translate(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> =
        throw Refusals.notYetImplemented("the $operator collection macro")
}
