package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * The eager pre-walk scan behind [NullAttributeRepresentation.OMITTED]: a plan that compares an
 * attribute against a null constant is refused BEFORE anything is built, when the attribute's
 * convention (its own declaration, else the call-level option) is OMITTED.
 *
 * It matches on the OPERAND, never on an operator allowlist, and is deliberately wider than the
 * over-granting shapes: negation is applied around a built predicate, so a leaf cannot know whether
 * an enclosing `not` will flip it.
 *
 * OWNED BY THE SCALAR SIDE. Stub: it accepts everything, which is only safe while [LeafTranslator]
 * refuses every null constant.
 */
internal object NullOperandScan {
    @Suppress("UNUSED_PARAMETER")
    fun assertTranslatable(condition: Operand, options: Options) {
        // Intentionally empty until the scalar side lands.
    }
}
