package dev.cerbos.queryplan.exposed

/**
 * Every refusal in this package is raised through one of these factories, which is what makes the
 * classification (malformed plan, mapping shortfall, inexpressible shape) a property of the walk
 * rather than of the message text.
 *
 * Messages describe SHAPES and TYPES, never constant values: a plan constant can carry a folded
 * principal attribute, and an exception message is logged.
 */
internal object Refusals {
    fun unsupported(message: String): UnsupportedPlanShapeException = UnsupportedPlanShapeException(message)

    fun malformed(message: String, cause: Throwable? = null): MalformedPlanException =
        MalformedPlanException(message, cause)

    fun unmapped(message: String): UnmappedAttributeException = UnmappedAttributeException(message)

    /**
     * Deliberately NOT a refusal: a branch only an adapter bug can reach is an
     * [IllegalStateException], so a caller catching [IllegalArgumentException] never mistakes one
     * for a classified refusal.
     */
    fun internal(message: String): IllegalStateException = IllegalStateException(message)

    fun unknownAttribute(variable: String): UnmappedAttributeException =
        unmapped("Unknown attribute: $variable")

    /**
     * SCAFFOLDING. Marks a shape whose translation has not been written yet. It fails closed, so
     * the conformance harness reports the gap honestly. No call to this may survive to a release:
     * every remaining refusal must name the real mechanism, because its message is pinned in
     * conformance/actions.json.
     */
    fun notYetImplemented(what: String): UnsupportedPlanShapeException =
        unsupported("Not implemented yet: $what")
}
