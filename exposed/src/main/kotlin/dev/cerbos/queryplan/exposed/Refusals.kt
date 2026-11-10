package dev.cerbos.queryplan.exposed

/** Raises every refusal, so its classification is a property of the walk. Messages never include constant values. */
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
}
