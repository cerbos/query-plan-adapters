package dev.cerbos.queryplan.exposed

/**
 * Everything a translation is told beyond the plan itself.
 *
 * Immutable. Build one with [of] and refine it with the `with…` methods, each of which returns a
 * new instance. It is a class with withers rather than a positional constructor so a setting can
 * be added later without breaking a caller.
 */
public class Options private constructor(
    /** Resolves the plan's attribute references onto columns and relations. */
    public val mapping: AttributeResolver,
    /** The call-level NULL convention; an [AttributeMapping.Field] may override it. */
    public val nullAttributeRepresentation: NullAttributeRepresentation,
    /**
     * How deeply collection macros (`exists`, `all`, `exists_one`, …) may nest. Each level
     * multiplies the correlated subqueries the filter carries, so the bound is a cost guard, and a
     * plan nested past it is refused rather than emitted.
     */
    public val maxMacroDepth: Int,
) {
    init {
        require(maxMacroDepth >= 1) { "maxMacroDepth must be at least 1, got $maxMacroDepth" }
    }

    public fun withMapping(mapping: AttributeResolver): Options =
        Options(mapping, nullAttributeRepresentation, maxMacroDepth)

    public fun withNullAttributeRepresentation(representation: NullAttributeRepresentation): Options =
        Options(mapping, representation, maxMacroDepth)

    public fun withMaxMacroDepth(maxMacroDepth: Int): Options =
        Options(mapping, nullAttributeRepresentation, maxMacroDepth)

    override fun toString(): String =
        "Options(nullAttributeRepresentation=$nullAttributeRepresentation, maxMacroDepth=$maxMacroDepth)"

    public companion object {
        /** The default for [maxMacroDepth], the same bound the reference adapter uses. */
        public const val DEFAULT_MAX_MACRO_DEPTH: Int = 5

        /** Options over [mapping] with every other setting at its default. */
        @JvmStatic
        public fun of(mapping: AttributeResolver): Options =
            Options(mapping, NullAttributeRepresentation.EXPLICIT, DEFAULT_MAX_MACRO_DEPTH)
    }
}
