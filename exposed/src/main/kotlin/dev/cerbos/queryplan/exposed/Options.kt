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
     * How deeply collection macros (`exists`, `all`, `exists_one`, …) may nest before a plan is
     * refused rather than translated.
     *
     * The bound counts MACRO LEVELS, of both kinds the walk distinguishes, because both multiply.
     * A macro over a mapped relation costs a level because it emits a correlated subquery, and a
     * nested one emits that subquery once per enclosing level. A macro over a LITERAL list emits no
     * subquery at all — it substitutes each element into the lambda body and walks the resulting
     * `or`/`and` chain — but it duplicates everything under it once per element, so it costs a
     * level too: the bound is on the size of the emitted expression, not only on its subquery
     * count.
     *
     * It bounds nothing else. A relation CHAIN is one level however many hops it has, and so is a
     * `size()`, a membership test or a hierarchy relation over one.
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
