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
    /**
     * How the caller sends a NULL column that no [AttributeMapping.Field] declares a convention
     * for — and, for those, ONLY whether a null OPERAND is refused.
     *
     * It is not a default for how an undeclared column RENDERS. An undeclared column renders as if
     * it were NOT NULL, which is the historical behaviour and what
     * [AttributeMapping.Field.nullAttributeRepresentation] documents; inheriting this setting there
     * would hand definite equality to every column a caller never thought about
     * (docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).
     *
     * One consequence is deliberate and worth knowing. Under the default, an UNDECLARED attribute
     * compared against a null literal renders `IS NULL` — the pre-walk scan takes the caller at
     * their word that NULLs are sent explicitly — while `!= "x"` over the same attribute keeps the
     * undeclared rendering and excludes its NULL rows. So one attribute can be read under both
     * conventions within one call. It UNDER-grants rather than over-grants (the `!=` drops rows a
     * declared attribute would keep) and is pinned in `MappingDslTest`; declare the attribute to
     * make both halves definite.
     */
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
