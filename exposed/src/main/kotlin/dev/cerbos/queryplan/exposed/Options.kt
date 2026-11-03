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
     * What is counted, exactly: one level per COLLECTION MACRO the walk enters — `exists`, `all`,
     * `exists_one`, and `size(filter(…))` — whether it ranges over a mapped relation or over a
     * literal list. Both multiply: a relation macro emits its correlated subquery once per
     * enclosing level, and a literal fold substitutes each element into the lambda body, so an
     * N-element fold duplicates everything under it N times.
     *
     * It is NOT a bound on the size of the emitted expression, and must not be read as one. A
     * single 100-element fold is one level. A ternary doubles its subtree per level and a
     * zero-capable division branches per level, and neither is counted at all. A relation CHAIN is
     * one level however many hops it has, and so is a `size()`, a membership test or a hierarchy
     * relation over one.
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
