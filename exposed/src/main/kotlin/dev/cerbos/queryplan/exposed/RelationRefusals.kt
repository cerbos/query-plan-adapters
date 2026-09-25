package dev.cerbos.queryplan.exposed

/**
 * The relation side's named refusals, each delegating to [Refusals] so the classification stays a
 * property of the walk.
 *
 * Every message names the MECHANISM that makes the shape inexpressible — the thing a reader has to
 * change to get a filter — rather than restating the operator. Each one is pinned in
 * `conformance/actions.json` and asserted by every harness, so changing the text is a deliberate
 * corpus edit. None of them names a constant VALUE: a plan constant can carry a folded principal
 * attribute, and an exception message is logged.
 */
internal object RelationRefusals {

    /**
     * A macro or projection whose collection operand is neither a mapped relation nor a literal
     * list — `tags.map(t, t.name).exists(…)`, `tags.filter(…).map(…)`. Legal CEL; there is no
     * table for the subquery to range over.
     */
    fun computedCollection(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "$operator ranges over a collection computed by another macro, which has no table for a " +
            "correlated subquery to range over; only a mapped relation or a literal list can be ranged over",
    )

    /** A macro the fold cannot express over a literal list, because SQL has no per-element UNKNOWN. */
    fun unfoldableValueCollection(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "$operator over a literal collection value is not supported; only exists(), all() and exists_one() fold " +
            "into a flat filter",
    )

    /**
     * A relation used as a value with no element column declared. An object collection has no
     * single column standing for the element, so the mapping has to name one.
     */
    fun noElementColumn(
        reference: String,
        relation: AttributeMapping.Relation,
    ): UnmappedAttributeException = Refusals.unmapped(
        "$reference denotes the elements of ${relation.table.tableName} themselves, but the relation " +
            "declares no element column; map it with element = <column>",
    )

    /**
     * A column reached THROUGH a to-many relation, in a position that needs one value per resource
     * row. `categories.name` is one value per element, and collapsing it to one value per resource
     * means choosing an aggregate the policy never asked for.
     */
    fun memberOfToMany(
        reference: String,
        relation: AttributeMapping.Relation,
    ): UnmappedAttributeException = Refusals.unmapped(
        "$reference reads a column through the to-many relation on ${relation.table.tableName}, which " +
            "has one value per element rather than one per row; reach it through a collection macro",
    )

    /** `in`/`hasIntersection` where the collection side is a column rather than a relation. */
    fun membershipNeedsRelation(operator: String, reference: String): UnmappedAttributeException =
        Refusals.unmapped(
            "$operator tests membership of $reference, which resolves to a scalar column; collection " +
                "membership needs a relation mapping",
        )

    /** An `in`/`hasIntersection` operand pair with no column-and-constant reading. */
    fun membershipOperands(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "$operator is applied to a pair this adapter cannot read as one mapped operand against a " +
            "constant list; the supported shapes are $operator(collection-attribute, [values]), " +
            "$operator(map(collection, lambda), [values]) and in(attribute, collection-attribute)",
    )

    /** `in` whose member is a list or map constant: an element no scalar column can hold. */
    fun structuredMember(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "$operator with a list or map as the member cannot be expressed: each element of a mapped " +
            "collection is one scalar column value, which SQL equality cannot compare with a list " +
            "or a map",
    )

    /** `map()` whose lambda computes rather than projecting a member. */
    fun computedProjection(): UnsupportedPlanShapeException = Refusals.unsupported(
        "map() projects a computed expression rather than a member of the element, so there is no " +
            "column for the subquery to select",
    )

    /** `size()` of something that is neither a mapped string column nor a mapped relation. */
    fun sizeOperand(): UnsupportedPlanShapeException = Refusals.unsupported(
        "size() is applied to a collection this adapter cannot reach as a table: its argument must be " +
            "a mapped attribute or filter(collection, lambda)",
    )

    /**
     * `size(x) op NaN` — the NaN a zero denominator produces, as a `size()` threshold.
     *
     * Not policy-reachable: CEL has no NaN literal and the planner does not fold `div(0, 0)` (the
     * `nan-ord-*` wire fixtures ship it unfolded), so the only way in is a hand-built plan. It is
     * refused rather than folded because the fold has no right answer to pick: every rounding of
     * NaN is a different comparison, and narrowing it silently yields 0, which turned
     * `size(x) > NaN` into the always-true `size(x) >= 0`.
     */
    fun nanSizeThreshold(): UnsupportedPlanShapeException = Refusals.unsupported(
        "size() compared against a NaN threshold is not supported: a character count and an " +
            "element count are both integral, and NaN has no integral bound to round to — " +
            "rounding it is not a number and narrowing that yields 0, which makes every ordering " +
            "against it a comparison the policy never wrote. CEL denies an ordering against NaN " +
            "under both polarities, so no bound reproduces it.",
    )

    /** `size(filter(...))` over a collection with no relation mapping. */
    fun sizeFilterNeedsRelation(reference: String): UnmappedAttributeException = Refusals.unmapped(
        "size(filter(...)) counts the elements of $reference, which resolves to a scalar column; " +
            "counting needs a relation mapping",
    )

    /**
     * `size(x.except(y))`. List difference is a set operation over two collections, and a row
     * filter has no set-valued operand to hold the right-hand side.
     */
    fun exceptUnsupported(): UnsupportedPlanShapeException = Refusals.unsupported(
        "except() computes a list difference, which SQL cannot express as a row filter; write the " +
            "policy as size(x.filter(e, !(e in y))) instead",
    )

    /**
     * A folded element whose dotted reference the literal element does not carry. That element's
     * own CEL evaluation would error and deny, and a fold has no per-element UNKNOWN to carry.
     */
    fun unresolvableFoldedElement(reference: String, segment: String): UnsupportedPlanShapeException =
        Refusals.unsupported(
            "Cannot resolve $reference: an element of the folded literal collection has no '$segment' field",
        )
}
