package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.Params
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.Op

/**
 * `in` and `hasIntersection`, over a scalar column, a relation chain, or a `map()` projection.
 *
 * Owns CEL's null-element semantics: a null list element is an `IS NULL` disjunct, never an
 * `IN (…, NULL)` that SQL silently drops. It also owns the two VIEWS of a NULL member column that
 * decide which one applies — under the scalar-projection view a NULL member IS the null element,
 * while under member ACCESS through `map()` it is a missing element attribute and therefore an
 * UNKNOWN row.
 *
 * Every existence test over a chain goes through [Subqueries.chainContains], so an absent to-one
 * parent stays UNKNOWN under both polarities.
 */
internal class MembershipTranslator(private val translation: Translation) {

    fun translateIn(operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operands.size != 2) {
            throw Refusals.malformed("in requires exactly 2 operands, got ${operands.size}")
        }
        // Both shapes — `field in [values]` and `value in collection-field` — read the same way
        // once normalised field-first: the MAPPING kind decides whether this is collection
        // membership or a scalar comparison, not the operand order.
        val normalized = NormalizedBinary.of("in", operands).operands
        val member = normalized[0]
        val container = normalized[1]

        // in(variable, variable) — a scalar attribute tested against a collection attribute on the
        // same resource. CEL's `in` is receiver-shaped, the member always first, and two variables
        // rank equally so normalisation never swaps them: planner source order is authoritative.
        if (member.nodeCase == Operand.NodeCase.VARIABLE && container.nodeCase == Operand.NodeCase.VARIABLE) {
            return attributeInAttribute(member.variable, container.variable, scope)
        }
        val constant = PlanValues.builtConstant(container)
        if (member.nodeCase != Operand.NodeCase.VARIABLE || constant === PlanValues.NotConstant) {
            throw RelationRefusals.membershipOperands("in")
        }
        // Value-first, `<constant> in collection`, the constant is ONE element, not a list of
        // candidates: `["public"] in tagNames` asks whether the list `["public"]` is itself an
        // element. Spreading it into its elements would answer `"public" in tagNames` instead and
        // return every row holding that tag. A list or map equals no scalar element, null elements
        // included, so over a collection of scalars the membership is a definite FALSE — the
        // never-matching body [collectionContainsAny] already gives an empty list.
        if (operands[0].nodeCase != Operand.NodeCase.VARIABLE && (constant is List<*> || constant is Map<*, *>)) {
            val resolved = scope.resolve(member.variable) as? Resolution.Collection
                ?: throw RelationRefusals.structuredMember("in")
            return collectionContainsAny(resolved, emptyList())
        }
        val values = elementsOf(constant)
        return when (val resolved = scope.resolve(member.variable)) {
            is Resolution.Collection -> collectionContainsAny(resolved, values)
            is Resolution.Scalar -> scalarIsAnyOf(resolved, values)
        }
    }

    fun translateHasIntersection(operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operands.size != 2) {
            throw Refusals.malformed("hasIntersection requires exactly 2 operands, got ${operands.size}")
        }
        // Intersection is symmetric and the planner preserves policy source order, so the constant
        // side can arrive first. Normalising puts the attribute or projection first; the operator
        // is its own mirror.
        val normalized = NormalizedBinary.of("hasIntersection", operands).operands
        val first = normalized[0]
        val second = normalized[1]

        val constant = PlanValues.builtConstant(second)
        if (first.nodeCase == Operand.NodeCase.EXPRESSION && first.expression.operator == "map") {
            if (constant === PlanValues.NotConstant) {
                throw RelationRefusals.membershipOperands("hasIntersection")
            }
            return projectionIntersects(first.expression, elementsOf(constant), scope)
        }
        if (first.nodeCase != Operand.NodeCase.VARIABLE || constant === PlanValues.NotConstant) {
            throw RelationRefusals.membershipOperands("hasIntersection")
        }
        val values = elementsOf(constant)
        return when (val resolved = scope.resolve(first.variable)) {
            is Resolution.Collection -> collectionContainsAny(resolved, values)
            is Resolution.Scalar -> scalarIsAnyOf(resolved, values)
        }
    }

    /**
     * Membership of a scalar in a literal list, as the disjunction of the equalities CEL itself
     * evaluates.
     *
     * Every element goes through [LeafTranslator.applyLeaf] under `eq`, which is what gives the
     * fold the same null convention, the same definite-equality handling and the same value-typed
     * binding a written-out `x == a || x == b` gets. Exposed's own `inList` is not used: it binds
     * every element through the COLUMN's type, and an empty list it rewrites to a constant.
     */
    private fun scalarIsAnyOf(target: Resolution.Scalar, values: List<Any?>): Op<Boolean> {
        // CEL membership in an empty list is FALSE for a row whose attribute is PRESENT, and an
        // empty SQL `IN ()` is a syntax error on every store here. It is not definite, though: a
        // missing attribute makes CEL raise before it ever looks at the list, so the witness has to
        // survive into the constant — unless the caller declares the explicit-null convention, in
        // which case the NULL is a null VALUE and `null in []` really is a definite FALSE.
        if (values.isEmpty()) {
            if (translation.leaf.isExplicitNull(target)) return Op.FALSE
            return TriLogic.baseUnlessUnknown(Op.FALSE, IsNullOp(target.expression))
        }
        return TriLogic.or(values.map { translation.leaf.applyLeaf("eq", target, it) })
    }

    /**
     * `in`/`hasIntersection` against a stored collection: an existence test whose body matches the
     * element column against the constants.
     *
     * A related row whose member column is NULL is an explicitly-null ELEMENT under the
     * scalar-projection view, which is the convention the corpus pins (`null in tagNames` is TRUE
     * for a row with a NULL tag name). SQL's `= NULL` never matches it, so the null constant
     * becomes an `IS NULL` disjunct inside the subquery body.
     */
    private fun collectionContainsAny(collection: Resolution.Collection, values: List<Any?>): Op<Boolean> {
        val element = collection.tail.element
            ?: throw RelationRefusals.noElementColumn(collection.variable, collection.tail)
        // An empty list is a NEVER-MATCHING BODY, not a short circuit. `Subqueries.chainContains`
        // is the only place the absent-parent guard lives, so answering `Op.FALSE` before reaching
        // it left a row with no parent at all — a missing-path deny — returned by the negation. An
        // empty list is not an authoring mistake either: it is what `P.attr.<something>` folds to
        // for a principal who holds none of whatever the list enumerates.
        return translation.subqueries.chainContains(collection) { alias ->
            if (values.isEmpty()) {
                Op.FALSE
            } else {
                matchesAnyOf(collection.variable, alias[element.column], values)
            }
        }
    }

    /**
     * `in(attribute, collection-attribute)`: the member column compared against the element column
     * inside the subquery.
     *
     * Under the EXPLICIT convention a NULL on either side is a null VALUE, so a null member and a
     * null element are equal and a null on one side alone is a definite mismatch — which plain `=`
     * cannot say, because it answers UNKNOWN to both and the row then survives the negation it
     * should not. Under the OMITTED convention a NULL column carries no attribute at all, CEL
     * raises a missing-attribute error and denies, and plain equality's UNKNOWN is exactly that.
     */
    private fun attributeInAttribute(memberVar: String, collectionVar: String, scope: Scope): Op<Boolean> {
        val collection = scope.resolve(collectionVar) as? Resolution.Collection
            ?: throw RelationRefusals.membershipNeedsRelation("in", collectionVar)
        val member = scope.scalar(memberVar)
        val element = collection.tail.element
            ?: throw RelationRefusals.noElementColumn(collection.variable, collection.tail)
        if (!ScalarColumnTypes.comparable(member.column, element.column)) {
            throw ScalarRefusals.columnTypeMismatch(
                "in",
                memberVar,
                member.column,
                collectionVar,
                element.column,
            )
        }
        // THE declared owner of that question, with no fallback of its own. Reading
        // `field.nullAttributeRepresentation ?: options.nullAttributeRepresentation` here handed
        // the call-level default — EXPLICIT — to an attribute that declares nothing, so one
        // mapping got the definite reading for `in` and the omitted one for `eq`
        // (docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).
        val explicitNulls = translation.leaf.isExplicitNull(member)
        val contains = translation.subqueries.chainContains(collection) { alias ->
            val elementColumn = alias[element.column]
            val equality = EqOp(elementColumn, member.expression)
            if (!explicitNulls) {
                equality
            } else {
                TriLogic.or(
                    equality,
                    TriLogic.and(IsNullOp(elementColumn), IsNullOp(member.expression)),
                )
            }
        }
        if (explicitNulls) return contains
        // A NULL MEMBER under the omitted convention sends no attribute at all, so CEL raises and
        // `check()` denies under BOTH polarities. Over a chain the count guard already carries
        // that; over a DIRECT relation the existence test is a plain `EXISTS`, which is two-valued
        // — it reads the UNKNOWN body as "no element matched" and answers FALSE, and `NOT FALSE`
        // hands the row back. Only the row-level witness keeps both polarities excluding it, the
        // same shape [projectionIntersects] uses for its own NULL witness. A NULL ELEMENT needs no
        // guard: under the scalar-projection view it IS the null element, so CEL's own answer for
        // it is a definite FALSE and the `EXISTS` collapse is exactly right.
        return TriLogic.baseUnlessUnknown(contains, IsNullOp(member.expression))
    }

    /**
     * `hasIntersection(map(collection, lambda), [values])`.
     *
     * CEL's `map()` has NO error absorption: a NULL projected column is a missing element
     * attribute, so the whole intersection is an evaluation error even when another element would
     * intersect. That is [TriLogic.baseUnlessUnknown] with a null-witness subquery as the detector.
     *
     * A null element in the constant list is inert here, which is the opposite of
     * [collectionContainsAny] and is the second of the two views of a NULL member column: member
     * ACCESS can never yield an explicit null from a column model, so a null constant is stripped
     * rather than rendered as a never-matching `IN (…, NULL)`, while NULL-projection rows stay
     * UNKNOWN.
     */
    private fun projectionIntersects(
        projection: PlanResourcesFilter.Expression,
        values: List<Any?>,
        scope: Scope,
    ): Op<Boolean> {
        // No short circuit for the empty list: the `present.isEmpty()` arm below reaches the same
        // constant, and reaches it INSIDE the NULL-witness guard, which carries the absent-parent
        // guard with it.
        val operands = projection.operandsList
        if (operands.size != 2) {
            throw Refusals.malformed("map requires exactly 2 operands, got ${operands.size}")
        }
        if (operands[0].nodeCase != Operand.NodeCase.VARIABLE) {
            throw RelationRefusals.computedCollection("map")
        }
        val lambda = ParsedLambda.parse(
            operands[1],
            "map second operand must be a lambda",
            "map supports single-variable lambdas only",
            "map lambda variable must be a variable operand",
        )
        if (lambda.body.nodeCase != Operand.NodeCase.VARIABLE) {
            throw RelationRefusals.computedProjection()
        }
        val projected = lambda.body.variable
        val member = when {
            projected == lambda.variable -> ""
            projected.startsWith("${lambda.variable}.") -> projected.substring(lambda.variable.length + 1)
            else -> throw Refusals.malformed("map lambda body must project its own iteration variable")
        }
        val collection = scope.collection(operands[0].variable)
        val column = relationMemberColumn(collection.tail, member, projected)

        val present = values.filterNotNull()
        val base = if (present.isEmpty()) {
            Op.FALSE
        } else {
            translation.subqueries.chainContains(collection) { alias ->
                matchesAnyOf(projected, alias[column], present)
            }
        }
        return TriLogic.baseUnlessUnknown(
            base,
            translation.subqueries.chainContains(collection) { alias -> IsNullOp(alias[column]) },
        )
    }

    /**
     * The subquery body of a membership test: the element column against each constant.
     *
     * EVERY element is type-checked against the column. A constant of a recognised family other
     * than the column's is a definite FALSE in CEL and drops out; a column of an unrecognised
     * family refuses the WHOLE membership, for the reason [ScalarRefusals.constantTypeMismatch]
     * gives — a number tested against a text element column is an almost-total match on MySQL.
     * A null element is inert to the check: it renders as `IS NULL`, which coerces nothing.
     *
     * Dropping EVERY mismatched element — on the argument that CEL answers that one equality
     * `false`, so `x OR false` is `x` under either polarity — was tried and reverted. The argument
     * only holds where the adapter KNOWS what CEL would answer, and it does not for the column
     * kinds [ScalarColumnTypes.familyOf] reads as unrecognised: there `accepts` is false for
     * EVERY value, so `["<uuid>", null]` against a `UUIDTable` id kept the null alone, emitted
     * `IS NULL` by itself, and `NOT (id IS NULL)` handed back every row the PDP denies.
     */
    private fun matchesAnyOf(reference: String, element: Column<*>, values: List<Any?>): Op<Boolean> {
        val arms = values.mapNotNull { value ->
            when {
                value == null -> IsNullOp(element)
                // A constant CEL KNOWS to be of another type than every element: that equality is
                // a definite FALSE for every element, null elements included, so it drops out of
                // the disjunction under either polarity. Only where both families are recognised:
                // see the note above on why an unrecognised column still refuses.
                ScalarColumnTypes.knownMismatch(element, value) -> null
                !ScalarColumnTypes.accepts(element, value) ->
                    throw ScalarRefusals.constantTypeMismatch("in", reference, element, value)
                else -> EqOp(element, Params.of(value))
            }
        }
        return if (arms.isEmpty()) Op.FALSE else TriLogic.or(arms)
    }

    /**
     * A plan constant as the list of elements the operator ranges over.
     *
     * A scalar stays a one-element list — `null in R.attr.items` is planner-emitted and the wrapper
     * has to tolerate it. A STRUCT is CEL's map, whose `in` tests its KEYS, so it contributes its
     * key set rather than being compared as a value.
     */
    private fun elementsOf(value: Any?): List<Any?> = when (value) {
        is List<*> -> value
        is Map<*, *> -> value.keys.toList()
        else -> listOf(value)
    }
}
