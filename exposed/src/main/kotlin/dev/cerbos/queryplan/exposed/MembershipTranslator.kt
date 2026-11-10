package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.ExactString
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.Params
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.Op

/**
 * `in` and `hasIntersection`, over a scalar column, a relation chain, or a `map()` projection. A null
 * list element is an `IS NULL` disjunct. A NULL member column is the null element under scalar
 * projection, but a missing attribute (UNKNOWN) under `map()` member access.
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
        mappedLiteralMembership(member, container, scope)?.let { return it }
        val constant = PlanValues.builtConstant(container)
        if (member.nodeCase != Operand.NodeCase.VARIABLE || constant === PlanValues.NotConstant) {
            throw RelationRefusals.membershipOperands("in")
        }
        // `<list constant> in collection` asks whether the list itself is an element, which no scalar
        // element equals: a definite FALSE.
        if (operands[0].nodeCase != Operand.NodeCase.VARIABLE && (constant is List<*> || constant is Map<*, *>)) {
            val resolved = scope.resolve(member.variable) as? Resolution.Collection
                ?: throw RelationRefusals.structuredMember("in")
            return collectionContainsAny(resolved, emptyList())
        }
        val values = elementsOf(constant)
        return when (val resolved = scope.resolve(member.variable)) {
            is Resolution.Collection -> collectionContainsAny(resolved, values)
            // `in` over a scalar, either operand order, is a CEL no-overload error.
            is Resolution.Scalar ->
                if (constant is List<*> || constant is Map<*, *>) scalarIsAnyOf(resolved, values) else TriLogic.unknown()
        }
    }

    /**
     * `attribute in [literal list].map(t, expr)` as `OR_i attribute == expr[t := L_i]`, exact under
     * three-valued logic. `null` when the container is not that shape.
     */
    private fun mappedLiteralMembership(member: Operand, container: Operand, scope: Scope): Op<Boolean>? {
        if (member.nodeCase != Operand.NodeCase.VARIABLE ||
            container.nodeCase != Operand.NodeCase.EXPRESSION ||
            container.expression.operator != "map" ||
            container.expression.operandsCount != 2
        ) {
            return null
        }
        val elements = PlanValues.literalListElements(container.expression.getOperands(0)) ?: return null
        val lambda = ParsedLambda.parse(
            container.expression.getOperands(1),
            "map second operand must be a lambda",
            "map supports single-variable lambdas only",
            "map lambda variable must be a variable operand",
        )
        return translation.walker.enterMacro("map") {
            if (elements.isEmpty()) {
                return@enterMacro scalarIsAnyOf(scope.scalar(member.variable), emptyList())
            }
            val disjunction = PlanResourcesFilter.Expression.newBuilder().setOperator("or")
            elements.forEach { element ->
                disjunction.addOperands(
                    Operand.newBuilder().setExpression(
                        PlanResourcesFilter.Expression.newBuilder()
                            .setOperator("eq")
                            .addOperands(member)
                            .addOperands(translation.collections.substitute(lambda.body, lambda.variable, element)),
                    ),
                )
            }
            translation.walker.traverse(Operand.newBuilder().setExpression(disjunction).build(), scope)
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
            // hasIntersection over a scalar, or against one, is a CEL no-overload error.
            if (constant !is List<*>) return TriLogic.unknown()
            return projectionIntersects(first.expression, constant, scope)
        }
        if (first.nodeCase != Operand.NodeCase.VARIABLE || constant === PlanValues.NotConstant) {
            throw RelationRefusals.membershipOperands("hasIntersection")
        }
        val resolved = scope.resolve(first.variable)
        if (constant !is List<*> || resolved !is Resolution.Collection) return TriLogic.unknown()
        return collectionContainsAny(resolved, constant)
    }

    /**
     * Membership of a scalar in a literal list, as the OR of [LeafTranslator.applyLeaf] `eq`s. Not
     * Exposed's `inList`, which binds through the column type.
     */
    private fun scalarIsAnyOf(target: Resolution.Scalar, values: List<Any?>): Op<Boolean> {
        // `x in []` is FALSE for a present attribute but CEL raises on a missing one, so the NULL witness
        // stays unless the attribute is explicit-null.
        if (values.isEmpty()) {
            if (translation.leaf.isExplicitNull(target)) return Op.FALSE
            return TriLogic.baseUnlessUnknown(Op.FALSE, IsNullOp(target.expression))
        }
        return TriLogic.or(values.map { translation.leaf.applyLeaf("eq", target, it) })
    }

    /**
     * `in`/`hasIntersection` against a stored collection. A NULL element column is the null element, so
     * a null constant becomes an `IS NULL` disjunct.
     */
    private fun collectionContainsAny(collection: Resolution.Collection, values: List<Any?>): Op<Boolean> {
        val element = collection.tail.element
            ?: throw RelationRefusals.noElementColumn(collection.variable, collection.tail)
        // An empty list is a never-matching body, so [Subqueries.chainContains] still guards an absent parent.
        return translation.subqueries.chainContains(collection) { alias ->
            if (values.isEmpty()) {
                Op.FALSE
            } else {
                matchesAnyOf(collection.variable, alias[element.column], values)
            }
        }
    }

    /**
     * `in(attribute, collection-attribute)`. Under EXPLICIT, nulls on both sides are equal values and
     * one alone is a definite mismatch; under OMITTED, plain `=`'s UNKNOWN is CEL's error.
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
        // The declared convention, with no call-level fallback (docs/adr/0004).
        val explicitNulls = translation.leaf.isExplicitNull(member)
        val contains = translation.subqueries.chainContains(collection) { alias ->
            val elementColumn = alias[element.column]
            val equality = EqOp(ExactString.of(elementColumn, "eq"), member.expression)
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
        // An omitted-convention NULL member makes CEL raise; a plain EXISTS over a direct relation would read
        // that as FALSE, so the row-level witness keeps it UNKNOWN.
        return TriLogic.baseUnlessUnknown(contains, IsNullOp(member.expression))
    }

    /**
     * `hasIntersection(map(collection, lambda), [values])`. `map()` absorbs no error, so a NULL projected
     * column makes the whole intersection UNKNOWN; a null constant is stripped.
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
     * The subquery body of a membership test. A constant of another recognised type drops out; a column
     * of an unrecognised type refuses the whole membership, since dropping there over-granted.
     */
    private fun matchesAnyOf(reference: String, element: Column<*>, values: List<Any?>): Op<Boolean> {
        val arms = values.mapNotNull { value ->
            when {
                value == null -> IsNullOp(element)
                // A constant CEL knows to be of another type: a definite FALSE, so it drops out.
                ScalarColumnTypes.knownMismatch(element, value) -> null
                !ScalarColumnTypes.accepts(element, value) ->
                    throw ScalarRefusals.constantTypeMismatch("in", reference, element, value)
                else -> EqOp(element, Params.of(value))
            }
        }
        return if (arms.isEmpty()) Op.FALSE else TriLogic.or(arms)
    }

    /** A plan constant as the elements the operator ranges over; a struct contributes its keys. */
    private fun elementsOf(value: Any?): List<Any?> = when (value) {
        is List<*> -> value
        is Map<*, *> -> value.keys.toList()
        else -> listOf(value)
    }
}
