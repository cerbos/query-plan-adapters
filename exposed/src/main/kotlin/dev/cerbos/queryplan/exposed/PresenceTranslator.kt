package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IsNotNullOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.Op

/**
 * `has(x)`, the presence test the planner emits from Cerbos 0.57.0 for an attribute it does not
 * fold into a guard: whether the attributes the caller sends to `check()` carry the key at all.
 *
 * Presence is a property of the NULL convention, not of the column:
 *  - declared [NullAttributeRepresentation.EXPLICIT], a NULL column is sent as a present null, so
 *    the attribute is present on every row: TRUE;
 *  - declared [NullAttributeRepresentation.OMITTED], a NULL column sends no key, so the attribute
 *    is present exactly when the column `IS NOT NULL`;
 *  - undeclared, the column renders as if it were NOT NULL, as it does everywhere else in the walk.
 *    On a row where it is NULL after all, the convention is not known, and the two readings give
 *    opposite answers, so that row is UNKNOWN: denied under both polarities rather than granted
 *    under one. The call-level [Options.nullAttributeRepresentation] does not decide it, for the
 *    reason [AttributeMapping.Field.nullAttributeRepresentation] gives.
 *
 * A to-one relation is present when its row exists. A to-many relation is refused: CEL holds a
 * list, empty or not, on every row the caller sends one for, and nothing in the mapping says the
 * caller does.
 *
 * `has()` never errors on a missing value, so the predicate is two-valued and [TriLogic.not] over
 * it is exact. The one error it raises is selecting THROUGH a missing to-one hop (`has(parent.x)`
 * on a parentless row), which denies under both polarities, so an absent leading hop is UNKNOWN.
 */
internal class PresenceTranslator(private val translation: Translation) {

    fun translate(operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operands.size != 1 || operands[0].nodeCase != Operand.NodeCase.VARIABLE) {
            throw Refusals.malformed("has requires exactly 1 attribute reference, got ${operands.size} operand(s)")
        }
        val variable = operands[0].variable
        val path = scope.path(variable)
        val plural = path.hops.firstOrNull { it.cardinality == AttributeMapping.Relation.Cardinality.MANY }
        if (plural != null) {
            if (path.leaf == null && plural === path.hops.last()) throw collectionPresence(variable, plural)
            throw RelationRefusals.memberOfToMany(variable, plural)
        }
        val leaf = path.leaf
        if (leaf == null) {
            val exists = translation.subqueries.hopsExist(path.owner, path.hops)
            return throughLeadingHops(path.owner, path.hops.dropLast(1), exists)
        }
        val present = columnPresence(scope.scalar(variable).expression, leaf.nullAttributeRepresentation)
        return throughLeadingHops(path.owner, path.hops, present)
    }

    /** [present], made UNKNOWN on a row where a to-one hop it is selected through is absent. */
    private fun throughLeadingHops(
        owner: Scope,
        hops: List<AttributeMapping.Relation>,
        present: Op<Boolean>,
    ): Op<Boolean> {
        if (hops.isEmpty()) return present
        return TriLogic.baseUnlessUnknown(present, TriLogic.not(translation.subqueries.hopsExist(owner, hops)))
    }

    private fun columnPresence(value: Expression<*>, convention: NullAttributeRepresentation?): Op<Boolean> =
        when (convention) {
            NullAttributeRepresentation.EXPLICIT -> Op.TRUE
            NullAttributeRepresentation.OMITTED -> IsNotNullOp(value)
            null -> TriLogic.or(IsNotNullOp(value), TriLogic.and(IsNullOp(value), TriLogic.unknown()))
        }

    private fun collectionPresence(variable: String, relation: AttributeMapping.Relation) = Refusals.unsupported(
        "has($variable) tests the to-many relation on ${relation.table.tableName}: CEL holds a list, empty or " +
            "not, wherever the caller sends one, and no row of that table says whether the caller sends it",
    )
}
