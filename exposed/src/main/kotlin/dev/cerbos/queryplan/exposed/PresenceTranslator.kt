package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IsNotNullOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.Op

/**
 * `has(x)`, emitted from Cerbos 0.57.0. Under EXPLICIT the attribute is always present; under OMITTED
 * it is present when `IS NOT NULL`; undeclared, a NULL row is UNKNOWN, since the convention decides it.
 * A to-one relation is present when its row exists; a to-many one is refused. An absent leading hop
 * is UNKNOWN.
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
