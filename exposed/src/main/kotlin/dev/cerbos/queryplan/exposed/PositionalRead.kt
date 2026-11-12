package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op

/**
 * `list[n]` or `list[n].member` over a relation that declares [AttributeMapping.Relation.orderBy]:
 * the row at offset `n` of the subquery in that order. The plan's `index()` is rewritten into a
 * variable spelled `list[n]` ([rewrite]), so every translator reads the element as one scalar.
 * Past the end, or at a negative or fractional position, CEL raises, and the comparison around the
 * element is UNKNOWN.
 */
internal class PositionalRead private constructor(
    private val relation: AttributeMapping.Relation,
    private val position: Double,
    private val member: String?,
) {
    private val offset: Long? =
        position.takeIf { it >= 0 && it == Math.floor(it) && it <= Int.MAX_VALUE }?.toLong()

    fun resolve(translation: Translation, owner: Scope, variable: String): Resolution.Scalar {
        val field = when (member) {
            null -> {
                val element = relation.element ?: throw RelationRefusals.noElementColumn(variable, relation)
                AttributeMapping.Field(element.column, NullAttributeRepresentation.EXPLICIT)
            }
            else -> relation.fields[member] as? AttributeMapping.Field
                ?: throw Refusals.unknownAttribute(variable)
        }
        val order = relation.orderBy ?: throw Refusals.internal("a positional read needs an order column")
        val hops = listOf(relation)
        return Resolution.Scalar(
            variable,
            translation.subqueries.elementAt(owner, hops, field.column, order, offset ?: 0),
            field.column,
            field,
            owner,
            presence = { offset?.let { translation.subqueries.countExceeds(owner, hops, it) } ?: Op.FALSE },
        )
    }

    companion object {
        private val SPELLING = Regex("""^(.+)\[([^\[\]]+)](?:\.([^.\[\]]+))?$""")

        /** The positional read [variable] spells, or `null` when it is not one over an ordered relation. */
        fun parse(variable: String, mapping: AttributeResolver): PositionalRead? {
            val match = SPELLING.matchEntire(variable) ?: return null
            if (mapping.resolve(variable) != null) return null
            val relation = ordered(match.groupValues[1], mapping) ?: return null
            val position = match.groupValues[2].toDoubleOrNull() ?: return null
            return PositionalRead(relation, position, match.groupValues[3].ifEmpty { null })
        }

        /** The declared field a positional [variable] reads, for the null-operand scan. */
        fun declaredField(variable: String, mapping: AttributeResolver): AttributeMapping.Field? {
            val read = parse(variable, mapping) ?: return null
            return when (read.member) {
                null -> read.relation.element?.let {
                    AttributeMapping.Field(it.column, NullAttributeRepresentation.EXPLICIT)
                }
                else -> read.relation.fields[read.member] as? AttributeMapping.Field
            }
        }

        private fun ordered(reference: String, mapping: AttributeResolver): AttributeMapping.Relation? =
            (mapping.resolve(reference) as? AttributeMapping.Relation)?.takeIf {
                it.cardinality == AttributeMapping.Relation.Cardinality.MANY && it.orderBy != null
            }

        /**
         * Rewrites `index(list, n)` and `get-field(index(list, n), member)` over an ordered relation
         * into variables, and `list == [c0, c1]` into `size(list) == 2 && list[0] == c0 && list[1] == c1`.
         */
        fun rewrite(operand: Operand, mapping: AttributeResolver): Operand {
            if (operand.nodeCase != Operand.NodeCase.EXPRESSION) return operand
            val expression = operand.expression
            spell(expression.operator, expression.operandsList, mapping)?.let {
                return Operand.newBuilder().setVariable(it).build()
            }
            wholeListEquality(expression.operator, expression.operandsList, mapping)?.let { return it }
            val rebuilt = expression.toBuilder().clearOperands()
            expression.operandsList.forEach { rebuilt.addOperands(rewrite(it, mapping)) }
            return Operand.newBuilder().setExpression(rebuilt).build()
        }

        private fun wholeListEquality(operator: String, operands: List<Operand>, mapping: AttributeResolver): Operand? {
            if ((operator != "eq" && operator != "ne") || operands.size != 2) return null
            val literal = operands.firstOrNull {
                it.nodeCase == Operand.NodeCase.VALUE && it.value.kindCase == Value.KindCase.LIST_VALUE
            } ?: return null
            val (list, member) = orderedList(operands.first { it !== literal }, mapping) ?: return null
            val elements = literal.value.listValue.valuesList
            val size = PlanRewrites.expression(
                "size",
                Operand.newBuilder().setVariable(list).build(),
            )
            val terms = mutableListOf(
                PlanRewrites.expression(
                    "eq",
                    size,
                    PlanRewrites.constant(Value.newBuilder().setNumberValue(elements.size.toDouble()).build()),
                ),
            )
            elements.forEachIndexed { index, element ->
                val read = if (member == null) "$list[$index]" else "$list[$index].$member"
                terms += PlanRewrites.expression(
                    "eq",
                    Operand.newBuilder().setVariable(read).build(),
                    PlanRewrites.constant(element),
                )
            }
            val equal = if (terms.size == 1) terms[0] else PlanRewrites.expression("and", *terms.toTypedArray())
            return if (operator == "eq") equal else PlanRewrites.expression("not", equal)
        }

        /**
         * The ordered relation [operand] reads as a whole list, and the member each element
         * projects: the list itself, or `map(list, t, t.member)` over a member no row can lack.
         */
        private fun orderedList(operand: Operand, mapping: AttributeResolver): Pair<String, String?>? {
            if (operand.nodeCase == Operand.NodeCase.VARIABLE) {
                val relation = ordered(operand.variable, mapping) ?: return null
                return if (relation.element == null) null else operand.variable to null
            }
            if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator != "map" ||
                operand.expression.operandsCount != 2
            ) {
                return null
            }
            val (source, lambda) = operand.expression.operandsList
            if (source.nodeCase != Operand.NodeCase.VARIABLE || lambda.nodeCase != Operand.NodeCase.EXPRESSION ||
                lambda.expression.operator != "lambda" || lambda.expression.operandsCount != 2
            ) {
                return null
            }
            val relation = ordered(source.variable, mapping) ?: return null
            val (body, bound) = lambda.expression.operandsList
            if (body.nodeCase != Operand.NodeCase.VARIABLE || bound.nodeCase != Operand.NodeCase.VARIABLE) return null
            val prefix = "${bound.variable}."
            if (!body.variable.startsWith(prefix)) return null
            val member = body.variable.removePrefix(prefix)
            val field = relation.fields[member] as? AttributeMapping.Field ?: return null
            // map() raises on an element missing the member, whatever the list's length.
            if (field.column.columnType.nullable) return null
            return source.variable to member
        }

        private fun spell(operator: String, operands: List<Operand>, mapping: AttributeResolver): String? {
            if (operands.size != 2) return null
            val (target, key) = operands
            if (operator == "index") {
                if (target.nodeCase != Operand.NodeCase.VARIABLE || key.nodeCase != Operand.NodeCase.VALUE ||
                    key.value.kindCase != Value.KindCase.NUMBER_VALUE || ordered(target.variable, mapping) == null
                ) {
                    return null
                }
                val position = key.value.numberValue
                val spelled = if (position == Math.floor(position) && Math.abs(position) < 1e15) {
                    position.toLong().toString()
                } else {
                    position.toString()
                }
                return "${target.variable}[$spelled]"
            }
            if (operator == "get-field" && target.nodeCase == Operand.NodeCase.EXPRESSION) {
                val element = spell(target.expression.operator, target.expression.operandsList, mapping) ?: return null
                val member = when {
                    key.nodeCase == Operand.NodeCase.VARIABLE -> key.variable
                    key.nodeCase == Operand.NodeCase.VALUE && key.value.kindCase == Value.KindCase.STRING_VALUE ->
                        key.value.stringValue
                    else -> return null
                }
                return "$element.$member"
            }
            return null
        }
    }
}
