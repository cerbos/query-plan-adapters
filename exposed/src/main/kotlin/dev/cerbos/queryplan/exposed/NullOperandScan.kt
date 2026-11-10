package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * Refuses, before anything is built, a plan comparing an attribute against null when that attribute's
 * convention is OMITTED. Matches on the operand, because a leaf cannot know about an enclosing `not`.
 */
internal object NullOperandScan {

    /** The operators CEL evaluates to a definite boolean over a null value. */
    private val EQUALITY_FAMILY = setOf("eq", "ne", "in")

    /** The operators [LeafTranslator] answers UNKNOWN-when-NULL for an OMITTED attribute. */
    private val UNKNOWN_WHEN_NULL = setOf("eq", "ne")

    fun assertTranslatable(condition: Operand, options: Options) {
        if (condition.nodeCase != Operand.NodeCase.EXPRESSION) return
        val expression = condition.expression
        val operands = expression.operandsList

        // A comparison between a mapped attribute and a literal is decided by that attribute's own
        // declaration, which is what lets one call carry both conventions (#308). Confined to that
        // shape: a null buried in a macro over a literal list reaches a comparison long after this
        // scan, and nothing here can say which column it will land against, so those keep using
        // the call-level default.
        val declared = declaredForComparedAttribute(expression.operator, operands, options.mapping)
        if (declared != null) {
            // `==` and `!=` against a bare null are answerable under OMITTED: the NULL column is a
            // missing attribute, which CEL denies under both polarities, so [LeafTranslator]
            // renders the comparison UNKNOWN for it and definite for a present value. A list
            // carrying a null is not: SQL silently drops a NULL inside an `IN` list.
            if (declared == NullAttributeRepresentation.OMITTED &&
                operands.any(::carriesNull) &&
                !(expression.operator in UNKNOWN_WHEN_NULL && operands.any(::isBareNull))
            ) {
                throw ScalarRefusals.nullOperandUnderOmitted(expression.operator)
            }
            return
        }

        if (options.nullAttributeRepresentation == NullAttributeRepresentation.OMITTED &&
            operands.any(::carriesNull)
        ) {
            throw ScalarRefusals.nullOperandUnderOmitted(expression.operator)
        }
        operands.forEach { assertTranslatable(it, options) }
    }

    /** The declared convention of the attribute a binary comparison names, or `null` for any other node. */
    private fun declaredForComparedAttribute(
        operator: String,
        operands: List<Operand>,
        mapping: AttributeResolver,
    ): NullAttributeRepresentation? {
        if (operator !in EQUALITY_FAMILY || operands.size != 2) return null
        val (variable, literal) = if (operands[1].nodeCase == Operand.NodeCase.VARIABLE) {
            operands[1] to operands[0]
        } else {
            operands[0] to operands[1]
        }
        if (variable.nodeCase != Operand.NodeCase.VARIABLE || literal.nodeCase != Operand.NodeCase.VALUE) return null
        return declaredField(variable.variable, mapping)?.nullAttributeRepresentation
    }

    /** The field [variable] names, directly or through to-one relations, by the same walk [RootScope] uses. */
    private fun declaredField(variable: String, mapping: AttributeResolver): AttributeMapping.Field? {
        when (val direct = mapping.resolve(variable)) {
            is AttributeMapping.Field -> return direct
            is AttributeMapping.Relation -> return null
            null -> Unit
        }
        val parts = variable.split('.')
        for (length in parts.size - 1 downTo 1) {
            val head = mapping.resolve(parts.subList(0, length).joinToString(".")) as? AttributeMapping.Relation
                ?: continue
            val walked = ChainWalk.from(head, parts.subList(length, parts.size)) ?: continue
            return walked.leaf
        }
        return null
    }

    private fun isBareNull(operand: Operand): Boolean =
        operand.nodeCase == Operand.NodeCase.VALUE && operand.value.kindCase == Value.KindCase.NULL_VALUE

    /** A null constant, or a list carrying one — SQL silently drops a NULL inside an `IN` list. */
    private fun carriesNull(operand: Operand): Boolean {
        if (operand.nodeCase != Operand.NodeCase.VALUE) return false
        val value = operand.value
        return when (value.kindCase) {
            Value.KindCase.NULL_VALUE -> true
            Value.KindCase.LIST_VALUE ->
                value.listValue.valuesList.any { it.kindCase == Value.KindCase.NULL_VALUE }
            else -> false
        }
    }
}
