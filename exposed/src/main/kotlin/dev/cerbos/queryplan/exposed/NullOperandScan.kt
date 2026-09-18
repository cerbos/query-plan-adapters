package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * The eager pre-walk scan behind [NullAttributeRepresentation.OMITTED]: a plan that compares an
 * attribute against a null constant is refused BEFORE anything is built, when the attribute's
 * convention (its own declaration, else the call-level option) is OMITTED.
 *
 * It matches on the OPERAND, never on an operator allowlist, and is deliberately wider than the
 * over-granting shapes: negation is applied around a built predicate, so a leaf cannot know whether
 * an enclosing `not` will flip it. It is eager, and not deferred to the leaf that would emit
 * `IS NULL`, because the refusal is a property of the whole plan under the caller's declaration —
 * a caller who gets a filter back should not have to run it to discover the plan was untranslatable.
 */
internal object NullOperandScan {

    /** The operators CEL evaluates to a definite boolean over a null value. */
    private val EQUALITY_FAMILY = setOf("eq", "ne", "in")

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
            if (declared == NullAttributeRepresentation.OMITTED && operands.any(::carriesNull)) {
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

    /**
     * The declared NULL convention of the attribute a binary comparison names, or `null` when the
     * node is not a comparison between one mapped attribute and one literal. Anything else — a
     * collection macro, `hasIntersection`, a string match — keeps using the call-level default,
     * because a declaration says nothing about what its null means there.
     */
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
        return (mapping.resolve(variable.variable) as? AttributeMapping.Field)?.nullAttributeRepresentation
    }

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
