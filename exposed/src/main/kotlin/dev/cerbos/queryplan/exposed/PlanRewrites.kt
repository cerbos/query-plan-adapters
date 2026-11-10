package dev.cerbos.queryplan.exposed

import com.google.protobuf.ListValue
import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * Plan-to-plan rewrites: list and map shapes that have an exact spelling in shapes the walk already
 * translates. Each returns the rewritten operand, or `null` when the input is not its shape, and
 * none of them reads a column: the walk translates what they return like any other plan.
 */
internal object PlanRewrites {

    /** A lambda variable no plan names, bound by the rewrites that introduce a macro. */
    private const val ELEMENT = "__cerbos_rewrite_element"

    fun expression(operator: String, vararg operands: Operand): Operand =
        Operand.newBuilder().setExpression(
            PlanResourcesFilter.Expression.newBuilder().setOperator(operator).addAllOperands(operands.toList()),
        ).build()

    fun constant(value: Value): Operand = Operand.newBuilder().setValue(value).build()

    private fun variable(name: String): Operand = Operand.newBuilder().setVariable(name).build()

    private fun number(value: Double): Operand = constant(Value.newBuilder().setNumberValue(value).build())

    /**
     * Rewrites `attr["key"]` as the attribute `attr.key` where the mapping declares `attr.key` and not
     * `attr`; both raise on a missing key.
     */
    fun selectMapKeys(operand: Operand, mapping: AttributeResolver): Operand {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION) return operand
        val expression = operand.expression
        if (expression.operator == "index" && expression.operandsCount == 2 &&
            expression.getOperands(0).nodeCase == Operand.NodeCase.VARIABLE &&
            expression.getOperands(1).nodeCase == Operand.NodeCase.VALUE &&
            expression.getOperands(1).value.kindCase == Value.KindCase.STRING_VALUE
        ) {
            val attribute = expression.getOperands(0).variable
            val selected = "$attribute.${expression.getOperands(1).value.stringValue}"
            if (mapping.resolve(attribute) == null && mapping.resolve(selected) != null) {
                return variable(selected)
            }
        }
        val rebuilt = expression.toBuilder().clearOperands()
        expression.operandsList.forEach { rebuilt.addOperands(selectMapKeys(it, mapping)) }
        return Operand.newBuilder().setExpression(rebuilt).build()
    }

    /**
     * Folds `add(list, list)` of two constant lists, at any depth, into the list CEL's `+`
     * concatenates them into. What a ternary substitution leaves behind under a list `+`
     * (`["a"] + (c ? ["b"] : [])`). Anything else is returned as it was.
     */
    fun foldListConcatenation(operand: Operand): Operand {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator != "add" ||
            operand.expression.operandsCount != 2
        ) {
            return operand
        }
        val left = foldListConcatenation(operand.expression.getOperands(0))
        val right = foldListConcatenation(operand.expression.getOperands(1))
        val leftList = PlanValues.literalListElements(left)
        val rightList = PlanValues.literalListElements(right)
        if (leftList == null || rightList == null) {
            return Operand.newBuilder().setExpression(
                operand.expression.toBuilder().setOperands(0, left).setOperands(1, right),
            ).build()
        }
        return constant(
            Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(leftList).addAllValues(rightList)).build(),
        )
    }

    /**
     * `x in coll.map(t, body)` over a relation as `size(coll.filter(t, x == body)) > 0`, and
     * `x in coll.filter(t, p)` as `size(coll.filter(t, p ? x == t : false)) > 0`; both keep CEL's errors
     * UNKNOWN. `null` for other shapes, or when `x` reads the lambda variable's name.
     */
    fun membershipInProjection(operands: List<Operand>): Operand? {
        if (operands.size != 2 || operands[1].nodeCase != Operand.NodeCase.EXPRESSION) return null
        val macro = operands[1].expression
        val isMap = macro.operator == "map"
        if ((!isMap && macro.operator != "filter") || macro.operandsCount != 2) return null
        val range = macro.getOperands(0)
        val rangeIsLiteral = PlanValues.literalListElements(range) != null
        if (range.nodeCase != Operand.NodeCase.VARIABLE && !(rangeIsLiteral && !isMap)) return null
        val lambda = ParsedLambda.parse(
            macro.getOperands(1),
            "${macro.operator} second operand must be a lambda",
            "${macro.operator} supports single-variable lambdas only",
            "${macro.operator} lambda variable must be a variable operand",
        )
        val needle = operands[0]
        if (readsName(needle, lambda.variable)) return null
        val element = variable(lambda.variable)
        val body = if (isMap) {
            expression("eq", needle, lambda.body)
        } else {
            expression(
                "if",
                lambda.body,
                expression("eq", needle, element),
                constant(Value.newBuilder().setBoolValue(false).build()),
            )
        }
        val filter = expression("filter", range, expression("lambda", body, element))
        return expression("gt", expression("size", filter), number(0.0))
    }

    /**
     * `x in a + b` as `x in a || x in b`, when every part is a literal list or a direct relation, neither
     * of which can raise.
     */
    fun membershipInConcatenation(operands: List<Operand>, scope: Scope): Operand? {
        if (operands.size != 2) return null
        val parts = ArrayList<Operand>()
        if (!concatenationParts(operands[1], scope, parts) || parts.size < 2) return null
        val any = PlanResourcesFilter.Expression.newBuilder().setOperator("or")
        parts.forEach { any.addOperands(expression("in", operands[0], it)) }
        return Operand.newBuilder().setExpression(any).build()
    }

    private fun concatenationParts(operand: Operand, scope: Scope, parts: MutableList<Operand>): Boolean =
        when (operand.nodeCase) {
            Operand.NodeCase.VALUE -> {
                parts.add(operand)
                operand.value.kindCase == Value.KindCase.LIST_VALUE
            }
            Operand.NodeCase.VARIABLE -> {
                parts.add(operand)
                isDirectRelation(operand.variable, scope)
            }
            Operand.NodeCase.EXPRESSION -> {
                val expression = operand.expression
                expression.operator == "add" && expression.operandsCount == 2 &&
                    concatenationParts(expression.getOperands(0), scope, parts) &&
                    concatenationParts(expression.getOperands(1), scope, parts)
            }
            else -> false
        }

    private fun isDirectRelation(variable: String, scope: Scope): Boolean = try {
        (scope.resolve(variable) as? Resolution.Collection)?.leadingHops?.isEmpty() == true
    } catch (unmapped: UnmappedAttributeException) {
        false
    }

    /**
     * `rel.isSubset([...])`, Cerbos's "every element of `rel` is in the list", as
     * `rel.all(e, e in [...])`. `null` for any other shape.
     */
    fun subsetAsMembership(operands: List<Operand>, scope: Scope): Operand? {
        if (operands.size != 2) return null
        val (receiver, other) = operands
        if (receiver.nodeCase != Operand.NodeCase.VARIABLE || !isRelation(receiver.variable, scope)) return null
        if (PlanValues.literalListElements(other) == null) return null
        return everyElementIn(receiver, other)
    }

    /** `rel.except([...]) == []` as `rel.all(e, e in [...])`, and `!= []` as its negation. `null` otherwise. */
    fun differenceEmptiness(operator: String, operands: List<Operand>, scope: Scope): Operand? {
        if ((operator != "eq" && operator != "ne") || operands.size != 2) return null
        for (i in 0..1) {
            val difference = operands[i]
            if (!isEmptyList(operands[1 - i]) || difference.nodeCase != Operand.NodeCase.EXPRESSION ||
                difference.expression.operator != "except" || difference.expression.operandsCount != 2
            ) {
                continue
            }
            val receiver = difference.expression.getOperands(0)
            val removed = difference.expression.getOperands(1)
            if (receiver.nodeCase != Operand.NodeCase.VARIABLE || !isRelation(receiver.variable, scope) ||
                PlanValues.literalListElements(removed) == null
            ) {
                return null
            }
            val all = everyElementIn(receiver, removed)
            return if (operator == "eq") all else expression("not", all)
        }
        return null
    }

    /** `intersect(a, b) == []` as `!hasIntersection(a, b)`, and `!= []` as `hasIntersection(a, b)`. `null` otherwise. */
    fun intersectionEmptiness(operator: String, operands: List<Operand>): Operand? {
        if ((operator != "eq" && operator != "ne") || operands.size != 2) return null
        for (i in 0..1) {
            val intersect = operands[i]
            if (intersect.nodeCase == Operand.NodeCase.EXPRESSION && intersect.expression.operator == "intersect" &&
                intersect.expression.operandsCount == 2 && isEmptyList(operands[1 - i])
            ) {
                val shared = Operand.newBuilder()
                    .setExpression(intersect.expression.toBuilder().setOperator("hasIntersection"))
                    .build()
                return if (operator == "eq") expression("not", shared) else shared
            }
        }
        return null
    }

    private fun everyElementIn(collection: Operand, list: Operand): Operand {
        val element = variable(ELEMENT)
        return expression("all", collection, expression("lambda", expression("in", element, list), element))
    }

    private fun isEmptyList(operand: Operand): Boolean = PlanValues.literalListElements(operand)?.isEmpty() == true

    private fun isRelation(variable: String, scope: Scope): Boolean = try {
        scope.resolve(variable) is Resolution.Collection
    } catch (unmapped: UnmappedAttributeException) {
        false
    }

    /** Whether [operand] reads [name], or a member of it, anywhere. */
    private fun readsName(operand: Operand, name: String): Boolean = when (operand.nodeCase) {
        Operand.NodeCase.VARIABLE -> operand.variable == name || operand.variable.startsWith("$name.")
        Operand.NodeCase.EXPRESSION -> operand.expression.operandsList.any { readsName(it, name) }
        else -> false
    }
}
