package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.ScalarColumnKind
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import dev.cerbos.queryplan.exposed.sql.ScalarValueFamily
import org.jetbrains.exposed.v1.core.Op

/** The one walk over the plan. Negation wraps a built predicate ([TriLogic.not]); errors must be UNKNOWN, never FALSE. */
internal class PlanWalker(private val translation: Translation) {
    private var macroDepth = 0

    fun traverse(operand: Operand, scope: Scope): Op<Boolean> = when (operand.nodeCase) {
        Operand.NodeCase.EXPRESSION -> traverseExpression(operand.expression, scope)
        // A bare boolean attribute in condition position: `R.attr.aBool`.
        Operand.NodeCase.VARIABLE -> translation.hopGuards.guarded {
            val target = scope.scalar(operand.variable)
            // A text or numeric column in condition position is CEL's no-overload error, and so is a
            // null: the plain comparison keeps a NULL UNKNOWN under either null convention.
            when (ScalarColumnTypes.familyOf(target.column)) {
                ScalarValueFamily.TEXT, ScalarValueFamily.NUMERIC -> TriLogic.unknown()
                ScalarValueFamily.BOOLEAN -> translation.leaf.defaultLeaf("eq", target, true)
                else -> translation.leaf.applyLeaf("eq", target, true)
            }
        }
        // A boolean constant as a whole condition. The planner never ships one at the root, but a
        // ternary rewrite substitutes one in: `aBool ? true : false` walks each branch through
        // here. Folded rather than routed to a leaf, because there is no column to compare.
        Operand.NodeCase.VALUE -> when (val constant = PlanValues.toKotlin(operand.value)) {
            true -> Op.TRUE
            false -> Op.FALSE
            else -> throw Refusals.malformed(
                "A constant in condition position must be a boolean, got ${PlanValues.typeName(constant)}",
            )
        }
        Operand.NodeCase.NODE_NOT_SET, null ->
            throw Refusals.malformed("Plan operand has no node set")
    }

    fun traverseExpression(expression: Expression, scope: Scope): Op<Boolean> {
        val operator = expression.operator
        val operands = expression.operandsList
        return when (operator) {
            "and" -> TriLogic.and(junction(operator, operands, scope))
            "or" -> TriLogic.or(junction(operator, operands, scope))
            "not" -> {
                if (operands.size != 1) throw Refusals.malformed("not requires exactly 1 operand, got ${operands.size}")
                TriLogic.not(traverse(operands[0], scope))
            }
            else -> translation.hopGuards.guarded { traverseLeaf(operator, operands, scope) }
        }
    }

    private fun traverseLeaf(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> {
        return when (operator) {
            "exists", "exists_one", "all" -> translation.collections.translate(operator, operands, scope)
            // filter() and map() return a LIST, not a boolean. `filter(...)` in condition position is
            // not `size(filter(...)) > 0`, and lowering it as if it were over-grants.
            "filter" -> throw Refusals.unsupported(
                "filter() returns a list, not a boolean, so it cannot be a condition on its own; " +
                    "only size(filter(...)) gives the list a scalar meaning",
            )
            "map" -> throw Refusals.unsupported(
                "map() returns a list, not a boolean, so it cannot be a condition on its own; " +
                    "only hasIntersection(map(...), [...]) gives the list a scalar meaning",
            )
            "except" -> throw ScalarRefusals.exceptUnsupported()
            "hasIntersection", "has_intersection" -> translation.membership.translateHasIntersection(operands, scope)
            "in" -> membership(operands, scope)
            "isSubset" -> PlanRewrites.subsetAsMembership(operands, scope)?.let { traverse(it, scope) }
                ?: translation.comparisons.translate(operator, operands, scope)
            "if" -> translation.ternary.translateBare(operands, scope)
            "has" -> translation.presence.translate(operands, scope)
            "overlaps", "ancestorOf", "descendentOf" -> translation.hierarchy.translate(operator, operands, scope)
            "matches" -> matches(operands, scope)
            else -> matchesComparedWithBoolean(operator, operands, scope)
                ?: textOfConditionCompared(operator, operands, scope)
                ?: (
                    PlanRewrites.intersectionEmptiness(operator, operands)
                        ?: PlanRewrites.differenceEmptiness(operator, operands, scope)
                    )?.let { traverse(it, scope) }
                ?: mapLiteralLookup(operator, operands, scope)
                ?: translation.comparisons.translate(operator, operands, scope)
        }
    }

    /**
     * `in`, after the shapes that rewrite into others: a ternary operand (substituted in place),
     * two constants (decided here), a `map()` or `filter()` projection, a list concatenation, and a
     * list built from expressions. What is left is [MembershipTranslator]'s.
     */
    private fun membership(operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operands.size != 2) return translation.membership.translateIn(operands, scope)
        translation.ternary.tryTernaryComparison("in", operands, scope)?.let { return it }
        val needle = PlanValues.builtConstant(operands[0])
        val haystack = PlanValues.builtConstant(operands[1])
        if (needle !== PlanValues.NotConstant && haystack !== PlanValues.NotConstant) {
            return constantIn(needle, haystack)
        }
        (
            PlanRewrites.membershipInProjection(operands)
                ?: PlanRewrites.membershipInConcatenation(operands, scope)
            )?.let { return traverse(it, scope) }
        if (haystack === PlanValues.NotConstant && isComputedList(operands[1])) {
            val elements = operands[1].expression.operandsList
            return overComputedList(elements, elements.map { PlanRewrites.expression("eq", operands[0], it) }, true, scope)
        }
        computedNeedle(operands[0], haystack)?.let { return traverse(it, scope) }
        return translation.membership.translateIn(operands, scope)
    }

    /**
     * `expr in [e1, e2]` for a computed needle against a constant list, as `expr == e1 || expr == e2`.
     * A null or an element of another type is never equal; an erroring needle errors each term.
     */
    private fun computedNeedle(needle: Operand, haystack: Any?): Operand? {
        if (needle.nodeCase != Operand.NodeCase.EXPRESSION || haystack !is List<*>) return null
        val operator = needle.expression.operator
        val boolean = operator in BOOLEAN_OPERATORS
        if (!boolean && operator != "size" && operator != "if") return null
        val terms = haystack.mapNotNull { element ->
            when {
                element == null -> null
                boolean -> when (element) {
                    true -> needle
                    false -> PlanRewrites.expression("not", needle)
                    else -> null
                }
                operator == "size" && element !is Number -> null
                else -> PlanRewrites.expression("eq", needle, PlanRewrites.constant(PlanValues.toValue(element) ?: return null))
            }
        }.distinct()
        if (terms.isEmpty()) return null
        return if (terms.size == 1) terms[0] else PlanRewrites.expression("or", *terms.toTypedArray())
    }

    /**
     * `value in list` or `value in map` over two constants, which a ternary substitution leaves
     * behind (`"r" in (c ? ["r"] : [])`): CEL list membership is element equality, numbers compared
     * by value, and map membership is a key test.
     */
    private fun constantIn(needle: Any?, haystack: Any?): Op<Boolean> {
        val holds = when (haystack) {
            is List<*> -> haystack.any { celEquals(needle, it) }
            is Map<*, *> -> needle is String && haystack.containsKey(needle)
            // CEL has no `in` over a scalar, so the planner cannot emit this.
            else -> throw Refusals.malformed("in requires a list or map, got ${PlanValues.typeName(haystack)}")
        }
        return if (holds) Op.TRUE else Op.FALSE
    }

    /**
     * A macro or membership over a list built from expressions, folded per element. One erroring element
     * errors the whole result, so the fold sits under an `e == e || !(e == e)` guard.
     */
    fun overComputedList(elements: List<Operand>, perElement: List<Operand>, any: Boolean, scope: Scope): Op<Boolean> {
        if (perElement.isEmpty()) return if (any) Op.FALSE else Op.TRUE
        val fold = PlanResourcesFilter.Expression.newBuilder().setOperator(if (any) "or" else "and")
            .addAllOperands(perElement).build()
        val guards = elements.filter { it.nodeCase != Operand.NodeCase.VALUE }.map { element ->
            val same = PlanRewrites.expression("eq", element, element)
            PlanRewrites.expression("or", same, PlanRewrites.expression("not", same))
        }
        if (guards.isEmpty()) return traverseExpression(fold, scope)
        val guard = PlanResourcesFilter.Expression.newBuilder().setOperator("and").addAllOperands(guards).build()
        return TriLogic.ternary(traverseExpression(guard, scope), traverseExpression(fold, scope), Op.FALSE)
    }

    /** `{"k1": v1, ...}[key] ==/!= c`, UNKNOWN when `key` is missing. `null` for any other shape. */
    private fun mapLiteralLookup(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        if ((operator != "eq" && operator != "ne") || operands.size != 2) return null
        for (i in 0..1) {
            val index = operands[i]
            val compared = operands[1 - i]
            if (index.nodeCase != Operand.NodeCase.EXPRESSION || index.expression.operator != "index" ||
                index.expression.operandsCount != 2 || compared.nodeCase != Operand.NodeCase.VALUE
            ) {
                continue
            }
            val map = PlanValues.builtConstant(index.expression.getOperands(0)) as? Map<*, *> ?: continue
            val key = index.expression.getOperands(1)
            if (key.nodeCase == Operand.NodeCase.VALUE) continue
            val target = PlanValues.toKotlin(compared.value)
            val keys = com.google.protobuf.ListValue.newBuilder()
            val selected = com.google.protobuf.ListValue.newBuilder()
            for ((k, v) in map) {
                val keyValue = PlanValues.toValue(k) ?: return null
                keys.addValues(keyValue)
                if (celEquals(v, target) == (operator == "eq")) selected.addValues(keyValue)
            }
            val present = PlanRewrites.expression(
                "in",
                key,
                PlanRewrites.constant(com.google.protobuf.Value.newBuilder().setListValue(keys).build()),
            )
            val matching = PlanRewrites.expression(
                "in",
                key,
                PlanRewrites.constant(com.google.protobuf.Value.newBuilder().setListValue(selected).build()),
            )
            return TriLogic.ternary(traverse(present, scope), traverse(matching, scope), TriLogic.unknown())
        }
        return null
    }

    /**
     * `matches(attribute, "pattern")`. A pattern that is not a string constant (read from a column,
     * or computed) has nothing to translate at build time and is refused; a non-string constant has
     * no `matches()` overload, so it is UNKNOWN.
     */
    private fun matches(operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operands.size != 2) throw Refusals.malformed("matches requires exactly 2 operands, got ${operands.size}")
        val (receiver, pattern) = operands
        if (receiver.nodeCase != Operand.NodeCase.VARIABLE || pattern.nodeCase != Operand.NodeCase.VALUE) {
            throw Refusals.unsupported(
                "matches() is translated only between a mapped attribute and a constant pattern: " +
                    "the pattern is compiled into LIKE at translation time",
            )
        }
        val target = scope.scalar(receiver.variable)
        val text = PlanValues.toKotlin(pattern.value) as? String ?: return TriLogic.unknown()
        return translation.regex.matches(target, text)
    }

    /**
     * `matches(...) == true`, `!= false` and their mirrors, which keep their wrapper on the wire: the
     * match itself, or its negation. `null` when the node is not that shape.
     */
    private fun matchesComparedWithBoolean(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        if ((operator != "eq" && operator != "ne") || operands.size != 2) return null
        val match = operands.firstOrNull {
            it.nodeCase == Operand.NodeCase.EXPRESSION && it.expression.operator == "matches"
        } ?: return null
        val other = operands.first { it !== match }
        if (other.nodeCase != Operand.NodeCase.VALUE) return null
        val expected = PlanValues.toKotlin(other.value) as? Boolean ?: return null
        val translated = matches(match.expression.operandsList, scope)
        return if (expected == (operator == "eq")) translated else TriLogic.not(translated)
    }

    /** `string(condition) ==/!= "literal"`: the condition, its negation, or FALSE, keeping its UNKNOWN. `null` otherwise. */
    private fun textOfConditionCompared(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        if ((operator != "eq" && operator != "ne") || operands.size != 2) return null
        val cast = operands.firstOrNull {
            it.nodeCase == Operand.NodeCase.EXPRESSION && it.expression.operator == "string" &&
                it.expression.operandsCount == 1 && isBooleanValued(it.expression.getOperands(0), scope)
        } ?: return null
        val other = operands.first { it !== cast }
        if (other.nodeCase != Operand.NodeCase.VALUE) return null
        val text = PlanValues.toKotlin(other.value) as? String ?: return null
        val condition = traverse(cast.expression.getOperands(0), scope)
        val equal = when (text) {
            "true" -> condition
            "false" -> TriLogic.not(condition)
            else -> TriLogic.baseUnlessUnknown(Op.FALSE, TriLogic.not(TriLogic.determined(condition)))
        }
        return if (operator == "eq") equal else TriLogic.not(equal)
    }

    /**
     * Whether [operand] certainly evaluates to a CEL bool (or raises): an operator that only
     * yields a bool, a boolean constant, a boolean column, or a ternary whose arms all are.
     */
    private fun isBooleanValued(operand: Operand, scope: Scope): Boolean = when (operand.nodeCase) {
        Operand.NodeCase.VALUE -> operand.value.kindCase == com.google.protobuf.Value.KindCase.BOOL_VALUE
        Operand.NodeCase.VARIABLE -> (scope.resolve(operand.variable) as? Resolution.Scalar)?.let {
            ScalarColumnTypes.kindOf(it.column) == ScalarColumnKind.BOOLEAN
        } == true
        Operand.NodeCase.EXPRESSION -> {
            val expression = operand.expression
            when (expression.operator) {
                in BOOLEAN_OPERATORS -> true
                "if" -> expression.operandsCount == 3 &&
                    isBooleanValued(expression.getOperands(1), scope) &&
                    isBooleanValued(expression.getOperands(2), scope)
                else -> false
            }
        }
        else -> false
    }

    private fun junction(operator: String, operands: List<Operand>, scope: Scope): List<Op<Boolean>> {
        if (operands.isEmpty()) throw Refusals.malformed("$operator requires at least 1 operand")
        return operands.map { traverse(it, scope) }
    }

    /**
     * Runs [body] one macro level deeper, refusing past [Options.maxMacroDepth]. Every collection
     * macro wraps its translation in this: each level multiplies the correlated subqueries the
     * filter carries, and the bound is what keeps that finite.
     */
    fun <T> enterMacro(operator: String, body: () -> T): T {
        val limit = translation.options.maxMacroDepth
        if (macroDepth >= limit) {
            throw Refusals.unsupported(
                "$operator is nested ${macroDepth + 1} collection macros deep, past maxMacroDepth=$limit",
            )
        }
        macroDepth++
        try {
            return body()
        } finally {
            macroDepth--
        }
    }

    companion object {
        /** A `list(...)` the literal fold could not read as a constant: an element is computed. */
        fun isComputedList(operand: Operand): Boolean =
            operand.nodeCase == Operand.NodeCase.EXPRESSION && operand.expression.operator == "list" &&
                PlanValues.builtConstant(operand) === PlanValues.NotConstant

        /** CEL equality between two plan constants: numbers by value, everything else by kind and value. */
        private fun celEquals(left: Any?, right: Any?): Boolean =
            if (left is Number && right is Number) left.toDouble() == right.toDouble() else left == right

        /** Operators whose CEL result is always a bool (or an error). */
        private val BOOLEAN_OPERATORS = setOf(
            "eq", "ne", "lt", "le", "gt", "ge", "and", "or", "not", "in", "hasIntersection",
            "has_intersection", "contains", "startsWith", "endsWith", "matches", "exists", "all",
            "exists_one", "overlaps", "ancestorOf", "descendentOf", "isSubset", "has",
        )
    }
}
