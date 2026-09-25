package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter

/** Protobuf-to-Kotlin conversion of plan constants, and the folding rules the `add` shapes need. */
internal object PlanValues {
    private const val TWO_POW_63 = 9.223372036854775808E18

    /**
     * Largest magnitude at which every `Long` is exactly an IEEE double (2^53). CEL attribute
     * arithmetic is always double-typed at check time, so beyond this bound the check-time
     * arithmetic has gaps between representable integers and a long-space solve could disagree
     * with what the PDP evaluates.
     */
    private const val MAX_EXACT_DOUBLE_LONG = 1L shl 53

    /**
     * A plan constant as a Kotlin value: `String`, `Long`, `Double`, `Boolean`, `null`,
     * `List<Any?>` or `Map<String, Any?>`.
     *
     * A whole number becomes a `Long` only inside `[-2^63, 2^63)`: converting a double outside that
     * range saturates to `Long.MIN_VALUE` / `MAX_VALUE` and silently changes the constant.
     * Out-of-range values stay doubles. So does negative zero, whose sign a `Long` cannot hold and
     * IEEE division depends on.
     */
    fun toKotlin(value: Value): Any? = when (value.kindCase) {
        Value.KindCase.STRING_VALUE -> value.stringValue
        Value.KindCase.NUMBER_VALUE -> {
            val d = value.numberValue
            val isNegativeZero = d == 0.0 && 1.0 / d < 0.0
            if (d == Math.floor(d) && !d.isInfinite() && !isNegativeZero && d >= -TWO_POW_63 && d < TWO_POW_63) {
                d.toLong()
            } else {
                d
            }
        }
        Value.KindCase.BOOL_VALUE -> value.boolValue
        Value.KindCase.NULL_VALUE -> null
        Value.KindCase.LIST_VALUE -> value.listValue.valuesList.map(::toKotlin)
        Value.KindCase.STRUCT_VALUE -> LinkedHashMap<String, Any?>().also { struct ->
            value.structValue.fieldsMap.forEach { (key, field) -> struct[key] = toKotlin(field) }
        }
        Value.KindCase.KIND_NOT_SET, null ->
            throw Refusals.malformed("Protobuf Value has no kind set: the planner emitted a malformed operand")
    }

    /** The marker [builtConstant] answers for an operand that is not built from constants alone. */
    object NotConstant

    /**
     * A `list(...)` or `struct(set-field(...)...)` expression the planner assembled from constants
     * alone, as the Kotlin `List` / `Map` it denotes; a plain value as itself; or [NotConstant]
     * when anything inside it is not a constant. A list holding an attribute is NOT a constant: the
     * attribute may be missing, and CEL raises building the list before it compares it.
     */
    fun builtConstant(operand: PlanResourcesFilter.Expression.Operand): Any? = when (operand.nodeCase) {
        PlanResourcesFilter.Expression.Operand.NodeCase.VALUE -> toKotlin(operand.value)
        PlanResourcesFilter.Expression.Operand.NodeCase.EXPRESSION -> {
            val expression = operand.expression
            when (expression.operator) {
                "list" -> expression.operandsList.map { element ->
                    val value = builtConstant(element)
                    if (value === NotConstant) return NotConstant
                    value
                }
                "struct" -> LinkedHashMap<Any?, Any?>().also { struct ->
                    expression.operandsList.forEach { field ->
                        if (field.nodeCase != PlanResourcesFilter.Expression.Operand.NodeCase.EXPRESSION ||
                            field.expression.operator != "set-field" || field.expression.operandsCount != 2
                        ) {
                            return NotConstant
                        }
                        val key = builtConstant(field.expression.getOperands(0))
                        val value = builtConstant(field.expression.getOperands(1))
                        if (key === NotConstant || value === NotConstant) return NotConstant
                        struct[key] = value
                    }
                }
                else -> NotConstant
            }
        }
        else -> NotConstant
    }

    /**
     * Folds `add(left, right)` where both operands are constants: strings concatenate, numbers add.
     *
     * Reports TYPES, never values: a plan constant can carry a folded principal attribute. Malformed
     * rather than unsupported for the non-string, non-numeric pairings — `null + x` and `true + x`
     * are CEL no-overload errors whatever `x` is, so a planner folding constants never emits them.
     */
    fun foldAdd(left: Any?, right: Any?): Any {
        if (left == null || right == null) {
            throw Refusals.malformed("add requires non-null operands, got ${typeName(left)} + ${typeName(right)}")
        }
        if (left is String || right is String) return "$left$right"
        if (left is Number && right is Number) {
            return if (left is Long && right is Long) left + right else left.toDouble() + right.toDouble()
        }
        throw Refusals.malformed(
            "add requires string or numeric operands, got ${typeName(left)} + ${typeName(right)}",
        )
    }

    /**
     * Whether `field + addConstant eq/ne comparisonValue` has to be lowered to SQL-side double
     * arithmetic instead of being solved algebraically here.
     *
     * IEEE subtraction does not invert IEEE addition: `fl(fl(t - c) + c) != t` for many double
     * pairs — `t = 0.1, c = 0.7` solves to exactly `-0.6`, yet `-0.6 + 0.7` is
     * `0.09999999999999998` — so a pre-solved `field = -0.6` filter returns rows the PDP's
     * `check()` denies, and the `ne` mirror hides rows it allows. Only long/long pairs whose
     * solution also stays within ±2^53 remain on the solve path: there the solve and the
     * check-time double arithmetic are both exact. Non-numeric pairings return `false` so
     * [solveAdd] keeps owning string concatenation and its own type-mismatch messages.
     */
    fun requiresSqlLowering(comparisonValue: Any?, addConstant: Any?): Boolean {
        if (comparisonValue !is Number || addConstant !is Number) return false
        return !(comparisonValue is Long && addConstant is Long && isExactLongSolve(comparisonValue, addConstant))
    }

    /**
     * Solves `field + addConstant == comparisonValue` for the field, for the ALGEBRAICALLY EXACT
     * shapes only. Returns `null` when no field value can satisfy the equation (the comparison
     * value does not carry the constant's prefix or suffix), which the caller collapses.
     */
    fun solveAdd(comparisonValue: Any?, addConstant: Any?, fieldIsLeft: Boolean): Any? {
        if (comparisonValue is String && addConstant is String) {
            return if (fieldIsLeft) {
                // field + const == comparison -> field == comparison stripped of its suffix
                if (!comparisonValue.endsWith(addConstant)) null
                else comparisonValue.substring(0, comparisonValue.length - addConstant.length)
            } else {
                if (!comparisonValue.startsWith(addConstant)) null
                else comparisonValue.substring(addConstant.length)
            }
        }
        if (comparisonValue is Number && addConstant is Number) {
            // Both orderings give the same equation: field = comparison - const.
            if (comparisonValue is Long && addConstant is Long && isExactLongSolve(comparisonValue, addConstant)) {
                return comparisonValue - addConstant
            }
            throw Refusals.internal(
                "Numeric add-solve is only exact for integer constants within ±2^53; " +
                    "this shape must be lowered to SQL double arithmetic instead",
            )
        }
        // `R.attr.x + 1 == "abc"` is well-formed CEL over a dyn attribute — a string x errors, a
        // numeric x compares false — and neither outcome has a column translation.
        throw Refusals.unsupported(
            "add comparison type mismatch: ${typeName(comparisonValue)} vs ${typeName(addConstant)}",
        )
    }

    /**
     * Escapes `LIKE` wildcards; pair with an explicit `\` escape character.
     *
     * `[` is escaped because SQL Server and Sybase `LIKE` treat `[...]` as a character class even
     * when an `ESCAPE` clause is declared, so an unescaped `'[SEC]%'` matches one character from
     * `{S,E,C}` instead of the literal prefix. With the escape declared, `\[` is a literal `[` on
     * every dialect this adapter targets, where `[` is otherwise inert. `]` needs no escaping: it
     * is only special as the closer of a class, and no class can open once every `[` is escaped.
     */
    fun escapeLike(value: String): String =
        value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("[", "\\[")

    /** The runtime type of a converted plan constant, for error messages — never its value. */
    fun typeName(value: Any?): String = if (value == null) "null" else value::class.simpleName ?: "value"

    /** True when `t - c` is exact in long and in double space: t, c and the solution all ≤ 2^53. */
    private fun isExactLongSolve(t: Long, c: Long): Boolean =
        withinExactDoubleRange(t) && withinExactDoubleRange(c) && withinExactDoubleRange(t - c)

    /** Range check without `abs`, which is not safe for [Long.MIN_VALUE]. */
    private fun withinExactDoubleRange(value: Long): Boolean =
        value in -MAX_EXACT_DOUBLE_LONG..MAX_EXACT_DOUBLE_LONG
}
