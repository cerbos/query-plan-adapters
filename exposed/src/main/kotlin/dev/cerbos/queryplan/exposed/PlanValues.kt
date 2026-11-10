package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter

/** Protobuf-to-Kotlin conversion of plan constants, and the folding rules the `add` shapes need. */
internal object PlanValues {
    private const val TWO_POW_63 = 9.223372036854775808E18

    /** 2^53: beyond it a long-space solve can disagree with the PDP's double arithmetic. */
    private const val MAX_EXACT_DOUBLE_LONG = 1L shl 53

    /**
     * A plan constant as a Kotlin value. A whole number becomes a `Long` only inside `[-2^63, 2^63)`, and
     * negative zero stays a double.
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

    /**
     * A value [builtConstant] produced, back as the protobuf `Value` a plan would carry, or `null`
     * when it has no such spelling (a map with a non-string key).
     */
    fun toValue(value: Any?): Value? {
        return when (value) {
            null -> Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build()
            is String -> Value.newBuilder().setStringValue(value).build()
            is Boolean -> Value.newBuilder().setBoolValue(value).build()
            is Long -> Value.newBuilder().setNumberValue(value.toDouble()).build()
            is Double -> Value.newBuilder().setNumberValue(value).build()
            is List<*> -> {
                val list = com.google.protobuf.ListValue.newBuilder()
                for (element in value) list.addValues(toValue(element) ?: return null)
                Value.newBuilder().setListValue(list).build()
            }
            is Map<*, *> -> {
                val struct = com.google.protobuf.Struct.newBuilder()
                for ((key, element) in value) {
                    if (key !is String) return null
                    struct.putFields(key, toValue(element) ?: return null)
                }
                Value.newBuilder().setStructValue(struct).build()
            }
            else -> null
        }
    }

    /**
     * The elements of a literal list operand — a list value, or a `list(...)` built from constants
     * alone — as protobuf values, or `null` when the operand is neither.
     */
    fun literalListElements(operand: PlanResourcesFilter.Expression.Operand): List<Value>? {
        if (operand.nodeCase == PlanResourcesFilter.Expression.Operand.NodeCase.VALUE) {
            return if (operand.value.kindCase == Value.KindCase.LIST_VALUE) operand.value.listValue.valuesList else null
        }
        val built = builtConstant(operand) as? List<*> ?: return null
        return built.map { toValue(it) ?: return null }
    }

    /** The marker [builtConstant] answers for an operand that is not built from constants alone. */
    object NotConstant

    /** A `list(...)` or `struct(...)` built from constants alone, or [NotConstant]. A list holding an attribute is not constant. */
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

    /** Folds constant `add(left, right)`. Reports types, never values. */
    fun foldAdd(left: Any?, right: Any?): Any {
        if (left == null || right == null) {
            throw Refusals.unsupported("add requires non-null operands, got ${typeName(left)} + ${typeName(right)}")
        }
        if (left is String && right is String) return left + right
        if (left is Number && right is Number) {
            return if (left is Long && right is Long) left + right else left.toDouble() + right.toDouble()
        }
        throw Refusals.unsupported(
            "add requires two strings or two numbers, got ${typeName(left)} + ${typeName(right)}",
        )
    }

    /**
     * Whether `field + c eq/ne v` must be lowered to SQL double arithmetic: IEEE subtraction does not
     * invert addition, so only long pairs within ±2^53 are solved here.
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

    /** Escapes `LIKE` wildcards and `[` (a class on SQL Server); pair with an explicit `\` escape. */
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
