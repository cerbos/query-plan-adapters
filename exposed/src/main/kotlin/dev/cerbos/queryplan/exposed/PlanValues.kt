package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value

/** Protobuf-to-Kotlin conversion of plan constants. */
internal object PlanValues {
    private const val TWO_POW_63 = 9.223372036854775808E18

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
}
