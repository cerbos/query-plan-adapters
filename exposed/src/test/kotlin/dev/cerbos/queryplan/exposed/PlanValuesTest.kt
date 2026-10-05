package dev.cerbos.queryplan.exposed

import com.google.protobuf.ListValue
import com.google.protobuf.Struct
import com.google.protobuf.Value
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * The plan-constant conversions, and the two folding rules the `add` shapes turn on.
 *
 * These are pure functions over protobuf values, so they are asked directly rather than through a
 * plan: the interesting inputs — an unset kind, a number outside the exact-long range, a negative
 * zero — are either malformed or unreachable through a wire fixture.
 */
class PlanValuesTest {

    @Test
    fun `a whole number becomes a Long only inside the range a Long can hold`() {
        assertEquals(5L, PlanValues.toKotlin(number(5.0)))
        // Converting a double outside [-2^63, 2^63) saturates to Long.MIN/MAX_VALUE and silently
        // changes the constant, so those stay doubles and compare in double space.
        assertEquals(1.0E19, PlanValues.toKotlin(number(1.0E19)))
        assertEquals(-1.0E19, PlanValues.toKotlin(number(-1.0E19)))
        assertEquals(1.5, PlanValues.toKotlin(number(1.5)))
    }

    @Test
    fun `negative zero stays a Double, because a Long cannot hold its sign`() {
        val converted = PlanValues.toKotlin(number(-0.0))
        assertTrue(converted is Double)
        assertEquals(
            java.lang.Double.doubleToRawLongBits(-0.0),
            java.lang.Double.doubleToRawLongBits(converted as Double),
            "IEEE division reads the sign bit of a zero divisor to decide which infinity",
        )
    }

    @Test
    fun `a protobuf value with no kind is a malformed plan`() {
        val error = assertThrows<MalformedPlanException> { PlanValues.toKotlin(Value.getDefaultInstance()) }
        assertTrue(error.message!!.startsWith("Protobuf Value has no kind set"), error.message)
    }

    @Test
    fun `lists and structs convert recursively, preserving order and nulls`() {
        val list = PlanValues.toKotlin(
            Value.newBuilder().setListValue(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("a"))
                    .addValues(Value.newBuilder().setNullValueValue(0)),
            ).build(),
        )
        assertEquals(listOf("a", null), list)

        val struct = PlanValues.toKotlin(
            Value.newBuilder().setStructValue(
                Struct.newBuilder()
                    .putFields("inner", Value.newBuilder().setNullValueValue(0).build()),
            ).build(),
        )
        assertEquals(mapOf("inner" to null), struct)
    }

    @Test
    fun `foldAdd concatenates strings and adds numbers`() {
        assertEquals("prefix:1", PlanValues.foldAdd("prefix:", "1"))
        assertEquals(3L, PlanValues.foldAdd(1L, 2L))
        assertEquals(3.5, PlanValues.foldAdd(1L, 2.5))
    }

    @Test
    fun `foldAdd reports TYPES, never the operand values`() {
        // A plan constant can carry a folded principal attribute, and an exception message is
        // logged. `null + x` and `true + x` are CEL no-overload errors whatever x is, so a planner
        // folding constants never emits them: malformed, not unsupported.
        val error = assertThrows<MalformedPlanException> { PlanValues.foldAdd(null, "s3cret") }
        assertEquals("add requires non-null operands, got null + String", error.message)
        assertFalse(error.message!!.contains("s3cret"))

        val typed = assertThrows<MalformedPlanException> { PlanValues.foldAdd(true, 1L) }
        assertEquals("add requires string or numeric operands, got Boolean + Long", typed.message)
    }

    @Test
    fun `an add-solve is taken only where it is algebraically exact`() {
        // IEEE subtraction does not invert IEEE addition: solving `x + 0.7 == 0.1` yields exactly
        // -0.6, yet -0.6 + 0.7 is 0.09999999999999998 — so the solved filter returns a row the
        // PDP's check() denies, and the `ne` mirror hides one it allows.
        assertTrue(PlanValues.requiresSqlLowering(0.1, 0.7))
        assertTrue(PlanValues.requiresSqlLowering(1L, 0.5))
        assertFalse(PlanValues.requiresSqlLowering(3L, 1L))
        // Beyond 2^53 the check-time double arithmetic has gaps between representable integers, so
        // a long-space solve could disagree with what the PDP evaluates.
        assertTrue(PlanValues.requiresSqlLowering(1L shl 54, 1L))
        assertTrue(PlanValues.requiresSqlLowering(-(1L shl 54), 1L))
        // A non-numeric pairing stays on the solve path, which owns string concatenation.
        assertFalse(PlanValues.requiresSqlLowering("projects:f1", "projects:"))
    }

    @Test
    fun `solveAdd strips the constant from the side the field is not on`() {
        assertEquals("f1", PlanValues.solveAdd("projects:f1", "projects:", fieldIsLeft = false))
        assertEquals("projects", PlanValues.solveAdd("projects:f1", ":f1", fieldIsLeft = true))
        assertEquals(2L, PlanValues.solveAdd(3L, 1L, fieldIsLeft = true))
    }

    @Test
    fun `solveAdd returns null when no field value can satisfy the equation`() {
        // `"projects:123" == "users:" + R.id` can never be true. The caller collapses eq to FALSE
        // and ne to IS NOT NULL — never to an unconditional TRUE, because a missing attribute
        // makes the concatenation a CEL error and the row must stay excluded.
        assertEquals(null, PlanValues.solveAdd("projects:123", "users:", fieldIsLeft = false))
    }

    @Test
    fun `escapeLike escapes the escape character first`() {
        // Any other order double-escapes what the earlier replacements introduced.
        assertEquals("100\\%", PlanValues.escapeLike("100%"))
        assertEquals("a\\_b", PlanValues.escapeLike("a_b"))
        assertEquals("\\\\", PlanValues.escapeLike("\\"))
        assertEquals("\\\\\\%", PlanValues.escapeLike("\\%"))
        // `[` is escaped because SQL Server treats `[...]` as a character class even under an
        // ESCAPE clause; on every dialect this adapter targets `\[` is a literal `[`.
        assertEquals("\\[SEC]", PlanValues.escapeLike("[SEC]"))
    }

    private fun number(value: Double): Value = Value.newBuilder().setNumberValue(value).build()
}
