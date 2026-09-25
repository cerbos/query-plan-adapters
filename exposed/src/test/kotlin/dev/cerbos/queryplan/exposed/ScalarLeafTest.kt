package dev.cerbos.queryplan.exposed

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue

/**
 * The scalar leaf, executed: which rows each corpus wire fixture's filter returns from H2.
 *
 * Every expectation is what CEL decides over [Scalars]'s rows, written out. The hazards these
 * cover are the ones an emitted filter can get wrong while still looking right — LIKE
 * metacharacters, the NULL row under both polarities, the two NULL conventions over one column,
 * value-first operand order, and a fractional constant against an integer column.
 */
class ScalarLeafTest {

    @Nested
    inner class StringEquality {
        @Test
        fun `CEL string equality is exact, so a case-only twin must not match`() =
            assertSelects("string/equals/case-sensitive", "r1", "c1")

        @Test
        fun `an empty-string constant matches nothing rather than everything`() =
            assertSelects("string/equals/empty-string")

        @Test
        fun `a constant carrying astral-plane characters compares by code point`() =
            assertSelects("string/equals/non-ascii-literal")

        @Test
        fun `a value-first inequality is mirrored, not inverted`() =
            assertSelects("comparison/not-equals/value-first", "r2", "r3", "r4", "r5", "r6", "r7", "f1")
    }

    @Nested
    inner class StringMatching {
        @Test
        fun `contains, startsWith and endsWith are case-sensitive`() {
            assertSelects("string/contains/case-sensitive", "r1", "c1")
            assertSelects("string/starts-with/case-sensitive", "r1", "c1")
            assertSelects("string/ends-with/case-sensitive", "r1", "c1")
        }

        @Test
        fun `a percent in the needle is a literal, not a wildcard`() =
            // Unescaped, `LIKE '100%%'` would also return r6 ("100abc").
            assertSelects("string/starts-with/percent-in-needle", "r3")

        @Test
        fun `an underscore in the needle is a literal, not a single-character wildcard`() =
            // Unescaped, `LIKE '%a_b%'` would return r5 ("xaXby") against an empty CEL answer.
            assertSelects("string/contains/underscore-in-needle")

        @Test
        fun `a bracket in the needle is a literal, not a character class`() =
            assertSelects("string/starts-with/bracket-in-needle", "r4")

        @Test
        fun `a backslash in the needle is a literal, not an escape`() =
            assertSelects("string/ends-with/backslash-in-needle", "r7")

        @Test
        fun `a constant receiver keeps the column as the NEEDLE, never the haystack`() {
            // `"…".contains(R.attr.aString)` — swapping the two would test the constant for
            // membership in each row's string instead, which is a different question entirely.
            assertSelects("string/contains/value-first", "r1", "r7", "c1")
            assertSelects("string/starts-with/value-first", "r5")
            assertSelects("string/ends-with/value-first", "r5")
        }
    }

    @Nested
    inner class ColumnNeedles {
        @Test
        fun `a column needle is escaped at query time`() {
            // r5's needle is a bare "%": unescaped it would match every row whose haystack is
            // present, which is the over-grant direction.
            assertSelects("string/contains/field-to-field", "r1", "r3")
            assertSelects("string/starts-with/field-to-field", "r1", "r3")
            assertSelects("string/ends-with/field-to-field", "r1")
        }

        @Test
        fun `a NULL needle stays UNKNOWN under negation, so its rows never leak`() {
            // The hazard of cerbos/query-plan-adapters#387: spelled as `needle IS NOT NULL AND
            // haystack LIKE …`, a NULL needle is a definite FALSE and NOT flips it to TRUE, so
            // r2, r4 and r7 would come back from a shape the PDP denies under both polarities.
            assertSelects("string/contains/negated-field-to-field", "r5", "r6", "f1", "c1")
            assertSelects("string/starts-with/negated-field-to-field", "r5", "r6", "f1", "c1")
        }
    }

    @Nested
    inner class NullOperands {
        @Test
        fun `eq against null selects the NULL rows under the explicit convention`() =
            assertSelects("null/equals/null-literal", "r2", "r4", "r7")

        @Test
        fun `ne against null selects the rest, in every spelling`() {
            assertSelects("null/not-equals/null-literal", "r1", "r3", "r5", "r6", "f1", "c1")
            assertSelects("null/equals/negated-null-literal", "r1", "r3", "r5", "r6", "f1", "c1")
            assertSelects("null/not-equals/null-literal-value-first", "r1", "r3", "r5", "r6", "f1", "c1")
        }

        @Test
        fun `an explicit-null attribute compares DEFINITELY against a non-null constant`() {
            // CEL holds a null VALUE there, so `null != "x"` is TRUE and the NULL rows are
            // allowed. Plain SQL answers UNKNOWN and drops them (#308).
            assertSelects("null/not-equals/explicit-null-against-literal", "r1", "r2", "r3", "r4", "r5", "r7", "f1", "c1")
            assertSelects("null/equals/negated-explicit-null-against-literal", "r1", "r2", "r3", "r4", "r5", "r7", "f1", "c1")
        }

        @Test
        fun `the same column under the OMITTED convention keeps its NULL rows excluded`() =
            // `aOptionalString` is the very column `owner` aliases, declaring nothing. A NULL there
            // sends no attribute, CEL raises a missing-attribute error, and check() denies — which
            // is what UNKNOWN reproduces. Declaring nothing must therefore NOT inherit the
            // call-level EXPLICIT default, or this returns r2, r4 and r7 as well.
            assertSelects("null/not-equals/missing-attribute-against-literal", "r1", "r3", "r5", "f1", "c1")

        @Test
        fun `two explicit-null columns are EQUAL when both are NULL`() =
            assertSelects("null/equals/field-to-field-both-explicit-null", "r2", "r7")

        @Test
        fun `mixing the two conventions across one comparison is refused, not guessed`() {
            val error = assertThrows<UnmappedAttributeException> { Scalars.ids("null/not-equals/field-to-field-mixed-null-conventions") }
            assertTrue(
                error.message!!.contains("between two columns under mixed null conventions"),
                error.message,
            )
        }
    }

    @Nested
    inner class Ordering {
        @Test
        fun `bare and negated orderings agree on the complement`() {
            assertSelects("comparison/greater-than/field-against-literal", "r2", "r5", "r6", "r7")
            assertSelects("logic/not/greater-than", "r1", "r3", "r4", "f1", "c1")
            assertSelects("comparison/less-or-equal/field-against-literal", "r1", "r2", "r3", "r4", "f1", "c1")
            assertSelects("logic/not/less-than", "r2", "r5", "r6", "r7")
        }

        @Test
        fun `a value-first ordering mirrors the operator`() {
            // `2 >= R.attr.aNumber` is `aNumber <= 2`. Inverting instead of mirroring is this
            // repository's canonical bug class.
            assertSelects("comparison/greater-or-equal/value-first", "r1", "r2", "r3", "r4", "f1", "c1")
            assertSelects("comparison/less-or-equal/value-first", "r5", "r6", "r7")
            assertSelects("comparison/less-than/value-first", "r2", "r5", "r6", "r7")
        }

        @Test
        fun `a negative constant is a constant, not an operator`() = assertSelects("comparison/less-than/negative-literal", "r4")

        @Test
        fun `a fractional constant against an integer column compares in double space`() =
            assertSelects("comparison/greater-or-equal/fractional-threshold", "r2", "r5", "r6", "r7")

        @Test
        fun `a constant too large for an exact long stays a double`() {
            assertSelects("comparison/greater-than/double-below-int64-range", *Scalars.ALL.toTypedArray())
            assertSelects("comparison/less-than/double-below-int64-range")
        }
    }

    @Nested
    inner class Connectives {
        @Test
        fun `a bare boolean attribute is a condition in its own right`() =
            assertSelects("logic/bare-attribute/boolean", "r1", "r3", "r5", "r7", "f1")

        @Test
        fun `a disjunction of a bare boolean and a comparison`() =
            assertSelects("logic/or/at-root", "r1", "r3", "r4", "r5", "r7", "f1")

        @Test
        fun `De Morgan - the negated conjunction a DENY rule compiles to`() =
            assertSelects("logic/not/over-and", "r1", "r2", "r4", "r6", "c1")

        @Test
        fun `negations compose, so an even count is the identity and an odd one is not`() {
            assertSelects("logic/not/double-negation", "r1", "r3", "r5", "r7", "f1")
            assertSelects("logic/not/triple-negation", "r2", "r4", "r6", "c1")
        }
    }

    @Nested
    inner class TheResourceKey {
        @Test
        fun `the primary key is an ordinary filterable attribute`() {
            assertSelects("identifier/equals/literal", "f1")
            assertSelects("identifier/equals/field-to-field", "f1")
            assertSelects("identifier/not-equals/field-to-field", "r1", "r2", "r3", "r4", "r5", "r6", "r7", "c1")
        }
    }

    @Nested
    inner class Concatenation {
        @Test
        fun `a constant-plus-key concatenation lowers to the dialect's own operator`() =
            assertSelects("identifier/equals/concatenation", "f1")

        @Test
        fun `a value-first concatenation is solved for the key rather than emitted`() =
            assertSelects("identifier/equals/concatenation-value-first", "f1")

        @Test
        fun `two text columns concatenate, and a NULL operand propagates`() =
            // Guessing arithmetic here is a hard error on PostgreSQL, 0 on SQLite, and on MySQL an
            // over-grant that matches almost every row (#391). The column types settle it.
            assertSelects("string/concatenate/field-to-field", "c1")
    }

    @Nested
    inner class Casts {
        @Test
        fun `string() over a boolean column lowers portably, not to a CAST`() =
            // SQLite and MySQL store a boolean as 1/0 and would render "1" where CEL and
            // PostgreSQL render "true" (#376); a CASE says the same thing on every engine.
            assertSelects("cast/string/from-boolean", "r1", "r3", "r5", "r7", "f1")

        @Test
        fun `string() over a floating-point column renders the shortest round-tripping decimal`() =
            assertSelects("cast/string/from-double", "r2")

        @Test
        fun `int() and double() are refused, in every position`() {
            listOf("cast/int/negative-fraction", "cast/int/malformed-string", "cast/double/malformed-string").forEach { action ->
                val error = assertThrows<UnsupportedPlanShapeException>(action) { Scalars.ids(action) }
                assertTrue(
                    error.message!!.contains("SQL CAST reads the numeric prefix"),
                    "$action: ${error.message}",
                )
            }
        }
    }

    @Nested
    inner class UnmappedAndUnsupported {
        @Test
        fun `an unmapped attribute is refused, never guessed from the reference`() {
            val error = assertThrows<UnmappedAttributeException> { Scalars.ids("comparison/equals/nested-map-member") }
            assertEquals("Unknown attribute: request.resource.attr.obj.inner", error.message)
        }
    }
}
