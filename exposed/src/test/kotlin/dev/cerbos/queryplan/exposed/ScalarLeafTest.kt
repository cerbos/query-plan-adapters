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
            assertSelects("cs-eq", "r1", "c1")

        @Test
        fun `an empty-string constant matches nothing rather than everything`() =
            assertSelects("empty-string-eq")

        @Test
        fun `a constant carrying astral-plane characters compares by code point`() =
            assertSelects("unicode-eq")

        @Test
        fun `a value-first inequality is mirrored, not inverted`() =
            assertSelects("vf-ne", "r2", "r3", "r4", "r5", "r6", "r7", "f1")
    }

    @Nested
    inner class StringMatching {
        @Test
        fun `contains, startsWith and endsWith are case-sensitive`() {
            assertSelects("cs-contains", "r1", "c1")
            assertSelects("cs-startswith", "r1", "c1")
            assertSelects("cs-endswith", "r1", "c1")
        }

        @Test
        fun `a percent in the needle is a literal, not a wildcard`() =
            // Unescaped, `LIKE '100%%'` would also return r6 ("100abc").
            assertSelects("like-percent", "r3")

        @Test
        fun `an underscore in the needle is a literal, not a single-character wildcard`() =
            // Unescaped, `LIKE '%a_b%'` would return r5 ("xaXby") against an empty CEL answer.
            assertSelects("like-underscore")

        @Test
        fun `a bracket in the needle is a literal, not a character class`() =
            assertSelects("like-bracket", "r4")

        @Test
        fun `a backslash in the needle is a literal, not an escape`() =
            assertSelects("like-backslash", "r7")

        @Test
        fun `a constant receiver keeps the column as the NEEDLE, never the haystack`() {
            // `"…".contains(R.attr.aString)` — swapping the two would test the constant for
            // membership in each row's string instead, which is a different question entirely.
            assertSelects("cr-contains", "r1", "r7", "c1")
            assertSelects("cr-startswith", "r5")
            assertSelects("cr-endswith", "r5")
        }
    }

    @Nested
    inner class ColumnNeedles {
        @Test
        fun `a column needle is escaped at query time`() {
            // r5's needle is a bare "%": unescaped it would match every row whose haystack is
            // present, which is the over-grant direction.
            assertSelects("f2f-contains", "r1", "r3")
            assertSelects("f2f-startswith", "r1", "r3")
            assertSelects("f2f-endswith", "r1")
        }

        @Test
        fun `a NULL needle stays UNKNOWN under negation, so its rows never leak`() {
            // The hazard of cerbos/query-plan-adapters#387: spelled as `needle IS NOT NULL AND
            // haystack LIKE …`, a NULL needle is a definite FALSE and NOT flips it to TRUE, so
            // r2, r4 and r7 would come back from a shape the PDP denies under both polarities.
            assertSelects("not-contains", "r5", "r6", "f1", "c1")
            assertSelects("not-startswith", "r5", "r6", "f1", "c1")
        }
    }

    @Nested
    inner class NullOperands {
        @Test
        fun `eq against null selects the NULL rows under the explicit convention`() =
            assertSelects("null-eq", "r2", "r4", "r7")

        @Test
        fun `ne against null selects the rest, in every spelling`() {
            assertSelects("null-ne", "r1", "r3", "r5", "r6", "f1", "c1")
            assertSelects("null-not-eq", "r1", "r3", "r5", "r6", "f1", "c1")
            assertSelects("vf-null-ne", "r1", "r3", "r5", "r6", "f1", "c1")
        }

        @Test
        fun `an explicit-null attribute compares DEFINITELY against a non-null constant`() {
            // CEL holds a null VALUE there, so `null != "x"` is TRUE and the NULL rows are
            // allowed. Plain SQL answers UNKNOWN and drops them (#308).
            assertSelects("null-value-ne-const", "r1", "r2", "r3", "r4", "r5", "r7", "f1", "c1")
            assertSelects("null-value-not-eq-const", "r1", "r2", "r3", "r4", "r5", "r7", "f1", "c1")
        }

        @Test
        fun `the same column under the OMITTED convention keeps its NULL rows excluded`() =
            // `aOptionalString` is the very column `owner` aliases, declaring nothing. A NULL there
            // sends no attribute, CEL raises a missing-attribute error, and check() denies — which
            // is what UNKNOWN reproduces. Declaring nothing must therefore NOT inherit the
            // call-level EXPLICIT default, or this returns r2, r4 and r7 as well.
            assertSelects("optional-ne", "r1", "r3", "r5", "f1", "c1")

        @Test
        fun `two explicit-null columns are EQUAL when both are NULL`() =
            assertSelects("null-value-f2f", "r2", "r7")

        @Test
        fun `mixing the two conventions across one comparison is refused, not guessed`() {
            val error = assertThrows<UnmappedAttributeException> { Scalars.ids("null-value-f2f-mixed") }
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
            assertSelects("gt-bare", "r2", "r5", "r6", "r7")
            assertSelects("not-gt", "r1", "r3", "r4", "f1", "c1")
            assertSelects("le-bare", "r1", "r2", "r3", "r4", "f1", "c1")
            assertSelects("not-lt", "r2", "r5", "r6", "r7")
        }

        @Test
        fun `a value-first ordering mirrors the operator`() {
            // `2 >= R.attr.aNumber` is `aNumber <= 2`. Inverting instead of mirroring is this
            // repository's canonical bug class.
            assertSelects("vf-ge", "r1", "r2", "r3", "r4", "f1", "c1")
            assertSelects("vf-le", "r5", "r6", "r7")
            assertSelects("vf-lt", "r2", "r5", "r6", "r7")
        }

        @Test
        fun `a negative constant is a constant, not an operator`() = assertSelects("neg-number", "r4")

        @Test
        fun `a fractional constant against an integer column compares in double space`() =
            assertSelects("double-threshold", "r2", "r5", "r6", "r7")

        @Test
        fun `a constant too large for an exact long stays a double`() {
            assertSelects("double-huge-gt", *Scalars.ALL.toTypedArray())
            assertSelects("double-huge-lt")
        }
    }

    @Nested
    inner class Connectives {
        @Test
        fun `a bare boolean attribute is a condition in its own right`() =
            assertSelects("root-bare-bool", "r1", "r3", "r5", "r7", "f1")

        @Test
        fun `a disjunction of a bare boolean and a comparison`() =
            assertSelects("root-or", "r1", "r3", "r4", "r5", "r7", "f1")

        @Test
        fun `De Morgan - the negated conjunction a DENY rule compiles to`() =
            assertSelects("not-and", "r1", "r2", "r4", "r6", "c1")

        @Test
        fun `negations compose, so an even count is the identity and an odd one is not`() {
            assertSelects("double-negation", "r1", "r3", "r5", "r7", "f1")
            assertSelects("triple-negation", "r2", "r4", "r6", "c1")
        }
    }

    @Nested
    inner class TheResourceKey {
        @Test
        fun `the primary key is an ordinary filterable attribute`() {
            assertSelects("id-eq-const", "f1")
            assertSelects("id-f2f", "f1")
            assertSelects("id-f2f-ne", "r1", "r2", "r3", "r4", "r5", "r6", "r7", "c1")
        }
    }

    @Nested
    inner class Concatenation {
        @Test
        fun `a constant-plus-key concatenation lowers to the dialect's own operator`() =
            assertSelects("id-concat", "f1")

        @Test
        fun `a value-first concatenation is solved for the key rather than emitted`() =
            assertSelects("id-concat-vf", "f1")

        @Test
        fun `two text columns concatenate, and a NULL operand propagates`() =
            // Guessing arithmetic here is a hard error on PostgreSQL, 0 on SQLite, and on MySQL an
            // over-grant that matches almost every row (#391). The column types settle it.
            assertSelects("concat-f2f", "c1")
    }

    @Nested
    inner class Casts {
        @Test
        fun `string() over a boolean column lowers portably, not to a CAST`() =
            // SQLite and MySQL store a boolean as 1/0 and would render "1" where CEL and
            // PostgreSQL render "true" (#376); a CASE says the same thing on every engine.
            assertSelects("cast-string-bool", "r1", "r3", "r5", "r7", "f1")

        @Test
        fun `string() over a floating-point column renders the shortest round-tripping decimal`() =
            assertSelects("cast-string-double", "r2")

        @Test
        fun `int() and double() are refused, in every position`() {
            listOf("cast-int-double", "cast-int-string", "cast-double-string").forEach { action ->
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
            val error = assertThrows<UnmappedAttributeException> { Scalars.ids("p-struct") }
            assertEquals("Unknown attribute: request.resource.attr.obj.inner", error.message)
        }

        @Test
        fun `matches is refused, because CEL's regex dialect is not any SQL engine's`() {
            listOf("p-matches", "matches-alt").forEach { action ->
                val error = assertThrows<UnsupportedPlanShapeException>(action) { Scalars.ids(action) }
                assertTrue(error.message!!.startsWith("Unsupported operator: matches"), error.message)
            }
        }
    }
}
