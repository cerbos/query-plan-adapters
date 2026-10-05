package dev.cerbos.queryplan.exposed

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

/**
 * The pre-walk scan behind [NullAttributeRepresentation.OMITTED].
 *
 * The planner emits the SAME `eq(attr, null)` node under both conventions — `null-eq` and
 * `null-eq-missing` have byte-identical wire fixtures apart from the attribute name — so the wire
 * cannot say which one the caller uses and the adapter has to be told. Told wrongly, `IS NULL`
 * returns exactly the rows `check()` denies (#302).
 *
 * The call-level setting is a caller-supplied argument the corpus structurally cannot vary:
 * `actions.json` classifies each action against ONE mapping per adapter. That makes this the right
 * place for it, permanently.
 */
class NullOperandScanTest {

    private val omitted = Options.of(Scalars.MAPPING)
        .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)

    @Test
    fun `a null comparison under the omitted convention is refused before anything is built`() {
        val error = assertThrows<UnsupportedPlanShapeException> { Scalars.ids("null/equals/null-literal-on-missing-attribute", omitted) }
        assertTrue(
            error.message!!.contains("missing-attribute error"),
            error.message,
        )
    }

    @Test
    fun `the over-grant that refusal prevents is real, not hypothetical`() =
        // Under the default the same plan emits `IS NULL` and returns three rows. `check()` denies
        // every one of them, because a NULL column sends no attribute at all. That is the
        // divergence the option exists to close, and pinning it is what stops the test above from
        // passing for some unrelated reason.
        assertSelects("null/equals/null-literal-on-missing-attribute", "r2", "r4", "r7")

    @Test
    fun `a per-attribute declaration beats the call-level option`() {
        // `owner` declares EXPLICIT over the very column `aOptionalString` leaves undeclared, so
        // one call carries both conventions (#308) and the scan has to decide per attribute.
        assertDoesNotThrow { Scalars.ids("null/equals/null-literal", omitted) }
        assertSelects("null/equals/null-literal", omitted, "r2", "r4", "r7")
    }

    @Test
    fun `the declaration is consulted through an enclosing negation too`() =
        // The scan recurses, so `not(eq(owner, null))` still reaches the declaration rather than
        // stopping at the `not` node and falling back to the call-level option.
        assertSelects("null/equals/negated-null-literal", omitted, "r1", "r3", "r5", "r6", "f1", "c1")

    @Test
    fun `a plan with no null operand at all is unaffected`() {
        assertSelects("null/not-equals/missing-attribute-against-literal", omitted, "r1", "r3", "r5", "f1", "c1")
        assertSelects("string/equals/case-sensitive", omitted, "r1", "c1")
    }

    @Test
    fun `the scan matches the OPERAND, not an operator allowlist`() {
        // Deliberately wider than the shapes that over-grant today. Negation is applied around a
        // built predicate, so a leaf cannot know whether an enclosing `not` will flip it, and an
        // allowlist would have to be re-derived every time a new operator learns to see a null.
        listOf("null/not-equals/null-literal-value-first", "null/not-equals/null-literal").forEach { action ->
            // Both name `owner`, which declares EXPLICIT — so they translate…
            assertDoesNotThrow(action) { Scalars.ids(action, omitted) }
        }
        // …while the undeclared column is refused whichever way round the operands arrive.
        assertThrows<UnsupportedPlanShapeException> { Scalars.ids("null/equals/null-literal-on-missing-attribute", omitted) }
    }
}
