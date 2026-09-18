package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * One shape, one refusal message, from all three places the walk meets `except()`.
 *
 * `except(list, list)` computes a list difference, and a row filter has no set-valued operand to
 * hold the right-hand side. The walk reaches that conclusion from three sites — the condition
 * position, a comparison operand, and inside `size()` — and each used to spell it out separately,
 * so two of the three said the same thing in different words. A caller routing an inexpressible
 * shape elsewhere matches on the type, but `conformance/actions.json` pins the TEXT, and a reader
 * comparing two spellings would conclude the adapter distinguishes cases it does not.
 *
 * The corpus carries no `except` action at all, which is why nothing else asks this.
 *
 * KIND 3 — a policy can reach these, and the corpus does not carry them yet. Each case names the
 * CEL that reaches it; the plans are hand-built because there is no fixture to read. Delete these
 * when the corpus actions land (https://github.com/cerbos/query-plan-adapters/issues/414).
 */
class ExceptRefusalTest {

    @Test
    fun `every site raises the identical message`() {
        // CEL, in order: `R.attr.tags.except(["x"])` as a whole condition — not a boolean, but the
        // walk names except() before it says so; `R.attr.tags.except(["x"]) == []` as a comparison
        // operand; and `size(R.attr.tags.except(["x"])) > 0`, the shape a real policy writes.
        val messages = listOf(
            "condition position" to except(),
            "comparison operand" to ReviewPlans.expression("eq", except(), ReviewPlans.value(listOf<String>())),
            "inside size()" to ReviewPlans.expression(
                "gt",
                ReviewPlans.expression("size", except()),
                ReviewPlans.value(0),
            ),
        ).map { (site, condition) ->
            site to assertThrows<UnsupportedPlanShapeException>(site) {
                ExposedQueryPlanAdapter.toFilter(
                    ReviewPlans.conditional(condition),
                    Options.of(ColumnTypeGuardTest.MAPPING),
                )
            }.message
        }

        val expected = "except() computes a list difference, which SQL cannot express as a row " +
            "filter; write the policy as size(x.filter(e, !(e in y))) instead"
        messages.forEach { (site, message) -> assertEquals(expected, message, site) }
    }

    /** `R.attr.tags.except(["x"])`. */
    private fun except(): Operand = ReviewPlans.expression(
        "except",
        ReviewPlans.variable("request.resource.attr.tagNames"),
        ReviewPlans.value(listOf("x")),
    )
}
