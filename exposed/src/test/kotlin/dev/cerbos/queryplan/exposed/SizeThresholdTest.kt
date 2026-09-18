package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.ObjectMapper
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Files
import java.nio.file.Path

/**
 * The threshold arithmetic behind `size(x) op N`, for the constants a rounding has no answer for.
 *
 * KIND 1 — the planner cannot produce these, and no corpus action can substitute. Two independent
 * reasons, both checked rather than asserted:
 *
 *  - CEL has no NaN literal, and the planner does NOT fold `div(0, 0)`. The corpus says so in
 *    prose (`conformance/policies/adversarial.yaml`, the constant NaN / ±Infinity section) and the
 *    `nan-ord-ternary` wire fixture shows it — the else arm arrives as an unfolded `div` subtree —
 *    which the first case below re-reads from the corpus rather than taking on trust.
 *  - `SizeTranslator.trySizeComparison` reads its threshold from a VALUE operand only, so an
 *    unfolded `div` never becomes one. `size(x) > 0.0/0.0` is refused further up, by the
 *    arithmetic lowering, with its own message.
 *
 * The same reasoning already covers the fractional `eq`/`ne` arms: CEL rejects `size(...) == 1.5`
 * with "found no matching overload for '_==_' applied to '(int, double)'", which the corpus
 * records, so no policy reaches those either.
 */
class SizeThresholdTest {

    @Test
    fun `the planner really does ship a zero division unfolded, which is why this is hand-built`() {
        // The evidence for KIND 1. If the planner ever starts folding `div(0, 0)` to a NaN
        // constant this fails, and the refusal below becomes a corpus action rather than a
        // permanently unreachable branch.
        val fixture = Path.of(
            System.getProperty("user.dir"),
            "..",
            "conformance",
            "wire-fixtures",
            "nan-ord-ternary.json",
        )
        val shipped = ObjectMapper().readTree(Files.readString(fixture)).toString()
        assertTrue(shipped.contains("\"operator\":\"div\""), shipped)
        assertTrue(!shipped.contains("NaN"), shipped)
    }

    @Test
    fun `a NaN threshold is refused for every operator and both operand orders`() {
        // `NaN == rint(NaN)` is false, so NaN fell into the fractional arms: `ceil(NaN).toLong()`
        // is 0, and `size(x) > NaN` became `size(x) >= 0` — true for every row, where CEL denies
        // every one of them.
        listOf("eq", "ne", "lt", "le", "gt", "ge").forEach { operator ->
            listOf(true, false).forEach { sizeFirst ->
                val size = ReviewPlans.expression("size", ReviewPlans.variable("request.resource.attr.aString"))
                val threshold = ReviewPlans.value(Double.NaN)
                val condition = if (sizeFirst) {
                    ReviewPlans.expression(operator, size, threshold)
                } else {
                    ReviewPlans.expression(operator, threshold, size)
                }
                val error = assertThrows<UnsupportedPlanShapeException>("$operator sizeFirst=$sizeFirst") {
                    translate(condition)
                }
                assertEquals(
                    "size() compared against a NaN threshold is not supported: a character count " +
                        "and an element count are both integral, and NaN has no integral bound to " +
                        "round to — rounding it is not a number and narrowing that yields 0, which " +
                        "makes every ordering against it a comparison the policy never wrote. CEL " +
                        "denies an ordering against NaN under both polarities, so no bound " +
                        "reproduces it.",
                    error.message,
                )
            }
        }
    }

    @Test
    fun `a NaN threshold over a relation is refused by the same rule`() {
        // The threshold is reduced before the `size()` argument is even resolved, so one guard
        // covers a character count and an element count alike.
        val error = assertThrows<UnsupportedPlanShapeException> {
            ExposedQueryPlanAdapter.toFilter(
                ReviewPlans.conditional(
                    ReviewPlans.expression(
                        "gt",
                        ReviewPlans.expression("size", ReviewPlans.variable("request.resource.attr.tagNames")),
                        ReviewPlans.value(Double.NaN),
                    ),
                ),
                Options.of(ColumnTypeGuardTest.MAPPING),
            )
        }
        assertTrue(error.message!!.startsWith("size() compared against a NaN threshold"), error.message)
    }

    @Test
    fun `an infinite threshold is decided, and decided the way IEEE orders it`() {
        // NOT refused with the NaN: the infinities are totally ordered, so every comparison
        // against one has a right answer and the out-of-range arms already give it. `> +Infinity`
        // is false for every string and `< +Infinity` is true for every string there IS.
        assertEquals(emptyList<String>(), idsOf(sizeAgainst("gt", Double.POSITIVE_INFINITY)))
        assertEquals(Scalars.ALL, idsOf(sizeAgainst("lt", Double.POSITIVE_INFINITY)))
        assertEquals(Scalars.ALL, idsOf(sizeAgainst("gt", Double.NEGATIVE_INFINITY)))
        assertEquals(emptyList<String>(), idsOf(sizeAgainst("lt", Double.NEGATIVE_INFINITY)))
    }

    @Test
    fun `an infinite threshold still excludes a NULL string under BOTH polarities`() {
        // The decided answer is not a bare constant: a NULL column is a missing attribute, CEL
        // raises and `check()` denies, so the `IS NULL` witness has to survive the fold — which is
        // the same property the huge-threshold corpus actions pin for a finite bound.
        val present = listOf("c1", "f1", "r1", "r3", "r5", "r6")
        assertEquals(present, idsOf(optionalSizeAgainst("lt", Double.POSITIVE_INFINITY)))
        assertEquals(
            emptyList<String>(),
            idsOf(ReviewPlans.expression("not", optionalSizeAgainst("lt", Double.POSITIVE_INFINITY))),
        )
    }

    private fun sizeAgainst(operator: String, threshold: Double): Operand = ReviewPlans.expression(
        operator,
        ReviewPlans.expression("size", ReviewPlans.variable("request.resource.attr.aString")),
        ReviewPlans.value(threshold),
    )

    private fun optionalSizeAgainst(operator: String, threshold: Double): Operand = ReviewPlans.expression(
        operator,
        ReviewPlans.expression("size", ReviewPlans.variable("request.resource.attr.aOptionalString")),
        ReviewPlans.value(threshold),
    )

    private fun translate(condition: Operand): Op<Boolean> =
        ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), Options.of(Scalars.MAPPING)).toOp()

    private fun idsOf(condition: Operand): List<String> = Scalars.idsOf(translate(condition))
}
