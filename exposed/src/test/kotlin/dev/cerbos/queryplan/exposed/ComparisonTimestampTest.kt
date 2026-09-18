package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestampWithTimeZone
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * `timestamp()` comparisons, executed.
 *
 * Two things decide correctness and neither is visible in the emitted SQL: the literal has to be
 * parsed to the INSTANT it names — offsets normalised away, nanoseconds kept — and the constant
 * has to be bound through the mapped column's OWN column type, so the comparison reads the same
 * conversion the application's insert wrote.
 */
class ComparisonTimestampTest {

    @Test
    fun `a timestamp literal is compared as an instant`() {
        assertSelects("ts-eq", "r1", "r5", "c1")
        assertSelects("ts-ne", "r2", "r3", "r4", "r6", "r7", "f1")
    }

    @Test
    fun `an offset literal names the same instant as its UTC spelling`() =
        // `2024-06-01T02:00:00+02:00` IS `2024-06-01T00:00:00Z`; CEL timestamp equality is
        // equality of the absolute instant, so the offset must not survive into the comparison.
        assertSelects("ts-eq-offset", "r1", "r5", "c1")

    @Test
    fun `a column that does not name an absolute instant is refused, not guessed`() {
        // `p-timestamp` wraps an attribute the corpus maps to a text column. A wrong zone
        // assumption would silently include rows the PDP denies, so this fails closed with a
        // MAPPING error: the plan is fine, the mapping does not say enough.
        val error = assertThrows<UnmappedAttributeException> { Scalars.ids("p-timestamp") }
        assertTrue(
            error.message!!.contains("requires a column that stores an absolute instant"),
            error.message,
        )
    }

    @Test
    fun `an OffsetDateTime column is bound at UTC through its own column type`() {
        // A second temporal representation the caller may have declared. It is a caller-supplied
        // mapping the corpus cannot vary — actions.json classifies each action against ONE mapping
        // per adapter — so it is pinned here.
        val filter = ExposedQueryPlanAdapter.toFilter(
            Scalars.wireFixture("ts-eq"),
            Options.of(
                cerbosMapping {
                    "request.resource.attr.createdAt" to Zoned.at
                },
            ),
        )
        val rendered = Scalars.rendered(filter.toOp())
        assertEquals(1, rendered.args.size)
        assertEquals(
            java.time.OffsetDateTime.parse("2024-06-01T00:00:00Z"),
            rendered.args.single(),
            "the instant is bound at UTC, whatever offset the literal was written in",
        )
    }

    @Test
    fun `a literal outside CEL's instant range is a malformed plan`() {
        // CEL's own timestamp() rejects these, so the planner cannot emit one: they are wire
        // contract violations rather than shapes this adapter declines to express.
        listOf("0000-01-01T00:00:00Z", "2024-06-01 00:00:00Z", "2024-06-01T00:00:00", "not-a-time")
            .forEach { literal ->
                val error = assertThrows<MalformedPlanException>(literal) {
                    ExposedQueryPlanAdapter.toFilter(timestampEquality(literal), Options.of(Scalars.MAPPING))
                }
                assertTrue(error.message!!.startsWith("timestamp() constant"), "$literal: ${error.message}")
            }
    }

    @Test
    fun `a lowercase designator and a nanosecond fraction both parse`() {
        // RFC 3339 allows a lowercase `t` and `z`, and Cerbos emits the folded now() window at
        // sub-millisecond precision — the precision the Python and TypeScript adapters have to
        // refuse and this one does not.
        listOf("2024-06-01t00:00:00z", "2024-06-01T00:00:00.000000000Z").forEach { literal ->
            assertEquals(
                listOf("c1", "r1", "r5"),
                Scalars.idsOf(
                    ExposedQueryPlanAdapter.toFilter(
                        timestampEquality(literal),
                        Options.of(Scalars.MAPPING),
                    ).toOp(),
                ),
                literal,
            )
        }
    }

    private object Zoned : Table("scalar_zoned") {
        val at = timestampWithTimeZone("at")
    }

    private companion object {
        /**
         * `timestamp(R.attr.createdAt) == timestamp("<literal>")`. Hand-built because the literal
         * is the subject: every corpus fixture carries one the planner already validated, so the
         * rejected spellings have no wire fixture to come from.
         */
        fun timestampEquality(literal: String): PlanResourcesFilter = PlanResourcesFilter.newBuilder()
            .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
            .setCondition(
                Operand.newBuilder().setExpression(
                    PlanResourcesFilter.Expression.newBuilder()
                        .setOperator("eq")
                        .addOperands(timestampOf(Operand.newBuilder().setVariable("request.resource.attr.createdAt")))
                        .addOperands(
                            timestampOf(
                                Operand.newBuilder()
                                    .setValue(Value.newBuilder().setStringValue(literal).build()),
                            ),
                        ),
                ),
            )
            .build()

        fun timestampOf(argument: Operand.Builder): Operand = Operand.newBuilder()
            .setExpression(
                PlanResourcesFilter.Expression.newBuilder()
                    .setOperator("timestamp")
                    .addOperands(argument),
            )
            .build()
    }
}
