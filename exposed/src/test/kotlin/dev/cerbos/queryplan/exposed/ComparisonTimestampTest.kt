package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.dao.id.IdTable
import org.jetbrains.exposed.v1.datetime.timestamp as kotlinTimestamp
import org.jetbrains.exposed.v1.javatime.timestamp
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
    @OptIn(kotlin.time.ExperimentalTime::class)
    fun `a kotlin-datetime column is bound through its own column type too`() {
        // The third temporal representation, and the one that makes this class's central claim
        // checkable: NOTHING here references either datetime module, because `InstantColumnType` is
        // the abstract base in exposed-core that `exposed-java-time`'s `timestamp()` and
        // `exposed-kotlin-datetime`'s both extend. Only a column declared with the OTHER module can
        // show that one check really covers both, and the value bound is a `kotlin.time.Instant`
        // rather than a `java.time.Instant`, which is exactly the conversion the binder exists to
        // get right.
        val filter = ExposedQueryPlanAdapter.toFilter(
            Scalars.wireFixture("ts-eq"),
            Options.of(cerbosMapping { "request.resource.attr.createdAt" to KotlinTimes.at }),
        )
        val rendered = Scalars.rendered(filter.toOp())
        assertEquals(1, rendered.args.size)
        assertEquals(kotlin.time.Instant.parse("2024-06-01T00:00:00Z"), rendered.args.single())
    }

    @Test
    fun `a DAO id instant column is read through the same unwrap as every other path`() {
        // `TimestampBinder` used to read `column.columnType` raw, so an `EntityID`-wrapped or
        // `transform`ed instant was refused here while `ScalarColumnTypes` — the declared owner of
        // "the type that actually decides the SQL" — accepted it everywhere else. One column, two
        // answers, and the refusal named the WRAPPER type in its message.
        val filter = ExposedQueryPlanAdapter.toFilter(
            Scalars.wireFixture("ts-eq"),
            Options.of(cerbosMapping { "request.resource.attr.createdAt" to KeyedTimes.id }),
        )
        assertEquals(1, Scalars.rendered(filter.toOp()).args.size)
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

    /** Declared with `exposed-kotlin-datetime`, so its column type is the OTHER module's. */
    @OptIn(kotlin.time.ExperimentalTime::class)
    private object KotlinTimes : Table("scalar_kotlin_times") {
        val at = kotlinTimestamp("at")
    }

    /** An instant behind the `EntityID` wrapper a DAO id column puts in front of its type. */
    private object KeyedTimes : IdTable<java.time.Instant>("scalar_keyed_times") {
        override val id: Column<EntityID<java.time.Instant>> = timestamp("id").entityId()
        override val primaryKey = PrimaryKey(id)
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
