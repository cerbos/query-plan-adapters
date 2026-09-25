package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

/**
 * The zero-denominator lowering against rows [Scalars] cannot hold.
 *
 * Every division fixture in the corpus divides a NON-NULLABLE integer column, and three quantities
 * that decide the lowering never vary there: a NULL numerator independently of a NULL denominator,
 * a denominator that is zero while the numerator is not (the infinity arms, as opposed to the NaN
 * an `x/x` fixture always lands on), and a denominator WRITTEN as a negative zero. The plans are the
 * corpus's own wire fixtures; what varies is the MAPPING — a caller-supplied argument
 * `conformance/actions.json` structurally cannot vary, since it classifies each action against one
 * mapping per adapter, so this is where nullable integer columns can be pointed at them. (A
 * floating-point denominator is refused outright: its zero may be `-0.0`, below.)
 *
 * `cr-div-other-column` is the fixture that separates numerator from denominator, and the composed
 * shapes (`cr-div-then-add`, `cr-div-then-add-ne`) are the ones that prove a NaN survives the
 * surrounding `+` rather than being lowered to SQL NULL.
 */
class DivisionLoweringTest {

    @Test
    fun `a zero denominator yields NaN or a signed infinity, and a NULL yields neither`() {
        // d2 is 0/0 — NaN, and every ordering against NaN is false. d3 and d4 are the infinity
        // arms no self-division fixture can reach: +Infinity is allowed and -Infinity denied, so
        // an adapter that treated every zero denominator as "excluded" would lose d3. d5, d6 and
        // d7 are the CEL missing-attribute denies, one per side and both at once.
        assertDivides("arithmetic/divide/field-by-field", "d1", "d3")
    }

    @Test
    fun `a NaN survives arithmetic composed on the division`() {
        // `n/n + 1.0 > 1.0` and the same sum `!= 2.0`. The inequality is the discriminator: CEL's
        // `NaN != 2.0` is TRUE, so the zero row is the only row allowed — while a division lowered
        // to SQL NULL gives `NULL + 1 <> 2`, which is UNKNOWN, and returns nothing at all.
        assertDivides("arithmetic/add/self-division-plus-constant-greater-than", "d1", "d3", "d4", "d6", "d9")
        assertDivides("arithmetic/add/self-division-plus-constant-not-equals", "d2")
    }

    @Test
    fun `a NULL operand is excluded under BOTH polarities`() {
        // A CEL evaluation error denies whichever way the policy is written, so the predicate has
        // to be UNKNOWN for these rows rather than FALSE: `NOT FALSE` is TRUE and would readmit
        // them. The direct shapes and the composed ones reach it through the same arms.
        listOf("arithmetic/divide/field-by-field", "arithmetic/add/self-division-plus-constant-greater-than", "arithmetic/add/self-division-plus-constant-not-equals", "arithmetic/divide/self-division-greater-than", "arithmetic/divide/negated-self-division-equals")
            .forEach { action ->
                val op = translate(action)
                // An `x/x` shape never reads the denominator column, so d6 is legitimately decided
                // for it; only `cr-div-other-column` reads both sides.
                val unreadable =
                    if (action == "arithmetic/divide/field-by-field") setOf("d5", "d6", "d7") else setOf("d5", "d7")
                assertEquals(
                    emptySet<String>(),
                    unreadable.intersect((idsOf(op) + idsOf(TriLogic.not(op))).toSet()),
                    "$action decided a row whose operand is NULL",
                )
            }
    }

    @Test
    fun `a floating-point COLUMN denominator is refused, since its zero may be negative`() {
        // IEEE-754 keeps the sign of a zero, so CEL reads `2.0 / -0.0` as -Infinity and DENIES it
        // under `> 0.0`, where `2.0 / 0.0` is +Infinity and allows it. SQL compares -0.0 equal to
        // 0.0 and no portable function reads the sign bit, so a floating-point denominator is
        // refused (cerbos/query-plan-adapters#312). A CONSTANT denominator does carry its sign on
        // the wire and IS honoured — `ArithmeticTranslatorTest` pins that half.
        val floating = cerbosMapping {
            "request.resource.attr.aNumber" to Floats.numerator
            "request.resource.attr.aDouble" to Floats.denominator
        }
        assertThrows(UnsupportedPlanShapeException::class.java) {
            ExposedQueryPlanAdapter.toFilter(Scalars.wireFixture("arithmetic/divide/field-by-field"), Options.of(floating))
        }
    }

    private fun assertDivides(action: String, vararg expected: String) =
        assertEquals(expected.toList().sorted(), idsOf(translate(action)), action)

    private fun translate(action: String): Op<Boolean> = onH2 {
        ExposedQueryPlanAdapter.toFilter(Scalars.wireFixture(action), Options.of(MAPPING)).toOp()
    }

    private fun idsOf(op: Op<Boolean>): List<String> = onH2 {
        Divisions.selectAll().where { op }.map { it[Divisions.id] }.sorted()
    }

    private object Divisions : Table("division_docs") {
        val id = varchar("id", 32)

        /** Nullable on BOTH sides, which is what makes the two missing-attribute denies separable. */
        val numerator = integer("numerator").nullable()
        val denominator = integer("denominator").nullable()
        override val primaryKey = PrimaryKey(id)
    }

    /** Never created: the refusal happens at translation. */
    private object Floats : Table("division_floats") {
        val numerator = double("numerator").nullable()
        val denominator = double("denominator").nullable()
    }

    private companion object {
        val MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.attr.aNumber" to Divisions.numerator
            "request.resource.attr.aDouble" to Divisions.denominator
        }

        /**
         * One row per case the lowering distinguishes, written as a numerator/denominator pair so
         * each expectation is derivable from CEL's own rules rather than from the emitted SQL:
         * `0/0` is NaN, a non-zero over a zero is a signed infinity, and either operand absent is
         * an error.
         */
        val ROWS = listOf<Triple<String, Int?, Int?>>(
            Triple("d1", 6, 3),
            Triple("d2", 0, 0),
            Triple("d3", 2, 0),
            Triple("d4", -2, 0),
            Triple("d5", null, 3),
            Triple("d6", 2, null),
            Triple("d7", null, null),
            Triple("d9", -4, 2),
        )

        val database: Database by lazy {
            Database.connect("jdbc:h2:mem:cerbos_division;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
        }

        var seeded = false

        /** Seeded in a transaction of its own, so a body that throws cannot roll the rows back. */
        fun <T> onH2(body: () -> T): T {
            if (!seeded) {
                transaction(database) {
                    SchemaUtils.create(Divisions)
                    ROWS.forEach { (rowId, numerator, denominator) ->
                        Divisions.insert {
                            it[id] = rowId
                            it[Divisions.numerator] = numerator
                            it[Divisions.denominator] = denominator
                        }
                    }
                }
                seeded = true
            }
            return transaction(database) { body() }
        }
    }
}
