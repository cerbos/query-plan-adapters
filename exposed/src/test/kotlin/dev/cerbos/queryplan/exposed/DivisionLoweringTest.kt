package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
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
 * mapping per adapter, so this is where a nullable double column can be pointed at them.
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
        assertDivides("cr-div-other-column", "d1", "d3", "d8")
    }

    @Test
    fun `a NaN survives arithmetic composed on the division`() {
        // `n/n + 1.0 > 1.0` and the same sum `!= 2.0`. The inequality is the discriminator: CEL's
        // `NaN != 2.0` is TRUE, so the zero row is the only row allowed — while a division lowered
        // to SQL NULL gives `NULL + 1 <> 2`, which is UNKNOWN, and returns nothing at all.
        assertDivides("cr-div-then-add", "d1", "d3", "d4", "d6", "d8", "d9")
        assertDivides("cr-div-then-add-ne", "d2")
    }

    @Test
    fun `a NULL operand is excluded under BOTH polarities`() {
        // A CEL evaluation error denies whichever way the policy is written, so the predicate has
        // to be UNKNOWN for these rows rather than FALSE: `NOT FALSE` is TRUE and would readmit
        // them. The direct shapes and the composed ones reach it through the same arms.
        listOf("cr-div-other-column", "cr-div-then-add", "cr-div-then-add-ne", "cr-div-zero", "cr-div-zero-eq-neg")
            .forEach { action ->
                val op = translate(action)
                // An `x/x` shape never reads the denominator column, so d6 is legitimately decided
                // for it; only `cr-div-other-column` reads both sides.
                val unreadable =
                    if (action == "cr-div-other-column") setOf("d5", "d6", "d7") else setOf("d5", "d7")
                assertEquals(
                    emptySet<String>(),
                    unreadable.intersect((idsOf(op) + idsOf(TriLogic.not(op))).toSet()),
                    "$action decided a row whose operand is NULL",
                )
            }
    }

    @Test
    fun `a negative zero in a COLUMN denominator is unrecoverable, which is the documented limit`() {
        // IEEE-754 keeps the sign of a zero, so CEL reads d8 as `2.0 / -0.0` = -Infinity and DENIES
        // it under `> 0.0`, where `2.0 / 0.0` is +Infinity and allows it. The adapter assumes the
        // POSITIVE reading for a column denominator (cerbos/query-plan-adapters#312), and this is
        // the measurement behind that assumption rather than a restatement of it: d8 is inserted
        // with a negative zero and comes back a positive one, so the sign is already gone before
        // any predicate could branch on it. Even on a store that keeps it, `= 0` matches both and
        // no portable function reads the sign bit. A CONSTANT denominator does carry its sign on
        // the wire and IS honoured — `ArithmeticTranslatorTest` pins that half.
        val stored = onH2 {
            Divisions.selectAll().single { it[Divisions.id] == "d8" }[Divisions.denominator]
        }
        assertEquals(0.0, stored, "the witness row holds a zero")
        assertTrue(
            stored != null && 1.0 / stored > 0.0,
            "the store returned a SIGNED zero, so the sign is readable after all and #312 needs revisiting",
        )
        assertTrue("d8" in idsOf(translate("cr-div-other-column")), "which is the positive reading, emitted")
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
        val numerator = double("numerator").nullable()
        val denominator = double("denominator").nullable()
        override val primaryKey = PrimaryKey(id)
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
        val ROWS = listOf(
            Triple("d1", 6.0, 3.0),
            Triple("d2", 0.0, 0.0),
            Triple("d3", 2.0, 0.0),
            Triple("d4", -2.0, 0.0),
            Triple("d5", null, 3.0),
            Triple("d6", 2.0, null),
            Triple("d7", null, null),
            Triple("d8", 2.0, -0.0),
            Triple("d9", -4.0, 2.0),
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
