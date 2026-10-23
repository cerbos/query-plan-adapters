package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.stringParam
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * The three-valued algebra, evaluated by a real database rather than asserted as SQL text.
 *
 * SQL's `NOT` is what makes this load-bearing: a CEL evaluation error is a DENY under BOTH
 * polarities, so its SQL counterpart has to be UNKNOWN. `NOT UNKNOWN` is UNKNOWN and the row stays
 * out; `NOT FALSE` is TRUE and the row comes back — a row the PDP denies. Only a database can say
 * which of the two a composition actually produced, which is why every case here runs the predicate
 * and counts rows instead of matching on the emitted string.
 *
 * One seeded row gives three primitives: a known-TRUE predicate, a known-FALSE one, and an UNKNOWN
 * one (a comparison against its NULL column). The signature of UNKNOWN is a count of 0 for the
 * predicate AND 0 for its negation; FALSE flips to 1 under negation.
 *
 * H2 in memory, in process: no server, nothing started.
 */
class TriLogicTest {

    private object Rows : Table("tri_logic_rows") {
        val id = varchar("id", 32)
        val aString = varchar("a_string", 32)
        val aOptionalString = varchar("a_optional_string", 32).nullable()
        override val primaryKey = PrimaryKey(id)
    }

    companion object {
        private lateinit var database: Database

        @JvmStatic
        @BeforeAll
        fun seed() {
            database = Database.connect("jdbc:h2:mem:tri_logic;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(database) {
                SchemaUtils.create(Rows)
                Rows.insert {
                    it[id] = "seed"
                    it[aString] = "seed"
                    it[aOptionalString] = null
                }
            }
        }
    }

    /** 1 when the predicate is TRUE for the seeded row, 0 when it is FALSE **or** UNKNOWN. */
    private fun count(predicate: Op<Boolean>): Int =
        transaction(database) { Rows.selectAll().where { predicate }.count().toInt() }

    /** [count] of the same predicate under [TriLogic.not] — the other polarity. */
    private fun countNegated(predicate: Op<Boolean>): Int = count(TriLogic.not(predicate))

    /** TRUE for the seeded row. */
    private fun knownTrue(): Op<Boolean> = EqOp(Rows.aString, stringParam("seed"))

    /** FALSE for the seeded row. */
    private fun knownFalse(): Op<Boolean> = EqOp(Rows.aString, stringParam("something-else"))

    /** UNKNOWN for the seeded row: a comparison against its NULL column. */
    private fun unknownLeaf(): Op<Boolean> = EqOp(Rows.aOptionalString, stringParam("anything"))

    // -- the fixture itself ----------------------------------------------------------------------

    @Test
    fun `the primitives are what they claim to be`() {
        assertEquals(1, count(knownTrue()))
        assertEquals(0, count(knownFalse()))
        assertEquals(1, countNegated(knownFalse()))
        // The control for everything below: a comparison against a NULL column really is UNKNOWN,
        // not FALSE. If this ever reads 1 under negation, every guard here is guarding nothing.
        assertEquals(0, count(unknownLeaf()))
        assertEquals(0, countNegated(unknownLeaf()))
    }

    // -- unknown() -------------------------------------------------------------------------------

    @Test
    fun `the UNKNOWN constant is excluded under both polarities`() {
        assertEquals(0, count(TriLogic.unknown()))
        assertEquals(0, countNegated(TriLogic.unknown()))
    }

    @Test
    fun `UNKNOWN absorbs a conjunction and does not absorb a disjunction`() {
        assertEquals(0, count(TriLogic.and(knownTrue(), TriLogic.unknown())))
        assertEquals(0, countNegated(TriLogic.and(knownTrue(), TriLogic.unknown())))
        // FALSE AND UNKNOWN is FALSE: the one case where an UNKNOWN operand does not poison.
        assertEquals(0, count(TriLogic.and(knownFalse(), TriLogic.unknown())))
        assertEquals(1, countNegated(TriLogic.and(knownFalse(), TriLogic.unknown())))
        // TRUE OR UNKNOWN is TRUE, for the mirrored reason.
        assertEquals(1, count(TriLogic.or(knownTrue(), TriLogic.unknown())))
        assertEquals(0, count(TriLogic.or(knownFalse(), TriLogic.unknown())))
        assertEquals(0, countNegated(TriLogic.or(knownFalse(), TriLogic.unknown())))
    }

    // -- not() -----------------------------------------------------------------------------------

    @Test
    fun `negation flips the known values and composes`() {
        assertEquals(0, count(TriLogic.not(knownTrue())))
        assertEquals(1, count(TriLogic.not(knownFalse())))
        // Exposed's expression trees are immutable, so nothing collapses a double negation the way
        // Hibernate's stateful negation does; boolean algebra has to hold through the wrapper.
        assertEquals(1, count(TriLogic.not(TriLogic.not(knownTrue()))))
        assertEquals(0, count(TriLogic.not(TriLogic.not(knownFalse()))))
        assertEquals(0, count(TriLogic.not(TriLogic.not(TriLogic.not(knownTrue())))))
    }

    // -- and() / or() ----------------------------------------------------------------------------

    @Test
    fun `a single-element junction is the element`() {
        assertEquals(1, count(TriLogic.and(knownTrue())))
        assertEquals(0, count(TriLogic.and(knownFalse())))
        assertEquals(1, count(TriLogic.or(knownTrue())))
        assertEquals(0, count(TriLogic.or(unknownLeaf())))
        assertEquals(0, countNegated(TriLogic.or(unknownLeaf())))
    }

    @Test
    fun `an empty junction has no SQL form and is refused`() {
        // SQL has no empty conjunction, and the two plausible constants mean opposite things: an
        // empty AND folded to TRUE grants everything. Folding belongs to the caller, who knows
        // which it is.
        assertThrows(IllegalArgumentException::class.java) { TriLogic.and(emptyList()) }
        assertThrows(IllegalArgumentException::class.java) { TriLogic.or(emptyList()) }
    }

    // -- determined() ----------------------------------------------------------------------------

    @Test
    fun `determined is TRUE for a two-valued body and UNKNOWN for an UNKNOWN one`() {
        assertEquals(1, count(TriLogic.determined(knownTrue())))
        assertEquals(1, count(TriLogic.determined(knownFalse())))
        assertEquals(0, count(TriLogic.determined(unknownLeaf())))
        assertEquals(0, countNegated(TriLogic.determined(unknownLeaf())))
    }

    // -- ternary() -------------------------------------------------------------------------------

    @Test
    fun `a ternary with a known condition is decided by the branch it selects`() {
        assertEquals(1, count(TriLogic.ternary(knownTrue(), knownTrue(), knownFalse())))
        assertEquals(0, count(TriLogic.ternary(knownTrue(), knownFalse(), knownTrue())))
        assertEquals(1, countNegated(TriLogic.ternary(knownTrue(), knownFalse(), knownTrue())))
        assertEquals(1, count(TriLogic.ternary(knownFalse(), knownFalse(), knownTrue())))
        assertEquals(0, count(TriLogic.ternary(knownFalse(), knownTrue(), knownFalse())))
        assertEquals(1, countNegated(TriLogic.ternary(knownFalse(), knownTrue(), knownFalse())))
    }

    @Test
    fun `a ternary with an UNKNOWN condition is UNKNOWN, not FALSE`() {
        // Both branches TRUE, condition UNKNOWN. The two branch arms alone read FALSE and an
        // enclosing NOT would readmit the row; the third arm is what forces UNKNOWN.
        val ternary = TriLogic.ternary(unknownLeaf(), knownTrue(), knownTrue())
        assertEquals(0, count(ternary))
        assertEquals(0, countNegated(ternary))
    }

    // -- baseUnlessUnknown() ---------------------------------------------------------------------

    @Test
    fun `baseUnlessUnknown passes the base through when the witness is FALSE`() {
        assertEquals(1, count(TriLogic.baseUnlessUnknown(knownTrue(), knownFalse())))
        assertEquals(0, countNegated(TriLogic.baseUnlessUnknown(knownTrue(), knownFalse())))
        assertEquals(0, count(TriLogic.baseUnlessUnknown(knownFalse(), knownFalse())))
        assertEquals(1, countNegated(TriLogic.baseUnlessUnknown(knownFalse(), knownFalse())))
    }

    @Test
    fun `a TRUE witness poisons the result whatever the base says`() {
        // Strict, with no absorption: this is the case a plain AND would get wrong. A FALSE base
        // with a TRUE witness must be UNKNOWN, or NOT(...) returns exactly the rows the PDP denies.
        assertEquals(0, count(TriLogic.baseUnlessUnknown(knownFalse(), knownTrue())))
        assertEquals(0, countNegated(TriLogic.baseUnlessUnknown(knownFalse(), knownTrue())))
        assertEquals(0, count(TriLogic.baseUnlessUnknown(knownTrue(), knownTrue())))
        assertEquals(0, countNegated(TriLogic.baseUnlessUnknown(knownTrue(), knownTrue())))
    }

    @Test
    fun `an UNKNOWN witness poisons as well, because the guard is strict rather than absorbing`() {
        // `(base AND NOT witness) OR (witness AND UNKNOWN)` leaves BOTH arms UNKNOWN when the
        // witness is, whatever the base says. That is the safe direction and worth pinning: the
        // witness exists to say "CEL would have raised here", and a witness that cannot say whether
        // it holds is no reassurance that CEL would not have.
        assertEquals(0, count(TriLogic.baseUnlessUnknown(knownTrue(), unknownLeaf())))
        assertEquals(0, countNegated(TriLogic.baseUnlessUnknown(knownTrue(), unknownLeaf())))
        assertEquals(0, count(TriLogic.baseUnlessUnknown(knownFalse(), unknownLeaf())))
        assertEquals(0, countNegated(TriLogic.baseUnlessUnknown(knownFalse(), unknownLeaf())))
    }

    // -- the algebra renders on every dialect ------------------------------------------------------

    @Test
    fun `every composition renders under every dialect`() {
        // UnknownOp spells SQL UNKNOWN as a comparison against a NULL literal rather than a bound
        // parameter, because PostgreSQL cannot type a bare placeholder in `? IS NULL`. Rendering it
        // under all four is what says the spelling needs no dialect knowledge.
        val composed = TriLogic.or(
            TriLogic.ternary(unknownLeaf(), knownTrue(), knownFalse()),
            TriLogic.baseUnlessUnknown(knownFalse(), TriLogic.determined(knownTrue())),
        )
        OfflineRenderer.render(composed).forEach { (dialect, rendered) ->
            assertEquals(emptyList<RenderedParam>(), rendered.params.filter { it.value == null }, dialect)
            assertEquals(
                true,
                rendered.sql.contains("1 = NULL"),
                "$dialect must spell UNKNOWN as a NULL literal: ${rendered.sql}",
            )
        }
    }
}
