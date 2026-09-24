package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.longParam
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.mysql.MySQLContainer

/**
 * Review of what happens when a plan constant's TYPE does not match the mapped column's kind.
 *
 * `ScalarColumnTypes` is described as "the adapter's one advantage over the plan": the column
 * carries the type the plan does not. `ConcatTranslator` uses it to keep `text + text` out of SQL
 * arithmetic, `ArithmeticTranslator` refuses arithmetic over a non-numeric column, and `string()`
 * refuses a column whose SQL rendering is not CEL's. The plain equality family consulted it only to
 * decide whether to cast into double space, and never to refuse — so a numeric constant against a
 * text column was bound and compared, and the store decided what that meant.
 *
 * It now refuses. [ColumnTypeGuardTest] is the whole rule, across every operator and both operand
 * orders; what stays here is the store evidence that made the rule necessary.
 *
 * KIND 3 — a policy can reach these, and the corpus does not carry them yet. The corpus compares
 * every attribute against a value of its own type, so no action discriminates any of it; each case
 * names the CEL that reaches it and opens with *Corpus gap.* Delete them when the actions land
 * ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)) — the MySQL leg included,
 * because an oracle-compared action on the MySQL store says the same thing and says it of every
 * adapter.
 */
class ReviewOperandTypeTest {

    @Test
    @Tag("docker")
    fun `a numeric constant against a text column never reaches MySQL`() {
        // Corpus gap. CEL: `R.attr.aString == 0`. Legal over a `dyn` attribute — and the shape a policy lands
        // in whenever a principal attribute that folds to a number is compared against a string
        // attribute, e.g. `R.attr.aString == P.attr.level`. CEL's `==` is heterogeneous-safe and
        // answers FALSE for every row.
        //
        // LeafTranslator.defaultLeaf used to emit `A_STRING = ?` with the constant bound by the
        // VALUE's type, so MySQL compared a string against a number by coercing the STRING: 'abc'
        // becomes 0 and `0 = 0` is TRUE. Every row whose text does not start with a digit came
        // back — the same over-grant `conformance/README.md` records for `concat-f2f` ("MySQL …
        // `0 = 'oneset'` coerces the CONSTANT to 0 too … a silent over-grant"), reached here
        // without any `add` for ConcatTranslator to catch.
        //
        // The fix REFUSES rather than folding, which is why this asserts an exception where the
        // review asked for an empty id list: `Op.FALSE` would be right under the positive polarity
        // and wrong under a negation, where CEL denies a row whose attribute is MISSING; a
        // three-valued fold would then have to guess the null convention of an attribute the
        // policy never mentions. The reviewer's own recommendation was a refusal, and this is it.
        // The server leg stays because the coercion it demonstrates is the whole justification.
        withMySql { ids ->
            val error = assertThrows<UnmappedAttributeException> { ids(numericAgainstText()) }
            assertTrue(error.message!!.contains("maps to a VarCharColumnType column"), error.message)
            // …and the predicate the adapter USED to emit, built by hand here, still returns every
            // seeded row against this very server. The refusal is not theoretical.
            assertEquals(listOf("d1", "d2", "d3"), ids.raw(EqOp(TypedDocs.aString, longParam(0))))
        }
    }

    @Test
    fun `the same comparison is a loud failure on H2 rather than a silent over-grant`() {
        // Why the four-store conformance run did not catch this: H2 raised a conversion error, so
        // the shape was loud here and silent on MySQL. It is still a loud failure — now the
        // adapter's own named refusal, raised before a statement exists, on every store alike.
        val error = runCatching { idsOnH2(numericAgainstText()) }.exceptionOrNull()
        assertEquals(true, error != null, "H2 accepted a numeric constant against a VARCHAR column")
        assertEquals(UnmappedAttributeException::class, error!!::class)
    }

    @Test
    fun `a constant of the column's own type is still bound by the VALUE's type`() {
        // Corpus gap (the control for it). The control that keeps the fix honest. Binding by the value's type is what stops
        // `aNumber >= 1.5` becoming `>= 1`, so the kind check must reject the mismatched constant
        // WITHOUT reaching for the column's type to bind the matching one.
        val rendered = OfflineRenderer.render(
            OfflineRenderer.translate {
                ExposedQueryPlanAdapter.toFilter(
                    ReviewPlans.conditional(
                        ReviewPlans.expression(
                            "eq",
                            ReviewPlans.variable("request.resource.attr.aString"),
                            ReviewPlans.value("abc"),
                        ),
                    ),
                    Options.of(MAPPING),
                ).toOp()
            },
        )
        OfflineRenderer.DIALECTS.forEach { dialect ->
            val one = rendered.getValue(dialect)
            assertEquals(1, one.params.size, "$dialect: ${one.sql}")
            assertEquals("TextColumnType", one.params[0].type, "$dialect: ${one.sql}")
            assertEquals("abc", one.params[0].value, "$dialect: ${one.sql}")
        }
    }

    companion object {
        object TypedDocs : Table("review_typed_docs") {
            val id = varchar("id", 32)
            val aString = varchar("a_string", 64)
            override val primaryKey = PrimaryKey(id)
        }

        val MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.attr.aString" to TypedDocs.aString
        }

        /** `eq(R.attr.aString, 0)` — the wire shape of `R.attr.aString == 0`. */
        fun numericAgainstText(): Operand = ReviewPlans.expression(
            "eq",
            ReviewPlans.variable("request.resource.attr.aString"),
            ReviewPlans.value(0),
        )

        private val h2 by lazy {
            val db = Database.connect("jdbc:h2:mem:review_typed;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(db) { seed() }
            db
        }

        fun op(condition: Operand): Op<Boolean> =
            ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), Options.of(MAPPING)).toOp()

        fun idsOnH2(condition: Operand): List<String> = transaction(h2) {
            TypedDocs.selectAll().where(op(condition)).map { it[TypedDocs.id] }.sorted()
        }

        /**
         * The two questions the MySQL leg asks: what the ADAPTER does with a plan, and what the
         * server does with a predicate handed to it directly.
         */
        class MySqlQueries(private val database: Database) {
            /** Translates [condition] and runs it; the translation may refuse before any SQL. */
            operator fun invoke(condition: Operand): List<String> = raw(op(condition))

            /** Runs an already-built predicate, adapter-emitted or not. */
            fun raw(predicate: Op<Boolean>): List<String> = transaction(database) {
                TypedDocs.selectAll().where(predicate).map { it[TypedDocs.id] }.sorted()
            }
        }

        /**
         * Runs [body] against a throwaway MySQL server, pinned by `exposed/MYSQL_IMAGE` and
         * configured exactly as the conformance harness configures its own MySQL leg — a byte-exact
         * collation, so nothing below can be blamed on MySQL's default.
         */
        fun withMySql(body: (MySqlQueries) -> Unit) {
            // Skips without Docker rather than failing: the coercion it measures is a property of
            // a real MySQL server, and the refusal that protects against it is pinned offline in
            // `ColumnTypeGuardTest` either way.
            assumeTrue(dockerAvailable(), "Docker is not available")
            val container = MySQLContainer(DatabaseTestImages.MYSQL)
                .withCommand(
                    "--character-set-server=utf8mb4",
                    "--collation-server=${TestStore.DEFAULT_MYSQL_COLLATION}",
                )
            container.start()
            try {
                val database = Database.connect(
                    url = container.jdbcUrl,
                    driver = container.driverClassName,
                    user = container.username,
                    password = container.password,
                )
                transaction(database) { seed() }
                body(MySqlQueries(database))
            } finally {
                container.stop()
            }
        }

        /** `d1` and `d2` hold non-numeric text; `d3` holds text MySQL reads as a leading zero. */
        private fun seed() {
            SchemaUtils.create(TypedDocs)
            listOf("d1" to "abc", "d2" to "one", "d3" to "0x").forEach { (docId, text) ->
                TypedDocs.insert {
                    it[id] = docId
                    it[aString] = text
                }
            }
        }
    }
}
