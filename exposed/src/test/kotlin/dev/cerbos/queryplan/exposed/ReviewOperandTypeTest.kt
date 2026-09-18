package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.mysql.MySQLContainer

/**
 * Review of what happens when a plan constant's TYPE does not match the mapped column's kind.
 *
 * `ScalarColumnTypes` is described as "the adapter's one advantage over the plan": the column
 * carries the type the plan does not. `ConcatTranslator` uses it to keep `text + text` out of SQL
 * arithmetic, `ArithmeticTranslator` refuses arithmetic over a non-numeric column, and `string()`
 * refuses a column whose SQL rendering is not CEL's. The plain equality family consults it only to
 * decide whether to cast into double space, and never to refuse — so a numeric constant against a
 * text column is bound and compared, and the store decides what that means.
 */
class ReviewOperandTypeTest {

    @Test
    @Disabled("REVIEW FINDING 1: a numeric constant against a text column is emitted rather than refused, and MySQL's implicit coercion makes it match almost every row")
    @Tag("docker")
    fun `a numeric constant against a text column must not match on MySQL`() {
        // CEL: `R.attr.aString == 0`. Legal over a `dyn` attribute — and the shape a policy lands
        // in whenever a principal attribute that folds to a number is compared against a string
        // attribute, e.g. `R.attr.aString == P.attr.level`. CEL's `==` is heterogeneous-safe and
        // answers FALSE for every row.
        //
        // LeafTranslator.defaultLeaf (LeafTranslator.kt:123) emits `A_STRING = ?` with the
        // constant bound by the VALUE's type, so MySQL compares a string against a number by
        // coercing the STRING: 'abc' becomes 0 and `0 = 0` is TRUE. Every row whose text does not
        // start with a digit comes back — the same over-grant `conformance/README.md` records for
        // `concat-f2f` ("MySQL … `0 = 'oneset'` coerces the CONSTANT to 0 too … a silent
        // over-grant"), reached here without any `add` for ConcatTranslator to catch.
        //
        // Recommended fix: refuse in LeafTranslator when `ScalarColumnTypes.kindOf(target.column)`
        // is TEXT and the constant is a Number (or the converse) for the whole comparison family.
        // CEL decides those comparisons without looking at the column, so the refusal costs
        // nothing a policy can reach and closes a store-dependent over-grant.
        withMySql { ids ->
            assertEquals(emptyList<String>(), ids(numericAgainstText()))
        }
    }

    @Test
    fun `the same comparison is a loud failure on H2 rather than a silent over-grant`() {
        // Not a defect on this store, and the reason the four-store conformance run did not catch
        // it: H2 raises a conversion error, so the shape is loud here and silent on MySQL. The
        // corpus carries no action that compares a text attribute with a numeric constant.
        val error = runCatching { idsOnH2(numericAgainstText()) }.exceptionOrNull()
        assertEquals(true, error != null, "H2 accepted a numeric constant against a VARCHAR column")
    }

    @Test
    fun `the emitted predicate binds the constant by its own type, with no column-kind check`() {
        // The shape behind finding 1, pinned offline: one bare equality, a Long bound through
        // LongColumnType, and nothing that consults the column's TEXT kind.
        val rendered = OfflineRenderer.render(
            OfflineRenderer.translate {
                ExposedQueryPlanAdapter.toFilter(
                    ReviewPlans.conditional(numericAgainstText()),
                    Options.of(MAPPING),
                ).toOp()
            },
        )
        OfflineRenderer.DIALECTS.forEach { dialect ->
            val one = rendered.getValue(dialect)
            assertEquals(1, one.params.size, "$dialect: ${one.sql}")
            assertEquals("LongColumnType", one.params[0].type, "$dialect: ${one.sql}")
            assertEquals(0L, one.params[0].value, "$dialect: ${one.sql}")
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
         * Runs [body] against a throwaway MySQL server, pinned by `exposed/MYSQL_IMAGE` and
         * configured exactly as the conformance harness configures its own MySQL leg — a case- and
         * accent-sensitive collation, so nothing below can be blamed on MySQL's default.
         */
        fun withMySql(body: ((Operand) -> List<String>) -> Unit) {
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
                body { condition ->
                    transaction(database) {
                        TypedDocs.selectAll().where(op(condition)).map { it[TypedDocs.id] }.sorted()
                    }
                }
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
