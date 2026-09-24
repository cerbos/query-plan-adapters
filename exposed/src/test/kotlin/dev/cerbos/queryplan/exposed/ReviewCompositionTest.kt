package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.JoinType
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.alias
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Review of what the emitted predicate assumes about the query the CALLER puts it in.
 *
 * Nothing tells the adapter which table the caller selects from: a mapping names columns, and
 * `RootScope.read` returns them BARE. Everything here is a property of that arrangement, and none
 * of it is a question the conformance harness asks — its own query is always
 * `<root>.selectAll().where(filter)`.
 *
 * KIND 2, not a corpus gap. The subject is the CALLER's query — the joins and aliases they wrapped
 * the predicate in — which a corpus action cannot vary, because the harness owns its own query and
 * writes exactly one shape of it. So nothing here is waiting on
 * [#414](https://github.com/cerbos/query-plan-adapters/issues/414); it is permanent.
 */
class ReviewCompositionTest {

    @Test
    fun `an alias the caller already uses does not collide with the adapter's own`() {
        // AliasAllocator numbers `cerbos_1`, `cerbos_2`, … so the obvious hazard is a caller whose
        // query already declares one. It is not a hazard: every adapter subquery declares its own
        // FROM alias, which shadows the outer one inside the subquery, and the correlation reaches
        // the ROOT table by name rather than through any alias. Pinned because "always aliased,
        // prefixed so it cannot collide" is a claim AliasAllocator's KDoc makes and nothing tested.
        val shadow = CompTags.alias("cerbos_1")
        val filter = translate(existsPublic())
        val ids = transaction(database) {
            CompDocs.join(shadow, JoinType.LEFT, onColumn = CompDocs.id, otherColumn = shadow[CompTags.resourceId])
                .select(CompDocs.id)
                .where(filter)
                .map { it[CompDocs.id] }
                .distinct()
                .sorted()
        }
        assertEquals(listOf("d1"), ids)
    }

    @Test
    fun `the predicate reads the root table by name, so an aliased root fails loudly`() {
        // A limitation worth stating in the README rather than a defect: the mapping never names
        // the root table, so the adapter cannot emit `<caller's alias>.id` and the reference it
        // does emit is out of scope. It is a LOUD failure on every store here — the statement does
        // not resolve — rather than a filter that quietly reads the wrong row, which is the right
        // direction, but a caller writing `Documents.alias("d")` gets a SQL error with no hint
        // that the adapter is the reason.
        val aliased = CompDocs.alias("d")
        val filter = translate(existsPublic())
        val error = runCatching {
            transaction(database) {
                aliased.select(aliased[CompDocs.id]).where(filter).map { it[aliased[CompDocs.id]] }
            }
        }.exceptionOrNull()
        assertTrue(error != null, "an aliased root table silently accepted a bare-table correlation")
    }

    @Test
    fun `the same relation entered twice gets two aliases, so neither captures the other`() {
        // `exists(tags, t, exists(tags, u, u.name == t.name))` — one table, two scans. An
        // unaliased inner scan would correlate against itself and match every row.
        val sql = render(translate(nestedSameRelation()))
        assertTrue(sql.contains("cerbos_1") && sql.contains("cerbos_2"), sql)
        assertEquals(
            Regex("REVIEW_COMP_TAGS").findAll(sql).count(),
            Regex("REVIEW_COMP_TAGS cerbos_\\d+").findAll(sql).count(),
            sql,
        )
    }

    @Test
    fun `alias numbering is deterministic across translations of the same plan`() {
        // The golden asset depends on it, and a per-JVM counter or a hash-ordered map would break
        // it in a way only a rerun shows.
        assertEquals(render(translate(nestedSameRelation())), render(translate(nestedSameRelation())))
    }

    companion object {
        object CompDocs : Table("review_comp_docs") {
            val id = varchar("id", 32)
            override val primaryKey = PrimaryKey(id)
        }

        object CompTags : Table("review_comp_tags") {
            val id = varchar("id", 32)
            val resourceId = varchar("resource_id", 32)
            val name = varchar("name", 64)
            override val primaryKey = PrimaryKey(id)
        }

        val MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.attr.tags" to
                many(CompTags, from = CompDocs.id, to = CompTags.resourceId, element = CompTags.name) {
                    "name" to CompTags.name
                }
        }

        /** `R.attr.tags.exists(t, t.name == "public")`. */
        fun existsPublic(): Operand = ReviewPlans.expression(
            "exists",
            ReviewPlans.variable("request.resource.attr.tags"),
            ReviewPlans.expression(
                "lambda",
                ReviewPlans.expression("eq", ReviewPlans.variable("t.name"), ReviewPlans.value("public")),
                ReviewPlans.variable("t"),
            ),
        )

        /** `R.attr.tags.exists(t, R.attr.tags.exists(u, u.name == t.name))`. */
        fun nestedSameRelation(): Operand = ReviewPlans.expression(
            "exists",
            ReviewPlans.variable("request.resource.attr.tags"),
            ReviewPlans.expression(
                "lambda",
                ReviewPlans.expression(
                    "exists",
                    ReviewPlans.variable("request.resource.attr.tags"),
                    ReviewPlans.expression(
                        "lambda",
                        ReviewPlans.expression(
                            "eq",
                            ReviewPlans.variable("u.name"),
                            ReviewPlans.variable("t.name"),
                        ),
                        ReviewPlans.variable("u"),
                    ),
                ),
                ReviewPlans.variable("t"),
            ),
        )

        private val database by lazy {
            val db = Database.connect("jdbc:h2:mem:review_comp;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(db) { seed() }
            db
        }

        fun translate(condition: Operand): Op<Boolean> =
            ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), Options.of(MAPPING)).toOp()

        fun render(op: Op<Boolean>): String = transaction(database) { op.toString() }

        private fun seed() {
            SchemaUtils.create(CompDocs, CompTags)
            listOf("d1", "d2").forEach { docId -> CompDocs.insert { it[id] = docId } }
            listOf(Triple("t1", "d1", "public"), Triple("t2", "d2", "private")).forEach { (tagId, doc, tagName) ->
                CompTags.insert {
                    it[id] = tagId
                    it[resourceId] = doc
                    it[name] = tagName
                }
            }
        }
    }
}
