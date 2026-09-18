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
import org.junit.jupiter.api.Test

/**
 * Review of the empty-list short circuits in [MembershipTranslator].
 *
 * Three of them answer `Op.FALSE` before any subquery is built. An empty constant list is not a
 * policy-authoring mistake: it is what `P.attr.<something>` folds to for a principal who happens to
 * hold none of whatever the list enumerates, so the shape arrives for SOME principals and not
 * others. `ReviewPlannerShapeTest` records that the pinned PDP ships
 * `not(hasIntersection(variable, []))` rather than folding it away.
 *
 * KIND 3 — a policy can reach these, and the corpus does not carry them yet. Each case names the
 * CEL that reaches it; the plans are hand-built because there is no fixture to read. Delete these
 * when the corpus actions land (https://github.com/cerbos/query-plan-adapters/issues/414).
 */
class ReviewEmptyListTest {

    @Test
    fun `an empty intersection over a chain must still deny a row with no parent`() {
        // CEL: `!hasIntersection(R.attr.mainCategory.subNames, P.attr.allowedTeams)` for a
        // principal whose `allowedTeams` is empty. `w1-not-hasint-chain` is the same shape with a
        // NON-empty list, and it is oracle-compared; nothing in the corpus carries the empty one.
        //
        // MembershipTranslator.collectionContainsAny returned `Op.FALSE` before calling
        // Subqueries.chainContains, which is the only place the leading-hop guard lives.
        // `NOT FALSE` is TRUE, so d3 — which has no category at all, and whose `check()` is a
        // missing-path deny — came back.
        //
        // The empty list is now a never-matching BODY handed to the ordinary `chainContains`, so
        // the existing guard applies to it like any other body. `projectionIntersects` dropped its
        // own short circuit for the same reason — its `present.isEmpty()` arm reaches the same
        // constant from INSIDE the NULL-witness guard — and `scalarIsAnyOf` carries the member's
        // own `IS NULL` witness, even though the planner folds `x in []` away before it
        // (`<always-allowed>` in ReviewPlannerShapeTest).
        assertEquals(listOf("d1", "d2"), idsOf(translate(notEmptyIntersection())))
    }

    @Test
    fun `the same shape with a non-empty list guards the parentless row, as the corpus pins`() {
        // The control: one element in the list is enough to route through chainContains, and d3 is
        // then correctly excluded under the negation. d1 intersects and so is denied by the
        // negation; d2 has a category with no sub-categories, which is a determined FALSE the
        // negation allows. Only the EMPTY list skips the guard.
        assertEquals(listOf("d2"), idsOf(translate(notIntersectionWith("gold"))))
    }

    companion object {
        object EmptyDocs : Table("review_empty_docs") {
            val id = varchar("id", 32)
            override val primaryKey = PrimaryKey(id)
        }

        object EmptyCategories : Table("review_empty_categories") {
            val id = varchar("id", 32)
            val resourceId = varchar("resource_id", 32)
            override val primaryKey = PrimaryKey(id)
        }

        object EmptySubCategories : Table("review_empty_sub_categories") {
            val id = varchar("id", 32)
            val categoryId = varchar("category_id", 32)
            val name = varchar("name", 64)
            override val primaryKey = PrimaryKey(id)
        }

        /** The corpus's `mainCategory.subNames` shape: a chain whose leading hop can be absent. */
        val MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.attr.mainCategory" to
                many(EmptyCategories, from = EmptyDocs.id, to = EmptyCategories.resourceId) {
                    "subNames" to many(
                        EmptySubCategories,
                        from = EmptyCategories.id,
                        to = EmptySubCategories.categoryId,
                        element = EmptySubCategories.name,
                    )
                }
        }

        fun notEmptyIntersection(): Operand = ReviewPlans.expression(
            "not",
            ReviewPlans.expression(
                "hasIntersection",
                ReviewPlans.variable("request.resource.attr.mainCategory.subNames"),
                ReviewPlans.value(emptyList<String>()),
            ),
        )

        fun notIntersectionWith(element: String): Operand = ReviewPlans.expression(
            "not",
            ReviewPlans.expression(
                "hasIntersection",
                ReviewPlans.variable("request.resource.attr.mainCategory.subNames"),
                ReviewPlans.value(listOf(element)),
            ),
        )

        private val database by lazy {
            val db = Database.connect("jdbc:h2:mem:review_empty;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(db) { seed() }
            db
        }

        fun translate(condition: Operand): Op<Boolean> =
            ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), Options.of(MAPPING)).toOp()

        fun idsOf(op: Op<Boolean>): List<String> = transaction(database) {
            EmptyDocs.selectAll().where(op).map { it[EmptyDocs.id] }.sorted()
        }

        /**
         * | id | category | sub names | `!hasIntersection(subNames, [])` under CEL |
         * |----|----------|-----------|--------------------------------------------|
         * | d1 | c1       | ["gold"]  | TRUE                                       |
         * | d2 | c2       | []        | TRUE                                       |
         * | d3 | none     | —         | missing path -> DENY                       |
         */
        private fun seed() {
            SchemaUtils.create(EmptyDocs, EmptyCategories, EmptySubCategories)
            listOf("d1", "d2", "d3").forEach { docId -> EmptyDocs.insert { it[id] = docId } }
            listOf("c1" to "d1", "c2" to "d2").forEach { (catId, doc) ->
                EmptyCategories.insert {
                    it[id] = catId
                    it[resourceId] = doc
                }
            }
            EmptySubCategories.insert {
                it[id] = "s1"
                it[categoryId] = "c1"
                it[name] = "gold"
            }
        }
    }
}
