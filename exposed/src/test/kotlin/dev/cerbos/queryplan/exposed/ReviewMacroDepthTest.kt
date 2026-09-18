package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Review of [Options.maxMacroDepth], which `Options` documents as a cost guard: "Each level
 * multiplies the correlated subqueries the filter carries, so the bound is a cost guard, and a plan
 * nested past it is refused rather than emitted."
 *
 * `maxMacroDepth` is a caller-supplied argument the corpus structurally cannot vary, so nothing in
 * `conformance/` asks what it bounds.
 */
class ReviewMacroDepthTest {

    @Test
    fun `a macro over a literal collection counts towards the macro depth`() {
        // CEL: `["a","b","c"].exists(x, ["d","e","f"].exists(y, R.attr.tags.exists(t, t.name == x)))`
        // — three nested collection macros. The planner folds a macro over a list of 10 elements
        // or fewer itself, and ships the lambda with the folded list above that threshold, so a
        // principal attribute holding a longer list arrives in exactly this shape.
        //
        // CollectionTranslator.translate returned into foldKnownValues BEFORE
        // PlanWalker.enterMacro, so neither literal level was counted. At `maxMacroDepth = 1` the
        // plan was accepted and the emitted predicate carried 3 x 3 = 9 correlated subqueries; at
        // 11 elements a side it would have carried 121, and the bound never tripped whatever it
        // was set to. The fold is now wrapped in `enterMacro` like the relation branch, so it
        // costs a level.
        val plan = ReviewPlans.conditional(nestedFold())
        assertThrows<UnsupportedPlanShapeException> {
            ExposedQueryPlanAdapter.toFilter(plan, Options.of(MAPPING).withMaxMacroDepth(1))
        }
    }

    @Test
    fun `the fold really does multiply the subqueries, which is what the bound exists to stop`() {
        // The measurement behind finding 7, kept so the number stays on the record: two
        // three-element folds over one relation macro emit nine correlated subqueries.
        //
        // The review took this measurement at `maxMacroDepth = 1`, which the fix makes
        // unreachable — that plan is now exactly the three levels the case above refuses at 1 — so
        // it is taken at the bound those three levels need. Raising it is what keeps this a
        // measurement of the fold's cost rather than a second copy of the refusal.
        val op = buildOp(nestedFold(), Options.of(MAPPING).withMaxMacroDepth(3))
        assertEquals(9, Regex("NULLIF").findAll(render(op)).count(), render(op))
        // …and one level lower is the refusal, so the three levels really are what it costs.
        assertThrows<UnsupportedPlanShapeException> {
            buildOp(nestedFold(), Options.of(MAPPING).withMaxMacroDepth(2))
        }
    }

    @Test
    fun `a relation macro nested one level past the bound is refused, as the guard intends`() {
        // The control: the same nesting depth expressed over MAPPED relations is refused, so the
        // guard works exactly where the fold is not involved.
        val nested = ReviewPlans.expression(
            "exists",
            ReviewPlans.variable("request.resource.attr.categories"),
            ReviewPlans.expression(
                "lambda",
                ReviewPlans.expression(
                    "exists",
                    ReviewPlans.variable("c.subCategories"),
                    ReviewPlans.expression(
                        "lambda",
                        ReviewPlans.expression("eq", ReviewPlans.variable("s.name"), ReviewPlans.value("x")),
                        ReviewPlans.variable("s"),
                    ),
                ),
                ReviewPlans.variable("c"),
            ),
        )
        val error = assertThrows<UnsupportedPlanShapeException> {
            ExposedQueryPlanAdapter.toFilter(
                ReviewPlans.conditional(nested),
                Options.of(MAPPING).withMaxMacroDepth(1),
            )
        }
        assertTrue(error.message!!.contains("past maxMacroDepth=1"), error.message)
    }

    companion object {
        object DepthDocs : Table("review_depth_docs") {
            val id = varchar("id", 32)
            override val primaryKey = PrimaryKey(id)
        }

        object DepthTags : Table("review_depth_tags") {
            val id = varchar("id", 32)
            val resourceId = varchar("resource_id", 32)
            val name = varchar("name", 64)
            override val primaryKey = PrimaryKey(id)
        }

        object DepthCategories : Table("review_depth_categories") {
            val id = varchar("id", 32)
            val resourceId = varchar("resource_id", 32)
            override val primaryKey = PrimaryKey(id)
        }

        object DepthSubCategories : Table("review_depth_sub_categories") {
            val id = varchar("id", 32)
            val categoryId = varchar("category_id", 32)
            val name = varchar("name", 64)
            override val primaryKey = PrimaryKey(id)
        }

        val MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.attr.tags" to
                many(DepthTags, from = DepthDocs.id, to = DepthTags.resourceId, element = DepthTags.name) {
                    "name" to DepthTags.name
                }
            "request.resource.attr.categories" to
                many(DepthCategories, from = DepthDocs.id, to = DepthCategories.resourceId) {
                    "subCategories" to many(
                        DepthSubCategories,
                        from = DepthCategories.id,
                        to = DepthSubCategories.categoryId,
                    ) {
                        "name" to DepthSubCategories.name
                    }
                }
        }

        private val database by lazy {
            Database.connect("jdbc:h2:mem:review_depth;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
        }

        fun buildOp(condition: Operand, options: Options): Op<Boolean> =
            ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), options).toOp()

        fun render(op: Op<Boolean>): String = transaction(database) { op.toString() }

        /** `["a","b","c"].exists(x, ["d","e","f"].exists(y, R.attr.tags.exists(t, t.name == x)))`. */
        fun nestedFold(): Operand = ReviewPlans.expression(
            "exists",
            ReviewPlans.value(listOf("a", "b", "c")),
            ReviewPlans.expression(
                "lambda",
                ReviewPlans.expression(
                    "exists",
                    ReviewPlans.value(listOf("d", "e", "f")),
                    ReviewPlans.expression(
                        "lambda",
                        ReviewPlans.expression(
                            "exists",
                            ReviewPlans.variable("request.resource.attr.tags"),
                            ReviewPlans.expression(
                                "lambda",
                                ReviewPlans.expression(
                                    "eq",
                                    ReviewPlans.variable("t.name"),
                                    ReviewPlans.variable("x"),
                                ),
                                ReviewPlans.variable("t"),
                            ),
                        ),
                        ReviewPlans.variable("y"),
                    ),
                ),
                ReviewPlans.variable("x"),
            ),
        )
    }
}
