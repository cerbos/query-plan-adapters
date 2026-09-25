package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * What a caller can ask of the adapter that no corpus case can state (`CLAUDE.md`, "What a
 * translator unit test may pin"): a caller-supplied argument the harness's one mapping cannot vary,
 * and plans the planner cannot produce. Plans for policy shapes come from the recorded goldens.
 */
class ContractTest {

    private val options: Options = Options.of(MAPPING)

    private fun filterFor(caseId: String, options: Options = this.options): QueryPlanFilter =
        OfflineRenderer.translate { ExposedQueryPlanAdapter.toFilter(Corpus.plan(caseId), options) }

    private fun sqlFor(caseId: String, options: Options = this.options): String =
        OfflineRenderer.renderOn(OfflineRenderer.POSTGRESQL, filterFor(caseId, options).toOp()).sql

    /** Kind 2: the harness translates every case with one mapping and one [Options]. */
    @Nested
    inner class ContractsTheCorpusCannotVary {

        /** `aOptionalString == null`, over a column the corpus sends as a missing attribute. */
        private val nullOnMissing = "null/equals/null-literal-on-missing-attribute"

        @Test
        fun `the call-level option decides a null operand the mapping declares nothing for`() {
            // The planner emits the same `eq(attr, null)` node whichever convention the caller
            // uses, so the adapter has to be TOLD (#302). Without a per-attribute declaration the
            // call-level option decides.
            val undeclared = options.withMapping(MAPPING_WITHOUT_NULL_CONVENTIONS)
            val explicit = undeclared.withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT)
            assertTrue(sqlFor(nullOnMissing, explicit).contains("IS NULL")) { sqlFor(nullOnMissing, explicit) }

            val omitted = undeclared.withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
            val error = assertThrows(UnsupportedPlanShapeException::class.java) { filterFor(nullOnMissing, omitted) }
            assertTrue(error.message.orEmpty().contains("null operand")) { error.message.orEmpty() }
        }

        @Test
        fun `a per-attribute declaration overrides the call-level option`() {
            // #308. `owner` declares EXPLICIT in the corpus mapping, so a null literal against it
            // still translates under a call-level OMITTED…
            val case = "null/equals/null-literal"
            val omitted = options.withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
            assertEquals(sqlFor(case), sqlFor(case, omitted))

            // …and `aOptionalString` declares OMITTED, so a call-level EXPLICIT does not reach it:
            // it keeps its UNKNOWN-when-NULL rendering rather than the bare `IS NULL` the same
            // plan gets when nothing is declared and the option says EXPLICIT.
            val explicit = options.withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT)
            assertEquals(sqlFor(nullOnMissing), sqlFor(nullOnMissing, explicit))
            assertNotEquals(
                sqlFor(nullOnMissing, explicit.withMapping(MAPPING_WITHOUT_NULL_CONVENTIONS)),
                sqlFor(nullOnMissing, explicit),
            )

            // Stripping the declaration rejects `owner` under the same option, so the override
            // above is doing work rather than being equivalent to the default.
            assertThrows(UnsupportedPlanShapeException::class.java) {
                filterFor(case, omitted.withMapping(MAPPING_WITHOUT_NULL_CONVENTIONS))
            }
        }

        @Test
        fun `a resolver function resolves the same references as a declared table`() {
            // A function resolver is asked for progressively shorter PREFIXES of a dotted
            // reference, which a declared table answers from its own keys. A shallow reference and
            // one three relation hops deep.
            val byRule = options.withMapping(AttributeResolver { MAPPING.resolve(it) })
            listOf("string/equals/case-sensitive", "collection/exists/nested-three-levels").forEach { case ->
                OfflineRenderer.DIALECTS.forEach { dialect ->
                    assertEquals(
                        OfflineRenderer.renderOn(dialect, filterFor(case).toOp()).sql,
                        OfflineRenderer.renderOn(dialect, filterFor(case, byRule).toOp()).sql,
                    ) { "$case ($dialect) resolves differently through a function mapper" }
                }
            }
        }

        @Test
        fun `maxMacroDepth refuses a nesting the default admits`() {
            // Each macro level multiplies the correlated subqueries, so the bound is a cost guard,
            // and a plan nested past it is refused rather than emitted at whatever size it is.
            val case = "collection/exists/nested-three-levels"
            val error = assertThrows(UnsupportedPlanShapeException::class.java) {
                filterFor(case, options.withMaxMacroDepth(1))
            }
            assertTrue(error.message.orEmpty().contains("past maxMacroDepth=1")) { error.message.orEmpty() }
            assertTrue(filterFor(case) is QueryPlanFilter.Conditional)
        }
    }

    /**
     * Kind 1: input validation on a public function, not policy shapes. A caller who hands the
     * adapter a hand-rolled or half-decoded plan gets an error rather than a filter.
     */
    @Nested
    inner class PlansThePlannerCannotProduce {

        @Test
        fun `an unrecognised plan kind`() {
            val plan = PlanResourcesFilter.newBuilder().setKindValue(4242).build()
            val error = assertThrows(MalformedPlanException::class.java) {
                ExposedQueryPlanAdapter.toFilter(plan, options)
            }
            assertTrue(error.message!!.contains("Unknown filter kind"), error.message)
        }

        @Test
        fun `a conditional plan with no condition`() {
            val plan = PlanResourcesFilter.newBuilder().setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL).build()
            val error = assertThrows(MalformedPlanException::class.java) {
                ExposedQueryPlanAdapter.toFilter(plan, options)
            }
            assertTrue(error.message!!.contains("Conditional plan has no condition"), error.message)
        }

        @Test
        fun `an operand with no node`() {
            val error = assertThrows(MalformedPlanException::class.java) {
                translate(expression("and", Operand.getDefaultInstance()))
            }
            assertTrue(error.message!!.contains("Plan operand has no node set"), error.message)
        }

        @Test
        fun `not with two operands`() {
            val bool = variable("request.resource.attr.aBool")
            val error = assertThrows(MalformedPlanException::class.java) {
                translate(expression("not", bool, bool))
            }
            assertTrue(error.message!!.contains("not requires exactly 1 operand, got 2"), error.message)
        }

        @Test
        fun `a protobuf value with no kind`() {
            val error = assertThrows(MalformedPlanException::class.java) {
                translate(
                    expression(
                        "eq",
                        variable("request.resource.attr.aString"),
                        Operand.newBuilder().setValue(Value.getDefaultInstance()).build(),
                    ),
                )
            }
            assertTrue(error.message!!.contains("Protobuf Value has no kind set"), error.message)
        }

        private fun translate(condition: Operand): QueryPlanFilter = ExposedQueryPlanAdapter.toFilter(
            PlanResourcesFilter.newBuilder()
                .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                .setCondition(condition)
                .build(),
            options,
        )

        private fun variable(name: String): Operand = Operand.newBuilder().setVariable(name).build()

        private fun expression(operator: String, vararg operands: Operand): Operand = Operand.newBuilder()
            .setExpression(Expression.newBuilder().setOperator(operator).addAllOperands(operands.toList()))
            .build()
    }
}
