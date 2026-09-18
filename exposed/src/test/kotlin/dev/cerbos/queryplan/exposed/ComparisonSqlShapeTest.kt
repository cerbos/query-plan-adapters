package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Rules over the EMITTED SQL that the corpus-wide translator suite does not state.
 *
 * Each of these is a property no single action's row set can state: what a cast target is, which
 * concatenation operator a dialect gets, what a refusal names. A row set that happens to agree
 * proves nothing about the next needle.
 *
 * It used to open with a hand-maintained list of "every corpus action this side translates", and
 * sweep that list for the ESCAPE clause, for bound NULLs and for the two null-guard spellings.
 * Nothing enforced the list — an action that started translating simply fell out of every sweep,
 * which is what happened to the two division-then-add actions — and
 * `ExposedTranslatorTest.WhatTheEmittedSqlContains` now sweeps the WHOLE corpus for the first two.
 * The list and the sweeps that depended on it are gone; what stayed is what that suite does not
 * ask, plus the one property below, restated so it needs no list at all.
 */
class ComparisonSqlShapeTest {

    @Test
    fun `IS NOT NULL has exactly two sources, and no corpus action reaches a third`() {
        // A presence test is the over-grant direction: emitted where the plan did not ask for one,
        // it readmits exactly the rows an omitted-convention comparison has to drop. It has two
        // legitimate origins and no third —
        //
        //  - the asymmetric expansion an attribute DECLARED explicit-null earns, which is what
        //    makes its equality definite (`null-ne`, `vf-null-ne`, `null-value-*`);
        //  - `x != null` itself, whose whole meaning is a presence test, and which the pre-walk
        //    scan has already admitted under the call-level convention (`rel-ne-null-hop`).
        //
        // Stated WITHOUT a list of actions, which is what let the old sweep go stale: a new action
        // cannot fall out of a property quantified over the corpus.
        Corpus.wireFixtureActions().forEach { action ->
            val plan = Corpus.planFromWireFixture(action).filter
            val filter = runCatching {
                OfflineRenderer.translate { ExposedQueryPlanAdapter.toFilter(plan, Options.of(MAPPING)) }
            }.getOrNull() as? QueryPlanFilter.Conditional ?: return@forEach
            if (!OfflineRenderer.renderOn(OfflineRenderer.H2, filter.op).sql.contains("IS NOT NULL")) return@forEach
            val declaresExplicitNull = variablesOf(plan.condition).any { reference ->
                (MAPPING.resolve(reference) as? AttributeMapping.Field)
                    ?.nullAttributeRepresentation == NullAttributeRepresentation.EXPLICIT
            }
            assertTrue(
                declaresExplicitNull || carriesNullLiteral(plan.condition),
                "$action emits IS NOT NULL with neither a declared explicit-null attribute nor a null literal",
            )
        }
    }

    /** Every attribute reference in a plan subtree. */
    private fun variablesOf(operand: Operand): List<String> = when (operand.nodeCase) {
        Operand.NodeCase.VARIABLE -> listOf(operand.variable)
        Operand.NodeCase.EXPRESSION -> operand.expression.operandsList.flatMap(::variablesOf)
        else -> emptyList()
    }

    /** Whether a plan subtree carries a null constant anywhere. */
    private fun carriesNullLiteral(operand: Operand): Boolean = when (operand.nodeCase) {
        Operand.NodeCase.VALUE -> operand.value.kindCase == Value.KindCase.NULL_VALUE
        Operand.NodeCase.EXPRESSION -> operand.expression.operandsList.any(::carriesNullLiteral)
        else -> false
    }

    @Test
    fun `a fractional comparison casts the column into binary floating point`() {
        // H2 and PostgreSQL spell it DOUBLE PRECISION; MySQL spells it DOUBLE and SQLite REAL,
        // which only a run against those servers can prove. What is provable here is that the
        // cast is emitted at all: `Expression.as(Double)` in most builders is a type marker that
        // renders no SQL and leaves the comparison in exact decimal.
        assertTrue(
            Scalars.rendered(Scalars.op("double-threshold")).sql.contains("CAST(SCALAR_DOCS.A_NUMBER AS DOUBLE PRECISION)"),
            Scalars.rendered(Scalars.op("double-threshold")).sql,
        )
        assertTrue(
            Scalars.rendered(Scalars.op("p-double-frac")).sql.contains("AS DOUBLE PRECISION"),
        )
        // An integral constant needs no cast: CEL and SQL agree on an integer comparison.
        assertTrue(!Scalars.rendered(Scalars.op("gt-bare")).sql.contains("CAST"))
    }

    @Test
    fun `concatenation renders the dialect's NULL-propagating operator`() {
        // PostgreSQL's CONCAT() skips a NULL argument, which would compare a partial string and
        // match rows the PDP denies; `||` propagates. MySQL is the other way round and is proved
        // by the MySQL store leg.
        assertTrue(Scalars.rendered(Scalars.op("concat-f2f")).sql.contains(" || "))
        assertTrue(Scalars.rendered(Scalars.op("id-concat")).sql.contains(" || "))
    }

    @Test
    fun `a constant is bound by the VALUE's type, never coerced through the column's`() {
        // Exposed's own `column eq value` binds through the column type: `1.5` against an integer
        // column becomes `1`, and the comparison then returns rows the PDP denies.
        val args = Scalars.rendered(Scalars.op("double-threshold")).args
        assertEquals(listOf<Any?>(1.5), args)
    }

    @Test
    fun `an operator the leaf does not know is refused rather than approximated`() {
        val unknown = PlanResourcesFilter.newBuilder()
            .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
            .setCondition(
                Operand.newBuilder().setExpression(
                    PlanResourcesFilter.Expression.newBuilder()
                        .setOperator("unsupported_op")
                        .addOperands(Operand.newBuilder().setVariable("request.resource.attr.aString"))
                        .addOperands(
                            Operand.newBuilder()
                                .setValue(Value.newBuilder().setStringValue("one").build()),
                        ),
                ),
            )
            .build()
        // CEL cannot reach this: an unknown function is an "undeclared reference" at compile time,
        // so no policy produces it. It is pinned because the fallback has to be a refusal rather
        // than a nearest-operator guess.
        val error = assertThrows<UnsupportedPlanShapeException> {
            ExposedQueryPlanAdapter.toFilter(unknown, Options.of(Scalars.MAPPING))
        }
        assertEquals("Unsupported operator: unsupported_op", error.message)
    }

    @Test
    fun `a list or map constant against a scalar column is refused, and its elements never leak`() {
        // CORPUS GAP. `R.attr.tags == ["a", "b"]` is policy-reachable and the corpus carries no
        // action for it: `map-eq-list` reaches the refusal through a map() projection instead, so
        // this exact shape is pinned in one adapter and asked of none of the others. Delete this
        // test when the corpus action lands (cerbos/query-plan-adapters#414).
        val listConstant = PlanResourcesFilter.newBuilder()
            .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
            .setCondition(
                Operand.newBuilder().setExpression(
                    PlanResourcesFilter.Expression.newBuilder()
                        .setOperator("eq")
                        .addOperands(Operand.newBuilder().setVariable("request.resource.attr.aString"))
                        .addOperands(
                            Operand.newBuilder().setValue(
                                Value.newBuilder().setListValue(
                                    com.google.protobuf.ListValue.newBuilder()
                                        .addValues(Value.newBuilder().setStringValue("secret-a"))
                                        .addValues(Value.newBuilder().setStringValue("secret-b")),
                                ).build(),
                            ),
                        ),
                ),
            )
            .build()
        val error = assertThrows<UnsupportedPlanShapeException> {
            ExposedQueryPlanAdapter.toFilter(listConstant, Options.of(Scalars.MAPPING))
        }
        assertEquals(
            "eq comparison against a list of 2 elements constant is not supported for attribute " +
                "request.resource.attr.aString. Whole-list equality is not translatable to a " +
                "scalar column comparison; map the attribute as a relation and use " +
                "in/hasIntersection, or compare elements individually.",
            error.message,
        )
        // A plan constant can carry a folded principal attribute, and an exception message is
        // logged, so the shape is reported and the values are not.
        assertTrue(!error.message!!.contains("secret"), error.message)
    }

    @Test
    fun `an index or projection in a leaf operand names what has no column shape`() {
        listOf("index-scalar-list" to "Cannot translate index()", "p-index" to "Cannot translate get-field()")
            .forEach { (action, lead) ->
                val error = assertThrows<UnsupportedPlanShapeException>(action) { Scalars.op(action) }
                assertTrue(error.message!!.startsWith(lead), "$action: ${error.message}")
            }
        val mapComparison = assertThrows<UnsupportedPlanShapeException> { Scalars.op("map-eq-list") }
        assertTrue(
            mapComparison.message!!.startsWith("Direct comparison of map(...) to a value is not supported"),
            mapComparison.message,
        )
    }

    @Test
    fun `a list-valued macro in condition position is refused at the root and one level down`() {
        listOf("filter-as-condition", "filter-as-conjunct", "map-as-condition").forEach { action ->
            val error = assertThrows<UnsupportedPlanShapeException>(action) { Scalars.op(action) }
            assertTrue(
                error.message!!.contains("returns a list, not a boolean"),
                "$action: ${error.message}",
            )
        }
    }
}
