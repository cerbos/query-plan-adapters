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
 * ask, plus the two null-guard properties below, restated so they need no list at all.
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
        val reached = sweepFor("IS NOT NULL") { action, plan ->
            assertTrue(
                declaresExplicitNull(plan) || carriesNullLiteral(plan.condition),
                "$action emits IS NOT NULL with neither a declared explicit-null attribute nor a null literal",
            )
        }
        assertAntiVacuous("IS NOT NULL", reached, PRESENCE_TEST_FLOOR)
    }

    @Test
    fun `IS NULL appears only where a null operand, a declared convention or a named guard puts it`() {
        // The mirror property, and the one `3989257` retired with the stale list it was written
        // against. It is the same hazard read the other way: an `IS NULL` the plan never asked for
        // is an assumption about a column, and under a negation it hands back exactly the rows a
        // missing attribute makes check() deny. Four legitimate origins, and no fifth —
        //
        //  - a NULL OPERAND in the plan: the null-constant leaf, or a null list element;
        //  - an attribute DECLARED explicit-null, whose equality expands to a definite one and
        //    whose membership carries a null-element arm;
        //  - a COLUMN standing where a LIKE needle would otherwise be a constant, which
        //    `LikeEscaping.columnPattern` guards because a NULL needle must not match everything;
        //  - an operator whose own lowering is two-valued and therefore carries its own witness:
        //    `string()`'s portable CASE, `size()`'s out-of-int-range fold, and the `map()`
        //    projection's NULL-element subquery.
        //
        // Every disjunct names an operator or a declaration IN THE PLAN, never an action, so an
        // action added tomorrow is covered and one that reaches a fifth source fails here.
        val reached = sweepFor("IS NULL") { action, plan ->
            assertTrue(
                carriesNullLiteral(plan.condition) ||
                    declaresExplicitNull(plan) ||
                    hasColumnNeedle(plan.condition) ||
                    operatorsOf(plan.condition).any { it in SELF_GUARDING_OPERATORS },
                "$action emits IS NULL with no null operand, no declared explicit-null attribute, " +
                    "no column LIKE needle and none of $SELF_GUARDING_OPERATORS",
            )
        }
        assertAntiVacuous("IS NULL", reached, NULL_TEST_FLOOR)
    }

    /**
     * Runs [check] for every corpus action whose H2 rendering contains [needle], and returns how
     * many did — which is what the caller asserts against a floor.
     *
     * The refusal is narrowed to [IllegalArgumentException]: every classified refusal is one, and
     * anything else is an adapter BUG that a bare `getOrNull` would have swallowed as "this action
     * does not translate".
     */
    private fun sweepFor(needle: String, check: (String, PlanResourcesFilter) -> Unit): Int {
        var reached = 0
        Corpus.wireFixtureActions().forEach { action ->
            val plan = Corpus.planFromWireFixture(action).filter
            val filter = try {
                OfflineRenderer.translate { ExposedQueryPlanAdapter.toFilter(plan, Options.of(MAPPING)) }
            } catch (@Suppress("SwallowedException") refusal: IllegalArgumentException) {
                return@forEach
            }
            if (filter !is QueryPlanFilter.Conditional) return@forEach
            if (!OfflineRenderer.renderOn(OfflineRenderer.H2, filter.op).sql.contains(needle)) return@forEach
            reached++
            check(action, plan)
        }
        return reached
    }

    /**
     * Anti-vacuity for a sweep whose body only runs where the pattern appears: with none appearing,
     * every iteration returns early and the sweep passes having asserted nothing.
     */
    private fun assertAntiVacuous(needle: String, reached: Int, floor: Int) = assertTrue(
        reached > floor,
        "only $reached corpus actions emit $needle, so the property is near-vacuous",
    )

    /** Whether any attribute the plan names declares the explicit-null convention. */
    private fun declaresExplicitNull(plan: PlanResourcesFilter): Boolean =
        variablesOf(plan.condition).any { reference ->
            (MAPPING.resolve(reference) as? AttributeMapping.Field)
                ?.nullAttributeRepresentation == NullAttributeRepresentation.EXPLICIT
        }

    /** Every attribute reference in a plan subtree. */
    private fun variablesOf(operand: Operand): List<String> = when (operand.nodeCase) {
        Operand.NodeCase.VARIABLE -> listOf(operand.variable)
        Operand.NodeCase.EXPRESSION -> operand.expression.operandsList.flatMap(::variablesOf)
        else -> emptyList()
    }

    /** Every operator name in a plan subtree. */
    private fun operatorsOf(operand: Operand): Set<String> = when (operand.nodeCase) {
        Operand.NodeCase.EXPRESSION ->
            operand.expression.operandsList.flatMapTo(mutableSetOf(), ::operatorsOf) +
                operand.expression.operator
        else -> emptySet()
    }

    /**
     * Whether a plan subtree carries a null constant anywhere, INCLUDING one inside a list — the
     * same reading `NullOperandScan` takes, and for the same reason: a null list element is a null
     * operand, and `in-null-elem-hasint` is the corpus action that is nothing else.
     */
    private fun carriesNullLiteral(operand: Operand): Boolean = when (operand.nodeCase) {
        Operand.NodeCase.VALUE -> when (operand.value.kindCase) {
            Value.KindCase.NULL_VALUE -> true
            Value.KindCase.LIST_VALUE ->
                operand.value.listValue.valuesList.any { it.kindCase == Value.KindCase.NULL_VALUE }
            else -> false
        }
        Operand.NodeCase.EXPRESSION -> operand.expression.operandsList.any(::carriesNullLiteral)
        else -> false
    }

    /**
     * Whether a string match anywhere in the plan takes its NEEDLE from a column.
     *
     * The needle is the second operand whichever side the haystack is on: `x.contains(y)` and
     * `"a,b".contains(y)` both put `y` in the LIKE pattern, and only a CONSTANT needle can be
     * escaped at translation time without a guard.
     */
    private fun hasColumnNeedle(operand: Operand): Boolean = when (operand.nodeCase) {
        Operand.NodeCase.EXPRESSION -> {
            val expression = operand.expression
            val columnNeedle = expression.operator in ComparisonTranslator.STRING_MATCH_OPERATORS &&
                expression.operandsCount == 2 &&
                expression.getOperands(1).nodeCase == Operand.NodeCase.VARIABLE
            columnNeedle || expression.operandsList.any(::hasColumnNeedle)
        }
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
        // Corpus gap. `R.attr.tags == ["a", "b"]` is policy-reachable and the corpus carries no
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

    private companion object {
        /**
         * The operators whose own lowering is two-valued, and which therefore carry an `IS NULL`
         * witness of their own rather than inheriting one from an operand.
         *
         * `string()` lowers a boolean portably through a `CASE`, which needs its own NULL arm or a
         * NULL column falls through to the `ELSE`; a `size()` threshold outside int range folds to
         * a constant, which is right only for a row whose column is PRESENT; and a `map()`
         * projection has no error absorption, so a NULL projected column makes the whole
         * intersection an evaluation error.
         */
        val SELF_GUARDING_OPERATORS = setOf("string", "size", "map")

        /** Anti-vacuity floors. 10 actions emit `IS NOT NULL` today and 27 emit `IS NULL`. */
        const val PRESENCE_TEST_FLOOR = 5
        const val NULL_TEST_FLOOR = 15
    }
}
