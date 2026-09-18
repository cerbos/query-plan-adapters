package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Rules over the EMITTED SQL, where the shape is the point rather than the rows.
 *
 * Each of these is a property no single action's row set can state: "every LIKE this adapter can
 * emit carries an ESCAPE clause" is a claim about the whole surface, and a row set that happens to
 * agree proves nothing about the next needle.
 */
class ComparisonSqlShapeTest {

    /** Every corpus action this side translates, which is what makes the sweeps below exhaustive. */
    private val translated = listOf(
        "cs-eq", "cs-contains", "cs-startswith", "cs-endswith", "empty-string-eq", "unicode-eq",
        "vf-ne", "vf-ge", "vf-le", "vf-lt", "gt-bare", "le-bare", "not-gt", "not-lt", "neg-number",
        "double-threshold", "double-huge-gt", "double-huge-lt",
        "like-percent", "like-underscore", "like-bracket", "like-backslash",
        "cr-contains", "cr-startswith", "cr-endswith", "cr-startswith-concat", "p-startswith-concat",
        "f2f-contains", "f2f-startswith", "f2f-endswith", "not-contains", "not-startswith",
        "field-to-field", "null-eq", "null-ne", "null-not-eq", "vf-null-ne", "optional-ne",
        "null-value-ne-const", "null-value-not-eq-const", "null-value-f2f",
        "root-bare-bool", "root-or", "not-and", "double-negation", "triple-negation",
        "id-eq-const", "id-f2f", "id-f2f-ne", "id-concat", "id-concat-vf", "concat-f2f",
        "cast-string-bool", "cast-string-double",
        "arith-add", "arith-sub", "arith-mult-neg", "arith-div", "arith-div-frac", "arith-vf",
        "arith-both", "arith-add-eq-frac", "arith-add-ne-frac", "arith-add-eq-frac-exact",
        "p-double-frac", "cr-div-zero", "cr-div-zero-ne", "cr-div-zero-eq-neg", "cr-div-neg-zero",
        "cr-div-other-column", "nan-ord-ternary", "nan-ord-ternary-vf", "nan-ord-inf", "nan-ord-le",
        "ternary-bare", "ternary-cmp", "ternary-negated", "ternary-nested", "ternary-null-cond",
        "ternary-value-first", "ternary-expr-cond", "p-not-ternary-null", "p-ternary-of-ternaries",
        "p-ternary-vs-ternary",
        "hier-ancestor-cf", "hier-ancestor-ff", "hier-descendent-cf", "hier-descendent-ff",
        "hier-overlaps-cf", "hier-overlaps-ff", "hier-list-id", "hier-meta-in", "hier-meta-like",
        "hier-overlaps-meta", "hier-bracket",
        "ts-eq", "ts-ne", "ts-eq-offset",
    )

    @Test
    fun `every LIKE carries an ESCAPE clause`() {
        // The escape character is what makes the adapter's own metacharacter escaping mean
        // anything. Exposed BINDS it rather than inlining it, so no dialect's string-literal
        // backslash handling can change what it means.
        translated.forEach { action ->
            val sql = Scalars.rendered(Scalars.op(action)).sql
            assertEquals(
                sql.occurrencesOf(" LIKE "),
                sql.occurrencesOf(" ESCAPE "),
                "$action emitted a LIKE without an ESCAPE: $sql",
            )
        }
    }

    @Test
    fun `NULL is never a bound argument`() {
        // PostgreSQL cannot infer a type for a bare parameter with nothing around it to infer
        // from, and a NULL keyword needs no dialect knowledge. Nothing this side emits binds one.
        translated.forEach { action ->
            val args = Scalars.rendered(Scalars.op(action)).args
            assertTrue(args.none { it == null }, "$action bound a NULL: $args")
        }
    }

    @Test
    fun `IS NULL appears only where a null operand or a declared guard puts it`() {
        // Three sources, and no fourth: the null-constant leaf, the two-sided definite equality an
        // explicit-null declaration entitles the mapping to, and the NULL-yielding guard around a
        // column-valued LIKE pattern. Anywhere else it would be an assumption about a column the
        // plan never made.
        val nullTest = setOf(
            "null-eq", "null-not-eq", "null-value-f2f",
            "f2f-contains", "f2f-startswith", "f2f-endswith", "not-contains", "not-startswith",
            "cr-contains", "cr-startswith", "cr-endswith", "cr-startswith-concat",
            "cast-string-bool",
        )
        assertEquals(
            nullTest,
            translated.filter { Scalars.rendered(Scalars.op(it)).sql.contains("IS NULL") }.toSet(),
        )
    }

    @Test
    fun `IS NOT NULL appears only where the explicit-null convention was declared`() {
        // The asymmetric expansion that makes an explicit-null attribute compare DEFINITELY.
        // Emitting it for an undeclared column would readmit exactly the rows an omitted-convention
        // comparison has to drop.
        val presence = setOf(
            "null-ne", "vf-null-ne",
            "null-value-ne-const", "null-value-not-eq-const", "null-value-f2f",
        )
        assertEquals(
            presence,
            translated.filter { Scalars.rendered(Scalars.op(it)).sql.contains("IS NOT NULL") }.toSet(),
        )
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

    private fun String.occurrencesOf(needle: String): Int = split(needle).size - 1
}
