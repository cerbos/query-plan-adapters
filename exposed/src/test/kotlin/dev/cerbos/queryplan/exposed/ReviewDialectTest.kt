package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The dialect-rendering properties this review read by eye and found sound, pinned so they stay
 * that way.
 *
 * Each one was checked by rendering the shape under sqlite, h2, postgresql and mysql through
 * [OfflineRenderer] and reading the SQL: the cast targets, the concatenation operator, the
 * character-length function, the escape clause, and the two places the adapter emits a bare
 * boolean constant.
 *
 * KIND 2, not a corpus gap. The subject is RENDERING — which SQL text one predicate becomes under
 * four dialects — and a corpus action asks which rows come back. "Every arithmetic subtree is
 * parenthesised under every dialect this adapter claims" is not a question about rows, and no
 * action can state it however many are added, so nothing here is waiting on
 * [#414](https://github.com/cerbos/query-plan-adapters/issues/414).
 */
class ReviewDialectTest {

    @Test
    fun `nested arithmetic is parenthesised, so operator precedence cannot regroup it`() {
        // The smallest tree that would expose it: `(aNumber + 1) * 2 > 10`. Unparenthesised, SQL
        // reads `aNumber + 1 * 2 > 10`, which is a different filter — and CustomOperator carries
        // no ComplexExpression marker, so this is a property of Exposed's own rendering rather
        // than of anything the adapter declares.
        val op = OfflineRenderer.translate { translate(nestedArithmetic()) }
        OfflineRenderer.render(op).forEach { (dialect, rendered) ->
            assertTrue(
                Regex("\\(\\(CAST\\([^)]*\\) \\+ \\?\\) \\* \\?\\) > \\?").containsMatchIn(rendered.sql),
                "$dialect: ${rendered.sql}",
            )
        }
    }

    @Test
    fun `the constant UNKNOWN is a NULL comparison, never a bound parameter`() {
        // PostgreSQL cannot type a bare `$1 IS NULL`, so UnknownOp renders a literal. Pinned
        // across the four dialects because a change to it is invisible in a row set: `1 = NULL`
        // and a bound NULL behave identically everywhere except on the one store that rejects the
        // statement outright.
        val op = OfflineRenderer.translate { TriLogic.unknown() }
        OfflineRenderer.render(op).forEach { (dialect, rendered) ->
            assertEquals("1 = NULL", rendered.sql, dialect)
            assertEquals(emptyList<RenderedParam>(), rendered.params, dialect)
        }
    }

    @Test
    fun `the boolean constants the adapter folds to render per dialect, and SQLite spells them 1 and 0`() {
        // SQLite has no boolean type, so `Op.TRUE` is `1` there and `TRUE` on the other three.
        // Both are valid where the adapter puts them — inside AND/OR and as a whole WHERE clause —
        // but a reader of the golden asset should not be surprised by the difference, and a future
        // `ELSE <constant>` arm inside a CASE would inherit it.
        val rendered = OfflineRenderer.render(OfflineRenderer.translate { TriLogic.and(Op.TRUE, Op.FALSE) })
        assertEquals("1 AND 0", rendered.getValue(OfflineRenderer.SQLITE).sql)
        listOf(OfflineRenderer.H2, OfflineRenderer.POSTGRESQL, OfflineRenderer.MYSQL).forEach { dialect ->
            assertEquals("TRUE AND FALSE", rendered.getValue(dialect).sql, dialect)
        }
    }

    @Test
    fun `a column-valued LIKE pattern yields NULL rather than an empty string on every dialect`() {
        // The #387 shape: the guard has to make the PATTERN null, not the match false. MySQL uses
        // CONCAT (which propagates NULL) and the other three use `||`; both are wrapped in a CASE
        // whose NULL arm is what actually carries the guard, so neither spelling can degrade into
        // a match-anything `'%%'`.
        val op = OfflineRenderer.translate { translate(columnNeedle()) }
        val rendered = OfflineRenderer.render(op)
        rendered.forEach { (dialect, one) ->
            assertTrue(one.sql.contains("CASE WHEN") && one.sql.contains("THEN NULL"), "$dialect: ${one.sql}")
            assertTrue(one.sql.contains("LIKE") && one.sql.contains("ESCAPE"), "$dialect: ${one.sql}")
        }
        assertTrue(rendered.getValue(OfflineRenderer.MYSQL).sql.contains("CONCAT("))
        assertTrue(rendered.getValue(OfflineRenderer.POSTGRESQL).sql.contains(" || "))
    }

    companion object {
        object DialectDocs : Table("review_dialect_docs") {
            val aNumber = integer("a_number")
            val aString = varchar("a_string", 64)
            val needle = varchar("needle", 64).nullable()
        }

        val MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.attr.aNumber" to DialectDocs.aNumber
            "request.resource.attr.aString" to DialectDocs.aString
            "request.resource.attr.needle" to DialectDocs.needle
        }

        fun translate(condition: Operand): Op<Boolean> =
            ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), Options.of(MAPPING)).toOp()

        /** `(R.attr.aNumber + 1) * 2 > 10`. */
        fun nestedArithmetic(): Operand = ReviewPlans.expression(
            "gt",
            ReviewPlans.expression(
                "mult",
                ReviewPlans.expression(
                    "add",
                    ReviewPlans.variable("request.resource.attr.aNumber"),
                    ReviewPlans.value(1),
                ),
                ReviewPlans.value(2),
            ),
            ReviewPlans.value(10),
        )

        /** `R.attr.aString.contains(R.attr.needle)`. */
        fun columnNeedle(): Operand = ReviewPlans.expression(
            "contains",
            ReviewPlans.variable("request.resource.attr.aString"),
            ReviewPlans.variable("request.resource.attr.needle"),
        )
    }
}
