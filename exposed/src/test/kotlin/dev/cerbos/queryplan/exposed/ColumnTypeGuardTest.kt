package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.dao.id.IdTable
import org.jetbrains.exposed.v1.javatime.timestamp
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * The mapped column's DECLARED type against the operand it is compared with.
 *
 * `ScalarColumnTypes` is "the adapter's one advantage over the plan": a plan names no operand
 * types, and an Exposed `Column` carries its own. The cast, concatenation and arithmetic paths
 * always consulted it. The string matches, `size()`, the hierarchy operators and the plain
 * comparison family did not, so a mismatched operand was emitted and the STORE decided what it
 * meant — and the stores disagree. MySQL and SQLite coerce and match (`123 LIKE '%2%'` is TRUE,
 * `'abc' = 0` is TRUE, `CHAR_LENGTH(1)` is 1); PostgreSQL aborts the statement; H2 raises. CEL
 * raises a no-overload error for every one of them, which DENIES, so every match is a row the PDP
 * refuses.
 *
 * KIND 3 — a policy can reach these, and the corpus does not carry them yet. Every attribute the
 * corpus compares with a string is already a text column and every one it counts is already a
 * string, so no corpus action discriminates any of this. Each case below names the CEL that
 * reaches it; the plans are hand-built because there is no fixture to read. Delete these when the
 * corpus actions land (https://github.com/cerbos/query-plan-adapters/issues/414).
 * `ReviewPlannerShapeTest` records that the pinned PDP really does ship the
 * `R.attr.aString == P.attr.level` shape rather than folding it away.
 */
class ColumnTypeGuardTest {

    // -- a string match needs a text column, on whichever side the column lands ------------------

    @Test
    fun `a string match against a non-text column is refused, whichever operator`() {
        // CEL: `R.attr.aNumber.contains("2")`, `R.attr.aNumber.startsWith("1")`,
        // `R.attr.aBool.endsWith("e")`. All three compile over a `dyn` attribute and raise a
        // no-overload error at check time, which denies; `a_number LIKE '%2%'` is TRUE for 123 on
        // MySQL and on SQLite.
        listOf(
            "contains" to "request.resource.attr.aNumber",
            "startsWith" to "request.resource.attr.aNumber",
            "endsWith" to "request.resource.attr.aBool",
        ).forEach { (operator, variable) ->
            val error = assertThrows<UnmappedAttributeException>("$operator $variable") {
                translate(ReviewPlans.expression(operator, ReviewPlans.variable(variable), ReviewPlans.value("2")))
            }
            assertTrue(
                error.message!!.startsWith("$operator over '$variable' requires a text column"),
                error.message,
            )
            assertTrue(error.message!!.contains("no-overload error"), error.message)
        }
    }

    @Test
    fun `a column NEEDLE is checked as well as the haystack`() {
        // CEL: `R.attr.aString.contains(R.attr.aNumber)`. The needle is escaped and concatenated
        // into a LIKE pattern, so a numeric needle is coerced by the same stores.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "contains",
                    ReviewPlans.variable("request.resource.attr.aString"),
                    ReviewPlans.variable("request.resource.attr.aNumber"),
                ),
            )
        }
        assertEquals(
            "contains over 'request.resource.attr.aNumber' requires a text column, but it maps to " +
                "a IntegerColumnType column. CEL has no contains overload for that type, so the " +
                "expression raises a no-overload error and denies, while SQL coerces the column — " +
                "MySQL and SQLite match and PostgreSQL aborts the statement.",
            error.message,
        )
    }

    @Test
    fun `a constant RECEIVER whose needle is a non-text column is refused`() {
        // CEL: `"12,34".contains(R.attr.aNumber)` — the CONSTANT is the haystack and the COLUMN is
        // the needle, which NormalizedBinary deliberately leaves in source order.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "contains",
                    ReviewPlans.value("12,34"),
                    ReviewPlans.variable("request.resource.attr.aNumber"),
                ),
            )
        }
        assertTrue(error.message!!.contains("requires a text column"), error.message)
    }

    @Test
    fun `size() of a non-text column is refused rather than counting its characters`() {
        // CEL: `size(R.attr.aBool) > 0`. `CHAR_LENGTH(a_bool)` is 1 on MySQL and on SQLite, so
        // every row came back where check() allows none.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "gt",
                    ReviewPlans.expression("size", ReviewPlans.variable("request.resource.attr.aBool")),
                    ReviewPlans.value(0),
                ),
            )
        }
        assertTrue(
            error.message!!.startsWith("size() over 'request.resource.attr.aBool' requires a text column"),
            error.message,
        )
    }

    @Test
    fun `a hierarchy path read from a non-text column is refused`() {
        // CEL: `hierarchy(R.attr.aNumber).descendentOf(hierarchy("1.2"))`. Every hierarchy relation
        // is a prefix test lowered to `=` or to a prefix LIKE, so it is the same hole.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "descendentOf",
                    ReviewPlans.expression("hierarchy", ReviewPlans.variable("request.resource.attr.aNumber")),
                    ReviewPlans.expression("hierarchy", ReviewPlans.value("1.2")),
                ),
            )
        }
        assertTrue(
            error.message!!.startsWith("descendentOf over 'request.resource.attr.aNumber' requires a text column"),
            error.message,
        )
    }

    @Test
    fun `the text-column forms of every one of those still translate`() {
        // The control. The guard must cost nothing a text column can do, which is what every
        // corpus string-match and size() action already is.
        listOf(
            ReviewPlans.expression(
                "contains",
                ReviewPlans.variable("request.resource.attr.aString"),
                ReviewPlans.value("2"),
            ),
            ReviewPlans.expression(
                "startsWith",
                ReviewPlans.variable("request.resource.attr.aString"),
                ReviewPlans.variable("request.resource.attr.aText"),
            ),
            ReviewPlans.expression(
                "endsWith",
                ReviewPlans.value("a,b"),
                ReviewPlans.variable("request.resource.attr.aString"),
            ),
            ReviewPlans.expression(
                "gt",
                ReviewPlans.expression("size", ReviewPlans.variable("request.resource.attr.aString")),
                ReviewPlans.value(0),
            ),
            ReviewPlans.expression(
                "descendentOf",
                ReviewPlans.expression("hierarchy", ReviewPlans.variable("request.resource.attr.aString")),
                ReviewPlans.expression("hierarchy", ReviewPlans.value("a.b")),
            ),
        ).forEach { condition -> translate(condition) }
    }

    // -- the constant's type against the column's kind -------------------------------------------

    @Test
    fun `a constant of the wrong type is refused for every comparison operator and both orders`() {
        // CEL: `R.attr.aString == P.attr.level` for a principal whose level is a number, and the
        // value-first spelling of each. CEL answers the equality FALSE from the values alone and
        // raises for every ordering; MySQL coerces the COLUMN, so `'abc' = 0` is TRUE.
        listOf("eq", "ne", "lt", "le", "gt", "ge").forEach { operator ->
            val fieldFirst = assertThrows<UnmappedAttributeException>(operator) {
                translate(
                    ReviewPlans.expression(
                        operator,
                        ReviewPlans.variable("request.resource.attr.aString"),
                        ReviewPlans.value(3.5),
                    ),
                )
            }
            assertTrue(
                fieldFirst.message!!.contains("maps to a VarCharColumnType column"),
                "$operator: ${fieldFirst.message}",
            )
            // Value-first: NormalizedBinary mirrors the operator, and the leaf is reached either
            // way, so the refusal must not depend on which side the planner put the constant.
            assertThrows<UnmappedAttributeException>("$operator value-first") {
                translate(
                    ReviewPlans.expression(
                        operator,
                        ReviewPlans.value(3.5),
                        ReviewPlans.variable("request.resource.attr.aString"),
                    ),
                )
            }
        }
    }

    @Test
    fun `the refusal names the constant's TYPE and never its value`() {
        // A plan constant can carry a folded principal attribute, and an exception message is
        // logged, so the shape is reported and the value is not.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "eq",
                    ReviewPlans.variable("request.resource.attr.aNumber"),
                    ReviewPlans.value("s3cret-level"),
                ),
            )
        }
        assertTrue(error.message!!.contains("against a String constant"), error.message)
        assertTrue(!error.message!!.contains("s3cret"), error.message)
    }

    @Test
    fun `every element of an in-list is checked, and a null element stays legal`() {
        // CEL: `R.attr.tagNames.exists(t, t in ["a", 2])` reaches the element column with a mixed
        // list; `null in R.attr.tagNames` reaches it with a null, which renders IS NULL and coerces
        // nothing. The list case is the one a principal attribute lands in.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "hasIntersection",
                    ReviewPlans.variable("request.resource.attr.tagNames"),
                    ReviewPlans.value(listOf("a", 2)),
                ),
            )
        }
        assertTrue(error.message!!.contains("against a Long constant"), error.message)

        translate(
            ReviewPlans.expression(
                "in",
                ReviewPlans.value(listOf("a", null)),
                ReviewPlans.variable("request.resource.attr.tagNames"),
            ),
        )
    }

    @Test
    fun `two columns of different kinds are refused, with neither side constant`() {
        // CEL: `R.attr.aString == R.attr.aNumber`. Nothing here is a constant, so the constant
        // check cannot see it — and MySQL coerces the text column exactly the same way.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "eq",
                    ReviewPlans.variable("request.resource.attr.aString"),
                    ReviewPlans.variable("request.resource.attr.aNumber"),
                ),
            )
        }
        assertEquals(
            "eq compares 'request.resource.attr.aString' with 'request.resource.attr.aNumber', " +
                "which map to a VarCharColumnType and a IntegerColumnType column. CEL decides a " +
                "comparison between those from the values alone — equality is false and an " +
                "ordering raises a no-overload error — while SQL coerces one side, and MySQL " +
                "coerces the text one, so the filter returns rows the PDP denies. Map both " +
                "attributes onto columns of one type.",
            error.message,
        )
    }

    @Test
    fun `a member column and an element column of different kinds are refused`() {
        // CEL: `R.attr.aNumber in R.attr.tagNames` — the `in-var-var` shape with the two sides
        // mapped onto columns of different types, which is a mapping the corpus cannot vary.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "in",
                    ReviewPlans.variable("request.resource.attr.aNumber"),
                    ReviewPlans.variable("request.resource.attr.tagNames"),
                ),
            )
        }
        assertTrue(error.message!!.contains("which map to a IntegerColumnType and a VarCharColumnType"), error.message)
    }

    @Test
    fun `numeric with numeric keeps translating, including a fractional constant`() {
        // The control the corpus DOES carry (`double-threshold`, `p-double-frac`): an integer
        // column against a fractional double is numeric-with-numeric, and the double cast that
        // keeps it in CEL's arithmetic is the whole point of it still being translatable.
        val op = translate(
            ReviewPlans.expression(
                "ge",
                ReviewPlans.variable("request.resource.attr.aNumber"),
                ReviewPlans.value(1.5),
            ),
        )
        assertTrue(render(op).contains("AS DOUBLE PRECISION"), render(op))
        translate(
            ReviewPlans.expression(
                "eq",
                ReviewPlans.variable("request.resource.attr.aBool"),
                ReviewPlans.value(true),
            ),
        )
    }

    // -- what the DECLARED column is, when the expression is not the column ----------------------

    @Test
    fun `an EntityID column is read through its id column, so a text key still matches`() {
        // `request.resource.id` maps to a DAO id column in every real application, and the corpus's
        // `id-eq-const`, `id-concat` and hierarchy actions all compare it with strings. Reading the
        // WRAPPER type would classify a varchar key as unrecognised and refuse all of them.
        translate(
            ReviewPlans.expression(
                "eq",
                ReviewPlans.variable("request.resource.id"),
                ReviewPlans.value("doc-1"),
            ),
        )
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "eq",
                    ReviewPlans.variable("request.resource.id"),
                    ReviewPlans.value(1),
                ),
            )
        }
        assertTrue(error.message!!.contains("maps to a VarCharColumnType column"), error.message)
    }

    @Test
    fun `a column reached through a to-one hop is checked by its DECLARED type`() {
        // `Resolution.Scalar.expression` is a correlated scalar subquery there, and `column` is
        // still the declared column — which is what the guard reads.
        translate(
            ReviewPlans.expression(
                "contains",
                ReviewPlans.variable("request.resource.attr.parent.aString"),
                ReviewPlans.value("x"),
            ),
        )
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "contains",
                    ReviewPlans.variable("request.resource.attr.parent.aNumber"),
                    ReviewPlans.value("x"),
                ),
            )
        }
        assertTrue(error.message!!.contains("requires a text column"), error.message)
    }

    @Test
    fun `a column type the adapter has no CEL reading for fails closed`() {
        // A temporal column stands for the whole unrecognised bucket, which a custom `ColumnType`
        // also lands in: `kindOf` answers OTHER and `familyOf` answers null, so every comparison
        // against a constant is refused rather than guessed at. The `ts-*` corpus actions are
        // unaffected — `timestamp(field)` never reaches the leaf.
        listOf(ReviewPlans.value("2024-01-01T00:00:00Z"), ReviewPlans.value(1), ReviewPlans.value(true))
            .forEach { constant ->
                assertThrows<UnmappedAttributeException> {
                    translate(
                        ReviewPlans.expression("eq", ReviewPlans.variable("request.resource.attr.createdAt"), constant),
                    )
                }
            }
        // Two columns of ONE unrecognised type give a store nothing to coerce, so they still
        // compare: refusing them would be a restriction nothing about the stores asks for.
        translate(
            ReviewPlans.expression(
                "eq",
                ReviewPlans.variable("request.resource.attr.createdAt"),
                ReviewPlans.variable("request.resource.attr.updatedAt"),
            ),
        )
    }

    companion object {
        /** An `IdTable`, so `request.resource.id` is an `EntityID` over a text column. */
        object TypedDocs : IdTable<String>("column_type_docs") {
            override val id: Column<EntityID<String>> = varchar("id", 32).entityId()
            val aString = varchar("a_string", 64)
            val aText = varchar("a_text", 64)
            val aNumber = integer("a_number")
            val aBool = bool("a_bool")
            val createdAt = timestamp("created_at")
            val updatedAt = timestamp("updated_at")
            override val primaryKey = PrimaryKey(id)
        }

        object TypedParents : Table("column_type_parents") {
            val resourceId = varchar("resource_id", 32)
            val aString = varchar("a_string", 64)
            val aNumber = integer("a_number")
        }

        object TypedTags : Table("column_type_tags") {
            val resourceId = varchar("resource_id", 32)
            val name = varchar("name", 64).nullable()
        }

        val MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.id" to TypedDocs.id
            "request.resource.attr.aString" to TypedDocs.aString
            "request.resource.attr.aText" to TypedDocs.aText
            "request.resource.attr.aNumber" to TypedDocs.aNumber
            "request.resource.attr.aBool" to TypedDocs.aBool
            "request.resource.attr.createdAt" to TypedDocs.createdAt
            "request.resource.attr.updatedAt" to TypedDocs.updatedAt
            "request.resource.attr.parent" to
                one(TypedParents, from = TypedDocs.id, to = TypedParents.resourceId) {
                    "aString" to TypedParents.aString
                    "aNumber" to TypedParents.aNumber
                }
            "request.resource.attr.tagNames" to
                many(TypedTags, from = TypedDocs.id, to = TypedTags.resourceId, element = TypedTags.name)
        }

        fun translate(condition: Operand): Op<Boolean> = OfflineRenderer.translate {
            ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), Options.of(MAPPING)).toOp()
        }

        fun render(op: Op<Boolean>): String = OfflineRenderer.renderOn(OfflineRenderer.H2, op).sql
    }
}
