package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.dao.id.IdTable
import org.jetbrains.exposed.v1.core.dao.id.java.UUIDTable
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
 * The cases below are NOT all of one kind, and the banner over each block says which. Most are
 * KIND 3 corpus gaps and are deleted when their action lands; a few are KIND 2 — the column type is
 * a caller-supplied MAPPING, and `actions.json` classifies every action against ONE mapping per
 * adapter, so those have no corpus spelling and are permanent. Deleting a KIND 2 case on the KIND 3
 * trigger would remove coverage the corpus can never supply. Two more are neither: they are the
 * CONTROLS for the guard, over shapes the corpus already carries.
 *
 * Every case names the CEL that reaches it; the plans are hand-built because there is no fixture to
 * read. `ReviewPlannerShapeTest` records that the pinned PDP really does ship the
 * `R.attr.aString == P.attr.level` shape rather than folding it away.
 */
class ColumnTypeGuardTest {

    // ============================================================================================
    // KIND 3 — a policy can reach these, and the corpus does not carry them yet
    //
    // Every attribute the corpus compares with a string is already a text column and every one it
    // counts is already a string, so no corpus action discriminates any of this. Every test here
    // opens with `Corpus gap.`, is tracked by cerbos/query-plan-adapters#414, and is deleted when
    // its action lands — EXCEPT the two named `the CONTROL for …`, which are the guard's cost over
    // shapes the corpus already carries and stay behind when the refusals are ported.
    // ============================================================================================

    // -- a string match needs a text column, on whichever side the column lands ------------------

    @Test
    fun `a string match against a non-text column is refused, whichever operator`() {
        // Corpus gap. CEL: `R.attr.aNumber.contains("2")`, `R.attr.aNumber.startsWith("1")`,
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
        // Corpus gap. CEL: `R.attr.aString.contains(R.attr.aNumber)`. The needle is escaped and concatenated
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
        // Corpus gap. CEL: `"12,34".contains(R.attr.aNumber)` — the CONSTANT is the haystack and the COLUMN is
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
        // Corpus gap. CEL: `size(R.attr.aBool) > 0`. `CHAR_LENGTH(a_bool)` is 1 on MySQL and on SQLite, so
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
        // Corpus gap. CEL: `hierarchy(R.attr.aNumber).descendentOf(hierarchy("1.2"))`. Every hierarchy relation
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
    fun `the CONTROL for those refusals - the text-column forms of every one of them still translate`() {
        // NOT a corpus gap: this is the guard's cost, and what every corpus string-match and size()
        // action already is. It stays when the refusals above are ported.
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
        // Corpus gap. CEL: `R.attr.aString == P.attr.level` for a principal whose level is a number, and the
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
        // Corpus gap. A plan constant can carry a folded principal attribute, and an exception message is
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
    fun `every element of an in-list is checked, and one mismatch refuses the whole membership`() {
        // Corpus gap. `hasIntersection(R.attr.tagNames, P.attr.groups)` for a principal whose
        // groups are `["a", 1]` — legal principal data, and the list shape a principal attribute
        // really lands in.
        //
        // The mismatched element refuses the WHOLE membership rather than being dropped from the
        // disjunction. Dropping it was tried, on the argument that CEL answers `textColumn == 1`
        // with a definite FALSE and `x OR false` is `x` under either polarity — and the argument
        // is only available where the adapter KNOWS CEL's answer, which the test below is the
        // over-grant for. Refusing needs no such knowledge.
        listOf(listOf("a", 1), listOf(1, "a"), listOf(1, 2)).forEach { groups ->
            val error = assertThrows<UnmappedAttributeException>(groups.toString()) {
                translate(
                    ReviewPlans.expression(
                        "hasIntersection",
                        ReviewPlans.variable("request.resource.attr.tagNames"),
                        ReviewPlans.value(groups),
                    ),
                )
            }
            assertTrue(error.message!!.contains("against a Long constant"), error.message)
        }

        // A null element is still never refused: it is not a type mismatch, it renders IS NULL,
        // and it coerces nothing.
        val withNull = render(
            translate(
                ReviewPlans.expression(
                    "in",
                    ReviewPlans.value(listOf("a", null)),
                    ReviewPlans.variable("request.resource.attr.tagNames"),
                ),
            ),
        )
        assertTrue(withNull.contains("IS NULL"), withNull)
        assertEquals(1, Regex("\\?").findAll(withNull).count(), withNull)
    }

    @Test
    fun `a null element cannot suppress the refusal a temporal column earns`() {
        // Corpus gap. CEL: `!(R.attr.createdAt in P.attr.allowedStamps)` for a principal whose list
        // holds one instant string and a null. `createdAt` is already a temporal column in the
        // corpus's own mapping, so this is a shape a corpus action could reach today; a null inside
        // a principal list is legal data the corpus itself carries (`in-null-elem-mixed`), and the
        // DEFAULT null convention admits it — the pre-walk scan only refuses a null-carrying list
        // under OMITTED.
        //
        // THE OVER-GRANT, in the one column kind the corpus already maps. While a mismatched
        // element was dropped rather than refused, the drop ran for every column kind — including
        // the ones `familyOf` reads as unrecognised, where `accepts` is false for EVERY value and
        // the adapter has no CEL reading at all. The instant string was dropped, the null survived
        // alone, `created_at IS NULL` was emitted, and `NOT (…)` handed back every row.
        assertRefusesNullCarryingList(
            MAPPING,
            ReviewPlans.expression(
                "in",
                ReviewPlans.variable("request.resource.attr.createdAt"),
                ReviewPlans.value(listOf("2024-01-01T00:00:00Z", null)),
            ),
        )
    }

    @Test
    fun `two columns of different kinds are refused, with neither side constant`() {
        // Corpus gap. CEL: `R.attr.aString == R.attr.aNumber`. Nothing here is a constant, so the constant
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
                "coerces the text one, so the filter returns rows the PDP denies. Compare it against " +
                "a value of the column's own type, or map the attribute onto one of the kinds this " +
                "adapter compares: text, integer, floating-point, decimal or boolean. A temporal " +
                "column is compared by wrapping both sides in timestamp().",
            error.message,
        )
    }

    @Test
    fun `the CONTROL for those - numeric with numeric keeps translating, fractional constant and all`() {
        // NOT a corpus gap: this is the one shape here the corpus DOES carry (`double-threshold`,
        // `p-double-frac`). An integer column against a fractional double is numeric-with-numeric,
        // and the double cast that keeps it in CEL's arithmetic is the whole point of it still
        // being translatable.
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
    fun `a column type the adapter has no CEL reading for fails closed`() {
        // Corpus gap. A temporal column stands for the whole unrecognised bucket, which a custom `ColumnType`
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
    }

    @Test
    fun `two columns of ONE unrecognised type are refused, because coercion is not the failure`() {
        // Corpus gap. CEL: `R.attr.createdAt == R.attr.updatedAt`, with no `timestamp()` wrapper,
        // over two instant columns. It used to translate, on the argument that identical declared
        // types give a store nothing to coerce — but coercion is not the only divergence and here
        // it is not the one that matters.
        //
        // Cerbos transports a timestamp attribute as an RFC 3339 STRING, so this is a STRING
        // comparison in CEL and an INSTANT comparison in SQL. `"2020-01-01T00:00:00Z"` against
        // `"2020-01-01T00:00:00.000Z"`, or against `"2020-01-01T01:00:00+01:00"`, are unequal
        // strings and the same instant: SQL returns a row check() denies. Two ordinal enum columns
        // over different enums diverge the same way, and `describe` cannot tell them apart either.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "eq",
                    ReviewPlans.variable("request.resource.attr.createdAt"),
                    ReviewPlans.variable("request.resource.attr.updatedAt"),
                ),
            )
        }
        assertTrue(error.message!!.contains("which map to a JavaInstantColumnType and a JavaInstantColumnType"), error.message)
    }

    @Test
    fun `the timestamp() path is separate and still compares two instant columns`() {
        // Corpus gap (the control for it). The reason refusing above costs nothing a policy needs:
        // `timestamp(R.attr.createdAt) < timestamp(R.attr.updatedAt)` has SAID it means instants,
        // goes through the timestamp-field pair rather than the leaf, and still translates.
        translate(
            ReviewPlans.expression(
                "lt",
                ReviewPlans.expression("timestamp", ReviewPlans.variable("request.resource.attr.createdAt")),
                ReviewPlans.expression("timestamp", ReviewPlans.variable("request.resource.attr.updatedAt")),
            ),
        )
    }

    // -- concatenation: every leaf of a string `+` has to be text ---------------------------------

    @Test
    fun `a string concatenation over a non-text column is refused`() {
        // Corpus gap. CEL: `R.attr.aString + R.attr.aNumber == "one5"`. One text operand proves the `+` is a
        // concatenation, because CEL has no mixed-type `+`; the numeric one then has no overload, the
        // check raises and DENIES. `CONCAT(a_string, a_number)` renders the number as text on every
        // store, so the emitted filter would match the row the PDP refuses.
        val error = assertThrows<UnmappedAttributeException> {
            translate(
                ReviewPlans.expression(
                    "eq",
                    ReviewPlans.expression(
                        "add",
                        ReviewPlans.variable("request.resource.attr.aString"),
                        ReviewPlans.variable("request.resource.attr.aNumber"),
                    ),
                    ReviewPlans.value("one5"),
                ),
            )
        }
        assertEquals(
            "String concatenation over 'request.resource.attr.aNumber' requires a text column, but it " +
                "maps to a IntegerColumnType column",
            error.message,
        )
    }

    @Test
    fun `a string concatenation with a non-string constant is refused`() {
        // Corpus gap. CEL: `R.attr.aString + R.attr.aText + 5 == "one5"`. The same no-overload error, reached
        // with a constant leaf inside a concatenation two text columns have already proved. It is
        // an inexpressible shape rather than a mapping shortfall: no remapping makes `string + int`
        // mean anything, so it is the OTHER refusal type.
        val error = assertThrows<UnsupportedPlanShapeException> {
            translate(
                ReviewPlans.expression(
                    "eq",
                    ReviewPlans.expression(
                        "add",
                        ReviewPlans.expression(
                            "add",
                            ReviewPlans.variable("request.resource.attr.aString"),
                            ReviewPlans.variable("request.resource.attr.aText"),
                        ),
                        ReviewPlans.value(5),
                    ),
                    ReviewPlans.value("one5"),
                ),
            )
        }
        assertTrue(error.message!!.startsWith("String concatenation requires string operands, got "), error.message)
        assertTrue(error.message!!.contains("CEL has no mixed-type `+`"), error.message)
    }

    @Test
    fun `one column plus a non-string constant is refused by the add-fold, naming types and no value`() {
        // Corpus gap. CEL: `R.attr.aString + 5 == "one5"`. One column plus one constant is the add-fold's shape,
        // so it is refused there, before the concatenation path is asked, with the message ported
        // from the reference. What matters is that it IS refused and that the message carries the
        // two TYPES: a plan constant can hold a folded principal attribute, and messages are logged.
        val error = assertThrows<UnsupportedPlanShapeException> {
            translate(
                ReviewPlans.expression(
                    "eq",
                    ReviewPlans.expression(
                        "add",
                        ReviewPlans.variable("request.resource.attr.aString"),
                        ReviewPlans.value(5),
                    ),
                    ReviewPlans.value("one5"),
                ),
            )
        }
        assertEquals("add comparison type mismatch: String vs Long", error.message)
    }

    // ============================================================================================
    // KIND 2 — a caller-supplied mapping the corpus structurally cannot vary
    //
    // `actions.json` classifies every action against ONE mapping per adapter, and the column a
    // reference resolves to IS the mapping — so a DAO key, a to-one hop's declared type, a member
    // column and an element column of different kinds, and an element column the adapter has no
    // CEL reading for have no corpus spelling however many actions are added. PERMANENT: these do
    // not go when #414 lands, and deleting them on that trigger would remove coverage the corpus
    // can never supply.
    // ============================================================================================

    @Test
    fun `an EntityID column is read through its id column, so a text key still matches`() {
        // `request.resource.id` maps to a DAO id column in every real application, and the corpus's
        // `id-eq-const`, `id-concat` and hierarchy actions all compare it with strings. Reading the
        // WRAPPER type would classify a varchar key as unrecognised and refuse all of them — which
        // only a mapping that HAS the wrapper can show, and the corpus's maps a bare varchar.
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
        // still the declared column — which is what the guard reads. The corpus maps every hop
        // member onto a column of the type its actions compare it with, so only a mapping written
        // here puts a numeric column where a string match will land.
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
    fun `a null element cannot rescue a DAO key or an element column with no CEL reading`() {
        // The other two columns the null-suppression over-grant reaches, and the two the corpus
        // cannot map: a `UUIDTable` id — `EntityIDColumnType(UUIDColumnType)`, which unwraps into
        // the unrecognised bucket — and a temporal ELEMENT column, which asks `matchesAnyOf` the
        // same question `scalarIsAnyOf` is asked above. Whatever #414 ports, the corpus maps
        // `request.resource.id` to a varchar and `tagNames`'s element to one, so neither half of
        // this can become an action.
        assertRefusesNullCarryingList(
            UUID_MAPPING,
            ReviewPlans.expression(
                "in",
                ReviewPlans.variable("request.resource.id"),
                ReviewPlans.value(listOf("6d1f2c4e-0000-4000-8000-000000000000", null)),
            ),
        )
        assertRefusesNullCarryingList(
            MAPPING,
            ReviewPlans.expression(
                "hasIntersection",
                ReviewPlans.variable("request.resource.attr.stamps"),
                ReviewPlans.value(listOf("2024-01-01T00:00:00Z", null)),
            ),
        )
    }

    /**
     * [condition] is refused under BOTH polarities, on the constant's type.
     *
     * Both, because the over-grant this pins is a negation's: the suppressed refusal emitted a lone
     * `IS NULL`, which is merely wrong unnegated and hands back every row under `not(…)`.
     */
    private fun assertRefusesNullCarryingList(mapping: AttributeMappings, condition: Operand) {
        listOf(condition, ReviewPlans.expression("not", condition)).forEach { polarity ->
            val error = assertThrows<UnmappedAttributeException>(polarity.toString()) {
                translateWith(mapping, polarity)
            }
            assertTrue(error.message!!.contains("against a String constant"), error.message)
        }
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

        /** An ELEMENT column in the unrecognised bucket, so `matchesAnyOf` is asked the same question. */
        object TypedStamps : Table("column_type_stamps") {
            val resourceId = varchar("resource_id", 32)
            val at = timestamp("at").nullable()
        }

        /**
         * A DAO key in the unrecognised bucket. `UUIDTable.id` is
         * `EntityIDColumnType(UUIDColumnType)`, which `ScalarColumnTypes.unwrap` looks through to a
         * type `kindOf` answers OTHER for — the shape the null-suppression over-grant was found on.
         */
        object UuidDocs : UUIDTable("column_type_uuid_docs")

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
            "request.resource.attr.stamps" to
                many(TypedStamps, from = TypedDocs.id, to = TypedStamps.resourceId, element = TypedStamps.at)
        }

        val UUID_MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.id" to UuidDocs.id
        }

        fun translate(condition: Operand): Op<Boolean> = translateWith(MAPPING, condition)

        fun translateWith(mapping: AttributeMappings, condition: Operand): Op<Boolean> = OfflineRenderer.translate {
            ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), Options.of(mapping)).toOp()
        }

        fun render(op: Op<Boolean>): String = OfflineRenderer.renderOn(OfflineRenderer.H2, op).sql
    }
}
