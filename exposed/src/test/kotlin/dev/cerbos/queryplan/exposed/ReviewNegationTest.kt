package dev.cerbos.queryplan.exposed

import com.google.protobuf.ListValue
import com.google.protobuf.NullValue
import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

/**
 * Hand-built plan operands, shared by the review suites.
 *
 * Every shape built here is one the shared corpus does NOT carry, which is the whole reason it is
 * hand-built: finding a corpus gap is the point of this review and there is no fixture to read.
 * Each caller states the CEL a real policy would write to reach its shape. Where a fixture does
 * exist, the review reads it (docs/adr/0006).
 */
internal object ReviewPlans {

    fun variable(name: String): Operand = Operand.newBuilder().setVariable(name).build()

    fun value(constant: Any?): Operand = Operand.newBuilder().setValue(protobuf(constant)).build()

    fun expression(operator: String, vararg operands: Operand): Operand = Operand.newBuilder()
        .setExpression(
            PlanResourcesFilter.Expression.newBuilder()
                .setOperator(operator)
                .addAllOperands(operands.toList()),
        )
        .build()

    /** A conditional plan over [condition], as `ExposedQueryPlanAdapter.toFilter` takes it. */
    fun conditional(condition: Operand): PlanResourcesFilter = PlanResourcesFilter.newBuilder()
        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
        .setCondition(condition)
        .build()

    private fun protobuf(constant: Any?): Value = when (constant) {
        null -> Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
        is String -> Value.newBuilder().setStringValue(constant).build()
        is Boolean -> Value.newBuilder().setBoolValue(constant).build()
        is Number -> Value.newBuilder().setNumberValue(constant.toDouble()).build()
        is List<*> -> Value.newBuilder()
            .setListValue(ListValue.newBuilder().addAllValues(constant.map(::protobuf)))
            .build()
        else -> throw IllegalArgumentException("No protobuf Value for ${constant::class}")
    }
}

/**
 * Review of the two-valued constants the scalar side folds to, under an enclosing `not`.
 *
 * `TriLogic` exists because `NOT FALSE` is TRUE and `NOT TRUE` is FALSE while `NOT UNKNOWN` stays
 * UNKNOWN, so every fold to a CONSTANT is a claim that no row can make the expression a CEL
 * evaluation error. Two folds in `ComparisonTranslator` and one in `HierarchyTranslator` make that
 * claim while a mapped column is still in the expression, and the corpus carries no negated
 * spelling of either shape.
 */
class ReviewNegationTest {

    @Test
    fun `an unsolvable string concatenation must stay UNKNOWN for a NULL column`() {
        // CEL: `!((R.attr.aOptionalString + "!") == "nope")` — no value of the column can satisfy
        // the equation, because "nope" does not end in "!".
        //
        // ComparisonTranslator.solveAddComparison answered `Op.FALSE`, which is right under the
        // POSITIVE polarity and wrong under this one: for a row whose column is NULL, CEL
        // evaluates `null + "!"` as a no-overload error, `check()` denies, and `NOT (FALSE)` hands
        // the row straight back. d1 and d4 were those rows.
        //
        // Both arms now carry `TriLogic.baseUnlessUnknown(<constant>, IsNullOp(target.expression))`,
        // the same guard `stringLength` and `overlaps` use for their vacuous arms.
        val unsolvable = ReviewPlans.expression(
            "eq",
            ReviewPlans.expression(
                "add",
                ReviewPlans.variable("request.resource.attr.aOptionalString"),
                ReviewPlans.value("!"),
            ),
            ReviewPlans.value("nope"),
        )
        assertEquals(listOf("d2", "d3"), idsOf(translate(ReviewPlans.expression("not", unsolvable))))
    }

    @Test
    fun `a negated unsolvable inequality must not select the NULL rows`() {
        // CEL: `!((R.attr.aOptionalString + "!") != "nope")`. The `ne` arm of the same fold
        // returned `IsNotNullOp`, so the negation rendered `NOT (A_OPTIONAL_STRING IS NOT NULL)` —
        // a predicate that selects the NULL rows and nothing else, which is the exact complement
        // of what `check()` allows.
        val unsolvable = ReviewPlans.expression(
            "ne",
            ReviewPlans.expression(
                "add",
                ReviewPlans.variable("request.resource.attr.aOptionalString"),
                ReviewPlans.value("!"),
            ),
            ReviewPlans.value("nope"),
        )
        assertEquals(emptyList<String>(), idsOf(translate(ReviewPlans.expression("not", unsolvable))))
    }

    @Test
    @Disabled("REVIEW FINDING 5: an unconditional hierarchy overlap folds to Op.TRUE while a column segment is still in the path")
    fun `a hierarchy overlap that ignores its column segment must still deny the NULL rows`() {
        // CEL: `hierarchy("projects", ":").overlaps(hierarchy(["projects", R.attr.scope]))` —
        // the `hier-list-id` corpus shape with a ONE-segment constant, so the constant is a prefix
        // of the list whatever the column holds.
        //
        // HierarchyTranslator.overlaps (HierarchyTranslator.kt:76) folds that to `Op.TRUE`
        // because every COMPARED segment pair was two matching literals. The uncompared segment is
        // still a column read, though: for a row whose `scope` is NULL the caller sends no
        // attribute, `hierarchy([...])` raises inside CEL and `check()` denies — and `Op.TRUE`
        // returns it. Recommended fix: fold to
        // `TriLogic.baseUnlessUnknown(Op.TRUE, or(<every uncompared FieldSegment> IS NULL))`,
        // which is the guard the same method already applies on its `valid.isEmpty()` branch
        // (HierarchyTranslator.kt:69).
        val overlap = ReviewPlans.expression(
            "overlaps",
            ReviewPlans.expression("hierarchy", ReviewPlans.value("projects"), ReviewPlans.value(":")),
            ReviewPlans.expression(
                "hierarchy",
                ReviewPlans.expression(
                    "list",
                    ReviewPlans.value("projects"),
                    ReviewPlans.variable("request.resource.attr.scope"),
                ),
            ),
        )
        assertEquals(listOf("d2", "d3"), idsOf(translate(overlap)))
    }

    @Test
    fun `the positive polarity of the unsolvable add-solve is already right`() {
        // Not a defect, and the reason finding 4 survived the oracle: unnegated, `Op.FALSE` is
        // exactly what CEL decides for every row, NULL or not.
        val unsolvable = ReviewPlans.expression(
            "eq",
            ReviewPlans.expression(
                "add",
                ReviewPlans.variable("request.resource.attr.aOptionalString"),
                ReviewPlans.value("!"),
            ),
            ReviewPlans.value("nope"),
        )
        assertEquals(emptyList<String>(), idsOf(translate(unsolvable)))
    }

    @Test
    fun `a solvable concatenation under a negation keeps its NULL rows out`() {
        // The control: when the solve SUCCEEDS the shape routes through LeafTranslator, whose
        // `NeqOp` is three-valued, and the NULL rows stay excluded under both polarities. Only the
        // unsolvable branch short-circuits into a constant.
        val solvable = ReviewPlans.expression(
            "eq",
            ReviewPlans.expression(
                "add",
                ReviewPlans.variable("request.resource.attr.aOptionalString"),
                ReviewPlans.value("!"),
            ),
            ReviewPlans.value("a!"),
        )
        assertEquals(listOf("d2"), idsOf(translate(solvable)))
        assertEquals(listOf("d3"), idsOf(translate(ReviewPlans.expression("not", solvable))))
    }

    companion object {
        object NegDocs : Table("review_neg_docs") {
            val id = varchar("id", 32)

            /** Undeclared, so a NULL here is a MISSING attribute on the check side (ADR 0004). */
            val aOptionalString = varchar("a_optional_string", 64).nullable()
            val scope = varchar("scope", 64).nullable()
            override val primaryKey = PrimaryKey(id)
        }

        val MAPPING: AttributeMappings = cerbosMapping {
            "request.resource.attr.aOptionalString" to NegDocs.aOptionalString
            "request.resource.attr.scope" to NegDocs.scope
        }

        private val database by lazy {
            val db = Database.connect("jdbc:h2:mem:review_neg;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(db) { seed() }
            db
        }

        fun translate(condition: Operand): Op<Boolean> =
            ExposedQueryPlanAdapter.toFilter(ReviewPlans.conditional(condition), Options.of(MAPPING)).toOp()

        fun idsOf(op: Op<Boolean>): List<String> = transaction(database) {
            NegDocs.selectAll().where(op).map { it[NegDocs.id] }.sorted()
        }

        /**
         * | id | aOptionalString | scope     |
         * |----|-----------------|-----------|
         * | d1 | NULL            | NULL      |
         * | d2 | "a"             | "f1"      |
         * | d3 | "b"             | "f2"      |
         * | d4 | NULL            | NULL      |
         *
         * Two NULL rows and two present ones: a fold to a constant returns all four, and every
         * expectation below is the two present ones or the empty set.
         */
        private fun seed() {
            SchemaUtils.create(NegDocs)
            listOf(
                Triple("d1", null, null),
                Triple("d2", "a", "f1"),
                Triple("d3", "b", "f2"),
                Triple("d4", null, null),
            ).forEach { (docId, optional, scopeValue) ->
                NegDocs.insert {
                    it[id] = docId
                    it[aOptionalString] = optional
                    it[scope] = scopeValue
                }
            }
        }
    }
}
