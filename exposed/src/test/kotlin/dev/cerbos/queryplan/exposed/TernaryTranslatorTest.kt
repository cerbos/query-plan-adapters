package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * The CEL ternary, executed, in both positions.
 *
 * The load-bearing case is the UNKNOWN condition. A ternary whose condition is a missing attribute
 * is a CEL evaluation error, so `check()` denies under both polarities — and two false branches
 * collapse to a definite FALSE that an enclosing `NOT` flips straight back to TRUE. Every
 * assertion below that names a NULL row is guarding that.
 */
class TernaryTranslatorTest {

    @Test
    fun `a ternary in boolean position walks each branch as if it were written directly`() =
        assertSelects("ternary-bare", "r1", "r4")

    @Test
    fun `a ternary as a comparison operand is substituted back into the comparison`() {
        assertSelects("ternary-cmp", "r5", "r7")
        assertSelects("ternary-expr-cond", "r3", "r6")
        assertSelects("ternary-nested", "r5", "r7")
    }

    @Test
    fun `a value-first comparison with a ternary operand keeps source order`() =
        assertSelects("ternary-value-first", "r1", "r5", "r7", "f1")

    @Test
    fun `a negated ternary is the complement only where the condition is determined`() {
        // Every row here has a non-NULL aBool, so the ternary is two-valued and the negation is a
        // clean complement.
        assertSelects("ternary-cmp", "r5", "r7")
        assertSelects("ternary-negated", "r1", "r2", "r3", "r4", "r6", "f1", "c1")
    }

    @Test
    fun `an UNKNOWN condition excludes the row under BOTH polarities`() {
        // r2, r4 and r7 have a NULL aOptionalString, and the attribute declares nothing — so CEL
        // raises a missing-attribute error in the CONDITION and denies. Without the third arm of
        // the rewrite the ternary would read FALSE there, and the negated form would return all
        // three.
        assertSelects("ternary-null-cond", "r5")
        assertSelects("p-not-ternary-null", "r1", "r3", "r6", "f1", "c1")
    }

    @Test
    fun `nested ternaries on both sides of one comparison recurse`() {
        assertSelects("p-ternary-of-ternaries", "r2", "r5", "r6", "r7", "c1")
        assertSelects("p-ternary-vs-ternary", "r5", "r7")
    }

    @Test
    fun `a constant boolean condition folds to one branch, so a dead branch cannot fail the plan`() {
        // CEL cannot reach this: the planner evaluates a constant condition itself. It is pinned
        // because the fold is what stops an untranslatable else-branch — here, a bare `matches` —
        // from refusing a plan whose condition never selects it.
        val folded = ternary(
            constant(Value.newBuilder().setBoolValue(true).build()),
            comparison("eq", variable("request.resource.attr.aString"), string("one")),
            comparison("matches", variable("request.resource.attr.aString"), string("^o|e$")),
        )
        assertEquals(
            listOf("c1", "r1"),
            Scalars.idsOf(ExposedQueryPlanAdapter.toFilter(conditional(folded), Options.of(Scalars.MAPPING)).toOp()),
        )
    }

    @Test
    fun `a boolean constant in condition position folds rather than looking for a column`() {
        // Reached the same way: `aBool ? true : false` substitutes a bare VALUE into the walk.
        val branches = ternary(
            variable("request.resource.attr.aBool"),
            constant(Value.newBuilder().setBoolValue(true).build()),
            constant(Value.newBuilder().setBoolValue(false).build()),
        )
        assertEquals(
            listOf("f1", "r1", "r3", "r5", "r7"),
            Scalars.idsOf(ExposedQueryPlanAdapter.toFilter(conditional(branches), Options.of(Scalars.MAPPING)).toOp()),
        )
    }

    @Test
    fun `a ternary with the wrong arity is a malformed plan, not an unsupported shape`() {
        // KIND 1 — a branch CEL itself cannot reach, and therefore permanent. `cond ? a : b` always
        // parses to three operands, so no policy can make the planner ship an `if` with two; the
        // plan is hand-built because no fixture can exist. It pins input validation on a public
        // function: a wire-contract violation is a MalformedPlanException, never an
        // UnsupportedPlanShapeException a caller might route around.
        val truncated = PlanResourcesFilter.Expression.newBuilder()
            .setOperator("if")
            .addOperands(variable("request.resource.attr.aBool"))
            .addOperands(constant(Value.newBuilder().setBoolValue(true).build()))
            .build()
        val error = assertThrows<MalformedPlanException> {
            ExposedQueryPlanAdapter.toFilter(
                conditional(truncated),
                Options.of(Scalars.MAPPING),
            )
        }
        assertEquals(
            "if (ternary) requires exactly 3 operands (condition, then, else), got 2",
            error.message,
        )
    }

    private companion object {
        fun conditional(expression: PlanResourcesFilter.Expression): PlanResourcesFilter =
            PlanResourcesFilter.newBuilder()
                .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                .setCondition(Operand.newBuilder().setExpression(expression).build())
                .build()

        fun ternary(
            condition: Operand,
            whenTrue: PlanResourcesFilter.Expression,
            whenFalse: PlanResourcesFilter.Expression,
        ): PlanResourcesFilter.Expression = PlanResourcesFilter.Expression.newBuilder()
            .setOperator("if")
            .addOperands(condition)
            .addOperands(Operand.newBuilder().setExpression(whenTrue))
            .addOperands(Operand.newBuilder().setExpression(whenFalse))
            .build()

        fun ternary(condition: Operand, whenTrue: Operand, whenFalse: Operand): PlanResourcesFilter.Expression =
            PlanResourcesFilter.Expression.newBuilder()
                .setOperator("if")
                .addOperands(condition)
                .addOperands(whenTrue)
                .addOperands(whenFalse)
                .build()

        fun comparison(operator: String, left: Operand, right: Operand): PlanResourcesFilter.Expression =
            PlanResourcesFilter.Expression.newBuilder()
                .setOperator(operator)
                .addOperands(left)
                .addOperands(right)
                .build()

        fun variable(name: String): Operand = Operand.newBuilder().setVariable(name).build()

        fun constant(value: Value): Operand = Operand.newBuilder().setValue(value).build()

        fun string(value: String): Operand = constant(Value.newBuilder().setStringValue(value).build())
    }
}
