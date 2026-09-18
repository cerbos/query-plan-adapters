package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Arithmetic as a comparison operand, executed.
 *
 * The property under test is that the SQL computes in IEEE DOUBLE space, which is the only space
 * CEL can be evaluating in: Cerbos transports every attribute number as a protobuf double. A store
 * left to its own devices computes `intCol * 0.1` in exact decimal and answers TRUE where CEL says
 * `0.30000000000000004` — the rows that lands on are rows the PDP denies.
 */
class ArithmeticTranslatorTest {

    @Nested
    inner class DoubleSpace {
        @Test
        fun `add, sub, mult and div are emitted rather than solved`() {
            // Emitting rather than solving is also what makes multiplication by a NEGATIVE
            // constant need no inequality flip: `aNumber * -2 < 3` excludes only the -5 row.
            assertSelects("arith-add", "r2", "r5", "r6", "r7")
            assertSelects("arith-sub", "r1", "r2", "r3", "r4", "r5", "f1", "c1")
            assertSelects("arith-mult-neg", "r1", "r2", "r3", "r5", "r6", "r7", "f1", "c1")
            assertSelects("arith-div", "r2")
            assertSelects("arith-div-frac", "r5", "r6", "r7")
        }

        @Test
        fun `a value-first arithmetic comparison mirrors the operator`() =
            assertSelects("arith-vf", "r2", "r5", "r6", "r7")

        @Test
        fun `arithmetic on both sides compares two SQL expressions`() =
            assertSelects("arith-both", "r3", "r4")

        @Test
        fun `a fractional product is exact IEEE, not exact decimal`() =
            // `aNumber * 0.1 == 0.3` is FALSE in CEL for every integer, because 3 * 0.1 is
            // 0.30000000000000004. A store computing in NUMERIC answers TRUE for the 3 row.
            assertSelects("p-double-frac")

        @Test
        fun `a fractional add is lowered to SQL rather than inverted in Kotlin`() {
            // IEEE subtraction does not invert IEEE addition: solving `aDouble + 0.7 == 0.1`
            // algebraically yields exactly -0.6, and the -0.6 row would come back — yet
            // -0.6 + 0.7 is 0.09999999999999998, so the PDP denies it.
            assertSelects("arith-add-eq-frac")
            assertSelects("arith-add-eq-frac-exact")
            assertSelects("arith-add-ne-frac", *Scalars.ALL.toTypedArray())
        }
    }

    @Nested
    inner class DivisionByZero {
        @Test
        fun `a zero divisor yields NaN or a signed infinity, not SQL NULL`() {
            // Under an ORDERED comparison NaN and NULL agree — both exclude the row — so only the
            // INEQUALITY discriminates: `NaN != 1.0` is TRUE and the PDP allows the zero row,
            // while `NULL <> 1.0` is UNKNOWN and would drop it.
            assertSelects("cr-div-zero", "r1", "r2", "r4", "r5", "r6", "r7", "f1", "c1")
            assertSelects("cr-div-zero-ne", "r3")
            assertSelects("cr-div-zero-eq-neg", "r3")
        }

        @Test
        fun `a constant zero divisor decides which infinity by its SIGN`() {
            // IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from
            // `n / 0.0` (#312). The corpus fixture writes the divisor as `-0`, and protobuf's
            // canonical JSON mapping decodes that integral token to POSITIVE zero — so replaying
            // the fixture pins the positive reading. The sign only survives a binary transport,
            // and the hand-built plan below is the only way to reach the other branch here.
            assertSelects("cr-div-neg-zero", "r1", "r2", "r5", "r6", "r7", "f1", "c1")

            val negativeZero = comparison(
                "gt",
                operand { it.expression = expression("div", variable(NUMBER), number(-0.0)) },
                number(0.0),
            )
            assertEquals(
                listOf("r4"),
                Scalars.idsOf(ExposedQueryPlanAdapter.toFilter(negativeZero, Options.of(Scalars.MAPPING)).toOp()),
                "a negative zero divisor turns a positive dividend into NEGATIVE infinity",
            )
        }

        @Test
        fun `a column divisor is guarded so a zero row cannot abort the query`() =
            assertSelects("cr-div-other-column", "r1", "r5", "r6", "r7", "f1", "c1")

        @Test
        fun `arithmetic COMPOSED on a zero-capable division keeps the non-finite arm`() {
            // CEL carries the NaN through the surrounding sum, so the division stays symbolic and
            // the `+ 1.0` is applied to each IEEE arm instead: the sum reaching SQL is the finite
            // arm alone. `NaN + 1.0 != 2.0` is TRUE for the zero row — the one row the PDP allows —
            // while lowering the division to NULL gives `NULL + 1 <> 2`, UNKNOWN, and returns
            // nothing. The ORDERED spelling is the control: NaN and NULL agree there.
            assertSelects("cr-div-then-add", "r1", "r2", "r4", "r5", "r6", "r7", "f1", "c1")
            assertSelects("cr-div-then-add-ne", "r3")
        }
    }

    @Nested
    inner class NonFiniteConstants {
        @Test
        fun `a NaN or infinity folds against the comparison and is never bound`() {
            // SQL has no literal for either, so each arm is decided here. Comparing with a
            // total-order comparator instead of the IEEE operators would rank NaN above every
            // number and collapse `> 0.5` to always-true.
            assertSelects("nan-ord-ternary", "r1", "r3", "r5", "r7", "f1")
            assertSelects("nan-ord-ternary-vf", "r1", "r3", "r5", "r7", "f1")
            assertSelects("nan-ord-inf", "r1", "r3", "r5", "r7", "f1")
            assertSelects("nan-ord-le")
        }

        @Test
        fun `no NaN or infinity reaches the bound arguments`() {
            listOf(
                "nan-ord-ternary", "nan-ord-inf", "cr-div-neg-zero", "cr-div-zero",
                "cr-div-then-add", "cr-div-then-add-ne",
            ).forEach { action ->
                val args = Scalars.rendered(Scalars.op(action)).args
                assertTrue(
                    args.none { it is Double && (it.isNaN() || it.isInfinite()) },
                    "$action bound a non-finite double: $args",
                )
            }
        }
    }

    @Nested
    inner class Refusals {
        @Test
        fun `mod is refused, because the cast that makes it satisfiable is itself unlowerable`() {
            val error = assertThrows<UnsupportedPlanShapeException> { Scalars.ids("arith-mod") }
            assertTrue(error.message!!.startsWith("mod is not supported in comparisons"), error.message)
        }

        @Test
        fun `a non-finite arm meeting a COLUMN is refused rather than folded or bound`() {
            // CORPUS GAP. `R.attr.aNumber / R.attr.aNumber + R.attr.aDouble > 1.0` is
            // policy-reachable and no corpus action carries it, so the plan is hand-built here.
            // Delete this test when the corpus action lands (cerbos/query-plan-adapters#414).
            //
            // Every other composition folds: a division arm is NaN or an infinity, and `+ 1.0`
            // over either is exact in Kotlin. A COLUMN ends that. SQL has no literal for the arm,
            // and it does not collapse to one constant either — an infinity times or divided by a
            // column is +Infinity, -Infinity or NaN according to a sign no plan can state.
            val composed = comparison(
                "gt",
                operand {
                    it.expression = expression(
                        "add",
                        operand { inner -> inner.expression = expression("div", variable(NUMBER), variable(NUMBER)) },
                        variable("request.resource.attr.aDouble"),
                    )
                },
                number(1.0),
            )
            val error = assertThrows<UnsupportedPlanShapeException> {
                ExposedQueryPlanAdapter.toFilter(composed, Options.of(Scalars.MAPPING))
            }
            assertTrue(
                error.message!!.startsWith(
                    "arithmetic between a column and the NaN or infinity a zero denominator produces",
                ),
                error.message,
            )
        }

        @Test
        fun `arithmetic over a non-numeric column is refused rather than cast`() {
            // A CALLER-SUPPLIED MAPPING the corpus structurally cannot vary: actions.json
            // classifies each action against one mapping per adapter, so a column type that
            // settles nothing has no corpus spelling. A plan cannot say what type a column holds;
            // the mapping can. Emitting the cast anyway aborts the query on PostgreSQL and reads a
            // numeric prefix on SQLite, which is a wrong answer rather than an error.
            // A temporal column is neither text nor numeric, so nothing settles CEL's `+`
            // overload and the arithmetic path owns the refusal. (A TEXT column would make it a
            // concatenation instead, which is a different — and also refused — shape.)
            val mapping = cerbosMapping {
                "request.resource.attr.aNumber" to Scalars.Docs.createdAt
            }
            val error = assertThrows<UnmappedAttributeException> {
                Scalars.ids("arith-add", Options.of(mapping))
            }
            assertTrue(error.message!!.contains("requires a numeric column"), error.message)
        }
    }

    private companion object {
        const val NUMBER = "request.resource.attr.aNumber"

        /**
         * A hand-built conditional filter, for the ONE operand protobuf's canonical JSON mapping
         * cannot carry: an integral negative zero, which it decodes as positive zero.
         */
        fun comparison(operator: String, left: Operand, right: Operand): PlanResourcesFilter =
            PlanResourcesFilter.newBuilder()
                .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                .setCondition(operand { it.expression = expression(operator, left, right) })
                .build()

        fun expression(operator: String, vararg operands: Operand): PlanResourcesFilter.Expression =
            PlanResourcesFilter.Expression.newBuilder()
                .setOperator(operator)
                .also { builder -> operands.forEach(builder::addOperands) }
                .build()

        fun operand(build: (Operand.Builder) -> Unit): Operand = Operand.newBuilder().also(build).build()

        fun variable(name: String): Operand = operand { it.variable = name }

        fun number(value: Double): Operand =
            operand { it.value = Value.newBuilder().setNumberValue(value).build() }
    }
}
