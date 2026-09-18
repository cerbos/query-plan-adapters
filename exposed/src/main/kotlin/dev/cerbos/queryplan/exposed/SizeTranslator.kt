package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.CountChars
import dev.cerbos.queryplan.exposed.sql.Params
import dev.cerbos.queryplan.exposed.sql.ScoreCase
import dev.cerbos.queryplan.exposed.sql.nullIfUndetermined
import org.jetbrains.exposed.v1.core.Alias
import org.jetbrains.exposed.v1.core.Coalesce
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.GreaterEqOp
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.LessEqOp
import org.jetbrains.exposed.v1.core.LessOp
import org.jetbrains.exposed.v1.core.LongColumnType
import org.jetbrains.exposed.v1.core.Max
import org.jetbrains.exposed.v1.core.NeqOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.PlusOp
import org.jetbrains.exposed.v1.core.Sum
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.intLiteral
import org.jetbrains.exposed.v1.core.longLiteral
import kotlin.math.ceil
import kotlin.math.floor

/**
 * `size(x) op N`: string length over a column, element count over a relation chain, and the strict
 * `size(filter(...))` count. Owns the threshold arithmetic for fractional and out-of-range
 * constants, and the choice between the two-valued `EXISTS` emptiness shortcuts and the tri-state
 * count.
 *
 * It is a STEP of the comparison pipeline rather than a resolved-operand case: its SQL shapes are
 * subquery translations pinned by the differential oracle, not a (column, constant) pair.
 */
internal class SizeTranslator(private val translation: Translation) {

    /**
     * The comparison when one operand is a `size(...)` expression, or `null` when neither is.
     * Receives the NORMALISED operator and operands.
     */
    fun trySizeComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        // The size() operand is detected before the constant is converted: every ordinary leaf
        // comparison probes through here, and materialising a list or a struct only to discard it
        // is work the common case should not pay for.
        val size = operands.lastOrNull {
            it.nodeCase == Operand.NodeCase.EXPRESSION && it.expression.operator == "size"
        }?.expression ?: return null
        val threshold = operands.lastOrNull {
            it.nodeCase == Operand.NodeCase.VALUE && it.value.kindCase == Value.KindCase.NUMBER_VALUE
        }?.value?.numberValue ?: return null

        val comparison = Threshold.of(operator, threshold)
        val argument = size.operandsList
        if (argument.size != 1) {
            throw Refusals.malformed("size() takes exactly 1 argument, got ${argument.size}")
        }
        return when (val target = SizeTarget.of(argument[0])) {
            is SizeTarget.Whole -> whole(target.variable, comparison, scope)
            is SizeTarget.Filtered -> filtered(target, comparison, scope)
        }
    }

    /** `size(attribute)`: a string's length, or a relation's element count. */
    private fun whole(variable: String, comparison: Threshold, scope: Scope): Op<Boolean> =
        when (val resolved = scope.resolve(variable)) {
            is Resolution.Scalar -> stringLength(resolved, comparison)
            is Resolution.Collection -> elementCount(resolved, comparison)
        }

    /**
     * `size(string)`. The threshold arithmetic is decided statically rather than truncated: a
     * character count is integral, so a fractional constant can never be hit exactly, and
     * `LENGTH(s) >= 1` is a strictly wider filter than `size(s) >= 1.5` asks for.
     *
     * The vacuous arms are NOT unconditional. A NULL column is a missing attribute (or, under the
     * explicit convention, a null value with no `size()` overload), so CEL raises an evaluation
     * error and denies; folding to a constant TRUE would return those rows, and folding to a
     * constant FALSE would return them under a negation.
     */
    private fun stringLength(target: Resolution.Scalar, comparison: Threshold): Op<Boolean> {
        val definite = { answer: Boolean ->
            TriLogic.baseUnlessUnknown(if (answer) Op.TRUE else Op.FALSE, IsNullOp(target.expression))
        }
        comparison.vacuous?.let { return definite(it) }
        // A string's length never leaves int range, so a threshold outside it decides the
        // comparison the same way a fractional one does — and an unguarded narrowing cast would
        // silently wrap it (2147483648 becomes -2147483648) and flip the filter.
        if (comparison.value > Int.MAX_VALUE) {
            return when (comparison.operator) {
                "eq", "gt", "ge" -> definite(false)
                else -> definite(true)
            }
        }
        if (comparison.value < Int.MIN_VALUE) {
            return when (comparison.operator) {
                "eq", "lt", "le" -> definite(false)
                else -> definite(true)
            }
        }
        return compare(CountChars(target.expression), comparison)
    }

    /**
     * `size(relation)`. Counting rows evaluates no lambda, so no element can be UNKNOWN and the
     * plain comparisons are already exact — except for what the CHAIN adds: an absent to-one parent
     * and a childless one both count 0, and only requiring the hop keeps them apart.
     */
    private fun elementCount(collection: Resolution.Collection, comparison: Threshold): Op<Boolean> {
        val chained = collection.leadingHops.isNotEmpty()
        comparison.vacuous?.let { answer ->
            // A count is never fractional, so the comparison is statically decided — but not
            // unconditionally: a parentless row is a CEL missing-path error, and a constant is
            // two-valued, so `NOT(constant)` would readmit every one of them
            // (cerbos/query-plan-adapters#333). Carrying the guard on an EXPRESSION is what makes
            // both polarities inherit it.
            if (!chained) return if (answer) Op.TRUE else Op.FALSE
            val guarded = translation.subqueries.requireLeadingHops(collection, intLiteral(1), IntegerColumnType())
            return EqOp(guarded, intLiteral(if (answer) 1 else 0))
        }
        // The EXISTS emptiness shortcuts are TWO-valued, so a chain must not take them: `NOT EXISTS`
        // is TRUE for an absent to-one parent, which is why `!(size(chain) > 0)` readmitted every
        // parentless row even though `size(chain) == 0`, guarded separately, did not (#316).
        if (!chained) {
            val nonEmpty = (comparison.operator == "gt" && comparison.value == 0L) ||
                (comparison.operator == "ge" && comparison.value == 1L)
            val empty = (comparison.operator == "eq" && comparison.value == 0L) ||
                (comparison.operator == "le" && comparison.value == 0L) ||
                (comparison.operator == "lt" && comparison.value == 1L)
            val chain = { translation.subqueries.chain(collection.owner, collection.hops) }
            if (nonEmpty) return translation.subqueries.existsOver(chain())
            if (empty) return TriLogic.not(translation.subqueries.existsOver(chain()))
        }
        return compare(translation.subqueries.chainCount(collection), comparison)
    }

    /**
     * `size(collection.filter(x, body))`. CEL's `filter` has NO error absorption: one element whose
     * body errors errors the whole expression, even when the count comparison would otherwise hold.
     * The strict counter goes SQL NULL whenever any element body is UNKNOWN, so every comparison
     * built on it is UNKNOWN and the row stays excluded under both polarities.
     */
    private fun filtered(target: SizeTarget.Filtered, comparison: Threshold, scope: Scope): Op<Boolean> {
        val resolved = scope.resolve(target.variable)
        if (resolved !is Resolution.Collection) {
            throw RelationRefusals.sizeFilterNeedsRelation(target.variable)
        }
        return translation.walker.enterMacro("size(filter(...))") {
            if (comparison.vacuous != null) {
                // The count comparison is statically decided, but an erroring body still has to
                // deny: the poison term alone is 0 when every element is determined and SQL NULL
                // otherwise, which makes the collapse UNKNOWN exactly when CEL errors.
                val poison = translation.subqueries.chainAggregate(resolved, IntegerColumnType()) { alias ->
                    undeterminedPoison(body(resolved, target, scope, alias))
                }
                if (comparison.vacuous) EqOp(poison, intLiteral(0)) else NeqOp(poison, intLiteral(0))
            } else {
                val matched = translation.subqueries.chainAggregate(resolved, IntegerColumnType()) { alias ->
                    val element = body(resolved, target, scope, alias)
                    PlusOp<Int, Int>(
                        Coalesce<Int, Int?>(Sum<Int>(ScoreCase(element, 1, 0, 0), IntegerColumnType()), intLiteral(0)),
                        undeterminedPoison(element),
                        IntegerColumnType(),
                    )
                }
                compare(matched, comparison)
            }
        }
    }

    private fun body(
        collection: Resolution.Collection,
        target: SizeTarget.Filtered,
        scope: Scope,
        alias: Alias<Table>,
    ): Op<Boolean> = translation.walker.traverse(
        target.lambda.body,
        LambdaScope(translation, alias, collection.tail, target.lambda.variable, scope),
    )

    /** 0 when every element body is determined (and for the empty collection), SQL NULL otherwise. */
    private fun undeterminedPoison(element: Op<Boolean>) = nullIfUndetermined(
        Coalesce<Int, Int?>(Max<Int, Int>(ScoreCase(element, 0, 0, 1), IntegerColumnType()), intLiteral(0)),
    )

    /** A count or a length against the plan's own constant, bound by the VALUE's type. */
    private fun compare(size: Expression<*>, comparison: Threshold): Op<Boolean> {
        val constant = Params.of(comparison.value)
        return when (comparison.operator) {
            "eq" -> EqOp(size, constant)
            "ne" -> NeqOp(size, constant)
            "lt" -> LessOp(size, constant)
            "le" -> LessEqOp(size, constant)
            "gt" -> GreaterOp(size, constant)
            "ge" -> GreaterEqOp(size, constant)
            else -> throw Refusals.malformed("Unsupported size comparison operator: ${comparison.operator}")
        }
    }

    /**
     * What `size()` is applied to. `except()` is called out rather than falling into the generic
     * refusal because it is the PDP-verified wire shape of every real `except()` policy and the
     * rewrite that replaces it is worth naming.
     */
    private sealed interface SizeTarget {
        class Whole(val variable: String) : SizeTarget
        class Filtered(val variable: String, val lambda: ParsedLambda) : SizeTarget

        companion object {
            fun of(argument: Operand): SizeTarget {
                if (argument.nodeCase == Operand.NodeCase.VARIABLE) {
                    return Whole(argument.variable)
                }
                if (argument.nodeCase != Operand.NodeCase.EXPRESSION) {
                    throw RelationRefusals.sizeOperand()
                }
                if (argument.expression.operator == "except") {
                    throw RelationRefusals.exceptUnsupported()
                }
                if (argument.expression.operator != "filter") {
                    throw RelationRefusals.sizeOperand()
                }
                val operands = argument.expression.operandsList
                if (operands.size != 2) {
                    throw Refusals.malformed("filter requires exactly 2 operands, got ${operands.size}")
                }
                if (operands[0].nodeCase != Operand.NodeCase.VARIABLE) {
                    // filter() over a computed collection: legal CEL, no table to count over.
                    throw RelationRefusals.computedCollection("filter")
                }
                return Filtered(
                    operands[0].variable,
                    ParsedLambda.parse(
                        operands[1],
                        "filter second operand must be a lambda",
                        "filter supports single-variable lambdas only",
                        "filter lambda variable must be a variable operand",
                    ),
                )
            }
        }
    }

    /**
     * A `size()` comparison reduced to an integral threshold.
     *
     * A count and a character length are both integral, so a fractional constant never lands on
     * one: `eq 1.5` is always false, `ne 1.5` always true, and `>= 1.5` is `>= 2` — rounding, not
     * truncating. Truncating `>= 1.5` to `>= 1` returns rows the PDP denies, which is the bug the
     * corpus's fractional actions exist to catch.
     */
    private class Threshold(val operator: String, val value: Long, val vacuous: Boolean?) {
        companion object {
            fun of(operator: String, raw: Double): Threshold {
                if (raw == Math.rint(raw)) {
                    return Threshold(operator, raw.toLong(), null)
                }
                return when (operator) {
                    "eq" -> Threshold(operator, 0, false)
                    "ne" -> Threshold(operator, 0, true)
                    // An integral size makes gt and ge coincide once the bound is rounded, as do
                    // lt and le.
                    "gt", "ge" -> Threshold("ge", ceil(raw).toLong(), null)
                    "lt", "le" -> Threshold("le", floor(raw).toLong(), null)
                    // size() yields an int, so anything but a comparison over it is a CEL type
                    // error the planner would not have shipped.
                    else -> throw Refusals.malformed("Unsupported size comparison operator: $operator")
                }
            }
        }
    }
}
