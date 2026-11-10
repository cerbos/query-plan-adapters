package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.UnknownOp
import org.jetbrains.exposed.v1.core.AndOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.NotOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.OrOp

/**
 * The three-valued algebra behind error-means-deny: a CEL error must be SQL UNKNOWN, never FALSE, or
 * `NOT` readmits the row. Built from `Op` classes, not the sugar, which rewrites its input.
 */
internal object TriLogic {
    /** Constant SQL UNKNOWN. Composes like CEL error absorption: `x OR UNKNOWN`, `x AND UNKNOWN`. */
    fun unknown(): Op<Boolean> = UnknownOp

    fun not(predicate: Expression<Boolean>): Op<Boolean> = NotOp(predicate)

    fun and(vararg predicates: Expression<Boolean>): Op<Boolean> = and(predicates.toList())

    fun and(predicates: List<Expression<Boolean>>): Op<Boolean> {
        require(predicates.isNotEmpty()) { "an empty conjunction has no SQL form; fold it before calling" }
        return if (predicates.size == 1) wrap(predicates[0]) else AndOp(predicates)
    }

    fun or(vararg predicates: Expression<Boolean>): Op<Boolean> = or(predicates.toList())

    fun or(predicates: List<Expression<Boolean>>): Op<Boolean> {
        require(predicates.isNotEmpty()) { "an empty disjunction has no SQL form; fold it before calling" }
        return if (predicates.size == 1) wrap(predicates[0]) else OrOp(predicates)
    }

    /** TRUE when [body] is two-valued, UNKNOWN when it is UNKNOWN: `body OR NOT body`. */
    fun determined(body: Expression<Boolean>): Op<Boolean> = or(body, not(body))

    /** The CEL ternary: `(c AND a) OR (NOT c AND b) OR NOT (c OR NOT c)`; the last arm keeps an UNKNOWN `c` UNKNOWN. */
    fun ternary(
        condition: Expression<Boolean>,
        whenTrue: Expression<Boolean>,
        whenFalse: Expression<Boolean>,
    ): Op<Boolean> = or(
        and(condition, whenTrue),
        and(not(condition), whenFalse),
        not(determined(condition)),
    )

    /**
     * [base], unless [witness] is TRUE, in which case UNKNOWN:
     * `(base AND NOT witness) OR (witness AND UNKNOWN)`. A strict guard with no absorption: a TRUE
     * witness poisons the result whatever [base] says.
     */
    fun baseUnlessUnknown(base: Expression<Boolean>, witness: Expression<Boolean>): Op<Boolean> = or(
        and(base, not(witness)),
        and(witness, unknown()),
    )

    private fun wrap(predicate: Expression<Boolean>): Op<Boolean> =
        predicate as? Op<Boolean> ?: AndOp(listOf(predicate))
}
