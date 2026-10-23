package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.UnknownOp
import org.jetbrains.exposed.v1.core.AndOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.NotOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.OrOp

/**
 * The three-valued predicate algebra behind the error-means-deny contract.
 *
 * A CEL evaluation error makes Cerbos DENY. In SQL the counterpart must evaluate UNKNOWN, never
 * FALSE, so that `NOT (...)` cannot flip a NULL-derived row back into the result:
 * `NOT UNKNOWN` is UNKNOWN, but `NOT FALSE` is TRUE, and that TRUE is a row the PDP denies.
 *
 * Exposed expression trees are immutable, so a node may be shared between a positive and a negated
 * occurrence, and these are pure functions. (The reference adapter needs a supplier per polarity
 * and a "junction barrier" only because Hibernate's negation is stateful.)
 *
 * Built from the `Op` CLASSES, never the `and` / `or` / `eq` sugar: the sugar rewrites what it is
 * given, and this algebra has to own its own shape.
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

    /**
     * The CEL ternary as a predicate: `(c AND a) OR (NOT c AND b) OR NOT (c OR NOT c)`.
     *
     * The third arm is what drives the whole expression to UNKNOWN when the condition is UNKNOWN.
     * Without it both branches read FALSE, the ternary collapses to FALSE, and an enclosing NOT
     * readmits the row.
     */
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
