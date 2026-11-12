package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.CodePointOrdering
import dev.cerbos.queryplan.exposed.sql.ExactString
import dev.cerbos.queryplan.exposed.sql.IeeeDoubleCast
import dev.cerbos.queryplan.exposed.sql.LikeEscaping
import dev.cerbos.queryplan.exposed.sql.NulSafeMatch
import dev.cerbos.queryplan.exposed.sql.Params
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import dev.cerbos.queryplan.exposed.sql.ScalarValueFamily
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.GreaterEqOp
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.IsNotNullOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.LessEqOp
import org.jetbrains.exposed.v1.core.LessOp
import org.jetbrains.exposed.v1.core.LikeEscapeOp
import org.jetbrains.exposed.v1.core.NeqOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.stringParam

/**
 * The scalar leaf: one resolved value against one plan constant, under the normalised operator.
 * Owns the explicit-null rules: [isExplicitNull] and [definiteEquality].
 */
internal class LeafTranslator(private val translation: Translation) {

    fun applyLeaf(operator: String, target: Resolution.Scalar, value: Any?): Op<Boolean> {
        // A list or map is never equal to the one scalar a mapped column holds: CEL answers from
        // the types alone, as for any other mismatch.
        if (value is List<*> || value is Map<*, *>) {
            return typeMismatch(operator, listOf(target))
        }
        if (value == null) {
            if (target.field.nullAttributeRepresentation == NullAttributeRepresentation.OMITTED) {
                return unknownWhenNull(operator, target)
            }
            translation.hopGuards.require(target)
            return when (operator) {
                // Under the EXPLICIT convention, or with nothing declared under a call-level
                // EXPLICIT: the pre-walk scan refuses an undeclared null operand under a call-level
                // OMITTED before anything is built (see [NullOperandScan]).
                "eq" -> IsNullOp(target.expression)
                "ne" -> IsNotNullOp(target.expression)
                // `x < null` is legal CEL over a dyn attribute and errors at check time, which
                // denies; no ordering predicate reproduces that, so refuse it.
                else -> throw Refusals.unsupported(
                    "Null values are only supported with eq and ne operators (got $operator)",
                )
            }
        }

        // The column's DECLARED type against the operand, before anything is emitted. A plan names
        // no operand types, so this is the only place a comparison CEL decides from the values
        // alone can be told apart from one a store would decide by coercing the column.
        if (operator in ComparisonTranslator.STRING_MATCH_OPERATORS) {
            if (lacksTextOverload(target)) return TriLogic.unknown()
            requireText(operator, target)
        }
        if (ScalarColumnTypes.knownMismatch(target.column, value)) {
            return typeMismatch(operator, listOf(target))
        }
        if (!ScalarColumnTypes.accepts(target.column, value)) {
            throw ScalarRefusals.constantTypeMismatch(operator, target.variable, target.column, value)
        }

        // CEL orders strings by code point; H2 compares UTF-16 code units, which disagree only at or above
        // 0xD800, so such a literal is compared in code point order ([CodePointOrdering]).
        if (operator in ORDERING_OPERATORS && value is String && CodePointOrdering.needed(value) &&
            ScalarColumnTypes.isText(target.column)
        ) {
            return CodePointOrdering(
                ExactString.of(target.expression, operator),
                Params.of(value),
                CodePointOrdering.sign(operator),
            )
        }

        // An attribute the caller sends as an explicit null holds a null VALUE in CEL, so equality
        // against a non-null operand is DEFINITE — `null == "x"` is FALSE and `null != "x"` is
        // TRUE — where SQL answers UNKNOWN and excludes the row under both polarities (#308).
        if ((operator == "eq" || operator == "ne") && isExplicitNull(target)) {
            return definiteEquality(
                operator,
                comparable(target, value),
                Params.of(value),
                leftExplicit = true,
                rightExplicit = false,
            )
        }

        return defaultLeaf(operator, target, value)
    }

    private companion object {
        val ORDERING_OPERATORS = setOf("lt", "le", "gt", "ge")
    }

    /**
     * A comparison between values CEL knows to be of different types, answered from the types: `==`
     * FALSE, `!=` TRUE, anything else UNKNOWN. A NULL operand without the explicit convention is UNKNOWN.
     */
    fun typeMismatch(operator: String, operands: List<Resolution.Scalar>): Op<Boolean> {
        if (operator != "eq" && operator != "ne" && operator != "in") return TriLogic.unknown()
        val missing = operands.filterNot(::isExplicitNull).map { IsNullOp(it.expression) }
        val equality: Op<Boolean> = if (missing.isEmpty()) {
            Op.FALSE
        } else {
            TriLogic.and(TriLogic.or(missing), TriLogic.unknown())
        }
        return if (operator == "ne") TriLogic.not(equality) else equality
    }

    /**
     * `==` / `!=` against null over an OMITTED attribute: `x IS NULL AND UNKNOWN`, because CEL denies a
     * missing attribute under both polarities.
     */
    private fun unknownWhenNull(operator: String, target: Resolution.Scalar): Op<Boolean> {
        val missing = TriLogic.and(IsNullOp(target.expression), TriLogic.unknown())
        return when (operator) {
            "eq" -> missing
            "ne" -> TriLogic.not(missing)
            else -> throw Refusals.unsupported(
                "Null values are only supported with eq and ne operators (got $operator)",
            )
        }
    }

    /**
     * Whether [target] is sent as an explicit null. Read from the mapping, with no fallback to the call
     * option (docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).
     */
    fun isExplicitNull(target: Resolution.Scalar): Boolean =
        target.field.nullAttributeRepresentation == NullAttributeRepresentation.EXPLICIT

    /** Whether [target] is a number or boolean column, which CEL's string functions have no overload for. */
    fun lacksTextOverload(target: Resolution.Scalar): Boolean = when (ScalarColumnTypes.familyOf(target.column)) {
        ScalarValueFamily.NUMERIC, ScalarValueFamily.BOOLEAN -> true
        else -> false
    }

    /** Refuses [target] unless its column type is text. Public for paths that bypass [applyLeaf]. */
    fun requireText(operator: String, target: Resolution.Scalar) {
        if (!ScalarColumnTypes.isText(target.column)) {
            throw ScalarRefusals.textColumnRequired(operator, target.variable, target.column)
        }
    }

    /**
     * An equality that is never SQL UNKNOWN, for explicit-null operands. Not `IS NOT DISTINCT FROM`:
     * that is symmetric, and a NULL on an undeclared side must stay UNKNOWN.
     */
    fun definiteEquality(
        operator: String,
        left: Expression<*>,
        right: Expression<*>,
        leftExplicit: Boolean,
        rightExplicit: Boolean,
    ): Op<Boolean> {
        val present = buildList<Expression<Boolean>> {
            if (leftExplicit) add(IsNotNullOp(left))
            if (rightExplicit) add(IsNotNullOp(right))
            add(EqOp(left, right))
        }
        var equality = TriLogic.and(present)
        if (leftExplicit && rightExplicit) {
            equality = TriLogic.or(TriLogic.and(IsNullOp(left), IsNullOp(right)), equality)
        }
        return if (operator == "ne") TriLogic.not(equality) else equality
    }

    /** The default lowering of one leaf operator against one non-null constant. */
    fun defaultLeaf(operator: String, target: Resolution.Scalar, value: Any): Op<Boolean> {
        val constant = Params.of(value)
        val expression = ExactString.of(comparable(target, value), operator)
        if (operator in setOf("contains", "startsWith", "endsWith") && value is String && '\u0000' in value) {
            throw ScalarRefusals.nulNeedle(operator)
        }
        return when (operator) {
            "eq" -> EqOp(expression, constant)
            "ne" -> NeqOp(expression, constant)
            "lt" -> LessOp(expression, constant)
            "le" -> LessEqOp(expression, constant)
            "gt" -> GreaterOp(expression, constant)
            "ge" -> GreaterEqOp(expression, constant)
            "contains" -> like(target.expression, value, leadingWildcard = true, trailingWildcard = true)
            "startsWith" -> like(target.expression, value, leadingWildcard = false, trailingWildcard = true)
            "endsWith" -> like(target.expression, value, leadingWildcard = true, trailingWildcard = false)
            else -> throw ScalarRefusals.unsupportedOperator(operator)
        }
    }

    /** `haystack LIKE '<escaped needle>' ESCAPE '\'`; escaped here because `LikePattern.ofLiteral` reads the dialect. */
    private fun like(
        haystack: Expression<*>,
        needle: Any,
        leadingWildcard: Boolean,
        trailingWildcard: Boolean,
    ): Op<Boolean> {
        val escaped = PlanValues.escapeLike(needle.toString())
        val pattern = buildString {
            if (leadingWildcard) append('%')
            append(escaped)
            if (trailingWildcard) append('%')
        }
        return NulSafeMatch(
            LikeEscapeOp(haystack, ExactString.param(pattern), true, LikeEscaping.ESCAPE_CHAR),
            haystack,
            stringParam(needle.toString()),
            leadingWildcard,
            trailingWildcard,
        )
    }

    /** The target cast to IEEE double when the constant is fractional or too large for a `Long`, as CEL compares. */
    private fun comparable(target: Resolution.Scalar, value: Any): Expression<*> =
        if (value is Double && ScalarColumnTypes.isNumeric(target.column)) {
            IeeeDoubleCast(target.expression)
        } else {
            target.expression
        }
}
