package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.CodePointOrdering
import dev.cerbos.queryplan.exposed.sql.IeeeDoubleCast
import dev.cerbos.queryplan.exposed.sql.LikeEscaping
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
 * The scalar leaf: one resolved value against one plan constant.
 *
 * THE SINGLE ROUTING POINT. Every comparison of one mapped column against one constant goes
 * through [applyLeaf], under the NORMALISED operator name, whichever path reached it. Nothing hooks
 * into it today; it is what makes operator overrides a one-place change if they are ever added.
 *
 * It also owns the two rules of the explicit-null convention — which attributes the caller sends as
 * an explicit null ([isExplicitNull]) and the asymmetric definite equality that declaration
 * entitles them to ([definiteEquality]) — so those are a property of this class rather than of
 * every caller.
 */
internal class LeafTranslator(@Suppress("unused") private val translation: Translation) {

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

        // CEL orders strings by code point. A store may compare by UTF-16 code unit instead (H2
        // uses Java's String.compareTo), which puts an astral character's high surrogate (from
        // 0xD800) before U+E000–U+FFFF. The two orders can only disagree at a position where one
        // side holds a code unit at or above 0xD800, so a literal holding none cannot be ordered
        // differently; one that does is compared in code point order, spelled per dialect when
        // the statement renders ([CodePointOrdering]).
        if (operator in ORDERING_OPERATORS && value is String && CodePointOrdering.needed(value) &&
            ScalarColumnTypes.isText(target.column)
        ) {
            return CodePointOrdering(target.expression, Params.of(value), CodePointOrdering.sign(operator))
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
     * A comparison between values CEL knows to be of different types, answered from the types
     * alone rather than handed to a store that would coerce one side (MySQL coerces the column:
     * `'abc' = 0` is TRUE there).
     *
     * CEL's heterogeneous equality makes `==` a definite FALSE and `!=` a definite TRUE between
     * two PRESENT values, and an ordering or any other operator a no-overload error, which denies
     * under both polarities (SQL UNKNOWN). A missing attribute is itself an error, so an [operands]
     * column that is NULL without declaring the explicit-null convention drives the equality to
     * UNKNOWN: `(a IS NULL OR ...) AND UNKNOWN`, FALSE for every present row. A NULL under the
     * explicit convention is a null VALUE, and `null == "x"` is also a definite FALSE, so it adds
     * no witness.
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
     * `==` / `!=` against null over an attribute that declares [NullAttributeRepresentation.OMITTED].
     *
     * A NULL column sends no attribute, so CEL raises a missing-attribute error and `check()`
     * denies the row under BOTH polarities; a present value is never null, so `== null` is FALSE
     * and `!= null` TRUE. `x IS NULL AND UNKNOWN` is exactly that: UNKNOWN for the NULL column and
     * FALSE otherwise, and its negation UNKNOWN and TRUE. The same holds through a to-one relation,
     * whose correlated read is NULL for an absent row, which is also a missing attribute. `IS NULL`
     * alone would return the rows the PDP denies.
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
     * Whether [target] maps to a scalar the caller sends as an explicit null.
     *
     * Read from the MAPPING, never from the column: the corpus (and real applications) map one
     * column twice under two attribute names with two conventions, so the column cannot
     * discriminate them.
     *
     * Deliberately not falling back to [Options.nullAttributeRepresentation]. Declaring nothing
     * means "treat this column as NOT NULL", which is the historical rendering; the call-level
     * setting is the default for the pre-walk null-operand scan, not a blanket claim that every
     * mapped column sends its NULLs explicitly. Falling back would hand definite equality to an
     * undeclared column and readmit the rows an omitted-convention `!=` must drop
     * (docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).
     */
    fun isExplicitNull(target: Resolution.Scalar): Boolean =
        target.field.nullAttributeRepresentation == NullAttributeRepresentation.EXPLICIT

    /**
     * Whether [target] maps to a number or boolean column, which CEL's string matches, `size()`
     * and `hierarchy()` have NO overload for. At check time the call raises a no-overload error on
     * every row, present or NULL, and `check()` denies under both polarities: SQL UNKNOWN, with
     * no coercion left for a store to apply. A temporal or unrecognised column is not this case:
     * a timestamp attribute reaches CEL as a string, so [requireText] still refuses it.
     */
    fun lacksTextOverload(target: Resolution.Scalar): Boolean = when (ScalarColumnTypes.familyOf(target.column)) {
        ScalarValueFamily.NUMERIC, ScalarValueFamily.BOOLEAN -> true
        else -> false
    }

    /**
     * Refuses [target] unless its declared column type is one CEL's string matches and `size()`
     * have an overload for.
     *
     * Public because the same requirement holds wherever a COLUMN reaches a `LIKE` or a character
     * count without passing through [applyLeaf]: a column needle, a constant receiver's needle, a
     * hierarchy path, and `size(string)`.
     */
    fun requireText(operator: String, target: Resolution.Scalar) {
        if (!ScalarColumnTypes.isText(target.column)) {
            throw ScalarRefusals.textColumnRequired(operator, target.variable, target.column)
        }
    }

    /**
     * An equality that can never be SQL UNKNOWN, for operands the caller sends as explicit nulls.
     *
     * A null VALUE is what CEL holds under that convention, so `null == "x"` is a definite FALSE,
     * `null != "x"` a definite TRUE, and two nulls are EQUAL. SQL answers UNKNOWN to all three,
     * which excludes the row under BOTH polarities — so the NOT an enclosing negation applies has
     * nothing definite to flip.
     *
     * Deliberately not `IS NOT DISTINCT FROM`, which Exposed can spell per dialect. A null-safe
     * equality is SYMMETRIC and this rewrite must not be: when only ONE side declares the
     * convention, the other side's NULL is a MISSING attribute on the check side, CEL raises an
     * error and denies, and only the asymmetric expansion keeps propagating UNKNOWN for it. A
     * null-safe operator would match the two NULLs and over-grant.
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
        val expression = comparable(target, value)
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

    /**
     * `haystack LIKE '<escaped needle>' ESCAPE '\'` for a needle known at translation time.
     *
     * The adapter escapes the needle itself rather than calling Exposed's `LikePattern.ofLiteral`,
     * which reads the dialect the moment it is called and so cannot run outside a transaction. The
     * escape character is declared on every pattern, and Exposed BINDS it rather than inlining it.
     */
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
        return LikeEscapeOp(haystack, stringParam(pattern), true, LikeEscaping.ESCAPE_CHAR)
    }

    /**
     * The target as the comparison sees it: cast into IEEE double space when the constant is
     * fractional (or too large for an exact `Long`) and the column is numeric.
     *
     * `intColumn >= 1.5` is legal CEL that the planner emits verbatim, and a store left to its own
     * devices compares it in exact decimal. Casting keeps the comparison in the arithmetic CEL
     * actually performs at check time, where every attribute number is a double.
     */
    private fun comparable(target: Resolution.Scalar, value: Any): Expression<*> =
        if (value is Double && ScalarColumnTypes.isNumeric(target.column)) {
            IeeeDoubleCast(target.expression)
        } else {
            target.expression
        }
}
