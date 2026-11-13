package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.AsciiReplace
import dev.cerbos.queryplan.exposed.sql.ExactString
import dev.cerbos.queryplan.exposed.sql.IeeeDoubleCast
import dev.cerbos.queryplan.exposed.sql.LikeEscaping
import dev.cerbos.queryplan.exposed.sql.NulSafeMatch
import dev.cerbos.queryplan.exposed.sql.NullLiteral
import dev.cerbos.queryplan.exposed.sql.Params
import dev.cerbos.queryplan.exposed.sql.ScalarCase
import dev.cerbos.queryplan.exposed.sql.ScalarColumnKind
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import dev.cerbos.queryplan.exposed.sql.ScalarValueFamily
import dev.cerbos.queryplan.exposed.sql.StringIntCast
import dev.cerbos.queryplan.exposed.sql.IntRemainder
import dev.cerbos.queryplan.exposed.sql.CountChars
import dev.cerbos.queryplan.exposed.sql.TextCastExpression
import dev.cerbos.queryplan.exposed.sql.TimestampBinder
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.GreaterEqOp
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.LessEqOp
import org.jetbrains.exposed.v1.core.LessOp
import org.jetbrains.exposed.v1.core.LikeEscapeOp
import org.jetbrains.exposed.v1.core.NeqOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.booleanParam
import org.jetbrains.exposed.v1.core.longParam
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

/**
 * Comparisons and string matches: everything the walk does not route by name. Ternary rewrite on
 * raw operands, normalise field-first, arity check, `size()`, then resolve each operand and
 * [dispatch] the pair. Conversion is lazy because which error a malformed operand raises depends on
 * the whole comparison.
 */
internal class ComparisonTranslator(private val translation: Translation) {

    private val arithmetic = ArithmeticTranslator(this)

    fun translate(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operator !in COMPARISON_OPERATORS && operator !in STRING_MATCH_OPERATORS) {
            throw ScalarRefusals.unsupportedOperator(operator)
        }
        translation.ternary.tryTernaryComparison(operator, operands, scope)?.let { return it }

        val normalized = NormalizedBinary.of(operator, operands)
        if (normalized.operands.size != 2) {
            throw Refusals.malformed("$operator requires exactly 2 operands, got ${normalized.operands.size}")
        }
        translation.sizes.trySizeComparison(normalized.operator, normalized.operands, scope)?.let { return it }

        return dispatch(
            normalized.operator,
            resolve(normalized.operands[0]),
            resolve(normalized.operands[1]),
            normalized.operands,
            scope,
        )
    }

    // -- the operand-resolution seam --

    private sealed interface Resolved {
        /** A plan constant; [value] converts on demand. */
        class Constant(val operand: Operand) : Resolved {
            fun value(): Any? = PlanValues.toKotlin(operand.value)
        }

        /** A mapped attribute reference; it resolves at the consuming dispatch site. */
        class Field(val variable: String) : Resolved

        /** `add(value, value)` — a pure-constant subtree, folded by [PlanValues.foldAdd]. */
        class ConstantAdd(val left: Operand, val right: Operand) : Resolved {
            fun mixesScalarTypes(): Boolean {
                val l = PlanValues.toKotlin(left.value)
                val r = PlanValues.toKotlin(right.value)
                fun scalar(v: Any?) = v == null || v is String || v is Number || v is Boolean
                if (!scalar(l) || !scalar(r)) return false
                return !((l is String && r is String) || (l is Number && r is Number))
            }

            fun fold(): Any = PlanValues.foldAdd(
                PlanValues.toKotlin(left.value),
                PlanValues.toKotlin(right.value),
            )
        }

        /** `add(field, value)` or `add(value, field)`; solved for the field under eq/ne only when exact. */
        class FieldPlusConstant(val fieldVariable: String, val constant: Operand, val fieldIsLeft: Boolean) : Resolved

        /** Any other arithmetic-rooted expression, lowered by [ArithmeticTranslator]. */
        class Arithmetic(val operator: String) : Resolved

        /** `timestamp(variable)` — a temporal column wrapped in CEL's `timestamp()` cast. */
        class TimestampField(val variable: String) : Resolved

        /** `timestamp(value)`, including the planner's folded `now()` windows. Parsed lazily. */
        class TimestampConstant(val operand: Operand) : Resolved {
            fun instant(): Instant = parseInstant(PlanValues.toKotlin(operand.value))
        }

        /** `int(variable)` — CEL's truncating conversion over a mapped column. */
        class IntCast(val variable: String) : Resolved

        /** `string(variable)` — CEL's text conversion over a mapped column. */
        class TextCast(val variable: String) : Resolved

        /**
         * `timestamp(variable) + duration(value)`, `duration(value) + timestamp(variable)` or
         * `timestamp(variable) - duration(value)`, with the signed [shift].
         */
        class ShiftedTimestampField(val variable: String, val shift: Duration) : Resolved

        /** `timestamp(variable).timeSince()`. */
        class TimeSinceField(val variable: String) : Resolved

        /** `duration(value)`, parsed lazily like a timestamp constant. */
        class DurationConstant(val operand: Operand) : Resolved {
            fun duration(): Duration = CelDuration.parse(PlanValues.toKotlin(operand.value))
        }

        /** `variable.upperAscii()`. */
        class UpperAsciiField(val variable: String) : Resolved

        /** A `list(...)` or `struct(...)` the planner built from constants alone. */
        class BuiltConstant(val value: Any) : Resolved

        /** An operand no leaf comparison understands; [leafOperandError] reports from the raw shape. */
        object Opaque : Resolved
    }

    /** THE operand-resolution seam: classify one comparison operand. */
    private fun resolve(operand: Operand): Resolved = when (operand.nodeCase) {
        Operand.NodeCase.VALUE -> Resolved.Constant(operand)
        Operand.NodeCase.VARIABLE -> Resolved.Field(operand.variable)
        Operand.NodeCase.EXPRESSION -> {
            val expression = operand.expression
            val inner = expression.operator
            when {
                // timestamp(variable) / timestamp(value) are the only shapes the planner emits for
                // a temporal comparison. A nested expression inside timestamp() has no verified
                // translation and stays Opaque.
                inner == "timestamp" && expression.operandsCount == 1 -> {
                    val argument = expression.getOperands(0)
                    when (argument.nodeCase) {
                        Operand.NodeCase.VARIABLE -> Resolved.TimestampField(argument.variable)
                        Operand.NodeCase.VALUE -> Resolved.TimestampConstant(argument)
                        else -> Resolved.Opaque
                    }
                }
                inner == "duration" && expression.operandsCount == 1 &&
                    expression.getOperands(0).nodeCase == Operand.NodeCase.VALUE ->
                    Resolved.DurationConstant(expression.getOperands(0))
                inner == "timeSince" && expression.operandsCount == 1 ->
                    timestampOfVariable(expression.getOperands(0))?.let { Resolved.TimeSinceField(it) }
                        ?: Resolved.Opaque
                inner == "upperAscii" && expression.operandsCount == 1 &&
                    expression.getOperands(0).nodeCase == Operand.NodeCase.VARIABLE ->
                    Resolved.UpperAsciiField(expression.getOperands(0).variable)
                (inner == "add" || inner == "sub") && expression.operandsCount == 2 &&
                    shiftedTimestamp(inner, expression.getOperands(0), expression.getOperands(1)) != null ->
                    shiftedTimestamp(inner, expression.getOperands(0), expression.getOperands(1))!!
                inner == "int" && expression.operandsCount == 1 &&
                    expression.getOperands(0).nodeCase == Operand.NodeCase.VARIABLE ->
                    Resolved.IntCast(expression.getOperands(0).variable)
                inner == "string" && expression.operandsCount == 1 &&
                    expression.getOperands(0).nodeCase == Operand.NodeCase.VARIABLE ->
                    Resolved.TextCast(expression.getOperands(0).variable)
                (inner == "list" || inner == "struct") &&
                    PlanValues.builtConstant(operand).let { it !== PlanValues.NotConstant && it != null } ->
                    Resolved.BuiltConstant(PlanValues.builtConstant(operand)!!)
                inner !in ArithmeticTranslator.ARITHMETIC_OPS -> Resolved.Opaque
                inner == "add" && expression.operandsCount == 2 -> {
                    val left = expression.getOperands(0)
                    val right = expression.getOperands(1)
                    val leftIsValue = left.nodeCase == Operand.NodeCase.VALUE
                    val rightIsValue = right.nodeCase == Operand.NodeCase.VALUE
                    when {
                        leftIsValue && rightIsValue -> Resolved.ConstantAdd(left, right)
                        left.nodeCase == Operand.NodeCase.VARIABLE && rightIsValue ->
                            Resolved.FieldPlusConstant(left.variable, right, true)
                        leftIsValue && right.nodeCase == Operand.NodeCase.VARIABLE ->
                            Resolved.FieldPlusConstant(right.variable, left, false)
                        else -> Resolved.Arithmetic(inner)
                    }
                }
                else -> Resolved.Arithmetic(inner)
            }
        }
        else -> Resolved.Opaque
    }

    /** The variable of `timestamp(variable)`, else `null`. */
    private fun timestampOfVariable(operand: Operand): String? {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator != "timestamp" ||
            operand.expression.operandsCount != 1 ||
            operand.expression.getOperands(0).nodeCase != Operand.NodeCase.VARIABLE
        ) {
            return null
        }
        return operand.expression.getOperands(0).variable
    }

    /** The constant of `duration(value)`, else `null`. */
    private fun durationOperand(operand: Operand): Operand? {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator != "duration" ||
            operand.expression.operandsCount != 1 ||
            operand.expression.getOperands(0).nodeCase != Operand.NodeCase.VALUE
        ) {
            return null
        }
        return operand.expression.getOperands(0)
    }

    private fun shiftedTimestamp(operator: String, left: Operand, right: Operand): Resolved.ShiftedTimestampField? {
        val leftVariable = timestampOfVariable(left)
        val rightDuration = durationOperand(right)
        if (leftVariable != null && rightDuration != null) {
            val shift = CelDuration.parse(PlanValues.toKotlin(rightDuration.value))
            return Resolved.ShiftedTimestampField(leftVariable, if (operator == "sub") shift.negated() else shift)
        }
        val rightVariable = timestampOfVariable(right)
        val leftDuration = durationOperand(left)
        // duration - timestamp has no CEL overload.
        if (operator == "add" && rightVariable != null && leftDuration != null) {
            return Resolved.ShiftedTimestampField(
                rightVariable,
                CelDuration.parse(PlanValues.toKotlin(leftDuration.value)),
            )
        }
        return null
    }

    private fun isAddRooted(resolved: Resolved): Boolean =
        resolved is Resolved.ConstantAdd ||
            resolved is Resolved.FieldPlusConstant ||
            (resolved is Resolved.Arithmetic && resolved.operator == "add")

    private fun isArithmeticRooted(resolved: Resolved): Boolean =
        resolved is Resolved.ConstantAdd ||
            resolved is Resolved.FieldPlusConstant ||
            resolved is Resolved.Arithmetic

    // -- dispatch on the resolved pair --

    @Suppress("ReturnCount")
    private fun dispatch(
        operator: String,
        left: Resolved,
        right: Resolved,
        operands: List<Operand>,
        scope: Scope,
    ): Op<Boolean> {
        // Constant against constant is evaluated here. The planner never emits one directly, but
        // ternary substitution produces them: the else branch of `(aBool ? aNumber : 0) > 0`
        // becomes gt(value(0), value(0)).
        if (operator in COMPARISON_OPERATORS && left is Resolved.Constant && right is Resolved.Constant) {
            return constantComparison(operator, left.value(), right.value())
        }
        // `+` over a null or a string beside a number has no CEL overload: the comparison raises.
        if (listOf(left, right).any { it is Resolved.ConstantAdd && it.mixesScalarTypes() }) return TriLogic.unknown()

        // Constant-receiver string matches: `"a,b".contains(R.attr.x)` arrives as
        // contains(value, variable) — the CONSTANT is the haystack and the COLUMN the needle, and
        // NormalizedBinary deliberately leaves these in source order. An unfolded concat receiver
        // folds here too, NOT into the add-solve path, which would translate the INVERTED
        // column-haystack LIKE.
        if (operator in STRING_MATCH_OPERATORS && right is Resolved.Field) {
            val receiver = when (left) {
                is Resolved.Constant -> left.value()
                is Resolved.ConstantAdd -> left.fold()
                else -> null
            }
            if (receiver != null) {
                if (receiver !is String) {
                    // `5.contains(x)` has no overload in CEL; the planner never folds one.
                    throw Refusals.malformed(
                        "$operator requires a string receiver, got ${PlanValues.typeName(receiver)}",
                    )
                }
                val target = scope.scalar(right.variable)
                // The column is the NEEDLE here, and a needle is escaped and concatenated into a
                // LIKE pattern, so it has to be text for exactly the reason a haystack does.
                if (translation.leaf.lacksTextOverload(target)) return TriLogic.unknown()
                translation.leaf.requireText(operator, target)
                val needle = target.expression
                return when (operator) {
                    "contains" -> columnNeedleMatch(ExactString.param(receiver), needle, true, true)
                    "startsWith" -> columnNeedleMatch(ExactString.param(receiver), needle, false, true)
                    "endsWith" -> columnNeedleMatch(ExactString.param(receiver), needle, true, false)
                    else -> throw Refusals.internal("Unsupported string-match operator: $operator")
                }
            }
            // A NULL receiver constant is not a haystack; fall through so the null-constant leaf
            // branch below owns the message.
        }

        if (operator in COMPARISON_OPERATORS) {
            // timestamp(field) against timestamp(constant) — the wire shape of every time-window
            // policy. NormalizedBinary cannot reorder these (both operands are EXPRESSION nodes,
            // equal rank), so the value-first form is MIRRORED here, never inverted.
            if (left is Resolved.TimestampField && right is Resolved.TimestampConstant) {
                return timestampLeaf(operator, left, right, scope)
            }
            if (left is Resolved.TimestampConstant && right is Resolved.TimestampField) {
                return timestampLeaf(NormalizedBinary.mirror(operator), right, left, scope)
            }
            // Two constant instants — reachable through ternary substitution, like the numeric
            // fold above. Instant comparison is total and exact.
            if (left is Resolved.TimestampConstant && right is Resolved.TimestampConstant) {
                return constantInstantComparison(operator, left.instant(), right.instant())
            }
            if (left is Resolved.TimestampField && right is Resolved.TimestampField) {
                return timestampFieldPair(operator, left.variable, right.variable, scope)
            }
            if (left is Resolved.ShiftedTimestampField && right is Resolved.TimestampConstant) {
                return shiftedTimestampLeaf(operator, left, right.instant(), scope)
            }
            if (left is Resolved.TimestampConstant && right is Resolved.ShiftedTimestampField) {
                return shiftedTimestampLeaf(NormalizedBinary.mirror(operator), right, left.instant(), scope)
            }
            if (left is Resolved.TimeSinceField && right is Resolved.DurationConstant) {
                return timeSinceLeaf(operator, left, right.duration(), scope)
            }
            if (left is Resolved.DurationConstant && right is Resolved.TimeSinceField) {
                return timeSinceLeaf(NormalizedBinary.mirror(operator), right, left.duration(), scope)
            }
            // An attribute is never a CEL timestamp: check() receives JSON values, so a string,
            // number, bool, list or map ordered against a timestamp has no overload and raises,
            // whatever the mapped column's type. The comparison is UNKNOWN on every row.
            val bareField = (left as? Resolved.Field) ?: (right as? Resolved.Field)
            if (operator in ORDERING_OPERATORS && bareField != null &&
                (left is Resolved.TimestampConstant || right is Resolved.TimestampConstant)
            ) {
                // Resolved for its refusal: an unmapped attribute is still the caller's to fix.
                scope.resolve(bareField.variable)
                return TriLogic.unknown()
            }
            if (left is Resolved.UpperAsciiField && right is Resolved.Constant) {
                return upperAsciiComparison(operator, left, right.value(), scope)
            }
            if (left is Resolved.Constant && right is Resolved.UpperAsciiField) {
                return upperAsciiComparison(NormalizedBinary.mirror(operator), right, left.value(), scope)
            }
            if (left is Resolved.TextCast || right is Resolved.TextCast) {
                return textCastComparison(operator, left, right, operands, scope)
            }
            // Fold: `field op add(value, value)`. The folded constant compares like any plan
            // constant, and normalisation guarantees the field arrives first. Strings concatenate
            // here, matching CEL: this shape never enters double space.
            if (left is Resolved.Field && right is Resolved.ConstantAdd) {
                return translation.leaf.applyLeaf(operator, scope.scalar(left.variable), right.fold())
            }
            // Solve: `add(field, const) eq/ne constant`, for the ALGEBRAICALLY EXACT shapes only.
            // Fractional or oversized numeric pairs fall through to the SQL lowering, because IEEE
            // subtraction does not invert IEEE addition and a solve here would return rows the
            // PDP's check() denies.
            if ((operator == "eq" || operator == "ne") &&
                left is Resolved.FieldPlusConstant &&
                right is Resolved.Constant &&
                !PlanValues.requiresSqlLowering(right.value(), PlanValues.toKotlin(left.constant.value))
            ) {
                return solveAddComparison(operator, left, right, scope)
            }
            printedIntComparison(operator, operands, scope)?.let { return it }
            intChainComparison(operator, operands, scope)?.let { return it }
            remainderComparison(operator, operands, scope)?.let { return it }
            if (isArithmeticRooted(left) || isArithmeticRooted(right)) {
                // CEL overloads `+` on strings, so an add-rooted operand is only arithmetic once
                // the mapped column types say it is not a concatenation.
                if (ConcatTranslator.isConcatenation(operands[0], scope) ||
                    ConcatTranslator.isConcatenation(operands[1], scope)
                ) {
                    if (operands.any { ConcatTranslator.mixesNonText(it, scope) }) return TriLogic.unknown()
                    return compare(
                        operator,
                        ConcatTranslator.textExpression(operands[0], scope),
                        ConcatTranslator.textExpression(operands[1], scope),
                    )
                }
                if (operands.any(::alwaysRaises)) return TriLogic.unknown()
                if (orderedAgainstText(operator, left, right, operands, scope)) return TriLogic.unknown()
                return arithmetic.numericComparison(operator, operands, scope)
            }
        } else if (isAddRooted(left) || isAddRooted(right)) {
            // `add` under a string match: only the constant fold against a field translates.
            return addFoldOrError(operator, operands, scope)
        }

        if (left is Resolved.Field && right is Resolved.Field) {
            return fieldToFieldComparison(operator, left.variable, right.variable, scope)
        }

        // The ordinary scalar leaf: one mapped column against one plan constant. Order-insensitive
        // on purpose — receiver-sensitive operators are never normalised, so a null receiver
        // arrives value-first and must still reach the null-constant message.
        val field = (left as? Resolved.Field) ?: (right as? Resolved.Field)
        val constant = (left as? Resolved.Constant) ?: (right as? Resolved.Constant)
        if (field != null && constant != null) {
            return leafFieldValue(operator, field, constant.value(), scope)
        }
        if (operator in COMPARISON_OPERATORS) {
            if (left is Resolved.IntCast && right is Resolved.Constant) {
                return intCastComparison(operator, left.variable, right.value(), scope)
            }
            if (left is Resolved.Constant && right is Resolved.IntCast) {
                return intCastComparison(NormalizedBinary.mirror(operator), right.variable, left.value(), scope)
            }
        }
        val built = (left as? Resolved.BuiltConstant) ?: (right as? Resolved.BuiltConstant)
        if (field != null && built != null) {
            return leafFieldValue(operator, field, built.value, scope)
        }

        throw leafOperandError(operator, operands)
    }

    /**
     * Whether a text column is ordered against a column plus or minus a finite number. CEL has no
     * string-number ordering, so it raises on every row; one finite constant under `+`/`-` cannot be NaN.
     */
    private fun orderedAgainstText(
        operator: String,
        left: Resolved,
        right: Resolved,
        operands: List<Operand>,
        scope: Scope,
    ): Boolean {
        if (operator !in ORDERING_OPERATORS) return false
        val (field, arithmetic) = when {
            left is Resolved.Field && isArithmeticRooted(right) -> left to operands[1]
            right is Resolved.Field && isArithmeticRooted(left) -> right to operands[0]
            else -> return false
        }
        if (!isColumnShiftedByFiniteConstant(arithmetic)) return false
        val target = scope.resolve(field.variable) as? Resolution.Scalar ?: return false
        return ScalarColumnTypes.kindOf(target.column) == ScalarColumnKind.TEXT
    }

    /** `add` or `sub` of exactly one variable and one finite number constant, in either order. */
    private fun isColumnShiftedByFiniteConstant(operand: Operand): Boolean {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operandsCount != 2) return false
        if (operand.expression.operator != "add" && operand.expression.operator != "sub") return false
        val kinds = operand.expression.operandsList
        val variables = kinds.count { it.nodeCase == Operand.NodeCase.VARIABLE }
        val constants = kinds.count {
            it.nodeCase == Operand.NodeCase.VALUE && it.value.kindCase == com.google.protobuf.Value.KindCase.NUMBER_VALUE &&
                it.value.numberValue.isFinite()
        }
        return variables == 1 && constants == 1
    }

    /**
     * `timestamp(field) + shift op c`, solved as `timestamp(field) op c - shift`. A column within `shift`
     * of CEL's year-1..9999 range is UNKNOWN; a solved constant outside it is refused.
     */
    private fun shiftedTimestampLeaf(
        operator: String,
        shifted: Resolved.ShiftedTimestampField,
        constant: Instant,
        scope: Scope,
    ): Op<Boolean> {
        val shift = shifted.shift
        val solved = constant.minus(shift)
        if (solved < MIN_CEL_INSTANT || solved > MAX_CEL_INSTANT) {
            throw Refusals.unsupported(
                "timestamp() + duration() compared with a timestamp whose solved bound leaves CEL's " +
                    "timestamp range (years 1 to 9999)",
            )
        }
        val target = scope.scalar(shifted.variable)
        val base = compare(operator, target.expression, TimestampBinder.bind(solved, target.column, shifted.variable))
        if (shift.isZero) return base
        // The column values whose sum overflows: past MAX - shift, or before MIN - shift.
        val overflow = if (shift.isNegative) {
            LessOp(
                target.expression,
                TimestampBinder.bind(MIN_CEL_INSTANT.minus(shift), target.column, shifted.variable),
            )
        } else {
            GreaterOp(
                target.expression,
                TimestampBinder.bind(MAX_CEL_INSTANT.minus(shift), target.column, shifted.variable),
            )
        }
        return TriLogic.baseUnlessUnknown(base, overflow)
    }

    /**
     * `timestamp(field).timeSince() op d`, solved as `field op' now - d`. `now` is read when the filter
     * is built.
     */
    private fun timeSinceLeaf(
        operator: String,
        since: Resolved.TimeSinceField,
        duration: Duration,
        scope: Scope,
    ): Op<Boolean> {
        val bound = Instant.now().minus(duration)
        if (bound < MIN_CEL_INSTANT || bound > MAX_CEL_INSTANT) {
            throw Refusals.unsupported(
                "timeSince() compared with a duration whose solved bound leaves CEL's timestamp " +
                    "range (years 1 to 9999)",
            )
        }
        val target = scope.scalar(since.variable)
        return compare(
            NormalizedBinary.mirror(operator),
            target.expression,
            TimestampBinder.bind(bound, target.column, since.variable),
        )
    }

    /**
     * `column.upperAscii() ==/!= "text"`. SQL `UPPER` folds beyond ASCII, so the fold is 26 nested
     * case-sensitive `REPLACE`s. An ordering is refused: it would compare under the collation.
     */
    private fun upperAsciiComparison(
        operator: String,
        field: Resolved.UpperAsciiField,
        constant: Any?,
        scope: Scope,
    ): Op<Boolean> {
        val target = scope.scalar(field.variable)
        if (translation.leaf.lacksTextOverload(target)) return TriLogic.unknown()
        translation.leaf.requireText("upperAscii()", target)
        if (operator != "eq" && operator != "ne") {
            throw Refusals.unsupported(
                "upperAscii() is translated only under == and !=: an ordering over the folded text " +
                    "would compare under the store's collation, not by code point as CEL does",
            )
        }
        if (constant !is String) {
            val never = TriLogic.baseUnlessUnknown(Op.FALSE, IsNullOp(target.expression))
            return if (operator == "eq") never else TriLogic.not(never)
        }
        var folded: Expression<*> = target.expression
        for (lower in 'a'..'z') {
            folded = AsciiReplace(folded, lower.toString(), lower.uppercaseChar().toString())
        }
        return compare(operator, folded, ExactString.param(constant))
    }

    /**
     * Whether an arithmetic operand raises on every row: Cerbos sends attribute numbers as doubles, so
     * `attr % n` and `int(...) + attr` have no overload. Only a bare attribute is known to be a double.
     */
    private fun alwaysRaises(operand: Operand): Boolean {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION) return false
        val expression = operand.expression
        val operands = expression.operandsList
        val attribute = operands.any { it.nodeCase == Operand.NodeCase.VARIABLE }
        val intConversion = operands.any {
            it.nodeCase == Operand.NodeCase.EXPRESSION && it.expression.operator == "int"
        }
        return when (expression.operator) {
            "mod" -> attribute || operands.any(::alwaysRaises)
            "add", "sub", "mult", "div" -> (attribute && intConversion) || operands.any(::alwaysRaises)
            else -> false
        }
    }

    /**
     * `int(column) op constant` over a numeric column, solved for the column because CEL's `int()`
     * truncates and raises beyond ±2^63 or on NaN, where SQL casts round. Bounds stay within ±2^53 so
     * the double comparison is exact; anything else is refused.
     */
    private fun intCastComparison(operator: String, variable: String, value: Any?, scope: Scope): Op<Boolean> {
        val target = scope.scalar(variable)
        if (ScalarColumnTypes.familyOf(target.column) == ScalarValueFamily.TEXT) {
            return stringIntComparison(operator, target, value)
        }
        if (!ScalarColumnTypes.isNumeric(target.column)) throw ScalarRefusals.numericCastUnsupported("int")
        val constant = (value as? Number)?.toDouble() ?: throw ScalarRefusals.numericCastUnsupported("int")
        if (!constant.isFinite() || Math.abs(constant) > EXACT_DOUBLE_INTEGER) {
            throw ScalarRefusals.numericCastUnsupported("int")
        }
        val x = IeeeDoubleCast(target.expression)
        fun bound(m: Double) = Params.of(m)
        fun atLeast(m: Double): Op<Boolean> = if (m >= 1) GreaterEqOp(x, bound(m)) else GreaterOp(x, bound(m - 1))
        fun atMost(m: Double): Op<Boolean> = if (m <= -1) LessEqOp(x, bound(m)) else LessOp(x, bound(m + 1))
        val whole = constant == Math.rint(constant)
        val base: Op<Boolean> = when (operator) {
            "eq" -> if (whole) TriLogic.and(atLeast(constant), atMost(constant)) else Op.FALSE
            "ne" -> if (whole) TriLogic.not(TriLogic.and(atLeast(constant), atMost(constant))) else Op.TRUE
            "gt" -> atLeast(Math.floor(constant) + 1)
            "ge" -> atLeast(Math.ceil(constant))
            "lt" -> atMost(Math.ceil(constant) - 1)
            "le" -> atMost(Math.floor(constant))
            else -> throw Refusals.internal("Unsupported int() comparison operator: $operator")
        }
        val inRange = TriLogic.and(GreaterOp(x, bound(-INT64_BOUND)), LessOp(x, bound(INT64_BOUND)))
        return TriLogic.baseUnlessUnknown(base, TriLogic.not(inRange))
    }

    /**
     * `int(x) % c op k` over an integer or text column, or `size(s) % c op k` over a text column,
     * for a constant `c` other than 0 and -1. A NULL or an unreadable `int()` stays UNKNOWN.
     */
    private fun remainderComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        for (side in 0..1) {
            val mod = operands[side]
            val bound = operands[1 - side]
            if (mod.nodeCase != Operand.NodeCase.EXPRESSION || mod.expression.operator != "mod" ||
                mod.expression.operandsCount != 2 || bound.nodeCase != Operand.NodeCase.VALUE ||
                bound.value.kindCase != com.google.protobuf.Value.KindCase.NUMBER_VALUE
            ) {
                continue
            }
            val (dividend, divisorOperand) = mod.expression.operandsList
            if (divisorOperand.nodeCase != Operand.NodeCase.VALUE ||
                divisorOperand.value.kindCase != com.google.protobuf.Value.KindCase.NUMBER_VALUE
            ) {
                continue
            }
            val divisor = divisorOperand.value.numberValue
            if (divisor != Math.rint(divisor) || Math.abs(divisor) > EXACT_DOUBLE_INTEGER || divisor == 0.0 ||
                divisor == -1.0 || dividend.nodeCase != Operand.NodeCase.EXPRESSION ||
                dividend.expression.operandsCount != 1 ||
                dividend.expression.getOperands(0).nodeCase != Operand.NodeCase.VARIABLE
            ) {
                continue
            }
            val target = scope.scalar(dividend.expression.getOperands(0).variable)
            val family = ScalarColumnTypes.familyOf(target.column)
            val integer: org.jetbrains.exposed.v1.core.Expression<*> = when (dividend.expression.operator) {
                "int" -> when {
                    family == ScalarValueFamily.TEXT -> StringIntCast(target.expression)
                    ScalarColumnTypes.kindOf(target.column) == ScalarColumnKind.INTEGRAL -> target.expression
                    else -> continue
                }
                "size" -> {
                    if (translation.leaf.lacksTextOverload(target)) return TriLogic.unknown()
                    if (family != ScalarValueFamily.TEXT) continue
                    CountChars(target.expression)
                }
                else -> continue
            }
            val r = IntRemainder(integer, divisor.toLong())
            val k = bound.value.numberValue
            val fieldOperator = if (side == 0) operator else NormalizedBinary.mirror(operator)
            if (k.isNaN() || Math.abs(k) > EXACT_DOUBLE_INTEGER) continue
            fun at(m: Double) = longParam(m.toLong())
            val whole = k == Math.rint(k)
            val base: Op<Boolean> = when (fieldOperator) {
                "eq" -> if (whole) EqOp(r, at(k)) else Op.FALSE
                "ne" -> if (whole) NeqOp(r, at(k)) else Op.TRUE
                "gt" -> GreaterEqOp(r, at(Math.floor(k) + 1))
                "ge" -> GreaterEqOp(r, at(Math.ceil(k)))
                "lt" -> LessEqOp(r, at(Math.ceil(k) - 1))
                "le" -> LessEqOp(r, at(Math.floor(k)))
                else -> continue
            }
            return TriLogic.baseUnlessUnknown(base, IsNullOp(r))
        }
        return null
    }

    /** An int chain (`int(x) / 2`, `int(x) * 2 + 1`) against a number. */
    private fun intChainComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        for (side in 0..1) {
            val bound = operands[1 - side]
            val chain = IntChain.of(operands[side]) ?: continue
            if (operands[side].expression.operator == "int" || bound.nodeCase != Operand.NodeCase.VALUE ||
                bound.value.kindCase != com.google.protobuf.Value.KindCase.NUMBER_VALUE ||
                !bound.value.numberValue.isFinite()
            ) {
                continue
            }
            val fieldOperator = if (side == 0) operator else NormalizedBinary.mirror(operator)
            return chain.compare(fieldOperator, java.math.BigDecimal(bound.value.numberValue), scope)
        }
        return null
    }

    /**
     * `string(int(x)) == "n"`: CEL prints an int in plain decimal, so it is `int(x) == n`, and any
     * other spelling is never equal.
     */
    private fun printedIntComparison(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean>? {
        if (operator != "eq" && operator != "ne") return null
        for (side in 0..1) {
            val printed = operands[side]
            val bound = operands[1 - side]
            if (bound.nodeCase != Operand.NodeCase.VALUE ||
                bound.value.kindCase != com.google.protobuf.Value.KindCase.STRING_VALUE ||
                printed.nodeCase != Operand.NodeCase.EXPRESSION || printed.expression.operator != "string" ||
                printed.expression.operandsCount != 1
            ) {
                continue
            }
            val cast = printed.expression.getOperands(0)
            if (cast.nodeCase != Operand.NodeCase.EXPRESSION || cast.expression.operator != "int" ||
                cast.expression.operandsCount != 1 ||
                cast.expression.getOperands(0).nodeCase != Operand.NodeCase.VARIABLE
            ) {
                continue
            }
            val column = IntOfColumn(scope.scalar(cast.expression.getOperands(0).variable))
            val text = bound.value.stringValue
            val n = if (CANONICAL_INT.matches(text)) text.toBigIntegerOrNull() else null
            val equal = if (n != null && n.bitLength() < 64) column.equalTo(n) else column.never()
            return if (operator == "eq") equal else TriLogic.not(equal)
        }
        return null
    }

    /** `int(text column) op value`, compared as 64-bit integers; UNKNOWN where the string is not one. */
    private fun stringIntComparison(operator: String, target: Resolution.Scalar, value: Any?): Op<Boolean> {
        val constant = (value as? Number)?.toDouble() ?: throw ScalarRefusals.numericCastUnsupported("int")
        val x = StringIntCast(target.expression)
        fun bound(m: Double) = longParam(m.toLong())
        val base: Op<Boolean> = when {
            constant.isNaN() -> if (operator == "ne") Op.TRUE else Op.FALSE
            constant >= INT64_BOUND -> if (operator in setOf("ne", "lt", "le")) Op.TRUE else Op.FALSE
            constant < -INT64_BOUND -> if (operator in setOf("ne", "gt", "ge")) Op.TRUE else Op.FALSE
            else -> {
                val whole = constant == Math.rint(constant)
                when (operator) {
                    "eq" -> if (whole) EqOp(x, bound(constant)) else Op.FALSE
                    "ne" -> if (whole) NeqOp(x, bound(constant)) else Op.TRUE
                    "gt" -> GreaterEqOp(x, bound(Math.floor(constant) + 1))
                    "ge" -> GreaterEqOp(x, bound(Math.ceil(constant)))
                    "lt" -> LessEqOp(x, bound(Math.ceil(constant) - 1))
                    "le" -> LessEqOp(x, bound(Math.floor(constant)))
                    else -> throw Refusals.internal("Unsupported int() comparison operator: $operator")
                }
            }
        }
        return TriLogic.baseUnlessUnknown(base, IsNullOp(x))
    }

    /** `field op value`, or value-first for the operators normalisation leaves alone. */
    private fun leafFieldValue(
        operator: String,
        field: Resolved.Field,
        value: Any?,
        scope: Scope,
    ): Op<Boolean> {
        // A list or map against a relation-mapped attribute is whole-list equality, refused with
        // its own message rather than the generic "is mapped as a relation" one. Against a scalar
        // column it is a type mismatch [LeafTranslator.applyLeaf] answers from the types.
        if (value is List<*> || value is Map<*, *>) {
            val resolution = scope.resolve(field.variable)
            if (resolution !is Resolution.Scalar) {
                throw ScalarRefusals.structuredConstant(operator, field.variable, value)
            }
            return translation.leaf.applyLeaf(operator, resolution, value)
        }
        return translation.leaf.applyLeaf(operator, scope.scalar(field.variable), value)
    }

    /** `timestamp(field) op timestamp(constant)`. [operator] is already field-first; a NULL column is UNKNOWN. */
    private fun timestampLeaf(
        operator: String,
        field: Resolved.TimestampField,
        constant: Resolved.TimestampConstant,
        scope: Scope,
    ): Op<Boolean> {
        val instant = constant.instant()
        val target = scope.scalar(field.variable)
        return compare(operator, target.expression, TimestampBinder.bind(instant, target.column, field.variable))
    }

    /**
     * CEL's `string()` over a mapped column. A boolean lowers to a `CASE` with its own `IS NULL` arm,
     * because SQLite and MySQL cast it to `1`/`0`.
     */
    private fun textCastComparison(
        operator: String,
        left: Resolved,
        right: Resolved,
        operands: List<Operand>,
        scope: Scope,
    ): Op<Boolean> {
        solvedTextCastEquality(operator, left, right, scope)?.let { return it }
        val leftExpression = textCastOperand(left, operands[0], scope)
        val rightExpression = textCastOperand(right, operands[1], scope)
        return compare(operator, leftExpression, rightExpression)
    }

    /**
     * `string(column) ==/!= "literal"` over a double or boolean column, solved for the column: exactly
     * one double prints as a given CEL string. A zero is refused (CEL prints `-0.0` as `-0`). `null` when
     * the shape is not this one.
     */
    private fun solvedTextCastEquality(
        operator: String,
        left: Resolved,
        right: Resolved,
        scope: Scope,
    ): Op<Boolean>? {
        if (operator != "eq" && operator != "ne") return null
        val (cast, constant) = when {
            left is Resolved.TextCast && right is Resolved.Constant -> left to right
            right is Resolved.TextCast && left is Resolved.Constant -> right to left
            else -> return null
        }
        val target = scope.scalar(cast.variable)
        val kind = ScalarColumnTypes.kindOf(target.column)
        if (kind != ScalarColumnKind.FLOATING && kind != ScalarColumnKind.BOOLEAN) return null
        val text = constant.value() as? String ?: return null
        val solved: Any? = if (kind == ScalarColumnKind.BOOLEAN) {
            // CEL prints exactly "true" and "false". Solving rather than comparing the rendered
            // word keeps the comparison out of the store's collation: under MySQL's client-side
            // prepared statements a bound word lands in the connection's case-insensitive
            // collation, and `'true' = 'True'` would hold.
            when (text) {
                "true" -> true
                "false" -> false
                else -> null
            }
        } else {
            CelDoubleText.parseCanonical(text)
        }
        if (solved == null) {
            // FALSE for a present value and UNKNOWN for a NULL one; `ne` is its negation.
            val never = TriLogic.and(IsNullOp(target.expression), TriLogic.unknown())
            return if (operator == "eq") never else TriLogic.not(never)
        }
        // Smart-cast so the comparison is IEEE: a boxed -0.0 does not equal 0.0.
        if (solved is Double && solved == 0.0) throw ScalarRefusals.textCastUnsupported(ScalarColumnTypes.describe(target.column))
        return compare(operator, target.expression, Params.of(solved))
    }

    private fun textCastOperand(resolved: Resolved, operand: Operand, scope: Scope): Expression<*> = when (resolved) {
        is Resolved.TextCast -> {
            val target = scope.scalar(resolved.variable)
            when (ScalarColumnTypes.kindOf(target.column)) {
                ScalarColumnKind.BOOLEAN -> ScalarCase(
                    listOf(
                        IsNullOp(target.expression) to NullLiteral,
                        EqOp(target.expression, booleanParam(true)) to ExactString.param("true"),
                    ),
                    ExactString.param("false"),
                )
                // string() of a string is the identity, so a text column is compared as itself.
                // A CAST would be worse than redundant: MySQL gives `CAST(x AS CHAR)` the
                // CONNECTION's collation, not the column's, so the byte-exact collation the
                // column declares is lost and `'Set' = 'set'` becomes true
                // (`cast-not-string-missing` on MySQL).
                ScalarColumnKind.TEXT -> target.expression
                // An integer column holds only integral values, which CEL prints in plain decimal
                // below 1e21, and digits and a sign have no case, so the collation a cast lands
                // in cannot matter. A floating-point column is refused: see textCastUnsupported.
                ScalarColumnKind.INTEGRAL -> TextCastExpression(target.expression)
                else -> throw ScalarRefusals.textCastUnsupported(ScalarColumnTypes.describe(target.column))
            }
        }
        is Resolved.Constant -> {
            val value = resolved.value()
            if (value == null) throw Refusals.unsupported("string() cannot be compared against a null constant")
            Params.of(value)
        }
        // The other side of the comparison may be a plain column, which stands for its own text.
        // Only a text one: a numeric or temporal column compared against a rendered string would
        // be coerced by the store rather than compared as CEL compares two strings.
        is Resolved.Field -> {
            val target = scope.scalar(resolved.variable)
            if (ScalarColumnTypes.kindOf(target.column) != ScalarColumnKind.TEXT) {
                throw Refusals.unmapped(
                    "string() compared against '${resolved.variable}' requires a text column, but " +
                        "it maps to a ${ScalarColumnTypes.describe(target.column)} column",
                )
            }
            target.expression
        }
        else -> throw leafOperandError("string", listOf(operand))
    }

    /**
     * `timestamp(field) op timestamp(field)`, the only spelling that compares two temporal columns (a
     * bare comparison is CEL string comparison). Both columns must pin an absolute instant in the same
     * representation, since a store resolves `timestamp` against `timestamptz` through the session zone.
     */
    private fun timestampFieldPair(
        operator: String,
        leftVariable: String,
        rightVariable: String,
        scope: Scope,
    ): Op<Boolean> {
        val left = scope.scalar(leftVariable)
        val right = scope.scalar(rightVariable)
        if (!TimestampBinder.storesAbsoluteInstant(left.column)) TimestampBinder.ambiguous(left.column, leftVariable)
        if (!TimestampBinder.storesAbsoluteInstant(right.column)) {
            TimestampBinder.ambiguous(right.column, rightVariable)
        }
        if (!TimestampBinder.sameRepresentation(left.column, right.column)) {
            throw ScalarRefusals.timestampRepresentationMismatch(
                operator,
                leftVariable,
                left.column,
                rightVariable,
                right.column,
            )
        }
        return compare(operator, left.expression, right.expression)
    }

    /** Two constant instants, decided here. */
    private fun constantInstantComparison(operator: String, left: Instant, right: Instant): Op<Boolean> {
        val comparison = left.compareTo(right)
        val result = when (operator) {
            "eq" -> comparison == 0
            "ne" -> comparison != 0
            "lt" -> comparison < 0
            "gt" -> comparison > 0
            "le" -> comparison <= 0
            "ge" -> comparison >= 0
            else -> throw Refusals.internal("Unsupported constant timestamp comparison operator: $operator")
        }
        return if (result) Op.TRUE else Op.FALSE
    }

    /**
     * Solves `add(field, const) eq/ne constant` for the field. With no solution both arms keep the
     * `IS NULL` witness, because a missing attribute makes the concatenation raise under both polarities.
     */
    private fun solveAddComparison(
        operator: String,
        addition: Resolved.FieldPlusConstant,
        other: Resolved.Constant,
        scope: Scope,
    ): Op<Boolean> {
        val target = scope.scalar(addition.fieldVariable)
        val solved = PlanValues.solveAdd(
            other.value(),
            PlanValues.toKotlin(addition.constant.value),
            addition.fieldIsLeft,
        )
        if (solved == null) {
            return TriLogic.baseUnlessUnknown(
                if (operator == "eq") Op.FALSE else Op.TRUE,
                IsNullOp(target.expression),
            )
        }
        val leaf = translation.leaf.applyLeaf(operator, target, solved)
        // null + c is a CEL error, where the definite equality would answer for the null.
        if (!translation.leaf.isExplicitNull(target)) return leaf
        return TriLogic.baseUnlessUnknown(leaf, IsNullOp(target.expression))
    }

    /**
     * `add` under a string match. The only translatable shape is the constant fold against a
     * field; the rest report the add-specific shape errors, matching the raw operand layout
     * (either side may hold the `add`).
     */
    private fun addFoldOrError(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> {
        val additions = operands.filter {
            it.nodeCase == Operand.NodeCase.EXPRESSION && it.expression.operator == "add"
        }
        val others = operands - additions.toSet()
        val other = others.firstOrNull() ?: throw Refusals.unsupported(
            "$operator between two add() expressions is not supported: got " +
                "${ScalarRefusals.describeOperand(operands[0])} and " +
                "${ScalarRefusals.describeOperand(operands[1])}; one operand must be a mapped " +
                "attribute or constant",
        )
        val addOperands = additions.last().expression.operandsList
        if (addOperands.size != 2) throw Refusals.malformed("add requires exactly 2 operands")
        if (addOperands[0].nodeCase == Operand.NodeCase.VALUE && addOperands[1].nodeCase == Operand.NodeCase.VALUE) {
            val folded = PlanValues.foldAdd(
                PlanValues.toKotlin(addOperands[0].value),
                PlanValues.toKotlin(addOperands[1].value),
            )
            if (other.nodeCase != Operand.NodeCase.VARIABLE) {
                throw Refusals.unsupported("add(const, const) compared to a non-field operand is not supported")
            }
            return translation.leaf.applyLeaf(operator, scope.scalar(other.variable), folded)
        }
        throw Refusals.unsupported("add comparison with a field reference only supports eq/ne (got $operator)")
    }

    /**
     * Reports an operand shape no leaf case accepts, reading the RAW operands in order so each
     * shape keeps its own message.
     */
    private fun leafOperandError(operator: String, operands: List<Operand>): IllegalArgumentException {
        var variable: String? = null
        operands.forEach { operand ->
            when (operand.nodeCase) {
                Operand.NodeCase.VARIABLE -> variable = operand.variable
                // Conversion can itself reject a malformed VALUE — same order as reading the
                // operands left to right.
                Operand.NodeCase.VALUE -> PlanValues.toKotlin(operand.value)
                Operand.NodeCase.EXPRESSION -> throw when (val inner = operand.expression.operator) {
                    // A map() composition is only accepted inside hasIntersection; point at the
                    // supported shape rather than reporting a generic operand error.
                    "map" -> Refusals.unsupported(
                        "Direct comparison of map(...) to a value is not supported (operator: " +
                            "$operator). Wrap the map() expression in hasIntersection(map(...), " +
                            "[...]) instead.",
                    )
                    "except" -> ScalarRefusals.exceptUnsupported()
                    // `matches(...) == true` keeps its wrapper on the wire; the refusal is the
                    // regex one, wherever in the walk the matches() arrives.
                    "matches" -> ScalarRefusals.unsupportedOperator(inner)
                    // A map or list the planner assembles rather than ships as a constant.
                    "struct", "list" -> Refusals.unsupported(
                        "Cannot compare a column with a $inner() value: it builds a map or list, " +
                            "and a mapped column holds one scalar, which SQL equality cannot " +
                            "compare with either.",
                    )
                    "int", "double" -> ScalarRefusals.numericCastUnsupported(inner)
                    // string() reaches a leaf only when its argument is not a mapped column.
                    "string" -> Refusals.unsupported(
                        "Cannot translate string() over a computed value (a conversion, a " +
                            "ternary or a comparison): string() is lowered only over a mapped " +
                            "column, whose type decides CEL's spelling, and SQL CAST prints " +
                            "numbers and booleans differently from CEL (\"1e+06\", \"true\").",
                    )
                    "index" -> Refusals.unsupported(
                        "Cannot translate index(): a list element is addressed by position, and " +
                            "the rows a relation subquery returns carry no CEL list order to " +
                            "index into.",
                    )
                    "get-field" -> Refusals.unsupported(
                        "Cannot translate get-field(): it projects a member out of a single " +
                            "element, and a relation subquery has no single element to project " +
                            "from.",
                    )
                    // A computed operand — a cast, a lambda, a nested timestamp(): legal CEL the
                    // leaf cases have no column shape for.
                    else -> Refusals.unsupported("Unexpected $inner() expression in leaf operand of $operator")
                }
                else -> throw Refusals.malformed("Unexpected operand type in leaf expression: ${operand.nodeCase}")
            }
        }
        // Two constants under an operator the constant fold does not cover: a comparison the
        // planner evaluates itself and never ships.
        return if (variable == null) {
            Refusals.malformed("Missing variable operand for $operator")
        } else {
            Refusals.malformed("Missing value operand for $operator")
        }
    }

    /**
     * Evaluates a comparison between two plan constants with primitive IEEE operators, so a NaN
     * constant orders false as in CEL.
     */
    fun constantComparison(operator: String, left: Any?, right: Any?): Op<Boolean> {
        val result: Boolean = if (operator == "eq" || operator == "ne") {
            val equal = if (left is Number && right is Number) {
                left.toDouble() == right.toDouble()
            } else {
                left == right
            }
            (operator == "eq") == equal
        } else if (left is Number && right is Number) {
            val l = left.toDouble()
            val r = right.toDouble()
            when (operator) {
                "lt" -> l < r
                "gt" -> l > r
                "le" -> l <= r
                "ge" -> l >= r
                else -> throw Refusals.internal("Unsupported constant comparison operator: $operator")
            }
        } else if (left is String && right is String) {
            val comparison = left.compareTo(right)
            when (operator) {
                "lt" -> comparison < 0
                "gt" -> comparison > 0
                "le" -> comparison <= 0
                "ge" -> comparison >= 0
                else -> throw Refusals.internal("Unsupported constant comparison operator: $operator")
            }
        } else {
            // Ordering two booleans, or a string against a number, has no CEL overload: the
            // planner would have rejected the policy.
            throw Refusals.malformed(
                "Cannot order constant operands of $operator: " +
                    "${PlanValues.typeName(left)} vs ${PlanValues.typeName(right)}",
            )
        }
        return if (result) Op.TRUE else Op.FALSE
    }

    /**
     * The raw comparison of two SQL expressions: the shared dispatch of field-to-field comparisons,
     * arithmetic, timestamps and concatenations. A constant-RHS scalar leaf does NOT route here —
     * it goes through [LeafTranslator], which owns the null convention and the double cast.
     */
    fun compare(operator: String, left: Expression<*>, right: Expression<*>): Op<Boolean> =
        compareRaw(operator, ExactString.of(left, operator), ExactString.of(right, operator))

    private fun compareRaw(operator: String, left: Expression<*>, right: Expression<*>): Op<Boolean> = when (operator) {
        "eq" -> EqOp(left, right)
        "ne" -> NeqOp(left, right)
        "lt" -> LessOp(left, right)
        "gt" -> GreaterOp(left, right)
        "le" -> LessEqOp(left, right)
        "ge" -> GreaterEqOp(left, right)
        else -> throw Refusals.internal("Unsupported expression comparison operator: $operator")
    }

    /**
     * Compares two mapped columns directly, or pattern-matches one against another. Operand source
     * order is preserved: two variables rank equally, so [NormalizedBinary] never swaps them.
     */
    private fun fieldToFieldComparison(
        operator: String,
        leftVariable: String,
        rightVariable: String,
        scope: Scope,
    ): Op<Boolean> {
        val left = scope.scalar(leftVariable)
        val right = scope.scalar(rightVariable)
        if (operator in STRING_MATCH_OPERATORS) {
            if (translation.leaf.lacksTextOverload(left) || translation.leaf.lacksTextOverload(right)) {
                return TriLogic.unknown()
            }
            translation.leaf.requireText(operator, left)
            translation.leaf.requireText(operator, right)
        } else if (ScalarColumnTypes.knownMismatch(left.column, right.column)) {
            return translation.leaf.typeMismatch(operator, listOf(left, right))
        } else if (!ScalarColumnTypes.comparable(left.column, right.column)) {
            // `R.attr.aString == R.attr.aNumber` is the constant case with neither side constant,
            // and MySQL coerces the text column exactly the same way.
            throw ScalarRefusals.columnTypeMismatch(
                operator,
                leftVariable,
                left.column,
                rightVariable,
                right.column,
            )
        }
        val leftExplicit = translation.leaf.isExplicitNull(left)
        val rightExplicit = translation.leaf.isExplicitNull(right)
        if ((operator == "eq" || operator == "ne") && leftExplicit != rightExplicit) {
            // One side holds a null VALUE (definite), the other a missing attribute (an error).
            // The definite expansion over the declared side, made UNKNOWN when the undeclared side
            // is NULL: CEL raises there before it compares anything.
            val undeclared = if (leftExplicit) right else left
            return TriLogic.baseUnlessUnknown(
                translation.leaf.definiteEquality(
                    operator,
                    left.expression,
                    right.expression,
                    leftExplicit = leftExplicit,
                    rightExplicit = rightExplicit,
                ),
                IsNullOp(undeclared.expression),
            )
        }
        if ((operator == "eq" || operator == "ne") && leftExplicit && rightExplicit) {
            return translation.leaf.definiteEquality(
                operator,
                left.expression,
                right.expression,
                leftExplicit = true,
                rightExplicit = true,
            )
        }
        return when (operator) {
            "eq", "ne", "lt", "gt", "le", "ge" -> compare(operator, left.expression, right.expression)
            "contains" -> columnNeedleMatch(left.expression, right.expression, true, true)
            "startsWith" -> columnNeedleMatch(left.expression, right.expression, false, true)
            "endsWith" -> columnNeedleMatch(left.expression, right.expression, true, false)
            else -> throw ScalarRefusals.unsupportedOperator(operator)
        }
    }

    /**
     * `haystack LIKE wildcards(escape(needle)) ESCAPE '\'` where the needle is an EXPRESSION rather
     * than a constant — a column on either side of the match, or a constant receiver whose needle
     * is the column. [LikeEscaping] owns the escaping and the NULL-yielding guard.
     */
    private fun columnNeedleMatch(
        haystack: Expression<*>,
        needle: Expression<*>,
        leadingWildcard: Boolean,
        trailingWildcard: Boolean,
    ): Op<Boolean> = NulSafeMatch(
        LikeEscapeOp(
            ExactString.of(haystack, "eq"),
            LikeEscaping.columnPattern(needle, leadingWildcard, trailingWildcard),
            true,
            LikeEscaping.ESCAPE_CHAR,
        ),
        haystack,
        needle,
        leadingWildcard,
        trailingWildcard,
    )

    companion object {
        /** 2^63: cel-go's `int()` refuses a double at or beyond it in either direction. */
        private const val INT64_BOUND: Double = 9.223372036854775808E18

        /** 2^53: every whole double within it is exact, and so is its successor. */
        private val CANONICAL_INT = Regex("0|-?[1-9][0-9]*")
        private const val EXACT_DOUBLE_INTEGER: Double = 9.007199254740992E15

        val COMPARISON_OPERATORS: Set<String> = setOf("eq", "ne", "lt", "le", "gt", "ge")
        private val ORDERING_OPERATORS: Set<String> = setOf("lt", "le", "gt", "ge")
        val STRING_MATCH_OPERATORS: Set<String> = setOf("contains", "startsWith", "endsWith")

        /** CEL's supported instant range: year 1 through year 9999, inclusive. */
        private val MIN_CEL_INSTANT: Instant = Instant.parse("0001-01-01T00:00:00Z")
        private val MAX_CEL_INSTANT: Instant = Instant.parse("9999-12-31T23:59:59.999999999Z")

        /**
         * RFC 3339, to nanosecond precision, with a `Z` or a numeric offset. Anchored and checked
         * BEFORE parsing, because `Instant.parse` accepts forms RFC 3339 does not and CEL's own
         * `timestamp()` would have rejected.
         */
        private val RFC_3339 = Regex(
            "^\\d{4}-\\d{2}-\\d{2}[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d" +
                "(?:\\.\\d{1,9})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)$",
        )

        /** Parses a `timestamp()` literal to its instant. A non-string or unparseable one is a malformed plan. */
        private fun parseInstant(raw: Any?): Instant {
            if (raw !is String) {
                throw Refusals.malformed(
                    "timestamp() constant must be an RFC-3339 string, got ${PlanValues.typeName(raw)}",
                )
            }
            if (!RFC_3339.matches(raw) || raw.startsWith("0000")) {
                throw Refusals.malformed("timestamp() constant is not an RFC-3339 instant")
            }
            // Uppercased so a lowercase `t` or `z` — both legal in RFC 3339 — parses; the pattern
            // above has already ruled out anything else a case change could affect.
            val normalized = raw.uppercase()
            val instant = try {
                Instant.parse(normalized)
            } catch (e: DateTimeParseException) {
                try {
                    OffsetDateTime.parse(normalized).toInstant()
                } catch (nested: DateTimeParseException) {
                    nested.addSuppressed(e)
                    throw Refusals.malformed("timestamp() constant could not be parsed as an RFC-3339 instant", nested)
                }
            }
            if (instant < MIN_CEL_INSTANT || instant > MAX_CEL_INSTANT) {
                throw Refusals.malformed("timestamp() constant is outside CEL's supported instant range")
            }
            return instant
        }
    }
}
