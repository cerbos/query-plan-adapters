package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.LikeEscaping
import dev.cerbos.queryplan.exposed.sql.NullLiteral
import dev.cerbos.queryplan.exposed.sql.Params
import dev.cerbos.queryplan.exposed.sql.ScalarCase
import dev.cerbos.queryplan.exposed.sql.ScalarColumnKind
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import dev.cerbos.queryplan.exposed.sql.TextCastExpression
import dev.cerbos.queryplan.exposed.sql.TimestampBinder
import org.jetbrains.exposed.v1.core.Alias
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
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.booleanParam
import org.jetbrains.exposed.v1.core.stringParam
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

/**
 * Comparisons and string matches: everything the walk does not route by name.
 *
 * The reference adapter fixes the pipeline by CODE ORDER, and this follows it:
 * ternary rewrite (on RAW operands, before mirroring) -> normalise to field-first -> arity check
 * (before the size probe, which would otherwise translate a partial comparison) -> `size()`
 * comparisons -> operand resolution -> dispatch on the resolved pair.
 *
 * One seam classifies each operand into a [Resolved] shape, and [dispatch] translates the resolved
 * PAIR. Resolution is purely structural and conversion is LAZY, at the dispatch site that consumes
 * the operand: WHICH error a malformed operand raises depends on the shape of the whole
 * comparison — a non-numeric constant inside `add` is a fold type error against a field but an
 * "Arithmetic comparison requires numeric operands" when lowered to SQL — so converting eagerly
 * here would re-order messages the corpus pins.
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
            fun fold(): Any = PlanValues.foldAdd(
                PlanValues.toKotlin(left.value),
                PlanValues.toKotlin(right.value),
            )
        }

        /**
         * `add(field, value)` / `add(value, field)` — solvable for the field under eq/ne against a
         * constant when the solve is algebraically exact (string concatenation, in-range long
         * integers); every other pairing lowers to SQL arithmetic, because IEEE subtraction cannot
         * invert IEEE addition.
         */
        class FieldPlusConstant(val fieldVariable: String, val constant: Operand, val fieldIsLeft: Boolean) : Resolved

        /** Any other arithmetic-rooted expression, lowered by [ArithmeticTranslator]. */
        class Arithmetic(val operator: String) : Resolved

        /** `timestamp(variable)` — a temporal column wrapped in CEL's `timestamp()` cast. */
        class TimestampField(val variable: String) : Resolved

        /**
         * `timestamp(value)` — a constant instant. The planner constant-folds `now()` arithmetic
         * and re-wraps the result in `timestamp("<RFC-3339>")` on the wire, so both policy
         * literals and folded relative windows arrive in this shape. Parsed lazily, so the parse
         * errors belong to the comparison that consumes it.
         */
        class TimestampConstant(val operand: Operand) : Resolved {
            fun instant(): Instant = parseInstant(PlanValues.toKotlin(operand.value))
        }

        /** `string(variable)` — CEL's text conversion over a mapped column. */
        class TextCast(val variable: String) : Resolved

        /**
         * `index(collection, position)`, or `get-field(index(collection, position), member)`: a
         * positional read of a relation's element, or of one member of it.
         */
        class Positional(val variable: String, val position: Operand, val member: String?) : Resolved

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
                inner == "string" && expression.operandsCount == 1 &&
                    expression.getOperands(0).nodeCase == Operand.NodeCase.VARIABLE ->
                    Resolved.TextCast(expression.getOperands(0).variable)
                inner == "index" && positionalOf(expression) != null -> positionalOf(expression)!!
                inner == "get-field" && expression.operandsCount == 2 &&
                    expression.getOperands(0).nodeCase == Operand.NodeCase.EXPRESSION &&
                    expression.getOperands(1).nodeCase == Operand.NodeCase.VARIABLE &&
                    positionalOf(expression.getOperands(0).expression) != null ->
                    positionalOf(expression.getOperands(0).expression)!!.let {
                        Resolved.Positional(it.variable, it.position, expression.getOperands(1).variable)
                    }
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

    private fun positionalOf(expression: PlanResourcesFilter.Expression): Resolved.Positional? =
        if (expression.operator == "index" && expression.operandsCount == 2 &&
            expression.getOperands(0).nodeCase == Operand.NodeCase.VARIABLE &&
            expression.getOperands(1).nodeCase == Operand.NodeCase.VALUE
        ) {
            Resolved.Positional(expression.getOperands(0).variable, expression.getOperands(1), null)
        } else {
            null
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
                    "contains" -> columnNeedleMatch(stringParam(receiver), needle, true, true)
                    "startsWith" -> columnNeedleMatch(stringParam(receiver), needle, false, true)
                    "endsWith" -> columnNeedleMatch(stringParam(receiver), needle, true, false)
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
            if (isArithmeticRooted(left) || isArithmeticRooted(right)) {
                // CEL overloads `+` on strings, so an add-rooted operand is only arithmetic once
                // the mapped column types say it is not a concatenation.
                if (ConcatTranslator.isConcatenation(operands[0], scope) ||
                    ConcatTranslator.isConcatenation(operands[1], scope)
                ) {
                    return compare(
                        operator,
                        ConcatTranslator.textExpression(operands[0], scope),
                        ConcatTranslator.textExpression(operands[1], scope),
                    )
                }
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
            if (left is Resolved.Positional && right is Resolved.Constant) {
                return positionalComparison(operator, left, right.value(), scope)
            }
            if (left is Resolved.Constant && right is Resolved.Positional) {
                return positionalComparison(NormalizedBinary.mirror(operator), right, left.value(), scope)
            }
        }
        val built = (left as? Resolved.BuiltConstant) ?: (right as? Resolved.BuiltConstant)
        if (field != null && built != null) {
            return leafFieldValue(operator, field, built.value, scope)
        }

        throw leafOperandError(operator, operands)
    }

    /**
     * `list[k] op constant`, or `list[k].member op constant`, over a relation that declares its
     * [AttributeMapping.Relation.position] column.
     *
     * CEL reads the element at position `k` and raises when there is none (`k` past the end), when
     * `k` is negative, or when it is not a whole number; each is UNKNOWN here. The element itself
     * is compared by [LeafTranslator.applyLeaf], against the one row at that position: a scalar
     * element as a VALUE (a NULL element is CEL's null element, so the explicit-null convention),
     * a member under its own declared convention. `EXISTS` is two-valued, so the three answers are
     * read as two existence tests, `t` (the row at `k` compares TRUE) and `f` (it compares FALSE):
     * `t OR (NOT f AND UNKNOWN)` is TRUE, FALSE, or UNKNOWN when neither holds — no row at `k`, or
     * a comparison CEL raises on. Both tests go through [Subqueries.chainContains], so an absent
     * to-one hop stays UNKNOWN. A position read through a to-MANY leading hop has no single list to
     * index and is refused.
     */
    private fun positionalComparison(
        operator: String,
        positional: Resolved.Positional,
        value: Any?,
        scope: Scope,
    ): Op<Boolean> {
        val collection = scope.resolve(positional.variable) as? Resolution.Collection
            ?: throw RelationRefusals.positionalRead(positional.variable, "it is not mapped as a relation")
        if (collection.leadingHops.any { it.cardinality != AttributeMapping.Relation.Cardinality.ONE }) {
            throw RelationRefusals.positionalRead(
                positional.variable,
                "it flattens a to-many relation, so there is no single list to index",
            )
        }
        val index = PlanValues.toKotlin(positional.position.value)
        if (index !is Long || index < 0) return TriLogic.unknown()
        val tail = collection.tail
        val position = tail.position ?: throw RelationRefusals.positionalRead(
            positional.variable,
            "the relation declares no position column; declare one with position(column)",
        )
        val field = if (positional.member == null) {
            val element = tail.element ?: throw RelationRefusals.noElementColumn(collection.variable, tail)
            AttributeMapping.field(element.column, NullAttributeRepresentation.EXPLICIT)
        } else {
            tail.fields[positional.member] as? AttributeMapping.Field ?: throw RelationRefusals.positionalRead(
                positional.variable,
                "its element declares no field '${positional.member}'",
            )
        }
        fun atPosition(alias: Alias<Table>, holds: Boolean): Op<Boolean> {
            val leaf = translation.leaf.applyLeaf(
                operator,
                Resolution.Scalar(positional.variable, alias[field.column], field.column, field),
                value,
            )
            return TriLogic.and(
                EqOp(alias[position], Params.of(index)),
                if (holds) leaf else TriLogic.not(leaf),
            )
        }
        val isTrue = translation.subqueries.chainContains(collection) { atPosition(it, holds = true) }
        val isFalse = translation.subqueries.chainContains(collection) { atPosition(it, holds = false) }
        return TriLogic.or(isTrue, TriLogic.and(TriLogic.not(isFalse), TriLogic.unknown()))
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

    /**
     * `timestamp(field) op timestamp(constant)`. [operator] is already field-first.
     *
     * A NULL column makes every comparison UNKNOWN under SQL three-valued logic, so the row is
     * excluded under both polarities — matching CEL, where a missing attribute is an evaluation
     * error and `check()` denies.
     */
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
     * CEL's `string()` over a mapped column.
     *
     * A boolean is the one column type no single `CAST` gets right: SQLite and MySQL have no
     * boolean type and store 1/0, so `CAST(a_bool AS CHAR)` is `"1"` where CEL and PostgreSQL say
     * `"true"`. The column type is known here, so the boolean case lowers to a portable `CASE`
     * instead of being refused — with its own `IS NULL` arm, because a NULL column must stay NULL
     * rather than fall through to the `ELSE` and compare as `"false"`.
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
     * `string(column) ==/!= "literal"` over a floating-point or boolean column, SOLVED for the
     * column rather than rendered. A boolean prints as exactly `true` or `false`. No SQL CAST
     * prints a double as CEL does (Go's shortest `%g`: `1e+06`, `2`, `-9.5e+18`), but exactly one
     * double prints as a given string, so the comparison is the column against that double. A
     * literal that is not CEL's spelling of any value matches no row: FALSE for a present value,
     * UNKNOWN for a NULL one, which CEL denies under both polarities. A zero is refused, since CEL
     * prints `-0.0` as `-0` and SQL cannot tell it from `0.0`. `null` when the shape is not this
     * one; an ordering over it falls through to the refusal in [textCastOperand].
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
                        EqOp(target.expression, booleanParam(true)) to stringParam("true"),
                    ),
                    stringParam("false"),
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
     * `timestamp(field) op timestamp(field)` — two mapped columns the policy has SAID are instants.
     *
     * This is the ONLY spelling that compares two temporal columns. The bare
     * `R.attr.createdAt == R.attr.updatedAt` is refused, because Cerbos transports a timestamp
     * attribute as an RFC 3339 STRING and CEL therefore compares those two as strings, where SQL
     * compares them as instants and matches spellings CEL does not
     * ([ScalarColumnTypes.comparable]). Wrapping both in `timestamp()` is the policy saying it
     * means the instant, which is exactly what makes the SQL comparison the right one.
     *
     * Both columns must pin an absolute instant, and must pin it in the same REPRESENTATION: a
     * `timestamp` against a `timestamptz` is resolved by a store through the session's time zone,
     * which is not a property of the filter. Representation, not column-type name — the two
     * datetime modules declare different classes for the same plain instant, and comparing the
     * names refused that pair while claiming to be about time zones.
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
     * Solves `add(field, const) eq/ne constant` for the field.
     *
     * When no solution exists — `"projects:123" == "users:" + R.id` can never be true — `eq` is
     * false and `ne` is true for every row whose column is PRESENT, and neither is definite: a
     * missing attribute makes the concatenation a CEL evaluation error and denies under BOTH
     * polarities, so the answer has to be UNKNOWN for a NULL column.
     *
     * Both arms therefore carry the `IS NULL` witness rather than folding to a constant. A bare
     * `Op.FALSE` is right unnegated and wrong under `not(...)`, which is where every `Op.FALSE`
     * over a mapped column goes wrong; and `IsNotNullOp` alone is worse still, because `NOT (col
     * IS NOT NULL)` selects EXACTLY the rows `check()` denies.
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
        return translation.leaf.applyLeaf(operator, target, solved)
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
     * Evaluates a comparison between two plan constants.
     *
     * Numbers compare with the primitive IEEE operators, NOT a total-order comparator: the total
     * order ranks NaN above every number, so it would collapse `gt`/`ge` against a NaN constant —
     * reachable through an unfolded `div(0, 0)` — to always-true and return rows the PDP denies.
     * CEL and IEEE define every ordering comparison involving NaN as false.
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
    fun compare(operator: String, left: Expression<*>, right: Expression<*>): Op<Boolean> = when (operator) {
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
            throw ScalarRefusals.mixedNullConventions(operator)
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
    ): Op<Boolean> = LikeEscapeOp(
        haystack,
        LikeEscaping.columnPattern(needle, leadingWildcard, trailingWildcard),
        true,
        LikeEscaping.ESCAPE_CHAR,
    )

    companion object {
        val COMPARISON_OPERATORS: Set<String> = setOf("eq", "ne", "lt", "le", "gt", "ge")
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

        /**
         * Parses a `timestamp()` literal to the absolute instant it names, at nanosecond precision.
         *
         * Offsets are normalised away: CEL timestamp equality is equality of the instant, so the
         * offset a literal was written in must not survive into the comparison. CEL's own
         * `timestamp()` rejects a non-string or unparseable literal, so the planner cannot emit
         * one — both are malformed plans, not unsupported shapes.
         */
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
