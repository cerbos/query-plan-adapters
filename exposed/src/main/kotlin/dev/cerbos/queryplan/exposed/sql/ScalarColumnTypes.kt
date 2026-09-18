package dev.cerbos.queryplan.exposed.sql

import org.jetbrains.exposed.v1.core.BooleanColumnType
import org.jetbrains.exposed.v1.core.ByteColumnType
import org.jetbrains.exposed.v1.core.CharacterColumnType
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.ColumnWithTransform
import org.jetbrains.exposed.v1.core.DecimalColumnType
import org.jetbrains.exposed.v1.core.DoubleColumnType
import org.jetbrains.exposed.v1.core.EntityIDColumnType
import org.jetbrains.exposed.v1.core.FloatColumnType
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.LongColumnType
import org.jetbrains.exposed.v1.core.ShortColumnType
import org.jetbrains.exposed.v1.core.StringColumnType
import org.jetbrains.exposed.v1.core.UIntegerColumnType
import org.jetbrains.exposed.v1.core.ULongColumnType

/**
 * What a mapped column's declared type says about the values it holds.
 *
 * THIS IS THE ADAPTER'S ONE ADVANTAGE OVER THE PLAN. A query plan names no operand types, which is
 * why the adapters built on a type-blind builder need the caller to declare `ValueString` or
 * `ValueBool` before they can tell CEL's two `+` overloads apart or refuse `string()` over a
 * boolean. An Exposed `Column` carries its own type, so the same questions are answered from the
 * mapping the caller already wrote.
 */
internal enum class ScalarColumnKind {
    /** Anything CEL compares as a string, including `CHAR`. */
    TEXT,

    /** An exact integer: CEL's text rendering and SQL's agree, and arithmetic promotes cleanly. */
    INTEGRAL,

    /** Binary floating point: already IEEE, and rendered as the shortest round-tripping decimal. */
    FLOATING,

    BOOLEAN,

    /**
     * Fixed-point `DECIMAL`. Deliberately NOT numeric here: it renders its declared scale
     * (`1.50`) where CEL renders `1.5`, so `string()` over one diverges. Arithmetic over one is
     * still fine, because [IeeeDoubleCast] takes it into double space first.
     */
    DECIMAL,

    /** Temporal, binary, array, enum, or a type this adapter has no CEL reading for. */
    OTHER,
}

/**
 * The CEL value space a column's declared type compares in.
 *
 * Coarser than [ScalarColumnKind] on purpose: `string()` and the fractional cast care WHICH numeric
 * type a column is, and a comparison does not — CEL compares an int and a double numerically.
 */
internal enum class ScalarValueFamily { TEXT, NUMERIC, BOOLEAN }

internal object ScalarColumnTypes {

    fun kindOf(column: Column<*>): ScalarColumnKind = when (unwrap(column.columnType)) {
        is StringColumnType, is CharacterColumnType -> ScalarColumnKind.TEXT
        is ByteColumnType, is ShortColumnType, is IntegerColumnType, is LongColumnType,
        is UIntegerColumnType, is ULongColumnType,
        -> ScalarColumnKind.INTEGRAL
        is FloatColumnType, is DoubleColumnType -> ScalarColumnKind.FLOATING
        is BooleanColumnType -> ScalarColumnKind.BOOLEAN
        is DecimalColumnType -> ScalarColumnKind.DECIMAL
        else -> ScalarColumnKind.OTHER
    }

    /** Whether arithmetic and a fractional comparison may take this column into double space. */
    fun isNumeric(column: Column<*>): Boolean = when (kindOf(column)) {
        ScalarColumnKind.INTEGRAL, ScalarColumnKind.FLOATING, ScalarColumnKind.DECIMAL -> true
        else -> false
    }

    /** Whether CEL's string matches and `size()` have an overload for this column's values. */
    fun isText(column: Column<*>): Boolean = kindOf(column) == ScalarColumnKind.TEXT

    /**
     * The family [column] compares in, or `null` for a type this adapter has no CEL reading for.
     *
     * A `null` is FAIL-CLOSED everywhere it is consulted: a temporal, binary, array, enum or custom
     * column reaching a plain comparison is refused rather than compared, because what a store does
     * with a mismatched operand is store-specific and silent. Widening this is a deliberate act —
     * add the type to [kindOf] once its CEL reading is known.
     */
    fun familyOf(column: Column<*>): ScalarValueFamily? = when (kindOf(column)) {
        ScalarColumnKind.TEXT -> ScalarValueFamily.TEXT
        ScalarColumnKind.INTEGRAL, ScalarColumnKind.FLOATING, ScalarColumnKind.DECIMAL ->
            ScalarValueFamily.NUMERIC
        ScalarColumnKind.BOOLEAN -> ScalarValueFamily.BOOLEAN
        ScalarColumnKind.OTHER -> null
    }

    /** The family a non-null plan constant compares in, or `null` for a value no column holds. */
    fun familyOf(value: Any): ScalarValueFamily? = when (value) {
        is String -> ScalarValueFamily.TEXT
        is Boolean -> ScalarValueFamily.BOOLEAN
        is Number -> ScalarValueFamily.NUMERIC
        else -> null
    }

    /**
     * Whether a comparison of [column] against a non-null plan constant is one a store decides the
     * way CEL does.
     *
     * A plan names no operand types, so this is the ONLY thing standing between
     * `R.attr.aString == P.attr.level` — legal CEL, answered FALSE from the values alone — and
     * `a_string = 3.0`, which MySQL decides by coercing the COLUMN to a number.
     */
    fun accepts(column: Column<*>, value: Any): Boolean {
        val family = familyOf(column) ?: return false
        return family == familyOf(value)
    }

    /** [accepts], for two mapped columns compared against each other. */
    fun comparable(left: Column<*>, right: Column<*>): Boolean {
        val leftFamily = familyOf(left)
        val rightFamily = familyOf(right)
        // Two types with no CEL reading here — two temporal columns, say. The declared type is the
        // most this can check, and two columns of ONE type give a store nothing to coerce.
        if (leftFamily == null && rightFamily == null) return describe(left) == describe(right)
        return leftFamily != null && leftFamily == rightFamily
    }

    /** The name a refusal uses for a column's type; a type, never a value. */
    fun describe(column: Column<*>): String = unwrap(column.columnType)::class.simpleName ?: "unrecognised"

    /**
     * The type that actually decides the SQL, looked through the two wrappers Exposed puts in
     * front of it: a DAO id column wraps the real column type, and a `transform`ed column wraps
     * the type it is stored as. Reading the wrapper instead would classify a `varchar` DAO key as
     * [ScalarColumnKind.OTHER] and refuse a shape the store handles perfectly well.
     */
    private fun unwrap(columnType: IColumnType<*>): IColumnType<*> = when (columnType) {
        is EntityIDColumnType<*> -> unwrap(columnType.idColumn.columnType)
        is ColumnWithTransform<*, *> -> unwrap(columnType.originalColumnType)
        else -> columnType
    }
}
