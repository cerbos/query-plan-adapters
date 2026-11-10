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

/** What a mapped column's declared type says about its values; the plan names no operand types. */
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

/** The CEL value space a column compares in. Coarser than [ScalarColumnKind]: CEL compares int and double numerically. */
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

    /** The family [column] compares in, or `null` (refused everywhere) for a type with no known CEL reading. */
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

    /** Whether comparing [column] with a non-null constant is decided by the store as CEL decides it. */
    fun accepts(column: Column<*>, value: Any): Boolean {
        val family = familyOf(column) ?: return false
        return family == familyOf(value)
    }

    /** Whether [column] and [value] are both recognised and of different CEL types. */
    fun knownMismatch(column: Column<*>, value: Any): Boolean {
        // A list or map never equals the one scalar a column holds, whatever that scalar's type.
        if (value is List<*> || value is Map<*, *>) return true
        val family = familyOf(column) ?: return false
        val valueFamily = familyOf(value) ?: return false
        return family != valueFamily
    }

    /** [knownMismatch], for two mapped columns. */
    fun knownMismatch(left: Column<*>, right: Column<*>): Boolean {
        val leftFamily = familyOf(left) ?: return false
        val rightFamily = familyOf(right) ?: return false
        return leftFamily != rightFamily
    }

    /**
     * [accepts], for two columns. Both families must be known and equal: two timestamp columns compare
     * as RFC 3339 strings in CEL but as instants in SQL. `timestamp(a) < timestamp(b)` takes another path.
     */
    fun comparable(left: Column<*>, right: Column<*>): Boolean {
        val leftFamily = familyOf(left) ?: return false
        return leftFamily == familyOf(right)
    }

    /** The name a refusal uses for a column's type; a type, never a value. */
    fun describe(column: Column<*>): String = unwrap(column.columnType)::class.simpleName ?: "unrecognised"

    /** The type that decides the SQL, through a DAO id or `transform` wrapper. Ask here, not `columnType`. */
    fun unwrap(column: Column<*>): IColumnType<*> = unwrap(column.columnType)

    private fun unwrap(columnType: IColumnType<*>): IColumnType<*> = when (columnType) {
        is EntityIDColumnType<*> -> unwrap(columnType.idColumn.columnType)
        is ColumnWithTransform<*, *> -> unwrap(columnType.originalColumnType)
        else -> columnType
    }
}
