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
