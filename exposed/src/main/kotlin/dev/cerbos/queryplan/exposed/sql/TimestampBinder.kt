package dev.cerbos.queryplan.exposed.sql

import dev.cerbos.queryplan.exposed.Refusals
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.QueryParameter
import org.jetbrains.exposed.v1.core.datetime.InstantColumnType
import org.jetbrains.exposed.v1.core.datetime.OffsetDateTimeColumnType
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.time.ExperimentalTime

/**
 * Binds a parsed instant through the MAPPED COLUMN'S OWN column type.
 *
 * The column type is the only thing that knows how this database stores time, and it is also the
 * one converter guaranteed to agree with what the application wrote: binding a `java.time.Instant`
 * against a column the caller declared with `exposed-kotlin-datetime` would otherwise go through a
 * different conversion from the insert's.
 *
 * Nothing here references either datetime module. `InstantColumnType` and `OffsetDateTimeColumnType`
 * are the abstract bases in `exposed-core`, and `exposed-java-time`'s `timestamp()` /
 * `timestampWithTimeZone()` and `exposed-kotlin-datetime`'s extend them — so one check covers
 * `java.time.Instant`, `kotlin.time.Instant` and `java.time.OffsetDateTime` columns, and a
 * consumer needs neither module for this class to load.
 *
 * **Column-type contract.** Only a type that unambiguously denotes an absolute instant is
 * translated. A local date-time carries no zone, so the stored wall-clock reading could mean any
 * instant; a string column orders lexicographically, which agrees with chronology only for one
 * fixed-width zone-normalised layout. Guessing a zone would silently include rows the PDP denies,
 * so those fail closed with a MAPPING error — the plan is fine, the mapping does not say enough.
 *
 * The column type is read through [ScalarColumnTypes.unwrap], the declared owner of "the type that
 * actually decides the SQL". Reading `column.columnType` raw refused a DAO-id or `transform`ed
 * instant column here while every other path in the adapter accepted it — one column, two answers.
 */
internal object TimestampBinder {

    /**
     * How a column that pins an absolute instant SPELLS it. Two columns of one representation hold
     * comparable values; two of different ones do not, whichever module declared them.
     */
    private enum class Representation { INSTANT, OFFSET_DATE_TIME }

    /**
     * THE enumeration of the representations, asked by everything else here.
     *
     * One `when` rather than one per question: [storesAbsoluteInstant] used to carry its own copy
     * of the type list beside [bind]'s, so a third representation would have had to be added in two
     * places and a reader could not tell which of them was authoritative. Adding one here now makes
     * [bind]'s `when` non-exhaustive, which is a compile error rather than a silent omission.
     */
    private fun representationOf(column: Column<*>): Representation? =
        when (ScalarColumnTypes.unwrap(column)) {
            is InstantColumnType<*> -> Representation.INSTANT
            is OffsetDateTimeColumnType<*> -> Representation.OFFSET_DATE_TIME
            else -> null
        }

    /** Whether [column] unambiguously denotes an absolute instant, by the contract above. */
    fun storesAbsoluteInstant(column: Column<*>): Boolean = representationOf(column) != null

    /**
     * Whether two columns that both pin an absolute instant pin it the SAME way.
     *
     * Compared as REPRESENTATIONS, never as column-type names: `exposed-java-time`'s `timestamp()`
     * and `exposed-kotlin-datetime`'s declare different classes and both extend
     * [InstantColumnType], so they store the same thing and compare correctly. Reading the class
     * name instead refused that pair for no reason while claiming to be about time zones.
     */
    fun sameRepresentation(left: Column<*>, right: Column<*>): Boolean {
        val representation = representationOf(left) ?: return false
        return representation == representationOf(right)
    }

    @OptIn(ExperimentalTime::class)
    fun bind(instant: java.time.Instant, column: Column<*>, variable: String): Expression<*> {
        val columnType = ScalarColumnTypes.unwrap(column)
        @Suppress("UNCHECKED_CAST")
        val sqlType = columnType as IColumnType<Any>
        return when (representationOf(column)) {
            Representation.INSTANT -> QueryParameter(
                (columnType as InstantColumnType<*>).fromInstant(
                    kotlin.time.Instant.fromEpochSeconds(instant.epochSecond, instant.nano.toLong()),
                ),
                sqlType,
            )
            // Normalised to UTC: CEL timestamp equality is equality of the absolute instant, so the
            // offset a literal was written in must not reach the comparison.
            Representation.OFFSET_DATE_TIME -> QueryParameter(
                (columnType as OffsetDateTimeColumnType<*>).fromOffsetDateTime(
                    OffsetDateTime.ofInstant(instant, ZoneOffset.UTC),
                ),
                sqlType,
            )
            null -> throw ambiguous(column, variable)
        }
    }

    /** The refusal for a column whose type does not pin an absolute instant. */
    fun ambiguous(column: Column<*>, variable: String): Nothing = throw Refusals.unmapped(
        "timestamp() comparison requires a column that stores an absolute instant — an " +
            "exposed-java-time or exposed-kotlin-datetime timestamp() or " +
            "timestampWithTimeZone() column — but '$variable' maps to a " +
            "${ScalarColumnTypes.describe(column)} column. A local date-time, a date and a " +
            "string are all ambiguous about which instant they hold; remap the column.",
    )
}
