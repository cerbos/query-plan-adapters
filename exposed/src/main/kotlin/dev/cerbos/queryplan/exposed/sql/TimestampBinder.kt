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
 * Binds a parsed instant through the mapped column's own type, read via [ScalarColumnTypes.unwrap].
 * Only a column that denotes an absolute instant translates; a local date-time or string column is a
 * mapping error, since guessing a zone could over-grant.
 */
internal object TimestampBinder {

    /**
     * How a column that pins an absolute instant SPELLS it. Two columns of one representation hold
     * comparable values; two of different ones do not, whichever module declared them.
     */
    private enum class Representation { INSTANT, OFFSET_DATE_TIME }

    /** The representation of [column]'s instant; [bind]'s `when` is exhaustive over it. */
    private fun representationOf(column: Column<*>): Representation? =
        when (ScalarColumnTypes.unwrap(column)) {
            is InstantColumnType<*> -> Representation.INSTANT
            is OffsetDateTimeColumnType<*> -> Representation.OFFSET_DATE_TIME
            else -> null
        }

    /** Whether [column] unambiguously denotes an absolute instant, by the contract above. */
    fun storesAbsoluteInstant(column: Column<*>): Boolean = representationOf(column) != null

    /** Whether two absolute-instant columns use the same representation, whichever datetime module declared them. */
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
