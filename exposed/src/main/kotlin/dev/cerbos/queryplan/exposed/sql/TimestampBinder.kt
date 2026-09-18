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
 */
internal object TimestampBinder {

    @OptIn(ExperimentalTime::class)
    fun bind(instant: java.time.Instant, column: Column<*>, variable: String): Expression<*> {
        @Suppress("UNCHECKED_CAST")
        return when (val columnType = column.columnType) {
            is InstantColumnType<*> -> QueryParameter(
                columnType.fromInstant(kotlin.time.Instant.fromEpochSeconds(instant.epochSecond, instant.nano.toLong())),
                columnType as IColumnType<Any>,
            )
            // Normalised to UTC: CEL timestamp equality is equality of the absolute instant, so the
            // offset a literal was written in must not reach the comparison.
            is OffsetDateTimeColumnType<*> -> QueryParameter(
                columnType.fromOffsetDateTime(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)),
                columnType as IColumnType<Any>,
            )
            else -> throw Refusals.unmapped(
                "timestamp() comparison requires a column that stores an absolute instant — an " +
                    "exposed-java-time or exposed-kotlin-datetime timestamp() or " +
                    "timestampWithTimeZone() column — but '$variable' maps to a " +
                    "${columnType::class.simpleName} column. A local date-time, a date and a " +
                    "string are all ambiguous about which instant they hold; remap the column.",
            )
        }
    }
}
