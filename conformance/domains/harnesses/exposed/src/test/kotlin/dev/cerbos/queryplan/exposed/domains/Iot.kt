package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

internal object Iot : ExposedDomain {
    object Orgs : Table("orgs") {
        val id = varchar("id", 255)
        val name = varchar("name", 255)
        val plan = varchar("plan", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Sites : Table("sites") {
        val id = varchar("id", 255)
        val orgId = varchar("org_id", 255).references(Orgs.id)
        val region = varchar("region", 255)
        val name = varchar("name", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Devices : Table("devices") {
        val id = varchar("id", 255)
        val serial = varchar("serial", 255)
        val model = varchar("model", 255)
        val firmware = varchar("firmware", 255)
        val siteId = varchar("site_id", 255).references(Sites.id)
        val lastSeenAt = timestamp("last_seen_at").nullable()
        val online = bool("online")
        val batteryPct = integer("battery_pct").nullable()
        // JSON object column: a mapping takes a Column, never a JSON path expression into one.
        val labels = text("labels")
        val decommissioned = bool("decommissioned")
        override val primaryKey = PrimaryKey(id)
    }

    object Alerts : Table("device_alerts") {
        val id = varchar("id", 255)
        val deviceId = varchar("device_id", 255).references(Devices.id)
        val severity = integer("severity")
        val acknowledged = bool("acknowledged")
        val code = varchar("code", 255)
        override val primaryKey = PrimaryKey(id)
    }

    override val name = "iot"
    override val tables = listOf(Orgs, Sites, Devices, Alerts)
    override val root = Devices
    override val id = Devices.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Devices) {
        "$A.serial" to Devices.serial
        "$A.model" to Devices.model
        "$A.firmware" to Devices.firmware
        "$A.lastSeenAt" to field(Devices.lastSeenAt, OMITTED)
        "$A.online" to Devices.online
        "$A.batteryPct" to field(Devices.batteryPct, OMITTED)
        "$A.decommissioned" to Devices.decommissioned
        "$A.site" to relation(Sites) {
            "region" to Sites.region
            "org" to relation(Orgs) {
                "id" to Orgs.id
                "plan" to Orgs.plan
            }
        }
        "$A.alerts" to relation(Alerts) {
            "severity" to Alerts.severity
            "acknowledged" to Alerts.acknowledged
            "code" to Alerts.code
        }
    }

    override fun options() = Options.of(mapping)
}
