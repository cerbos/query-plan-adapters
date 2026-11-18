package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

internal object Logistics : ExposedDomain {
    object Hubs : Table("hubs") {
        val id = varchar("id", 255)
        val code = varchar("code", 255)
        val country = varchar("country", 255)
        val region = varchar("region", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Carriers : Table("carriers") {
        val id = varchar("id", 255)
        val name = varchar("name", 255)
        val scac = varchar("scac", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Shipments : Table("shipments") {
        val id = varchar("id", 255)
        val trackingNo = varchar("tracking_no", 255)
        val status = varchar("status", 255)
        val originHubId = varchar("origin_hub_id", 255).references(Hubs.id)
        val destinationHubId = varchar("destination_hub_id", 255).references(Hubs.id)
        val carrierId = varchar("carrier_id", 255).references(Carriers.id).nullable()
        val weightKg = double("weight_kg")
        val hazmat = bool("hazmat")
        val customsHold = bool("customs_hold").nullable()
        val shipperAccount = varchar("shipper_account", 255)
        val eta = timestamp("eta").nullable()
        val declaredValue = decimal("declared_value", 18, 2)
        override val primaryKey = PrimaryKey(id)
    }

    object Handling : Table("shipment_handling") {
        val shipmentId = varchar("shipment_id", 255).references(Shipments.id)
        val code = varchar("code", 255)
        override val primaryKey = PrimaryKey(shipmentId, code)
    }

    object Checkpoints : Table("checkpoints") {
        val id = varchar("id", 255)
        val shipmentId = varchar("shipment_id", 255).references(Shipments.id)
        val hubId = varchar("hub_id", 255).references(Hubs.id)
        val event = varchar("event", 255)
        val scannedAt = timestamp("scanned_at")
        override val primaryKey = PrimaryKey(id)
    }

    override val name = "logistics"
    override val tables = listOf(Hubs, Carriers, Shipments, Handling, Checkpoints)
    override val root = Shipments
    override val id = Shipments.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Shipments) {
        "$A.trackingNumber" to Shipments.trackingNo
        "$A.status" to Shipments.status
        "$A.weightKg" to Shipments.weightKg
        "$A.hazmat" to Shipments.hazmat
        "$A.customsHold" to field(Shipments.customsHold, OMITTED)
        "$A.shipper" to Shipments.shipperAccount
        "$A.eta" to field(Shipments.eta, OMITTED)
        "$A.declaredValue" to Shipments.declaredValue
        // Two keys reach hubs, so each names its own.
        "$A.origin" to relation(via = Shipments.originHubId) {
            "code" to Hubs.code
            "country" to Hubs.country
            "region" to Hubs.region
        }
        "$A.destination" to relation(via = Shipments.destinationHubId) {
            "code" to Hubs.code
            "country" to Hubs.country
            "region" to Hubs.region
        }
        "$A.carrier" to relation(Carriers) { "scac" to Carriers.scac }
        "$A.handling" to relation(Handling, element = Handling.code)
        "$A.checkpoints" to relation(Checkpoints) {
            "event" to Checkpoints.event
            "scannedAt" to Checkpoints.scannedAt
            "hub" to relation(Hubs) { "code" to Hubs.code }
        }
    }

    override fun options() = Options.of(mapping)
}
