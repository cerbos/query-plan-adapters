package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

internal object Retail : ExposedDomain {
    object Stores : Table("stores") {
        val id = varchar("id", 255)
        val region = varchar("region", 255)
        val country = varchar("country", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Customers : Table("customers") {
        val id = varchar("id", 255)
        val email = varchar("email", 255)
        val tier = varchar("tier", 255)
        val loyaltyPoints = integer("loyalty_points")
        override val primaryKey = PrimaryKey(id)
    }

    object Orders : Table("order") {
        val id = varchar("id", 255)
        val customerId = varchar("customer_id", 255).references(Customers.id)
        val storeId = varchar("store_id", 255).references(Stores.id).nullable()
        val status = varchar("status", 255)
        val channel = varchar("channel", 255)
        val totalCents = long("total_cents")
        val currency = varchar("currency", 255)
        val couponCode = varchar("coupon_code", 255).nullable()
        val placedAt = timestamp("placed_at")
        val gift = bool("gift")
        override val primaryKey = PrimaryKey(id)
    }

    object Lines : Table("order_line") {
        val id = varchar("id", 255)
        val orderId = varchar("order_id", 255).references(Orders.id)
        val lineNo = integer("line_no")
        val sku = varchar("sku", 255)
        val category = varchar("category", 255)
        val quantity = integer("quantity")
        val unitPriceCents = long("unit_price_cents")
        override val primaryKey = PrimaryKey(id)
    }

    override val name = "retail"
    override val tables = listOf(Stores, Customers, Orders, Lines)
    override val root = Orders
    override val id = Orders.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Orders) {
        "$A.status" to Orders.status
        "$A.channel" to Orders.channel
        "$A.totalCents" to Orders.totalCents
        "$A.currency" to Orders.currency
        "$A.couponCode" to field(Orders.couponCode, OMITTED)
        "$A.placedAt" to Orders.placedAt
        "$A.isGift" to Orders.gift
        "$A.customer" to relation(Customers) {
            "id" to Customers.id
            "tier" to Customers.tier
            "loyaltyPoints" to Customers.loyaltyPoints
        }
        "$A.store" to relation(Stores) {
            "region" to Stores.region
            "country" to Stores.country
        }
        "$A.lines" to relation(Lines, orderBy = Lines.lineNo) {
            "sku" to Lines.sku
            "category" to Lines.category
            "quantity" to Lines.quantity
            "unitPriceCents" to Lines.unitPriceCents
        }
    }

    override fun options() = Options.of(mapping)
}
