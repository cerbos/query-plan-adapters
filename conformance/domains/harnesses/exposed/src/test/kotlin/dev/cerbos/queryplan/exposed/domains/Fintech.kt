package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

internal object Fintech : ExposedDomain {
    object Branches : Table("branches") {
        val code = varchar("code", 255)
        val country = varchar("country", 255)
        override val primaryKey = PrimaryKey(code)
    }

    object Customers : Table("customers") {
        val id = varchar("id", 255)
        val legalName = varchar("legal_name", 255)
        val kycLevel = integer("kyc_level")
        val branchCode = varchar("branch_code", 255).references(Branches.code)
        val pep = bool("pep").nullable()
        override val primaryKey = PrimaryKey(id)
    }

    object Accounts : Table("accounts") {
        val id = varchar("id", 255)
        val customerId = varchar("customer_id", 255).references(Customers.id)
        val currency = varchar("currency", 255)
        val status = varchar("status", 255)
        val overdraftLimit = decimal("overdraft_limit", 18, 2).nullable()
        override val primaryKey = PrimaryKey(id)
    }

    object Payments : Table("payments") {
        val id = varchar("id", 255)
        val accountId = varchar("account_id", 255).references(Accounts.id)
        val amount = decimal("amount", 18, 2)
        val currency = varchar("currency", 255)
        val status = varchar("status", 255)
        val riskScore = double("risk_score").nullable()
        val createdAt = timestamp("created_at")
        val cpName = varchar("cp_name", 255)
        val cpIban = varchar("cp_iban", 255)
        val cpCountry = varchar("cp_country", 255)
        val initiatedBy = varchar("initiated_by", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Flags : Table("payment_flags") {
        val paymentId = varchar("payment_id", 255).references(Payments.id)
        val flag = varchar("flag", 255)
        override val primaryKey = PrimaryKey(paymentId, flag)
    }

    object Approvals : Table("payment_approvals") {
        val id = varchar("id", 255)
        val paymentId = varchar("payment_id", 255).references(Payments.id)
        val approverId = varchar("approver_id", 255)
        val decision = varchar("decision", 255)
        override val primaryKey = PrimaryKey(id)
    }

    override val name = "fintech"
    override val tables = listOf(Branches, Customers, Accounts, Payments, Flags, Approvals)
    override val root = Payments
    override val id = Payments.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Payments) {
        "$A.amount" to Payments.amount
        "$A.currency" to Payments.currency
        "$A.status" to Payments.status
        "$A.riskScore" to field(Payments.riskScore, OMITTED)
        "$A.createdAt" to Payments.createdAt
        "$A.initiatedBy" to Payments.initiatedBy
        "$A.counterparty.name" to Payments.cpName
        "$A.counterparty.iban" to Payments.cpIban
        "$A.counterparty.country" to Payments.cpCountry
        "$A.flags" to relation(Flags, element = Flags.flag)
        "$A.approvals" to relation(Approvals) {
            "approverId" to Approvals.approverId
            "decision" to Approvals.decision
        }
        "$A.account" to relation(Accounts) {
            "currency" to Accounts.currency
            "status" to Accounts.status
            "overdraftLimit" to field(Accounts.overdraftLimit, OMITTED)
            "customer" to relation(Customers) {
                "id" to Customers.id
                "kycLevel" to Customers.kycLevel
                "pep" to field(Customers.pep, OMITTED)
                "branch" to relation(Branches) {
                    "code" to Branches.code
                    "country" to Branches.country
                }
            }
        }
    }

    override fun options() = Options.of(mapping)
}
