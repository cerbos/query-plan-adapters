package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation

internal object Legal : ExposedDomain {
    object Clients : Table("clients") {
        val id = varchar("id", 255)
        val name = varchar("name", 255)
        val industry = varchar("industry", 255).nullable()
        override val primaryKey = PrimaryKey(id)
    }

    object Matters : Table("matters") {
        val id = varchar("id", 255)
        val clientId = varchar("client_id", 255).references(Clients.id)
        val practiceArea = varchar("practice_area", 255)
        val status = varchar("status", 255)
        val openedAt = timestamp("opened_at")
        val leadPartnerId = varchar("lead_partner_id", 255).nullable()
        override val primaryKey = PrimaryKey(id)
    }

    object Team : Table("matter_team") {
        val matterId = varchar("matter_id", 255).references(Matters.id)
        val userId = varchar("user_id", 255)
        override val primaryKey = PrimaryKey(matterId, userId)
    }

    object Screens : Table("matter_screens") {
        val matterId = varchar("matter_id", 255).references(Matters.id)
        val userId = varchar("user_id", 255)
        override val primaryKey = PrimaryKey(matterId, userId)
    }

    object Documents : Table("documents") {
        val id = varchar("id", 255)
        val matterId = varchar("matter_id", 255).references(Matters.id)
        val title = varchar("title", 255)
        val privilege = varchar("privilege", 255).nullable()
        val classification = integer("classification")
        val legalHold = bool("legal_hold")
        val createdBy = varchar("created_by", 255)
        val retentionUntil = timestamp("retention_until").nullable()
        override val primaryKey = PrimaryKey(id)
    }

    object Shares : Table("document_shares") {
        val id = varchar("id", 255)
        val documentId = varchar("document_id", 255).references(Documents.id)
        val partyType = varchar("party_type", 255)
        val partyId = varchar("party_id", 255)
        override val primaryKey = PrimaryKey(id)
    }

    override val name = "legal"
    override val tables = listOf(Clients, Matters, Team, Screens, Documents, Shares)
    override val root = Documents
    override val id = Documents.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Documents) {
        "$A.title" to Documents.title
        "$A.privilege" to field(Documents.privilege, EXPLICIT)
        "$A.classification" to Documents.classification
        "$A.legalHold" to Documents.legalHold
        "$A.createdBy" to Documents.createdBy
        "$A.retentionUntil" to field(Documents.retentionUntil, EXPLICIT)
        "$A.shares" to relation(Shares) {
            "partyType" to Shares.partyType
            "partyId" to Shares.partyId
        }
        "$A.matter" to relation(Matters) {
            "status" to Matters.status
            "practiceArea" to Matters.practiceArea
            "leadPartnerId" to field(Matters.leadPartnerId, EXPLICIT)
            "team" to relation(Team, element = Team.userId)
            "screened" to relation(Screens, element = Screens.userId)
            "client" to relation(Clients) {
                "id" to Clients.id
                "industry" to field(Clients.industry, EXPLICIT)
            }
        }
    }

    override fun options() = Options.of(mapping).withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT)
}
