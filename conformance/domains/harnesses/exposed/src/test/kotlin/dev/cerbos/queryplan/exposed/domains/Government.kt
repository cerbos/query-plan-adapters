package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation

internal object Government : ExposedDomain {
    object Agencies : Table("agencies") {
        val code = varchar("code", 255)
        val name = varchar("name", 255)
        val state = varchar("state", 255)
        override val primaryKey = PrimaryKey(code)
    }

    object Applicants : Table("applicants") {
        val id = varchar("id", 255)
        val countyFips = varchar("county_fips", 255)
        val householdSize = integer("household_size")
        val citizenship = varchar("citizenship", 255).nullable()
        override val primaryKey = PrimaryKey(id)
    }

    object Cases : Table("cases") {
        val id = varchar("id", 255)
        val caseNumber = varchar("case_number", 255)
        val agencyCode = varchar("agency_code", 255).references(Agencies.code)
        val applicantId = varchar("applicant_id", 255).references(Applicants.id)
        val status = varchar("status", 255)
        val clearanceLevel = integer("clearance_level")
        val program = varchar("program", 255)
        val openedOn = timestamp("opened_on")
        val closedOn = timestamp("closed_on").nullable()
        val pii = bool("pii")
        val assignedTo = varchar("assigned_to", 255).nullable()
        val monthlyAmount = decimal("monthly_amount", 18, 2).nullable()
        override val primaryKey = PrimaryKey(id)
    }

    object CaseFlags : Table("case_flags") {
        val caseId = varchar("case_id", 255).references(Cases.id)
        val flag = varchar("flag", 255)
        override val primaryKey = PrimaryKey(caseId, flag)
    }

    override val name = "government"
    override val tables = listOf(Agencies, Applicants, Cases, CaseFlags)
    override val root = Cases
    override val id = Cases.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Cases) {
        "$A.caseNumber" to Cases.caseNumber
        "$A.status" to Cases.status
        "$A.clearanceLevel" to Cases.clearanceLevel
        "$A.program" to Cases.program
        "$A.openedOn" to Cases.openedOn
        "$A.closedOn" to field(Cases.closedOn, EXPLICIT)
        "$A.pii" to Cases.pii
        "$A.assignedTo" to field(Cases.assignedTo, EXPLICIT)
        "$A.monthlyAmount" to field(Cases.monthlyAmount, EXPLICIT)
        "$A.flags" to relation(CaseFlags, element = CaseFlags.flag)
        "$A.agency" to relation(Agencies) {
            "code" to Agencies.code
            "state" to Agencies.state
        }
        "$A.applicant" to relation(Applicants) {
            "countyFips" to Applicants.countyFips
            "householdSize" to Applicants.householdSize
            "citizenship" to field(Applicants.citizenship, EXPLICIT)
        }
    }

    override fun options() = Options.of(mapping).withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT)
}
