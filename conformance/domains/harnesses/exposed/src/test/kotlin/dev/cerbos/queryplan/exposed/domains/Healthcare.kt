package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

internal object Healthcare : ExposedDomain {
    object Hospitals : Table("hospitals") {
        val id = varchar("id", 255)
        val name = varchar("name", 255)
        val region = varchar("region", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Wards : Table("wards") {
        val id = varchar("id", 255)
        val name = varchar("name", 255)
        val hospitalId = varchar("hospital_id", 255).references(Hospitals.id)
        val secure = bool("secure")
        override val primaryKey = PrimaryKey(id)
    }

    object Patients : Table("patients") {
        val id = varchar("id", 255)
        val mrn = varchar("mrn", 255)
        val wardId = varchar("ward_id", 255).references(Wards.id).nullable()
        val consentResearch = bool("consent_research")
        val vip = bool("vip").nullable()
        val birthYear = integer("birth_year")
        override val primaryKey = PrimaryKey(id)
    }

    object Notes : Table("clinical_notes") {
        val noteId = varchar("note_id", 255)
        val patientId = varchar("patient_id", 255).references(Patients.id)
        val authorId = varchar("author_id", 255)
        val type = varchar("type", 255)
        val sensitivity = integer("sensitivity").nullable()
        val signedAt = timestamp("signed_at").nullable()
        val isDeleted = bool("is_deleted")
        override val primaryKey = PrimaryKey(noteId)
    }

    object CareTeam : Table("note_care_team") {
        val noteId = varchar("note_id", 255).references(Notes.noteId)
        val clinicianId = varchar("clinician_id", 255)
        override val primaryKey = PrimaryKey(noteId, clinicianId)
    }

    object Codes : Table("note_codes") {
        val id = varchar("id", 255)
        val noteId = varchar("note_id", 255).references(Notes.noteId)
        val system = varchar("system", 255)
        val code = varchar("code", 255)
        override val primaryKey = PrimaryKey(id)
    }

    override val name = "healthcare"
    override val tables = listOf(Hospitals, Wards, Patients, Notes, CareTeam, Codes)
    override val root = Notes
    override val id = Notes.noteId

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Notes) {
        "$A.noteType" to Notes.type
        "$A.authorId" to Notes.authorId
        "$A.sensitivity" to field(Notes.sensitivity, OMITTED)
        "$A.signedAt" to field(Notes.signedAt, OMITTED)
        "$A.deleted" to Notes.isDeleted
        "$A.careTeam" to relation(CareTeam, element = CareTeam.clinicianId)
        "$A.codes" to relation(Codes) {
            "system" to Codes.system
            "code" to Codes.code
        }
        "$A.patient" to relation(Patients) {
            "mrn" to Patients.mrn
            "consentResearch" to Patients.consentResearch
            "vip" to field(Patients.vip, OMITTED)
            "birthYear" to Patients.birthYear
            "ward" to relation(Wards) {
                "id" to Wards.id
                "secure" to Wards.secure
                "hospital" to relation(Hospitals) { "region" to Hospitals.region }
            }
        }
    }

    override fun options() = Options.of(mapping)
}
