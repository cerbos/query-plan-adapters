package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

internal object Hr : ExposedDomain {
    object Departments : Table("departments") {
        val id = varchar("id", 255)
        val name = varchar("name", 255)
        val costCenter = varchar("cost_center", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Locations : Table("locations") {
        val code = varchar("code", 255)
        val country = varchar("country", 255)
        val city = varchar("city", 255)
        override val primaryKey = PrimaryKey(code)
    }

    object Employees : Table("employees") {
        val id = varchar("id", 255)
        val managerId = varchar("manager_id", 255).references(id).nullable()
        val departmentId = varchar("department_id", 255).references(Departments.id)
        val locationCode = varchar("location_code", 255).references(Locations.code)
        val level = integer("level")
        val employmentType = varchar("employment_type", 255)
        val salaryBand = varchar("salary_band", 255).nullable()
        val hiredOn = timestamp("hired_on")
        val terminatedOn = timestamp("terminated_on").nullable()
        val workEmail = varchar("work_email", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Skills : Table("employee_skills") {
        val employeeId = varchar("employee_id", 255).references(Employees.id)
        val skill = varchar("skill", 255)
        override val primaryKey = PrimaryKey(employeeId, skill)
    }

    override val name = "hr"
    override val tables = listOf(Departments, Locations, Employees, Skills)
    override val root = Employees
    override val id = Employees.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Employees) {
        "request.resource.id" to Employees.id
        "$A.level" to Employees.level
        "$A.employmentType" to Employees.employmentType
        "$A.salaryBand" to field(Employees.salaryBand, OMITTED)
        "$A.hiredOn" to Employees.hiredOn
        "$A.terminatedOn" to field(Employees.terminatedOn, OMITTED)
        "$A.email" to Employees.workEmail
        "$A.skills" to relation(Skills, element = Skills.skill)
        "$A.department" to relation(Departments) {
            "id" to Departments.id
            "costCenter" to Departments.costCenter
        }
        "$A.location" to relation(Locations) {
            "country" to Locations.country
            "city" to Locations.city
        }
        // A self reference is ambiguous to inference, so the key is named.
        "$A.manager" to relation(via = Employees.managerId) {
            "id" to Employees.id
            "manager" to relation(via = Employees.managerId) { "id" to Employees.id }
        }
    }

    override fun options() = Options.of(mapping)
}
