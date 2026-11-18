package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

internal object Education : ExposedDomain {
    object Departments : Table("departments") {
        val id = varchar("id", 255)
        val name = varchar("name", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Courses : Table("courses") {
        val id = varchar("id", 255)
        val code = varchar("code", 255)
        val term = varchar("term", 255)
        val departmentId = varchar("department_id", 255).references(Departments.id)
        override val primaryKey = PrimaryKey(id)
    }

    object CourseStaff : Table("course_staff") {
        val id = varchar("id", 255)
        val courseId = varchar("course_id", 255).references(Courses.id)
        val userId = varchar("user_id", 255)
        val role = varchar("role", 255)
        override val primaryKey = PrimaryKey(id)
    }

    object Assignments : Table("assignments") {
        val id = varchar("id", 255)
        val courseId = varchar("course_id", 255).references(Courses.id)
        val title = varchar("title", 255)
        val dueAt = timestamp("due_at")
        val maxPoints = integer("max_points")
        val groupWork = bool("group_work")
        override val primaryKey = PrimaryKey(id)
    }

    object Submissions : Table("submissions") {
        val id = varchar("id", 255)
        val assignmentId = varchar("assignment_id", 255).references(Assignments.id)
        val studentId = varchar("student_id", 255)
        val submittedAt = timestamp("submitted_at").nullable()
        val score = double("score").nullable()
        val late = bool("late")
        val plagiarismScore = double("plagiarism_score").nullable()
        val status = varchar("status", 255)
        val locked = bool("locked")
        override val primaryKey = PrimaryKey(id)
    }

    object Members : Table("submission_members") {
        val submissionId = varchar("submission_id", 255).references(Submissions.id)
        val studentId = varchar("student_id", 255)
        override val primaryKey = PrimaryKey(submissionId, studentId)
    }

    override val name = "education"
    override val tables = listOf(Departments, Courses, CourseStaff, Assignments, Submissions, Members)
    override val root = Submissions
    override val id = Submissions.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Submissions) {
        "$A.studentId" to Submissions.studentId
        "$A.submittedAt" to field(Submissions.submittedAt, OMITTED)
        "$A.score" to field(Submissions.score, OMITTED)
        "$A.late" to Submissions.late
        "$A.plagiarismScore" to field(Submissions.plagiarismScore, OMITTED)
        "$A.status" to Submissions.status
        "$A.locked" to Submissions.locked
        "$A.members" to relation(Members, element = Members.studentId)
        "$A.assignment" to relation(Assignments) {
            "dueAt" to Assignments.dueAt
            "maxPoints" to Assignments.maxPoints
            "groupWork" to Assignments.groupWork
            "course" to relation(Courses) {
                "code" to Courses.code
                "term" to Courses.term
                "department" to relation(Departments) { "id" to Departments.id }
                "staff" to relation(CourseStaff) {
                    "userId" to CourseStaff.userId
                    "role" to CourseStaff.role
                }
            }
        }
    }

    override fun options() = Options.of(mapping)
}
