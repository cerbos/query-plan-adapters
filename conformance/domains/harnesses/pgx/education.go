package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

var Education = func() Domain {
	assignment := join{"assignments", "assignment_id", "id"}
	course := join{"courses", "course_id", "id"}
	department := join{"departments", "department_id", "id"}
	a := newAttrs(cerbospgx.NullConventionOmitted).
		col("id", "id", str).
		col("attr.studentId", "student_id", str).
		col("attr.submittedAt", "submitted_at", ts).
		col("attr.score", "score", num).
		col("attr.late", "late", boo).
		col("attr.plagiarismScore", "plagiarism_score", num).
		col("attr.status", "status", str).
		col("attr.locked", "locked", boo).
		list("members", path(join{"submission_members", "id", "submission_id"}), "student_id", str).
		via("assignment.dueAt", path(assignment), "due_at", ts).
		via("assignment.maxPoints", path(assignment), "max_points", num).
		via("assignment.groupWork", path(assignment), "group_work", boo).
		via("assignment.course.code", path(assignment, course), "code", str).
		via("assignment.course.term", path(assignment, course), "term", str).
		via("assignment.course.department.id", path(assignment, course, department), "id", str).
		objects("assignment.course.staff", path(assignment, course, join{"course_staff", "id", "course_id"}), map[string]cerbospgx.Entry{
			"userId": f("user_id", str), "role": f("role", str),
		})
	return Domain{Name: "education", Table: "submissions", ID: "id", Nulls: cerbospgx.NullOmitted, Mapper: a.m}
}()
