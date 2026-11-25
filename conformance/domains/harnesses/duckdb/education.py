from _kit import BOOL, NUM, STR, TS, Attrs, f, path

ROOT, ID = "submissions_v", "id"

VIEWS = """
CREATE VIEW submissions_v AS
SELECT s.*,
    (SELECT coalesce(list(m.student_id ORDER BY m.student_id), []) FROM submission_members m
        WHERE m.submission_id = s.id) AS members
FROM submissions s;

CREATE VIEW courses_v AS
SELECT c.*,
    (SELECT coalesce(list({'user_id': x.user_id, 'role': x.role} ORDER BY x.id), []) FROM course_staff x
        WHERE x.course_id = c.id) AS staff
FROM courses c
"""


def mapping(nulls):
    assignment = ("assignments", "assignment_id", "id")
    course = ("courses_v", "course_id", "id")
    department = ("departments", "department_id", "id")
    return (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.studentId", "student_id", STR)
        .col("attr.submittedAt", "submitted_at", TS)
        .col("attr.score", "score", NUM)
        .col("attr.late", "late", BOOL)
        .col("attr.plagiarismScore", "plagiarism_score", NUM)
        .col("attr.status", "status", STR)
        .col("attr.locked", "locked", BOOL)
        .values("members", "members", STR)
        .via("assignment.dueAt", path(assignment), "due_at", TS)
        .via("assignment.maxPoints", path(assignment), "max_points", NUM)
        .via("assignment.groupWork", path(assignment), "group_work", BOOL)
        .via("assignment.course.code", path(assignment, course), "code", STR)
        .via("assignment.course.term", path(assignment, course), "term", STR)
        .via(
            "assignment.course.department.id",
            path(assignment, course, department),
            "id",
            STR,
        )
        .objects(
            "assignment.course.staff",
            "staff",
            {"userId": f("user_id", STR), "role": f("role", STR)},
            join=path(assignment, course),
        )
        .m
    )
