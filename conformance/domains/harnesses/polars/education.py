from _frames import many, one

R = "request.resource.attr."
ID = "id"


def frame(t):
    courses = one(
        t["courses"], "department_id", t["departments"], "id", "department", id="id"
    )
    courses = many(
        courses,
        "id",
        t["course_staff"],
        "course_id",
        "staff",
        "id",
        userId="user_id",
        role="role",
    )
    assignments = one(
        t["assignments"],
        "course_id",
        courses,
        "id",
        "course",
        code="code",
        term="term",
        department="department",
        staff="staff",
    )
    s = one(
        t["submissions"],
        "assignment_id",
        assignments,
        "id",
        "assignment",
        dueAt="due_at",
        maxPoints="max_points",
        groupWork="group_work",
        course="course",
    )
    return many(
        s,
        "id",
        t["submission_members"],
        "submission_id",
        "members",
        "student_id",
        "student_id",
    )


ATTR_MAP = {
    R + "studentId": "student_id",
    R + "submittedAt": "submitted_at",
    R + "plagiarismScore": "plagiarism_score",
}
