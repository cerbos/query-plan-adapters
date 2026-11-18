from _types import KEY, BOOL, FLOAT, INT, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

from cerbos_sqlalchemy import Relation, related, relation_overrides

Base = declarative_base()


class Department(Base):
    __tablename__ = "departments"
    id = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)


class Course(Base):
    __tablename__ = "courses"
    id = Column(KEY, primary_key=True)
    code = Column(STR, nullable=False)
    term = Column(STR, nullable=False)
    department_id = Column(KEY, ForeignKey("departments.id"), nullable=False)
    department = relationship(Department, viewonly=True)
    staff = relationship("CourseStaff", viewonly=True)


class CourseStaff(Base):
    __tablename__ = "course_staff"
    id = Column(KEY, primary_key=True)
    course_id = Column(KEY, ForeignKey("courses.id"), nullable=False)
    user_id = Column(STR, nullable=False)
    role = Column(STR, nullable=False)


class Assignment(Base):
    __tablename__ = "assignments"
    id = Column(KEY, primary_key=True)
    course_id = Column(KEY, ForeignKey("courses.id"), nullable=False)
    title = Column(STR, nullable=False)
    due_at = Column(TS, nullable=False)
    max_points = Column(INT, nullable=False)
    group_work = Column(BOOL, nullable=False)
    course = relationship(Course, viewonly=True)


class Submission(Base):
    __tablename__ = "submissions"
    id = Column(KEY, primary_key=True)
    assignment_id = Column(KEY, ForeignKey("assignments.id"), nullable=False)
    student_id = Column(STR, nullable=False)
    submitted_at = Column(TS, nullable=True)
    score = Column(FLOAT, nullable=True)
    late = Column(BOOL, nullable=False)
    plagiarism_score = Column(FLOAT, nullable=True)
    status = Column(STR, nullable=False)
    locked = Column(BOOL, nullable=False)
    assignment = relationship(Assignment, viewonly=True)
    members = relationship("SubmissionMember", viewonly=True)


class SubmissionMember(Base):
    __tablename__ = "submission_members"
    submission_id = Column(KEY, ForeignKey("submissions.id"), primary_key=True)
    student_id = Column(KEY, primary_key=True)


ROOT = Submission
R = "request.resource.attr."

INFERRED = {
    R + "members": related(Submission.members, SubmissionMember.student_id),
}

S = Submission
_assignment = (Assignment.id == S.assignment_id,)
_course = (Course.id == Assignment.course_id, *_assignment)
STAFF = Relation(
    [CourseStaff.course_id == Course.id, *_course],
    [S],
    hop_correlation=list(_course),
)
DECLARED = {
    R + "studentId": S.student_id,
    R + "submittedAt": S.submitted_at,
    R + "score": S.score,
    R + "late": S.late,
    R + "plagiarismScore": S.plagiarism_score,
    R + "status": S.status,
    R + "locked": S.locked,
    R + "members": Relation(
        [SubmissionMember.submission_id == S.id], [S], member=SubmissionMember.student_id
    ),
    R + "assignment.dueAt": scalar(S, Assignment.due_at, *_assignment),
    R + "assignment.maxPoints": scalar(S, Assignment.max_points, *_assignment),
    R + "assignment.groupWork": scalar(S, Assignment.group_work, *_assignment),
    R + "assignment.course.code": scalar(S, Course.code, *_course),
    R + "assignment.course.term": scalar(S, Course.term, *_course),
    R + "assignment.course.department.id": scalar(S, Course.department_id, *_course),
    R + "assignment.course.staff": STAFF,
    "s": STAFF,
    "s.userId": CourseStaff.user_id,
    "s.role": CourseStaff.role,
}


def mapping(style, dialect):
    if style == "inferred":
        return {"attr_map": INFERRED}
    return {"attr_map": DECLARED, "operator_override_fns": relation_overrides()}
