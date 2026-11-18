import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function education(k: Kit): DomainMapping {
  const departments = k.table("departments", {
    id: k.text("id").primaryKey(),
    name: k.text("name").notNull(),
  });

  const courses = k.table("courses", {
    id: k.text("id").primaryKey(),
    code: k.text("code").notNull(),
    term: k.text("term").notNull(),
    departmentId: k.text("department_id").notNull().references(() => departments.id),
  });

  const courseStaff = k.table("course_staff", {
    id: k.text("id").primaryKey(),
    courseId: k.text("course_id").notNull().references(() => courses.id),
    userId: k.text("user_id").notNull(),
    role: k.text("role").notNull(),
  });

  const assignments = k.table("assignments", {
    id: k.text("id").primaryKey(),
    courseId: k.text("course_id").notNull().references(() => courses.id),
    title: k.text("title").notNull(),
    dueAt: k.timestamp("due_at").notNull(),
    maxPoints: k.int("max_points").notNull(),
    groupWork: k.bool("group_work").notNull(),
  });

  const submissions = k.table("submissions", {
    id: k.text("id").primaryKey(),
    assignmentId: k.text("assignment_id").notNull().references(() => assignments.id),
    studentId: k.text("student_id").notNull(),
    submittedAt: k.timestamp("submitted_at"),
    score: k.float("score"),
    late: k.bool("late").notNull(),
    plagiarismScore: k.float("plagiarism_score"),
    status: k.text("status").notNull(),
    locked: k.bool("locked").notNull(),
  });

  const submissionMembers = k.table(
    "submission_members",
    {
      submissionId: k.text("submission_id").notNull().references(() => submissions.id),
      studentId: k.text("student_id").notNull(),
    },
    ["submissionId", "studentId"]
  );

  const schema = {
    departments,
    courses,
    courseStaff,
    assignments,
    submissions,
    submissionMembers,
    coursesRelations: relations(courses, ({ one, many }) => ({
      department: one(departments, { fields: [courses.departmentId], references: [departments.id] }),
      staff: many(courseStaff),
    })),
    courseStaffRelations: relations(courseStaff, ({ one }) => ({
      course: one(courses, { fields: [courseStaff.courseId], references: [courses.id] }),
    })),
    assignmentsRelations: relations(assignments, ({ one }) => ({
      course: one(courses, { fields: [assignments.courseId], references: [courses.id] }),
    })),
    submissionsRelations: relations(submissions, ({ one, many }) => ({
      assignment: one(assignments, { fields: [submissions.assignmentId], references: [assignments.id] }),
      members: many(submissionMembers),
    })),
    submissionMembersRelations: relations(submissionMembers, ({ one }) => ({
      submission: one(submissions, { fields: [submissionMembers.submissionId], references: [submissions.id] }),
    })),
  };

  return {
    table: submissions,
    id: submissions.id,
    schema,
    mapper: {
      "request.resource.attr.submittedAt": { column: submissions.submittedAt, valueType: "timestamp" },
      "request.resource.attr.assignment": {
        relation: {
          name: "assignment",
          fields: {
            dueAt: { column: assignments.dueAt, valueType: "timestamp" },
            course: {
              relation: { name: "course", fields: { staff: { relation: { name: "staff", field: courseStaff.userId } } } },
            },
          },
        },
      },
      "request.resource.attr.members": {
        collectionValueType: "scalar",
        relation: { name: "members", field: submissionMembers.studentId },
      },
    },
  };
}
