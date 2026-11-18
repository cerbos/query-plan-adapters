import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    studentId: v.string(),
    submittedAt: v.optional(v.string()),
    score: v.optional(v.number()),
    late: v.boolean(),
    plagiarismScore: v.optional(v.number()),
    status: v.string(),
    locked: v.boolean(),
    members: v.array(v.string()),
    assignment: v.object({
      dueAt: v.string(),
      maxPoints: v.number(),
      groupWork: v.boolean(),
      course: v.object({
        code: v.string(),
        term: v.string(),
        department: v.object({ id: v.string() }),
        staff: v.array(v.object({ userId: v.string(), role: v.string() })),
      }),
    }),
  },
  nulls: "omitted",
  mapper: mapperOf({
    studentId: false,
    submittedAt: true,
    score: true,
    late: false,
    plagiarismScore: true,
    status: false,
    locked: false,
    members: false,
    "assignment.dueAt": false,
    "assignment.maxPoints": false,
    "assignment.course.term": false,
    "assignment.course.department.id": false,
    "assignment.course.staff": false,
  }),
};

export default mapping;
