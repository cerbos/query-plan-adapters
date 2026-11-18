import { bool, flatten, float, id, int, json, list, opt, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("studentId"),
    opt("submittedAt"),
    opt("score", float),
    req("late", bool),
    opt("plagiarismScore", float),
    req("status"),
    req("locked", bool),
    list("members"),
    req("assignment.dueAt"),
    req("assignment.maxPoints", int),
    req("assignment.groupWork", bool),
    req("assignment.course.code"),
    req("assignment.course.term"),
    req("assignment.course.department.id"),
    json("assignment.course.staff", { required: true }),
  ]),
  toMetadata: (resource) => flatten(resource),
};

export default mapping;
