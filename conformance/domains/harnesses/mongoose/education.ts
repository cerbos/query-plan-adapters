import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const staff = new Schema({ userId: String, role: String }, sub);

const course = new Schema(
  { code: String, term: String, department: new Schema({ id: String }, sub), staff: [staff] },
  sub,
);

const assignment = new Schema(
  { dueAt: Date, maxPoints: Number, groupWork: Boolean, course },
  sub,
);

const schema = new Schema(
  {
    _id: String,
    studentId: String,
    submittedAt: Date,
    score: Number,
    late: Boolean,
    plagiarismScore: Number,
    status: String,
    locked: Boolean,
    members: [String],
    assignment,
  },
  root,
);

const mapping: DomainMapping = {
  schema,
  nulls: "omitted",
  mapper: {
    "request.resource.id": { field: "_id" },
  },
};

export default mapping;
