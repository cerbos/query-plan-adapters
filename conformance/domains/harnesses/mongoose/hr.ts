import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const skipManager = new Schema({ id: String }, sub);

const schema = new Schema(
  {
    _id: String,
    level: Number,
    employmentType: String,
    salaryBand: String,
    hiredOn: Date,
    terminatedOn: Date,
    email: String,
    skills: [String],
    department: new Schema({ id: String, costCenter: String }, sub),
    location: new Schema({ country: String, city: String }, sub),
    manager: new Schema({ id: String, manager: skipManager }, sub),
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
