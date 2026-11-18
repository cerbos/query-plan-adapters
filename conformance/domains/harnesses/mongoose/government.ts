import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const schema = new Schema(
  {
    _id: String,
    caseNumber: String,
    status: String,
    clearanceLevel: Number,
    program: String,
    openedOn: Date,
    closedOn: Date,
    pii: Boolean,
    assignedTo: String,
    monthlyAmount: Number,
    flags: [String],
    agency: new Schema({ code: String, state: String }, sub),
    applicant: new Schema(
      { countyFips: String, householdSize: Number, citizenship: String },
      sub,
    ),
  },
  root,
);

const mapping: DomainMapping = {
  schema,
  nulls: "explicit",
  mapper: {
    "request.resource.id": { field: "_id" },
  },
};

export default mapping;
