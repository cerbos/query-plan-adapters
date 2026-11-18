import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const hospital = new Schema({ region: String }, sub);
const ward = new Schema({ id: String, secure: Boolean, hospital }, sub);
const patient = new Schema(
  { mrn: String, consentResearch: Boolean, vip: Boolean, birthYear: Number, ward },
  sub,
);
const code = new Schema({ system: String, code: String }, sub);

const schema = new Schema(
  {
    _id: String,
    type: { type: String },
    authorId: String,
    sensitivity: Number,
    signedAt: Date,
    isDeleted: Boolean,
    careTeam: [String],
    codes: [code],
    patient,
  },
  root,
);

const mapping: DomainMapping = {
  schema,
  nulls: "omitted",
  mapper: {
    "request.resource.id": { field: "_id" },
    "request.resource.attr.noteType": { field: "type" },
    "request.resource.attr.deleted": { field: "isDeleted" },
  },
  toDocument: ({ id, attr: { noteType, deleted, ...rest } }) => ({
    _id: id,
    type: noteType,
    isDeleted: deleted,
    ...rest,
  }),
};

export default mapping;
