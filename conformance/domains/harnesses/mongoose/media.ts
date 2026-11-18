import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const person = new Schema({ id: String, staff: Boolean }, sub);

const schema = new Schema(
  {
    _id: String,
    slug: String,
    status: String,
    language: String,
    embargoUntil: Date,
    publishedAt: Date,
    paywalled: Boolean,
    wordCount: Number,
    keywords: [String],
    section: new Schema({ slug: String, desk: String }, sub),
    author: person,
    contributors: [person],
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
