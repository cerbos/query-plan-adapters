import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const person = v.object({ id: v.string(), staff: v.boolean() });

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    slug: v.string(),
    status: v.string(),
    language: v.string(),
    embargoUntil: v.optional(v.string()),
    publishedAt: v.optional(v.string()),
    paywalled: v.boolean(),
    wordCount: v.number(),
    keywords: v.array(v.string()),
    section: v.object({ slug: v.string(), desk: v.string() }),
    author: person,
    contributors: v.array(person),
  },
  nulls: "omitted",
  mapper: mapperOf({
    slug: false,
    status: false,
    language: false,
    embargoUntil: true,
    paywalled: false,
    wordCount: false,
    keywords: false,
    "section.desk": false,
    "author.id": false,
    "author.staff": false,
    contributors: false,
  }),
};

export default mapping;
