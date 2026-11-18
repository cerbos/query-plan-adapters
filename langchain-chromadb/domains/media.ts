import { bool, flatten, id, int, json, list, opt, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("slug"),
    req("status"),
    req("language"),
    opt("embargoUntil"),
    opt("publishedAt"),
    req("paywalled", bool),
    req("wordCount", int),
    list("keywords"),
    req("section.slug"),
    req("section.desk"),
    req("author.id"),
    req("author.staff", bool),
    json("contributors", { required: true }),
  ]),
  toMetadata: (resource) => flatten(resource),
};

export default mapping;
