import { bool, flatten, id, int, json, list, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const NULLABLE = ["privilege", "retentionUntil", "matter.leadPartnerId", "matter.client.industry"];

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("title"),
    json("privilege", { required: true }),
    req("classification", int),
    req("legalHold", bool),
    req("createdBy"),
    json("retentionUntil", { required: true }),
    json("shares", { required: true }),
    req("matter.status"),
    req("matter.practiceArea"),
    json("matter.leadPartnerId", { required: true }),
    list("matter.team"),
    list("matter.screened"),
    req("matter.client.id"),
    json("matter.client.industry", { required: true }),
  ]),
  toMetadata: (resource) => flatten(resource, NULLABLE),
};

export default mapping;
