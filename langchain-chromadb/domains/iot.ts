import { bool, flatten, id, int, json, opt, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("serial"),
    req("model"),
    req("firmware"),
    opt("lastSeenAt"),
    req("online", bool),
    opt("batteryPct", int),
    json("labels", { required: true }),
    req("decommissioned", bool),
    req("site.region"),
    req("site.org.id"),
    req("site.org.plan"),
    json("alerts", { required: true }),
  ]),
  toMetadata: (resource) => flatten(resource, ["labels"]),
};

export default mapping;
