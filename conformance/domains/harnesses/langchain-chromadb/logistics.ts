import { bool, flatten, float, id, json, list, opt, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const hub = (prefix: string) => [
  req(`${prefix}.code`),
  req(`${prefix}.country`),
  req(`${prefix}.region`),
];

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("trackingNumber"),
    req("status"),
    req("weightKg", float),
    req("hazmat", bool),
    opt("customsHold", bool),
    req("shipper"),
    opt("eta"),
    req("declaredValue", float),
    ...hub("origin"),
    ...hub("destination"),
    opt("carrier.scac"),
    list("handling"),
    json("checkpoints", { required: true }),
  ]),
  toMetadata: (resource) => flatten(resource),
};

export default mapping;
