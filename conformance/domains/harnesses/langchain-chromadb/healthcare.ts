import { bool, flatten, id, int, json, list, opt, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("noteType"),
    req("authorId"),
    opt("sensitivity", int),
    opt("signedAt"),
    req("deleted", bool),
    list("careTeam"),
    json("codes", { required: true }),
    req("patient.mrn"),
    req("patient.consentResearch", bool),
    opt("patient.vip", bool),
    req("patient.birthYear", int),
    opt("patient.ward.id"),
    opt("patient.ward.secure", bool),
    opt("patient.ward.hospital.region"),
  ]),
  toMetadata: (resource) => flatten(resource),
};

export default mapping;
