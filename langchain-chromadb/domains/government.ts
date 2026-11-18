import { bool, flatten, id, int, json, list, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const NULLABLE = ["closedOn", "assignedTo", "monthlyAmount", "applicant.citizenship"];

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("caseNumber"),
    req("status"),
    req("clearanceLevel", int),
    req("program"),
    req("openedOn"),
    json("closedOn", { required: true }),
    req("pii", bool),
    json("assignedTo", { required: true }),
    json("monthlyAmount", { required: true }),
    list("flags"),
    req("agency.code"),
    req("agency.state"),
    req("applicant.countyFips"),
    req("applicant.householdSize", int),
    json("applicant.citizenship", { required: true }),
  ]),
  toMetadata: (resource) => flatten(resource, NULLABLE),
};

export default mapping;
