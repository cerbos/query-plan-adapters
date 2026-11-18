import { flatten, id, int, list, opt, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("level", int),
    req("employmentType"),
    opt("salaryBand"),
    req("hiredOn"),
    opt("terminatedOn"),
    req("email"),
    list("skills"),
    req("department.id"),
    req("department.costCenter"),
    req("location.country"),
    req("location.city"),
    opt("manager.id"),
    opt("manager.manager.id"),
  ]),
  toMetadata: (resource) => flatten(resource),
};

export default mapping;
