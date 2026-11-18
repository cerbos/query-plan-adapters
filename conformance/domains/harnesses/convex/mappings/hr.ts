import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    level: v.number(),
    employmentType: v.string(),
    salaryBand: v.optional(v.string()),
    hiredOn: v.string(),
    terminatedOn: v.optional(v.string()),
    email: v.string(),
    skills: v.array(v.string()),
    department: v.object({ id: v.string(), costCenter: v.string() }),
    location: v.object({ country: v.string(), city: v.string() }),
    manager: v.optional(
      v.object({ id: v.string(), manager: v.optional(v.object({ id: v.string() })) }),
    ),
  },
  nulls: "omitted",
  mapper: mapperOf({
    level: false,
    employmentType: false,
    salaryBand: true,
    hiredOn: false,
    terminatedOn: true,
    email: false,
    skills: false,
    "department.id": false,
    "location.country": false,
    "manager.id": true,
    "manager.manager.id": true,
  }),
};

export default mapping;
