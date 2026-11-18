import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const nullable = <T extends Parameters<typeof v.union>[0]>(validator: T) => v.union(validator, v.null());

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    caseNumber: v.string(),
    status: v.string(),
    clearanceLevel: v.number(),
    program: v.string(),
    openedOn: v.string(),
    closedOn: nullable(v.string()),
    pii: v.boolean(),
    assignedTo: nullable(v.string()),
    monthlyAmount: nullable(v.number()),
    flags: v.array(v.string()),
    agency: v.object({ code: v.string(), state: v.string() }),
    applicant: v.object({
      countyFips: v.string(),
      householdSize: v.number(),
      citizenship: nullable(v.string()),
    }),
  },
  nulls: "explicit",
  mapper: mapperOf({
    caseNumber: false,
    status: false,
    clearanceLevel: false,
    closedOn: false,
    openedOn: false,
    pii: false,
    assignedTo: false,
    monthlyAmount: false,
    flags: false,
    "agency.code": false,
    "agency.state": false,
    "applicant.countyFips": false,
    "applicant.citizenship": false,
  }),
};

export default mapping;
