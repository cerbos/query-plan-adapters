import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function government(k: Kit): DomainMapping {
  const agencies = k.table("agencies", {
    code: k.text("code").primaryKey(),
    name: k.text("name").notNull(),
    state: k.text("state").notNull(),
  });

  const applicants = k.table("applicants", {
    id: k.text("id").primaryKey(),
    countyFips: k.text("county_fips").notNull(),
    householdSize: k.int("household_size").notNull(),
    citizenship: k.text("citizenship"),
  });

  const cases = k.table("cases", {
    id: k.text("id").primaryKey(),
    caseNumber: k.text("case_number").notNull(),
    agencyCode: k.text("agency_code").notNull().references(() => agencies.code),
    applicantId: k.text("applicant_id").notNull().references(() => applicants.id),
    status: k.text("status").notNull(),
    clearanceLevel: k.int("clearance_level").notNull(),
    program: k.text("program").notNull(),
    openedOn: k.timestamp("opened_on").notNull(),
    closedOn: k.timestamp("closed_on"),
    pii: k.bool("pii").notNull(),
    assignedTo: k.text("assigned_to"),
    monthlyAmount: k.decimal("monthly_amount"),
  });

  const caseFlags = k.table(
    "case_flags",
    { caseId: k.text("case_id").notNull().references(() => cases.id), flag: k.text("flag").notNull() },
    ["caseId", "flag"]
  );

  const schema = {
    agencies,
    applicants,
    cases,
    caseFlags,
    casesRelations: relations(cases, ({ one, many }) => ({
      agency: one(agencies, { fields: [cases.agencyCode], references: [agencies.code] }),
      applicant: one(applicants, { fields: [cases.applicantId], references: [applicants.id] }),
      flags: many(caseFlags),
    })),
    caseFlagsRelations: relations(caseFlags, ({ one }) => ({
      case: one(cases, { fields: [caseFlags.caseId], references: [cases.id] }),
    })),
  };

  return {
    table: cases,
    id: cases.id,
    schema,
    nullAttributeRepresentation: "explicit",
    mapper: {
      "request.resource.attr.openedOn": { column: cases.openedOn, valueType: "timestamp" },
      "request.resource.attr.closedOn": { column: cases.closedOn, valueType: "timestamp" },
      "request.resource.attr.flags": {
        collectionValueType: "scalar",
        relation: { name: "flags", field: caseFlags.flag },
      },
    },
  };
}
