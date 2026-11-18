import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function healthcare(k: Kit): DomainMapping {
  const hospitals = k.table("hospitals", {
    id: k.text("id").primaryKey(),
    name: k.text("name").notNull(),
    region: k.text("region").notNull(),
  });
  const wards = k.table("wards", {
    id: k.text("id").primaryKey(),
    name: k.text("name").notNull(),
    hospitalId: k.text("hospital_id").notNull().references(() => hospitals.id),
    secure: k.bool("secure").notNull(),
  });
  const patients = k.table("patients", {
    id: k.text("id").primaryKey(),
    mrn: k.text("mrn").notNull(),
    wardId: k.text("ward_id").references(() => wards.id),
    consentResearch: k.bool("consent_research").notNull(),
    vip: k.bool("vip"),
    birthYear: k.int("birth_year").notNull(),
  });
  const clinicalNotes = k.table("clinical_notes", {
    noteId: k.text("note_id").primaryKey(),
    patientId: k.text("patient_id").notNull().references(() => patients.id),
    authorId: k.text("author_id").notNull(),
    type: k.text("type").notNull(),
    sensitivity: k.int("sensitivity"),
    signedAt: k.timestamp("signed_at"),
    isDeleted: k.bool("is_deleted").notNull(),
  });
  const noteCareTeam = k.table(
    "note_care_team",
    {
      noteId: k.text("note_id").notNull().references(() => clinicalNotes.noteId),
      clinicianId: k.text("clinician_id").notNull(),
    },
    ["noteId", "clinicianId"]
  );
  const noteCodes = k.table("note_codes", {
    id: k.text("id").primaryKey(),
    noteId: k.text("note_id").notNull().references(() => clinicalNotes.noteId),
    system: k.text("system").notNull(),
    code: k.text("code").notNull(),
  });

  const schema = {
    hospitals,
    wards,
    patients,
    clinicalNotes,
    noteCareTeam,
    noteCodes,
    wardsRelations: relations(wards, ({ one }) => ({
      hospital: one(hospitals, { fields: [wards.hospitalId], references: [hospitals.id] }),
    })),
    patientsRelations: relations(patients, ({ one }) => ({
      ward: one(wards, { fields: [patients.wardId], references: [wards.id] }),
    })),
    clinicalNotesRelations: relations(clinicalNotes, ({ one, many }) => ({
      patient: one(patients, { fields: [clinicalNotes.patientId], references: [patients.id] }),
      careTeam: many(noteCareTeam),
      codes: many(noteCodes),
    })),
    noteCareTeamRelations: relations(noteCareTeam, ({ one }) => ({
      note: one(clinicalNotes, { fields: [noteCareTeam.noteId], references: [clinicalNotes.noteId] }),
    })),
    noteCodesRelations: relations(noteCodes, ({ one }) => ({
      note: one(clinicalNotes, { fields: [noteCodes.noteId], references: [clinicalNotes.noteId] }),
    })),
  };

  return {
    table: clinicalNotes,
    id: clinicalNotes.noteId,
    schema,
    mapper: {
      "request.resource.attr.noteType": clinicalNotes.type,
      "request.resource.attr.deleted": clinicalNotes.isDeleted,
      "request.resource.attr.signedAt": { column: clinicalNotes.signedAt, valueType: "timestamp" },
      "request.resource.attr.careTeam": {
        collectionValueType: "scalar",
        relation: { name: "careTeam", field: noteCareTeam.clinicianId },
      },
    },
  };
}
