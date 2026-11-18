import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function legal(k: Kit): DomainMapping {
  const clients = k.table("clients", {
    id: k.text("id").primaryKey(),
    name: k.text("name").notNull(),
    industry: k.text("industry"),
  });
  const matters = k.table("matters", {
    id: k.text("id").primaryKey(),
    clientId: k.text("client_id").notNull().references(() => clients.id),
    practiceArea: k.text("practice_area").notNull(),
    status: k.text("status").notNull(),
    openedAt: k.timestamp("opened_at").notNull(),
    leadPartnerId: k.text("lead_partner_id"),
  });
  const matterTeam = k.table(
    "matter_team",
    { matterId: k.text("matter_id").notNull().references(() => matters.id), userId: k.text("user_id").notNull() },
    ["matterId", "userId"]
  );
  const matterScreens = k.table(
    "matter_screens",
    { matterId: k.text("matter_id").notNull().references(() => matters.id), userId: k.text("user_id").notNull() },
    ["matterId", "userId"]
  );
  const documents = k.table("documents", {
    id: k.text("id").primaryKey(),
    matterId: k.text("matter_id").notNull().references(() => matters.id),
    title: k.text("title").notNull(),
    privilege: k.text("privilege"),
    classification: k.int("classification").notNull(),
    legalHold: k.bool("legal_hold").notNull(),
    createdBy: k.text("created_by").notNull(),
    retentionUntil: k.timestamp("retention_until"),
  });
  const documentShares = k.table("document_shares", {
    id: k.text("id").primaryKey(),
    documentId: k.text("document_id").notNull().references(() => documents.id),
    partyType: k.text("party_type").notNull(),
    partyId: k.text("party_id").notNull(),
  });

  const schema = {
    clients,
    matters,
    matterTeam,
    matterScreens,
    documents,
    documentShares,
    mattersRelations: relations(matters, ({ one, many }) => ({
      client: one(clients, { fields: [matters.clientId], references: [clients.id] }),
      team: many(matterTeam),
      screened: many(matterScreens),
    })),
    matterTeamRelations: relations(matterTeam, ({ one }) => ({
      matter: one(matters, { fields: [matterTeam.matterId], references: [matters.id] }),
    })),
    matterScreensRelations: relations(matterScreens, ({ one }) => ({
      matter: one(matters, { fields: [matterScreens.matterId], references: [matters.id] }),
    })),
    documentsRelations: relations(documents, ({ one, many }) => ({
      matter: one(matters, { fields: [documents.matterId], references: [matters.id] }),
      shares: many(documentShares),
    })),
    documentSharesRelations: relations(documentShares, ({ one }) => ({
      document: one(documents, { fields: [documentShares.documentId], references: [documents.id] }),
    })),
  };

  return {
    table: documents,
    id: documents.id,
    schema,
    nullAttributeRepresentation: "explicit",
    mapper: {
      "request.resource.attr.retentionUntil": { column: documents.retentionUntil, valueType: "timestamp" },
      "request.resource.attr.matter": {
        relation: {
          name: "matter",
          fields: {
            team: { collectionValueType: "scalar", relation: { name: "team", field: matterTeam.userId } },
            screened: { collectionValueType: "scalar", relation: { name: "screened", field: matterScreens.userId } },
          },
        },
      },
    },
  };
}
