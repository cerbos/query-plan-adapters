/**
 * The demo domain's single resource kind, as a consumer would model it: flat scalar columns and
 * no relations. The richer schemas the adapter is PROVED against live in ../../src/.
 *
 * Column names are snake_case while the TypeScript properties are camelCase, which is ordinary
 * Drizzle. It also makes the mapper in `main.ts` earn its place: a Cerbos attribute name is
 * neither of those two things, and something has to say which column it means.
 */
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

export const documents = sqliteTable("documents", {
  id: text("id").primaryKey(),
  ownerId: text("owner_id").notNull(),
  public: integer("public", { mode: "boolean" }).notNull(),
  // Never referenced by policy. `archived` and `region` are the application's own columns, and
  // ANDing them with the adapter's filter is usage shape 5.
  region: text("region").notNull(),
  archived: integer("archived", { mode: "boolean" }).notNull(),
});

/**
 * The DDL, kept beside the schema it has to agree with. A consumer would use drizzle-kit; one
 * table does not earn the dependency, and the two cannot drift silently — `main.ts` inserts every
 * seed row through the schema above, so a column this string renames or omits fails the run with
 * "table documents has no column named …" before a single query is planned.
 */
export const CREATE_TABLE = `
  CREATE TABLE documents (
    id       TEXT    PRIMARY KEY,
    owner_id TEXT    NOT NULL,
    public   INTEGER NOT NULL,
    region   TEXT    NOT NULL,
    archived INTEGER NOT NULL
  );
`;
