import * as fs from "fs";
import * as path from "path";

import { afterAll, describe, expect, test } from "@jest/globals";
import type { JsonObject } from "@bufbuild/protobuf";
import Database from "better-sqlite3";
import { sql } from "drizzle-orm";
import { drizzle as drizzleSqlite } from "drizzle-orm/better-sqlite3";
import { drizzle as drizzleMysql } from "drizzle-orm/mysql2";
import { drizzle as drizzlePostgres } from "drizzle-orm/node-postgres";
import mysql from "mysql2/promise";
import { Client, Pool } from "pg";

import { PlanKind, queryPlanToDrizzle, UnsupportedQueryPlanError } from "../../../../drizzle/src";
import { planOf } from "../../../../drizzle/test/golden-plans";
import type { Golden } from "../../../../drizzle/test/golden-plans";
import { kit } from "./kit";
import type { Dialect, DomainMapping, Kit } from "./kit";

const STORE = (process.env["DOMAIN_STORE"] ?? "sqlite") as Dialect;
const PG_URL = process.env["DOMAIN_PG_URL"] ?? "postgres://postgres:pw@127.0.0.1:15432";
const MYSQL_URL = process.env["DOMAIN_MYSQL_URL"] ?? "mysql://root:pw@127.0.0.1:13306";
const ONLY = process.env["DOMAIN_ONLY"]?.split(",");

const ROOT = path.join(__dirname, "..", "..");

const DOMAINS = fs
  .readdirSync(path.join(__dirname, "mappings"))
  .map((file) => file.replace(/\.ts$/, ""))
  .filter((domain) => !ONLY || ONLY.includes(domain))
  .sort();

interface DomainGolden {
  domain: string;
  pdp: string;
  principalName: string;
  action: string;
  plan: JsonObject;
  allowed: string[];
}

interface Verdict {
  domain: string;
  pdp: string;
  principal: string;
  action: string;
  verdict: "pass" | "refused" | "over-grant" | "under-grant" | "error";
  returned: string[];
  allowed: string[];
  detail: string;
}

const results: Verdict[] = [];
const notes: string[] = [];

function goldens(domain: string): DomainGolden[] {
  const out: DomainGolden[] = [];
  const base = path.join(ROOT, domain, "golden");

  for (const tag of fs.readdirSync(base).sort().reverse()) {
    for (const principal of fs.readdirSync(path.join(base, tag)).sort()) {
      for (const file of fs.readdirSync(path.join(base, tag, principal)).sort()) {
        out.push(JSON.parse(fs.readFileSync(path.join(base, tag, principal, file), "utf8")));
      }
    }
  }

  return out;
}

function statements(sqlText: string): string[] {
  return sqlText
    .split(/;\s*\n/)
    .map((s) => s.replace(/^\s*--.*$/gm, "").trim())
    .filter((s) => s.length > 0);
}

interface Store {
  select: (m: DomainMapping, filter: any) => Promise<string[]>;
  close: () => Promise<void>;
}

async function open(domain: string, m: DomainMapping): Promise<Store> {
  const seed = fs.readFileSync(path.join(ROOT, domain, "generated", "sql", `${STORE}.sql`), "utf8");
  const database = `drizzle_${domain}`;

  if (STORE === "sqlite") {
    const client = new Database(":memory:");
    client.exec(seed);
    const db = drizzleSqlite(client, { schema: m.schema as any });

    return {
      select: async (map, filter) => (await db.select({ id: map.id }).from(map.table).where(filter)).map((r: any) => r.id),
      close: async () => client.close(),
    };
  }

  if (STORE === "postgres") {
    const admin = new Client({ connectionString: `${PG_URL}/postgres` });
    await admin.connect();
    await admin.query(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
    // A linguistic collation, as the adapter's own PostgreSQL leg runs under.
    await admin.query(
      `CREATE DATABASE ${database} TEMPLATE template0 LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LOCALE 'en_US.utf8'`
    );
    await admin.end();
    const pool = new Pool({ connectionString: `${PG_URL}/${database}`, max: 2 });
    await pool.query(seed);
    const db = drizzlePostgres(pool, { schema: m.schema as any });

    return {
      select: async (map, filter) => (await db.select({ id: map.id }).from(map.table).where(filter)).map((r: any) => r.id),
      close: () => pool.end(),
    };
  }

  const admin = await mysql.createConnection(MYSQL_URL);

  const create = async (collation: string) => {
    await admin.query(`DROP DATABASE IF EXISTS ${database}`);
    await admin.query(`CREATE DATABASE ${database} CHARACTER SET utf8mb4 COLLATE ${collation}`);
    await admin.query(`USE ${database}`);

    for (const s of statements(seed)) await admin.query(s);
  };

  try {
    // The server default, as the adapter's own MySQL leg runs under.
    await create("utf8mb4_0900_ai_ci");
  } catch (error) {
    if ((error as { code?: string }).code !== "ER_DUP_ENTRY") throw error;
    notes.push(`${domain}: seed keys collide under utf8mb4_0900_ai_ci; loaded under utf8mb4_0900_bin`);
    await create("utf8mb4_0900_bin");
  }

  await admin.end();
  const pool = mysql.createPool({ uri: `${MYSQL_URL}/${database}`, connectionLimit: 2 });
  const db = drizzleMysql(pool, { schema: m.schema as any, mode: "default" });

  return {
    select: async (map, filter) => (await db.select({ id: map.id }).from(map.table).where(filter)).map((r: any) => r.id),
    close: () => pool.end(),
  };
}

function verdictOf(returned: string[], allowed: string[]): Verdict["verdict"] {
  const allow = new Set(allowed);

  if (returned.some((id) => !allow.has(id))) return "over-grant";

  if (returned.length === allowed.length) return "pass";

  return "under-grant";
}

function message(error: unknown): string {
  const text = error instanceof Error ? `${error.message}${(error as any).cause ? ` | cause: ${(error as any).cause.message}` : ""}` : String(error);

  return text.replace(/\s+/g, " ").slice(0, 400);
}

describe(`domains on ${STORE} (drizzle)`, () => {
  for (const domain of DOMAINS) {
    test(domain, async () => {
      const build: (k: Kit) => DomainMapping = require(`./mappings/${domain}`).default;
      const mapping = build(kit(STORE));
      let store: Store;

      try {
        store = await open(domain, mapping);
      } catch (error) {
        for (const golden of goldens(domain)) {
          results.push({
            domain,
            pdp: golden.pdp,
            principal: golden.principalName,
            action: golden.action,
            verdict: "error",
            returned: [],
            allowed: [...golden.allowed].sort(),
            detail: `load failed: ${message(error)}`,
          });
        }

        return;
      }

      try {
        for (const golden of goldens(domain)) {
          const record: Verdict = {
            domain,
            pdp: golden.pdp,
            principal: golden.principalName,
            action: golden.action,
            verdict: "error",
            returned: [],
            allowed: [...golden.allowed].sort(),
            detail: "",
          };

          let debug = "";

          try {
            const queryPlan = await planOf({
              id: `${domain}/${golden.pdp}/${golden.principalName}/${golden.action}`,
              plan: golden.plan,
            } as unknown as Golden);

            const result = queryPlanToDrizzle({
              queryPlan,
              table: mapping.table,
              schema: mapping.schema,
              mapper: mapping.mapper,
              ...(mapping.nullAttributeRepresentation
                ? { nullAttributeRepresentation: mapping.nullAttributeRepresentation }
                : {}),
            });

            if (result.kind !== PlanKind.ALWAYS_DENIED) {
              const filter = result.kind === PlanKind.CONDITIONAL ? result.filter : sql`1 = 1`;

              if (process.env["DOMAIN_DEBUG"]) {
                const dialect =
                  STORE === "sqlite"
                    ? new (require("drizzle-orm/sqlite-core").SQLiteSyncDialect)()
                    : STORE === "postgres"
                      ? new (require("drizzle-orm/pg-core").PgDialect)()
                      : new (require("drizzle-orm/mysql-core").MySqlDialect)();

                const q = dialect.sqlToQuery(filter);
                debug = `${q.sql} ${JSON.stringify(q.params)}`;
              }

              record.returned = (await store.select(mapping, filter)).sort();
            }

            record.verdict = verdictOf(record.returned, record.allowed);
          } catch (error) {
            record.verdict = error instanceof UnsupportedQueryPlanError ? "refused" : "error";
            record.detail = message(error);
          }

          if (process.env["DOMAIN_DEBUG"]) record.detail = `${record.detail} WHERE ${debug}`;
          results.push(record);
        }
      } finally {
        await store.close();
      }

      expect(results.length).toBeGreaterThan(0);
    });
  }
});

afterAll(() => {
  const out = path.join(ROOT, "results", "drizzle");
  fs.mkdirSync(out, { recursive: true });
  fs.writeFileSync(
    path.join(out, `${STORE}.json`),
    `${JSON.stringify({ adapter: "drizzle", store: STORE, results }, null, 2)}\n`
  );

  if (notes.length > 0) console.warn(notes.join("\n"));
});
