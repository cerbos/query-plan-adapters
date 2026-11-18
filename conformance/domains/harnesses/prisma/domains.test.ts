import { execFileSync } from "child_process";
import * as fs from "fs";
import * as path from "path";

import { afterAll, describe, expect, test } from "@jest/globals";
import type { JsonObject } from "@bufbuild/protobuf";

import { PlanKind, queryPlanToPrisma, UnsupportedQueryPlanError } from "@cerbos/orm-prisma";
import type { PrismaDatamodel } from "@cerbos/orm-prisma";
import { planOf } from "@cerbos/orm-prisma/corpus";
import type { Golden } from "@cerbos/orm-prisma/corpus";
import type { DomainMapping } from "./types";

const STORE = process.env["DOMAIN_STORE"] ?? "sqlite";
const MAJOR = process.env["PRISMA_VERSION"] ?? "7";
const PG_URL = process.env["DOMAIN_PG_URL"] ?? "postgres://postgres:pw@127.0.0.1:15432";
const MYSQL_URL = process.env["DOMAIN_MYSQL_URL"] ?? "mysql://root:pw@127.0.0.1:13306";
const ONLY = process.env["DOMAIN_ONLY"]?.split(",");

const ROOT = path.join(__dirname, "..", "..");
const GENERATED = path.join(__dirname, "generated", `v${MAJOR}`, STORE);

const DOMAINS = fs
  .readdirSync(path.join(__dirname, "schemas"))
  .map((file) => file.replace(/\.prisma$/, ""))
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

interface Column {
  name: string;
  type: string;
}

interface Table {
  name: string;
  columns: Column[];
  rows: Record<string, unknown>[];
}

interface ModelInfo {
  model: string;
  fields: Map<string, string>;
  id: string;
}

const results: Verdict[] = [];

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

function schemaModels(domain: string): Map<string, ModelInfo> {
  const text = fs.readFileSync(path.join(GENERATED, domain, "schema.prisma"), "utf8");
  const byTable = new Map<string, ModelInfo>();

  for (const block of text.matchAll(/^model (\w+) \{([\s\S]*?)^\}/gm)) {
    const model = block[1]!;
    const body = block[2]!;
    const table = body.match(/@@map\("([^"]+)"\)/)?.[1] ?? model;
    const fields = new Map<string, string>();
    let id = "";

    for (const line of body.split("\n")) {
      const m = line.match(/^\s+(\w+)\s+(\w+)(\[\])?(\?)?(.*)$/);

      if (!m) continue;
      const [, field, , , , rest] = m;

      if (/@relation/.test(rest!) || /^[A-Z]/.test(m[2]!) && !["String", "Int", "BigInt", "Float", "Decimal", "Boolean", "DateTime", "Json"].includes(m[2]!)) continue;
      const column = rest!.match(/@map\("([^"]+)"\)/)?.[1] ?? field!;
      fields.set(column, field!);

      if (/@id\b/.test(rest!)) id = field!;
    }

    byTable.set(table, { model, fields, id });
  }

  return byTable;
}

function delegateName(model: string): string {
  return model.charAt(0).toLowerCase() + model.slice(1);
}

function coerce(value: unknown, type: string): unknown {
  if (value === null || value === undefined) return value;

  switch (type) {
    case "timestamp":
      return new Date(String(value));
    case "bigint":
      return BigInt(value as number);
    case "decimal":
      return String(value);
    default:
      return value;
  }
}

async function runSql(sql: string, exec: (statement: string) => Promise<unknown>): Promise<void> {
  const statements = sql
    .split(/;\s*\n/)
    .map((s) => s.replace(/^\s*--.*$/gm, "").trim())
    .filter((s) => s.length > 0);

  for (const statement of statements) await exec(statement);
}

interface Loaded {
  client: any;
  datamodel: PrismaDatamodel;
  close: () => Promise<void>;
}

async function load(domain: string, models: Map<string, ModelInfo>): Promise<Loaded> {
  const dir = path.join(GENERATED, domain);
  const { PrismaClient, Prisma } = require(path.join(dir, "client", MAJOR === "7" ? "client" : "index"));

  const datamodel: PrismaDatamodel =
    MAJOR === "7" ? require(path.join(dir, "cerbos", "datamodel")).datamodel : Prisma.dmmf.datamodel;

  const database = `prisma_${domain}`;

  if (STORE === "sqlite") {
    const file = path.join(dir, "domain.db");

    for (const f of [file, `${file}-journal`]) fs.rmSync(f, { force: true });
    const cli = MAJOR === "7" ? "node_modules/prisma/build/index.js" : "node_modules/prisma-v6/build/index.js";
    const extra = MAJOR === "7" ? [`--url=file:${file}`] : ["--skip-generate"];
    execFileSync(process.execPath, [cli, "db", "push", `--schema=${path.join(dir, "schema.prisma")}`, ...extra], {
      cwd: path.join(__dirname, "..", "..", "..", "..", "prisma"),
      stdio: ["ignore", "ignore", "inherit"],
    });
    let client: any;

    if (MAJOR === "7") {
      const { PrismaBetterSqlite3 } = require("@prisma/adapter-better-sqlite3");
      client = new PrismaClient({ adapter: new PrismaBetterSqlite3({ url: file }) });
    } else {
      client = new PrismaClient();
    }

    await client.$executeRawUnsafe("PRAGMA case_sensitive_like = ON");

    const rows = JSON.parse(fs.readFileSync(path.join(ROOT, domain, "generated", "rows.json"), "utf8")) as {
      tables: Table[];
    };

    for (const table of rows.tables) {
      const info = models.get(table.name);

      if (!info) throw new Error(`${domain}: no model for table ${table.name}`);
      const types = new Map(table.columns.map((c) => [c.name, c.type]));

      const data = table.rows.map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([column, value]) => {
            const field = info.fields.get(column);

            if (!field) throw new Error(`${domain}: no field for ${table.name}.${column}`);

            return [field, coerce(value, types.get(column)!)];
          })
        )
      );

      if (data.length > 0) await client[delegateName(info.model)].createMany({ data });
    }

    return { client, datamodel, close: () => client.$disconnect() };
  }

  if (STORE === "postgres") {
    const { Client } = require("pg");
    const admin = new Client({ connectionString: `${PG_URL}/postgres` });
    await admin.connect();
    await admin.query(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
    await admin.query(`CREATE DATABASE ${database} TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C'`);
    await admin.end();
    const url = `${PG_URL}/${database}`;
    const seed = new Client({ connectionString: url });
    await seed.connect();
    await seed.query(fs.readFileSync(path.join(ROOT, domain, "generated", "sql", "postgres.sql"), "utf8"));
    await seed.end();
    let client: any;

    if (MAJOR === "7") {
      const { PrismaPg } = require("@prisma/adapter-pg");
      client = new PrismaClient({ adapter: new PrismaPg({ connectionString: url }) });
    } else {
      client = new PrismaClient({ datasourceUrl: url });
    }

    return { client, datamodel, close: () => client.$disconnect() };
  }

  const mariadb = require("mariadb");
  const { hostname, port, username, password } = new URL(MYSQL_URL);

  const conn = await mariadb.createConnection({
    host: hostname,
    port: Number(port),
    user: username,
    password,
    allowPublicKeyRetrieval: true,
  });

  await conn.query(`DROP DATABASE IF EXISTS ${database}`);
  await conn.query(`CREATE DATABASE ${database} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin`);
  await conn.query(`USE ${database}`);
  await runSql(fs.readFileSync(path.join(ROOT, domain, "generated", "sql", "mysql.sql"), "utf8"), (s) => conn.query(s));
  // README "Database collation is an authorization invariant": convert every table to utf8mb4_0900_bin.
  await conn.query("SET FOREIGN_KEY_CHECKS = 0");

  for (const table of models.keys()) {
    await conn.query(`ALTER TABLE \`${table}\` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin`);
  }

  await conn.query("SET FOREIGN_KEY_CHECKS = 1");
  await conn.end();
  const url = `${MYSQL_URL}/${database}`;
  let client: any;

  if (MAJOR === "7") {
    const { PrismaMariaDb } = require("@prisma/adapter-mariadb");
    client = new PrismaMariaDb(`${url}?allowPublicKeyRetrieval=true`);
    client = new PrismaClient({ adapter: client });
  } else {
    client = new PrismaClient({ datasourceUrl: url });
  }

  return { client, datamodel, close: () => client.$disconnect() };
}

function verdictOf(returned: string[], allowed: string[]): Verdict["verdict"] {
  const allow = new Set(allowed);

  if (returned.some((id) => !allow.has(id))) return "over-grant";

  if (returned.length === allowed.length) return "pass";

  return "under-grant";
}

describe(`domains on ${STORE} (Prisma ${MAJOR})`, () => {
  for (const domain of DOMAINS) {
    test(domain, async () => {
      const mapping: DomainMapping = require(`./mappings/${domain}`).default;
      const models = schemaModels(domain);
      const info = [...models.values()].find((m) => m.model === mapping.model)!;
      let loaded: Loaded;

      try {
        loaded = await load(domain, models);
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
            detail: `load failed: ${String(error instanceof Error ? error.message : error).slice(0, 300)}`,
          });
        }

        return;
      }

      const { client, datamodel, close } = loaded;

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

            const result = queryPlanToPrisma({
              queryPlan,
              model: mapping.model,
              datamodel,
              mapper: mapping.mapper,
              ...(mapping.nullAttributeRepresentation
                ? { nullAttributeRepresentation: mapping.nullAttributeRepresentation }
                : {}),
            });

            if (result.kind !== PlanKind.ALWAYS_DENIED) {
              const where = result.kind === PlanKind.CONDITIONAL ? result.filters : {};
              debug = JSON.stringify(where, (_k, v) => (typeof v === "bigint" ? `${v}n` : v));

              const rows = await client[delegateName(mapping.model)].findMany({
                where,
                select: { [info.id]: true },
              });

              record.returned = rows.map((row: Record<string, string>) => row[info.id]).sort();
            }

            record.verdict = verdictOf(record.returned, record.allowed);
          } catch (error) {
            record.verdict = error instanceof UnsupportedQueryPlanError ? "refused" : "error";
            record.detail = String(error instanceof Error ? error.message : error)
              .split("\n")
              .map((l) => l.trim())
              .filter(Boolean)
              .slice(-3)
              .join(" | ")
              .slice(0, 400);
          }

          if (process.env["DOMAIN_DEBUG"]) record.detail = `${record.detail} WHERE ${debug}`;
          results.push(record);
        }
      } finally {
        await close();
      }

      expect(results.length).toBeGreaterThan(0);
    });
  }
});

afterAll(() => {
  const out = path.join(ROOT, "results", "prisma");
  fs.mkdirSync(out, { recursive: true });
  const file = path.join(out, `${STORE}-v${MAJOR}.json`);
  fs.writeFileSync(file, `${JSON.stringify({ adapter: "prisma", store: `${STORE}-v${MAJOR}`, results }, null, 2)}\n`);
});
