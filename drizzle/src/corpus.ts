import * as fs from "fs";
import * as path from "path";

import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
  Value,
} from "@cerbos/core";
import type { AnyColumn, Table } from "drizzle-orm";
import {
  boolean,
  doublePrecision,
  integer as pgInteger,
  pgTable,
  text as pgText,
  timestamp,
} from "drizzle-orm/pg-core";
import {
  boolean as mysqlBoolean,
  datetime,
  double,
  int as mysqlInt,
  mysqlTable,
  varchar,
} from "drizzle-orm/mysql-core";
import { integer, real, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { PlanKind } from ".";
import type { MapperEntry, RelationMapping } from ".";

/**
 * The parts of the shared `../conformance/` corpus that both of this adapter's suites read, plus
 * the reader for the golden expectations this adapter owns.
 *
 * `adversarial.test.ts` plans against a real PDP and executes the translated query against a real
 * store; `translator.test.ts` reads the same actions off the golden wire fixtures and asserts
 * nothing but the emitted filter. They must agree on two things or they prove less than they
 * appear to:
 *
 * - **the schema and the mapper.** The unit test pins the SQL this adapter emits for a mapping;
 *   the harness proves that same SQL returns the rows the PDP allows. Two copies that drifted
 *   would leave the pinned SQL describing a mapping no harness ever executes, which is why
 *   `sqliteSchema()`, `postgresSchema()` and `buildMapper()` live here rather than in either
 *   suite.
 * - **the classification.** Which actions this adapter must refuse, and with which message, is a
 *   corpus decision (`actions.json`), not a per-suite one.
 *
 * The code in this file is duplicated across adapters **on purpose** — adapters share data, not
 * code, so that every adapter stays standalone. Do not extract it into `conformance/`, do not
 * import another adapter's copy, and do not add a drift check between them. See
 * [ADR 0007](../../docs/adr/0007-adapters-share-data-not-code.md).
 *
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const ADAPTER = "drizzle";

export const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

const WIRE_FIXTURES_DIR = path.join(CONFORMANCE_DIR, "wire-fixtures");

/** The golden expectations this adapter owns. Never under `conformance/` — see ADR 0007. */
export const GOLDEN_FILE = path.join(
  __dirname,
  "..",
  "golden",
  "expectations.json",
);

export function readCorpusJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

// -- actions.json --------------------------------------------------------------------------------

export interface UnsupportedShape {
  action: string;
  shape: string;
  /** One entry per adapter that must reject the shape; the corpus asserts the key set. */
  messages: Record<string, string>;
}

export interface AdapterUnsupportedEntry {
  action: string;
  reason: string;
  /** Absent on `adapterSupportedExpected` / `nullRepresentationOmitted`, required on a throw. */
  message?: string;
}

/**
 * A `nullRepresentationOmitted` entry. Every adapter must reject these — the two NULL conventions
 * are indistinguishable on the wire — so `messages` names the whole roster with no promotions to
 * subtract.
 */
export interface NullRepresentationOmittedEntry {
  action: string;
  reason: string;
  messages: Record<string, string>;
}

export interface KnownDivergence {
  action: string;
  adapters: string[];
}

export interface ActionsFile {
  conformance: string[];
  adapterUnsupported?: Record<string, AdapterUnsupportedEntry[]>;
  adapterSupportedExpected?: Record<string, AdapterUnsupportedEntry[]>;
  expectedUnsupported: UnsupportedShape[];
  nullRepresentationOmitted: NullRepresentationOmittedEntry[];
  knownDivergences?: KnownDivergence[];
}

/**
 * A shape this adapter must refuse, with the substring its error has to contain.
 *
 * The message is what turns "it threw" into "it threw for the declared reason": without it a
 * mapper typo or an unrelated validation satisfies the assertion just as well as the limitation
 * the corpus documents (cerbos/query-plan-adapters#326).
 */
export type ThrowingAction = readonly [
  action: string,
  reason: string,
  message: string,
];

export interface ActionClassification {
  oracleActions: string[];
  throwingActions: ThrowingAction[];
  supportedExpected: Set<string>;
}

/** The pinned message, or a failure — a throwing action without one asserts nothing. */
export function requireMessage(
  label: string,
  message: string | undefined,
): string {
  if (message === undefined || message === "") {
    throw new Error(
      `actions.json pins no throw message for ${label}: the throw suite would accept a failure for any reason`,
    );
  }
  return message;
}

export function classifyActionsForAdapter(
  manifest: ActionsFile,
  adapter: string,
): ActionClassification {
  const unsupported = manifest.adapterUnsupported?.[adapter] ?? [];
  const unsupportedActions = new Set(unsupported.map((entry) => entry.action));
  const supportedExpected = new Set(
    (manifest.adapterSupportedExpected?.[adapter] ?? []).map(
      (entry) => entry.action,
    ),
  );
  const oracleActions = [
    ...manifest.conformance.filter(
      (action) => !unsupportedActions.has(action),
    ),
    ...supportedExpected,
  ];
  const throwingActions: ThrowingAction[] = [
    ...unsupported.map((entry): ThrowingAction => [
      entry.action,
      entry.reason,
      requireMessage(
        `adapterUnsupported.${adapter}.${entry.action}`,
        entry.message,
      ),
    ]),
    ...manifest.expectedUnsupported
      .filter((entry) => !supportedExpected.has(entry.action))
      .map((entry): ThrowingAction => [
        entry.action,
        entry.shape,
        requireMessage(
          `expectedUnsupported.${entry.action}.messages.${adapter}`,
          entry.messages?.[adapter],
        ),
      ]),
  ];

  return {
    oracleActions: [...new Set(oracleActions)].sort(),
    throwingActions: throwingActions.sort(([left], [right]) =>
      left.localeCompare(right),
    ),
    supportedExpected,
  };
}

// -- the golden wire fixtures --------------------------------------------------------------------

/**
 * The instant `regenerate-wire-fixtures.sh` substitutes for the one operand it cannot pin.
 *
 * `ts-window` and `ts-vf` compare against `now() - duration("24h")`, which the planner folds to a
 * literal timestamp: a different value on every capture, so the script rewrites it to
 * `__NOW_MINUS_24H__` to keep the drift check deterministic. Reading the fixture back therefore
 * means choosing a value, and the choice is load-bearing rather than arbitrary — Cerbos emits the
 * PDP's clock at nanosecond precision, which is exactly why both actions are `adapterUnsupported`
 * for this adapter (`Timestamp value exceeds millisecond precision`). A tidy millisecond instant
 * here would translate cleanly and quietly contradict `actions.json`, so the fraction carries the
 * nine digits a real plan carries. `translator.test.ts` pins both sides of that boundary, which is
 * what the `plannedAt` override on `planFromWireFixture` is for.
 */
export const PLANNED_AT = "2026-08-11T09:13:39.123456789Z";

interface WireOperand {
  expression?: { operator: string; operands: WireOperand[] };
  variable?: string;
  value?: unknown;
}

interface WireFixture {
  action: string;
  resourceKind: string;
  filter: { kind: string; condition?: WireOperand };
}

function operandFromWire(
  node: WireOperand,
  plannedAt: string,
): PlanExpressionOperand {
  if (node.expression) {
    return new PlanExpression(
      node.expression.operator,
      node.expression.operands.map((child) => operandFromWire(child, plannedAt)),
    );
  }
  if (node.variable !== undefined) {
    return new PlanExpressionVariable(node.variable);
  }
  if (!("value" in node)) {
    throw new Error(
      `Wire fixture operand is neither an expression, a variable nor a value: ${JSON.stringify(node)}`,
    );
  }
  // The one cast in this file. A fixture is JSON the PDP produced, so its leaves are already
  // exactly the JSON shapes `Value` admits — but `JSON.parse` cannot say so, and re-validating a
  // file the corpus workflow regenerates and diffs would assert nothing new.
  return new PlanExpressionValue(
    (node.value === "__NOW_MINUS_24H__" ? plannedAt : node.value) as Value,
  );
}

/** Every action the corpus has a golden wire fixture for, sorted. */
export function wireFixtureActions(): string[] {
  return fs
    .readdirSync(WIRE_FIXTURES_DIR)
    .filter((name) => name.endsWith(".json"))
    .map((name) => name.slice(0, -".json".length))
    .sort();
}

/**
 * The plan the pinned PDP produced for `action`, decoded into the shape the SDK hands callers.
 *
 * The fixture is the PDP's HTTP response, so the decoding here is the one `@cerbos/http` performs
 * — `{expression|variable|value}` nodes into `PlanExpression` / `PlanExpressionVariable` /
 * `PlanExpressionValue`. It is deliberately not a hand-built plan: a plan somebody typed is a
 * belief about what the planner emits, and this repository keeps fixtures precisely because that
 * belief has been wrong before. See docs/adr/0006.
 */
export function planFromWireFixture(
  action: string,
  plannedAt: string = PLANNED_AT,
): PlanResourcesResponse {
  const fixture: WireFixture = JSON.parse(
    fs.readFileSync(path.join(WIRE_FIXTURES_DIR, `${action}.json`), "utf8"),
  );
  const base = {
    cerbosCallId: "",
    requestId: "",
    validationErrors: [],
    metadata: undefined,
  };
  switch (fixture.filter.kind) {
    case PlanKind.CONDITIONAL:
      if (!fixture.filter.condition) {
        throw new Error(
          `Wire fixture ${action} is conditional with no condition`,
        );
      }
      return {
        ...base,
        kind: PlanKind.CONDITIONAL,
        condition: operandFromWire(fixture.filter.condition, plannedAt),
      };
    case PlanKind.ALWAYS_ALLOWED:
    case PlanKind.ALWAYS_DENIED:
      return { ...base, kind: fixture.filter.kind };
    default:
      throw new Error(
        `Wire fixture ${action} has an unrecognised filter kind ${fixture.filter.kind}`,
      );
  }
}

// -- the golden expectations ---------------------------------------------------------------------

/**
 * One store's rendering of one emitted filter: the SQL text Drizzle's dialect produces and the
 * parameters the driver is asked to bind.
 *
 * Both halves are pinned because either alone hides a real defect. The text alone cannot tell a
 * folded `NaN` from a bound one — PostgreSQL accepts `'NaN'::double precision` and every
 * comparison against it is false, so the rows agree either way. The parameters alone cannot tell
 * `cast(x as float(53))` from `cast(x as real)`, which is the difference between IEEE double
 * arithmetic and single precision.
 */
export interface RenderedFilter {
  sql: string;
  params: unknown[];
}

/**
 * The translator output this adapter is pinned to produce for one corpus action.
 *
 * `kind` mirrors `QueryPlanToDrizzleResult`. `ALWAYS_ALLOWED` / `ALWAYS_DENIED` carry no
 * rendering, because there is no filter to render — those are ADR 0006's "expected plan kind"
 * bucket, kept in the same file as the filters so that one lookup answers "is this action
 * accounted for?".
 */
export type GoldenExpectation =
  | { kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED }
  | {
      kind: PlanKind.CONDITIONAL;
      /**
       * One entry per store the adversarial harness executes. Keyed by store rather than by
       * dialect on purpose: the parameters depend on the column types the mapper points at (a
       * SQLite boolean binds `1`, a PostgreSQL boolean binds `true`), so a rendering that no
       * harness executes would pin bytes nothing proves.
       *
       * MySQL joined the list when its leg started executing
       * (cerbos/query-plan-adapters#340). Until then it was claimed by the peer range and run
       * nowhere, so `translator.test.ts` held it to dialect *rules* over the whole corpus rather
       * than pinning bytes no oracle compared. Those rules are still there — a rule holds for a
       * corpus action nobody has added yet, and "never renders `instr(`" is not a statement any
       * one pinned filter makes — but the bytes are pinned now too, because they are proved now.
       */
      rendered: Record<GoldenStore, RenderedFilter>;
    };

/**
 * Appended to rather than reordered: the rendering of an existing store keeps its position in
 * the regenerated file, so adding a store shows up as added lines instead of a rewrite.
 */
export const GOLDEN_STORES = ["sqlite", "postgresql", "mysql"] as const;
export type GoldenStore = (typeof GOLDEN_STORES)[number];

/** The reserved key an entry may carry alongside its expectation; never compared. */
const NOTE_KEY = "note";

export interface GoldenEntry {
  /** Human commentary. Preserved verbatim when the file is regenerated. */
  note?: string;
  expectation: GoldenExpectation;
}

export interface GoldenFile {
  adapter: string;
  regenerate: string;
  expectations: Record<string, GoldenExpectation & { note?: string }>;
}

export const GOLDEN_REGENERATE_COMMAND = "npm run golden:update";

/**
 * The golden expectations, split into the commentary and the value the suite compares.
 *
 * `adapter` is checked rather than ignored: the file is a flat map of action names, so a copy
 * taken from another adapter parses cleanly and would be compared against this adapter's output
 * with only the diff to say something went wrong.
 */
export function readGoldenExpectations(): Map<string, GoldenEntry> {
  const file: GoldenFile = JSON.parse(fs.readFileSync(GOLDEN_FILE, "utf8"));
  if (file.adapter !== ADAPTER) {
    throw new Error(
      `${GOLDEN_FILE} declares adapter "${file.adapter}", not "${ADAPTER}"`,
    );
  }
  return new Map(
    Object.entries(file.expectations).map(([action, entry]) => {
      const { [NOTE_KEY]: note, ...expectation } = entry;
      return [
        action,
        {
          ...(note === undefined ? {} : { note }),
          expectation: expectation as GoldenExpectation,
        },
      ];
    }),
  );
}

/**
 * Rewrite the golden expectations, carrying every existing `note` across.
 *
 * Only ever called under `GOLDEN_UPDATE=1` (`npm run golden:update`). Regeneration is the same
 * deliberate act as `conformance/scripts/regenerate-wire-fixtures.sh`: the safety is the diff a
 * reviewer reads, which is why the entries are written sorted and one action per key.
 *
 * A missing file is not an error here, and only here — that is how a new adapter bootstraps one.
 * Reading a missing file for an assertion stays an error, because a suite that quietly asserts
 * nothing is the failure mode the completeness guard exists to prevent.
 */
export function writeGoldenExpectations(
  expectations: Map<string, GoldenExpectation>,
): void {
  const notes = new Map<string, string>();
  if (fs.existsSync(GOLDEN_FILE)) {
    for (const [action, entry] of readGoldenExpectations()) {
      if (entry.note !== undefined) {
        notes.set(action, entry.note);
      }
    }
  }
  const body: Record<string, GoldenExpectation & { note?: string }> = {};
  for (const action of [...expectations.keys()].sort()) {
    const note = notes.get(action);
    const expectation = expectations.get(action)!;
    body[action] = note === undefined ? expectation : { note, ...expectation };
  }
  const file: GoldenFile = {
    adapter: ADAPTER,
    regenerate: GOLDEN_REGENERATE_COMMAND,
    expectations: body,
  };
  fs.mkdirSync(path.dirname(GOLDEN_FILE), { recursive: true });
  fs.writeFileSync(GOLDEN_FILE, `${JSON.stringify(file, null, 2)}\n`, "utf8");
}

// -- the schema, as the mapper sees it -----------------------------------------------------------
//
// Structural `AnyColumn`/`Table` views so ONE mapper definition serves every dialect: the SQLite
// and PostgreSQL table objects differ in their column types, but the adapter only ever needs the
// table and the column. Two hand-written mappers could drift, and a drifted mapper reads different
// rows on one leg than the other — the same silent projection the corpus README warns about.

export interface AdversarialSchema {
  resources: Table & {
    id: AnyColumn;
    aBool: AnyColumn;
    aString: AnyColumn;
    aNumber: AnyColumn;
    aDouble: AnyColumn;
    aOptionalString: AnyColumn;
    createdBy: AnyColumn;
    scope: AnyColumn;
    createdAt: AnyColumn;
  };
  parents: Table & {
    id: AnyColumn;
    aBool: AnyColumn;
    aString: AnyColumn;
    aNumber: AnyColumn;
    aOptionalString: AnyColumn;
    resourceId: AnyColumn;
  };
  inners: Table & {
    id: AnyColumn;
    aBool: AnyColumn;
    aString: AnyColumn;
    aNumber: AnyColumn;
    aOptionalString: AnyColumn;
    parentId: AnyColumn;
  };
  tags: Table & {
    tagId: AnyColumn;
    name: AnyColumn;
    resourceId: AnyColumn;
  };
  categories: Table & { id: AnyColumn; name: AnyColumn; resourceId: AnyColumn };
  subCategories: Table & {
    id: AnyColumn;
    name: AnyColumn;
    categoryId: AnyColumn;
  };
  labels: Table & {
    id: AnyColumn;
    name: AnyColumn;
    subCategoryId: AnyColumn;
  };
}

/** The SQLite tables the `sqlite` store seeds and the `sqlite` golden rendering is taken from. */
export function sqliteSchema() {
  return {
    resources: sqliteTable("adversarial_resources", {
      id: text("id").primaryKey(),
      aBool: integer("a_bool", { mode: "boolean" }).notNull(),
      aString: text("a_string").notNull(),
      aNumber: integer("a_number").notNull(),
      aDouble: real("a_double"),
      aOptionalString: text("a_optional_string"),
      createdBy: text("created_by").notNull(),
      scope: text("scope"),
      createdAt: text("created_at"),
    }),

    // The corpus's one real to-one chain, one owned row per level and per resource.
    parents: sqliteTable("adversarial_parents", {
      id: text("id").primaryKey(),
      aBool: integer("a_bool", { mode: "boolean" }).notNull(),
      aString: text("a_string").notNull(),
      aNumber: integer("a_number").notNull(),
      aOptionalString: text("a_optional_string"),
      resourceId: text("resource_id").notNull().unique(),
    }),

    inners: sqliteTable("adversarial_inners", {
      id: text("id").primaryKey(),
      aBool: integer("a_bool", { mode: "boolean" }).notNull(),
      aString: text("a_string").notNull(),
      aNumber: integer("a_number").notNull(),
      aOptionalString: text("a_optional_string"),
      parentId: text("parent_id").notNull().unique(),
    }),

    tags: sqliteTable("adversarial_tags", {
      tagId: text("tag_id").primaryKey(),
      // NULLABLE on purpose: a NULL tag name is a missing element attribute on the check
      // side (a CEL error → deny) and must stay UNKNOWN — never FALSE — in SQL.
      name: text("name"),
      resourceId: text("resource_id").notNull(),
    }),

    categories: sqliteTable("adversarial_categories", {
      id: text("id").primaryKey(),
      name: text("name").notNull(),
      resourceId: text("resource_id").notNull(),
    }),

    subCategories: sqliteTable("adversarial_sub_categories", {
      id: text("id").primaryKey(),
      name: text("name").notNull(),
      categoryId: text("category_id").notNull(),
    }),

    labels: sqliteTable("adversarial_labels", {
      id: text("id").primaryKey(),
      name: text("name"),
      subCategoryId: text("sub_category_id").notNull(),
    }),
  };
}

/**
 * The PostgreSQL tables the `postgres` store seeds and the `postgresql` golden rendering is taken
 * from.
 *
 * The column types are the point: `boolean` and `timestamptz` exercise the typed paths SQLite
 * cannot reach — on SQLite a boolean is an integer and a timestamp is text compared
 * lexicographically, so a CASE arm yielding `1` instead of `true`, or a timestamp bound in a
 * layout only string comparison tolerates, passes there and fails here.
 */
export function postgresSchema() {
  return {
    resources: pgTable("adversarial_resources", {
      id: pgText("id").primaryKey(),
      aBool: boolean("a_bool").notNull(),
      aString: pgText("a_string").notNull(),
      aNumber: pgInteger("a_number").notNull(),
      aDouble: doublePrecision("a_double"),
      aOptionalString: pgText("a_optional_string"),
      createdBy: pgText("created_by").notNull(),
      scope: pgText("scope"),
      createdAt: timestamp("created_at", {
        withTimezone: true,
        mode: "string",
      }),
    }),

    // The corpus's one real to-one chain, one owned row per level and per resource.
    parents: pgTable("adversarial_parents", {
      id: pgText("id").primaryKey(),
      aBool: boolean("a_bool").notNull(),
      aString: pgText("a_string").notNull(),
      aNumber: pgInteger("a_number").notNull(),
      aOptionalString: pgText("a_optional_string"),
      resourceId: pgText("resource_id").notNull().unique(),
    }),

    inners: pgTable("adversarial_inners", {
      id: pgText("id").primaryKey(),
      aBool: boolean("a_bool").notNull(),
      aString: pgText("a_string").notNull(),
      aNumber: pgInteger("a_number").notNull(),
      aOptionalString: pgText("a_optional_string"),
      parentId: pgText("parent_id").notNull().unique(),
    }),

    tags: pgTable("adversarial_tags", {
      tagId: pgText("tag_id").primaryKey(),
      // NULLABLE on purpose: a NULL tag name is a missing element attribute on the check
      // side (a CEL error → deny) and must stay UNKNOWN — never FALSE — in SQL.
      name: pgText("name"),
      resourceId: pgText("resource_id").notNull(),
    }),

    categories: pgTable("adversarial_categories", {
      id: pgText("id").primaryKey(),
      name: pgText("name").notNull(),
      resourceId: pgText("resource_id").notNull(),
    }),

    subCategories: pgTable("adversarial_sub_categories", {
      id: pgText("id").primaryKey(),
      name: pgText("name").notNull(),
      categoryId: pgText("category_id").notNull(),
    }),

    labels: pgTable("adversarial_labels", {
      id: pgText("id").primaryKey(),
      name: pgText("name"),
      subCategoryId: pgText("sub_category_id").notNull(),
    }),
  };
}

/**
 * The MySQL tables the `mysql` store seeds and the `mysql` golden rendering is taken from
 * (cerbos/query-plan-adapters#340).
 *
 * MySQL is not a third spelling of the PostgreSQL schema. Three column choices are load-bearing
 * and each is a hazard the other two stores cannot reach:
 *
 * - **`varchar`, not `text`.** MySQL cannot put a `TEXT` column in a primary key or a unique
 *   constraint without a prefix length, and a prefix-indexed key compares a *truncated* value.
 *   The corpus's `id-eq-const` and `id-f2f-ne` filter on the primary key directly.
 * - **`int`, mirroring PostgreSQL's `integer`.** The width is what makes `size(aString) >
 *   4294967296` and `aNumber >= 1.5` interesting: a constant typed from the column rather than
 *   from the value overflows or truncates, which is the second of the two bugs the PostgreSQL leg
 *   found (#320). A `bigint` here would hide half of it.
 * - **`datetime(6)`, not `timestamp`.** MySQL's `TIMESTAMP` is converted between the session time
 *   zone and UTC on every read and write, so the instant a filter compares against would depend
 *   on a connection setting rather than on the stored row; `DATETIME` stores what it is given.
 *   The `(6)` is the corpus's own precision — the a5 seed carries microseconds.
 *
 * The COLLATION is deliberately absent from every string column, unlike `ent`'s hand-written MySQL
 * DDL: the harness starts the server with a case- and accent-sensitive default and the tables
 * inherit it, which keeps the requirement in ONE place rather than repeated on nine columns. It is
 * a requirement either way — see `adversarial.test.ts`, `MYSQL_COLLATION`.
 */
export function mysqlSchema() {
  return {
    resources: mysqlTable("adversarial_resources", {
      id: varchar("id", { length: 64 }).primaryKey(),
      aBool: mysqlBoolean("a_bool").notNull(),
      aString: varchar("a_string", { length: 255 }).notNull(),
      aNumber: mysqlInt("a_number").notNull(),
      aDouble: double("a_double"),
      aOptionalString: varchar("a_optional_string", { length: 255 }),
      createdBy: varchar("created_by", { length: 64 }).notNull(),
      scope: varchar("scope", { length: 255 }),
      createdAt: datetime("created_at", { mode: "string", fsp: 6 }),
    }),

    // The corpus's one real to-one chain, one owned row per level and per resource.
    parents: mysqlTable("adversarial_parents", {
      id: varchar("id", { length: 64 }).primaryKey(),
      aBool: mysqlBoolean("a_bool").notNull(),
      aString: varchar("a_string", { length: 255 }).notNull(),
      aNumber: mysqlInt("a_number").notNull(),
      aOptionalString: varchar("a_optional_string", { length: 255 }),
      resourceId: varchar("resource_id", { length: 64 }).notNull().unique(),
    }),

    inners: mysqlTable("adversarial_inners", {
      id: varchar("id", { length: 64 }).primaryKey(),
      aBool: mysqlBoolean("a_bool").notNull(),
      aString: varchar("a_string", { length: 255 }).notNull(),
      aNumber: mysqlInt("a_number").notNull(),
      aOptionalString: varchar("a_optional_string", { length: 255 }),
      parentId: varchar("parent_id", { length: 64 }).notNull().unique(),
    }),

    tags: mysqlTable("adversarial_tags", {
      tagId: varchar("tag_id", { length: 64 }).primaryKey(),
      // NULLABLE on purpose: a NULL tag name is a missing element attribute on the check
      // side (a CEL error → deny) and must stay UNKNOWN — never FALSE — in SQL.
      name: varchar("name", { length: 255 }),
      resourceId: varchar("resource_id", { length: 64 }).notNull(),
    }),

    categories: mysqlTable("adversarial_categories", {
      id: varchar("id", { length: 64 }).primaryKey(),
      name: varchar("name", { length: 255 }).notNull(),
      resourceId: varchar("resource_id", { length: 64 }).notNull(),
    }),

    subCategories: mysqlTable("adversarial_sub_categories", {
      id: varchar("id", { length: 64 }).primaryKey(),
      name: varchar("name", { length: 255 }).notNull(),
      categoryId: varchar("category_id", { length: 64 }).notNull(),
    }),

    labels: mysqlTable("adversarial_labels", {
      id: varchar("id", { length: 64 }).primaryKey(),
      name: varchar("name", { length: 255 }),
      subCategoryId: varchar("sub_category_id", { length: 64 }).notNull(),
    }),
  };
}

export function buildMapper(
  schema: AdversarialSchema,
): Record<string, MapperEntry> {
  const labelsRelation: RelationMapping = {
    type: "many",
    table: schema.labels,
    sourceColumn: schema.subCategories.id,
    targetColumn: schema.labels.subCategoryId,
    field: schema.labels.name,
    fields: {
      name: schema.labels.name,
    },
  };

  const subCategoriesRelation: RelationMapping = {
    type: "many",
    table: schema.subCategories,
    sourceColumn: schema.categories.id,
    targetColumn: schema.subCategories.categoryId,
    field: schema.subCategories.name,
    fields: {
      name: schema.subCategories.name,
      labels: { relation: labelsRelation },
    },
  };

  return {
    // The primary key, reached as `request.resource.id` rather than through `attr` (the `id-*`
    // actions). It is a mapping like any other here, which is the point: an adapter that resolves
    // references by stripping a `request.resource.attr.` prefix never sees this name.
    "request.resource.id": schema.resources.id,
    "request.resource.attr.aBool": schema.resources.aBool,
    "request.resource.attr.aString": schema.resources.aString,
    "request.resource.attr.aNumber": schema.resources.aNumber,
    "request.resource.attr.aDouble": schema.resources.aDouble,
    "request.resource.attr.aOptionalString": schema.resources.aOptionalString,
    "request.resource.attr.createdBy": schema.resources.createdBy,
    "request.resource.attr.scope": schema.resources.scope,
    "request.resource.attr.createdAt": {
      column: schema.resources.createdAt,
      valueType: "timestamp",
    },
    // `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under the
    // OTHER null convention: the oracle sends a real null attribute for them rather than omitting
    // it. Declaring that here is what makes the equality family definite for these two
    // attributes and leaves it untouched for every other mapping.
    "request.resource.attr.owner": {
      column: schema.resources.aOptionalString,
      nullAttributeRepresentation: "explicit",
    },
    "request.resource.attr.coOwner": {
      column: schema.resources.scope,
      nullAttributeRepresentation: "explicit",
    },
    // obj.inner is not a real nested column — mirrors aString, same trick the spring-data
    // and prisma reference harnesses use for the p-struct probe. `parent.inner` below is the
    // opposite: a real two-level join. The two are kept side by side on purpose.
    "request.resource.attr.obj.inner": schema.resources.aString,
    // The corpus's one REAL to-one chain (the `rel-*` actions). `type: "one"` is what tells the
    // adapter this hop can be ABSENT, which is what the negated shapes discriminate: an absent
    // parent sends no attribute, so CEL raises a missing-path error and the PDP denies, while an
    // unguarded `NOT EXISTS` over the join is TRUE for exactly those rows. `inner` nests the same
    // declaration one level further out.
    "request.resource.attr.parent": {
      relation: {
        type: "one",
        table: schema.parents,
        sourceColumn: schema.resources.id,
        targetColumn: schema.parents.resourceId,
        fields: {
          aBool: schema.parents.aBool,
          aString: schema.parents.aString,
          aNumber: schema.parents.aNumber,
          aOptionalString: schema.parents.aOptionalString,
          inner: {
            relation: {
              type: "one",
              table: schema.inners,
              sourceColumn: schema.parents.id,
              targetColumn: schema.inners.parentId,
              fields: {
                aBool: schema.inners.aBool,
                aString: schema.inners.aString,
                aNumber: schema.inners.aNumber,
                aOptionalString: schema.inners.aOptionalString,
              },
            },
          },
        },
      },
    },
    "request.resource.attr.tags": {
      relation: {
        type: "many",
        table: schema.tags,
        sourceColumn: schema.resources.id,
        targetColumn: schema.tags.resourceId,
        field: schema.tags.name,
        fields: {
          id: schema.tags.tagId,
          name: schema.tags.name,
        },
      },
    },
    "request.resource.attr.tagNames": {
      collectionValueType: "scalar",
      relation: {
        type: "many",
        table: schema.tags,
        sourceColumn: schema.resources.id,
        targetColumn: schema.tags.resourceId,
        field: schema.tags.name,
      },
    },
    "request.resource.attr.categories": {
      relation: {
        type: "many",
        table: schema.categories,
        sourceColumn: schema.resources.id,
        targetColumn: schema.categories.resourceId,
        fields: {
          name: schema.categories.name,
          subCategories: { relation: subCategoriesRelation },
        },
      },
    },
    // Multi-hop chain probe (W1): mainCategory mirrors the SAME categories/subCategories
    // relation as a single-object dotted chain on the check side (every seed holds at most
    // one category), pinning that the adapter joins through every intermediate hop, never
    // off the root. subNames flattens the tail's name column for plain `in` membership.
    "request.resource.attr.mainCategory": {
      relation: {
        type: "many",
        table: schema.categories,
        sourceColumn: schema.resources.id,
        targetColumn: schema.categories.resourceId,
        fields: {
          name: schema.categories.name,
          subCategories: { relation: subCategoriesRelation },
          subNames: { relation: subCategoriesRelation },
        },
      },
    },
  };
}
