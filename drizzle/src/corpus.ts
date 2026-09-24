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
  json as pgJson,
  jsonb,
  pgTable,
  text as pgText,
  timestamp,
} from "drizzle-orm/pg-core";
import {
  boolean as mysqlBoolean,
  datetime,
  double,
  int as mysqlInt,
  json as mysqlJson,
  mysqlTable,
  varchar,
} from "drizzle-orm/mysql-core";
import { integer, real, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { PlanKind } from ".";
import type { MapperEntry, RelationMapping } from ".";

/**
 * What both of this adapter's suites read from the shared `../conformance/` corpus: the recorded
 * golden plans, the schema each store is built with, and the one mapper every case is translated
 * through. `adversarial.test.ts` replays the goldens against a real store; `translator.test.ts`
 * uses the same mapper for the caller-option tests the corpus cannot vary.
 *
 * Duplicated across adapters on purpose — adapters share data, not code (ADR 0007).
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

export function readCorpusJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

// -- the golden files ----------------------------------------------------------------------------

interface WireOperand {
  expression?: { operator: string; operands: WireOperand[] };
  variable?: string;
  value?: unknown;
}

export interface Golden {
  id: string;
  pdp: string;
  tier: "core" | "extended" | "adversarial";
  plan: { kind: string; condition?: WireOperand };
  allowed: string[];
  plannerDivergence: { pdp?: string[]; reason: string } | null;
}

/** The PDP tags the goldens were recorded against: current first, then previous. */
export function pdpTags(): string[] {
  const versions = readCorpusJson("pdp-versions.json") as Record<
    "current" | "previous",
    { tag: string }
  >;
  return [versions.current.tag, versions.previous.tag];
}

/** Every golden file recorded against `tag`, sorted by case id. */
export function readGoldens(tag: string): Golden[] {
  const root = path.join(CONFORMANCE_DIR, "golden", tag);
  const files = (fs.readdirSync(root, { recursive: true }) as string[])
    .filter((file) => file.endsWith(".json"))
    .map((file) => JSON.parse(fs.readFileSync(path.join(root, file), "utf8")) as Golden);
  return files.sort((a, b) => a.id.localeCompare(b.id));
}

export function readGolden(tag: string, id: string): Golden {
  return JSON.parse(
    fs.readFileSync(path.join(CONFORMANCE_DIR, "golden", tag, `${id}.json`), "utf8"),
  ) as Golden;
}

/**
 * The instant substituted for `__NOW_MINUS_24H__`, the literal the planner folds
 * `now() - duration("24h")` into. The PDP folds its clock at nanosecond precision, so the
 * substitute carries sub-millisecond digits too: a tidy millisecond instant would translate here
 * while the same case refuses in production (`Timestamp value exceeds millisecond precision`).
 */
export function nowMinus24h(): string {
  const ms = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  return `${ms.slice(0, -1)}456789Z`;
}

function operandFromWire(node: WireOperand, now: string): PlanExpressionOperand {
  if (node.expression) {
    return new PlanExpression(
      node.expression.operator,
      node.expression.operands.map((child) => operandFromWire(child, now)),
    );
  }
  if (node.variable !== undefined) {
    return new PlanExpressionVariable(node.variable);
  }
  // The golden is JSON the PDP produced, so its leaves are already the shapes `Value` admits.
  return new PlanExpressionValue(
    (node.value === "__NOW_MINUS_24H__" ? now : node.value) as Value,
  );
}

/** A golden's plan decoded the way `@cerbos/http` decodes a PlanResources response. */
export function planOf(
  golden: Golden,
  now: string = nowMinus24h(),
): PlanResourcesResponse {
  const base = { cerbosCallId: "", requestId: "", validationErrors: [], metadata: undefined };
  const { kind, condition } = golden.plan;
  if (kind === PlanKind.CONDITIONAL && condition) {
    return { ...base, kind: PlanKind.CONDITIONAL, condition: operandFromWire(condition, now) };
  }
  if (kind === PlanKind.ALWAYS_ALLOWED || kind === PlanKind.ALWAYS_DENIED) {
    return { ...base, kind };
  }
  throw new Error(`${golden.id}: unrecognised plan ${JSON.stringify(golden.plan)}`);
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
    updatedAt: AnyColumn;
    tagNamesJson: AnyColumn;
    aNumberListJson: AnyColumn;
    aBoolListJson: AnyColumn;
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

/** The SQLite tables the `sqlite` store seeds. */
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
      updatedAt: text("updated_at"),
      tagNamesJson: text("tag_names_json", { mode: "json" }).$type<(string | null)[]>(),
      aNumberListJson: text("a_number_list_json", { mode: "json" }).$type<(number | null)[]>(),
      aBoolListJson: text("a_bool_list_json", { mode: "json" }).$type<(boolean | null)[]>(),
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
 * The PostgreSQL tables the `postgres` store seeds.
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
      tagNamesJson: jsonb("tag_names_json").$type<(string | null)[]>(),
      tagNamesPlainJson: pgJson("tag_names_plain_json").$type<(string | null)[]>(),
      tagNamesArray: pgText("tag_names_array").array(),
      aNumberListJson: jsonb("a_number_list_json").$type<(number | null)[]>(),
      aNumberListPlainJson: pgJson("a_number_list_plain_json").$type<(number | null)[]>(),
      aNumberListArray: pgInteger("a_number_list_array").array(),
      aBoolListJson: jsonb("a_bool_list_json").$type<(boolean | null)[]>(),
      aBoolListPlainJson: pgJson("a_bool_list_plain_json").$type<(boolean | null)[]>(),
      aBoolListArray: boolean("a_bool_list_array").array(),
      updatedAt: timestamp("updated_at", {
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
 * The MySQL tables the `mysql` store seeds (cerbos/query-plan-adapters#340).
 *
 * MySQL is not a third spelling of the PostgreSQL schema. Three column choices are load-bearing
 * and each is a hazard the other two stores cannot reach:
 *
 * - **`varchar`, not `text`.** MySQL cannot put a `TEXT` column in a primary key or a unique
 *   constraint without a prefix length, and a prefix-indexed key compares a *truncated* value.
 *   The corpus's `identifier/equals/literal` and `identifier/not-equals/field-to-field` filter on the primary key directly.
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
 * DDL: the harness starts the server with a byte-exact default and the tables
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
      updatedAt: datetime("updated_at", { mode: "string", fsp: 6 }),
      tagNamesJson: mysqlJson("tag_names_json").$type<(string | null)[]>(),
      aNumberListJson: mysqlJson("a_number_list_json").$type<(number | null)[]>(),
      aBoolListJson: mysqlJson("a_bool_list_json").$type<(boolean | null)[]>(),
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
    // The primary key, reached as `request.resource.id` rather than through `attr` (the `identifier/*`
    // cases). It is a mapping like any other here, which is the point: an adapter that resolves
    // references by stripping a `request.resource.attr.` prefix never sees this name.
    "request.resource.id": schema.resources.id,
    "request.resource.attr.aBool": schema.resources.aBool,
    "request.resource.attr.aString": schema.resources.aString,
    "request.resource.attr.aNumber": schema.resources.aNumber,
    "request.resource.attr.aDouble": schema.resources.aDouble,
    // The corpus's default NULL convention: a NULL column sends no attribute (resources.json omits
    // it), so `== null` is a missing-attribute error in CEL, never true. Declaring it is what makes
    // the adapter read a NULL column here as UNKNOWN instead of emitting an over-granting IS NULL.
    "request.resource.attr.aOptionalString": {
      column: schema.resources.aOptionalString,
      nullAttributeRepresentation: "omitted",
    },
    "request.resource.attr.createdBy": schema.resources.createdBy,
    "request.resource.attr.scope": schema.resources.scope,
    "request.resource.attr.createdAt": {
      column: schema.resources.createdAt,
      valueType: "timestamp",
    },
    "request.resource.attr.updatedAt": {
      column: schema.resources.updatedAt,
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
    // and prisma reference harnesses use for the `comparison/equals/nested-map-member` probe. `parent.inner` below is the
    // opposite: a real two-level join. The two are kept side by side on purpose.
    "request.resource.attr.obj.inner": schema.resources.aString,
    // The corpus's one REAL to-one chain (the `relation/*` cases). `type: "one"` is what tells the
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
      column: schema.resources.tagNamesJson,
      indexable: "json",
      collectionValueType: "scalar",
      relation: {
        type: "many",
        table: schema.tags,
        sourceColumn: schema.resources.id,
        targetColumn: schema.tags.resourceId,
        field: schema.tags.name,
      },
    },
    // Homogeneous number and boolean lists, read by position (`collection/index/first-element-of-number-list`,
    // `collection/index/first-element-of-boolean-list` and their negated and cross-type siblings)
    // and by membership (`membership/in/literal-in-resource-number-list`, the
    // `type-mismatch/has-intersection/resource-*-list-against-*` cases and their siblings). No relation: the ordered
    // column is the whole mapping, so membership searches its elements too. The cross-type probes
    // are why these exist — SQLite's `json_extract` reads a JSON `true` back as 1 and MySQL's
    // `TRUE` is the integer 1, so a comparison that drops the element's JSON type matches
    // `[true][0] == 1` or `[1][0] == true`, both false in CEL, and MySQL's string-to-number
    // conversion matches `"2" in [2]`.
    "request.resource.attr.aNumberList": {
      column: schema.resources.aNumberListJson,
      indexable: "json",
    },
    "request.resource.attr.aBoolList": {
      column: schema.resources.aBoolListJson,
      indexable: "json",
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
