import type { AnyColumn, Table } from "drizzle-orm";
import {
  boolean,
  doublePrecision,
  integer as pgInteger,
  json as pgJson,
  jsonb,
  pgTable,
  type AnyPgColumn,
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
  type AnyMySqlColumn,
  varchar,
} from "drizzle-orm/mysql-core";
import { integer, real, sqliteTable, text, type AnySQLiteColumn } from "drizzle-orm/sqlite-core";

import type { DeclaredRelation, MapperEntry } from "../../src";
import { loadModule } from "../../src/compat";
import type { LegacyRelationsDeclaration, RelationV2, RelationV2Column, RelationsSchema } from "../../src/types";

/**
 * The mapping the conformance corpus's resource is translated through: shared by the unit suite
 * (`../translator.test.ts`) and the conformance harness (`conformance/harnesses/drizzle/`).
 *
 * Test-only: outside `src/`, so it is never built or published.
 */

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
    peerId: AnyColumn;
    stepParentId: AnyColumn;
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
    position: AnyColumn;
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

interface LegacyRelationRef {
  readonly referencedTable: Table;
}

interface LegacyRelationHelpers {
  one(
    table: Table,
    config?: { fields: AnyColumn[]; references: AnyColumn[] },
  ): LegacyRelationRef;
  many(table: Table): LegacyRelationRef;
}

type LegacyRelations = (
  table: Table,
  config: (helpers: LegacyRelationHelpers) => Readonly<Record<string, LegacyRelationRef>>,
) => LegacyRelationsDeclaration;

type RelationFactoryV2 = (config: { from: RelationV2Column; to: RelationV2Column }) => RelationV2;

/** `defineRelations()`'s builder: `r.<table>.<column>`, `r.one.<table>(…)` and `r.many.<table>(…)`. */
type RelationsBuilderV2 = Readonly<Record<string, Readonly<Record<string, RelationV2Column | RelationFactoryV2>>>>;

type DefineRelations = (
  tables: Readonly<Record<string, Table>>,
  config: (r: RelationsBuilderV2) => Readonly<Record<string, Readonly<Record<string, RelationV2>>>>,
) => RelationsSchema;

interface RelationsModule {
  readonly relations?: LegacyRelations;
  readonly defineRelations?: DefineRelations;
}

const loadRelationsModule = (id: string): RelationsModule => loadModule<RelationsModule>(id) ?? {};

const isRelationFactory = (
  entry: RelationV2Column | RelationFactoryV2 | undefined,
): entry is RelationFactoryV2 => typeof entry === "function";

const isRelationColumn = (
  entry: RelationV2Column | RelationFactoryV2 | undefined,
): entry is RelationV2Column => entry !== undefined && !isRelationFactory(entry);

/** `r.one.parents({ from: r.resources.id, to: r.parents.resourceId })`, spelled out of a string-keyed builder. */
const relationV2 = (
  r: RelationsBuilderV2,
  kind: "one" | "many",
  target: string,
  from: [string, string],
  to: [string, string],
): RelationV2 => {
  const factory = r[kind]?.[target];
  const source = r[from[0]]?.[from[1]];
  const destination = r[to[0]]?.[to[1]];

  if (!isRelationFactory(factory) || !isRelationColumn(source) || !isRelationColumn(destination)) {
    throw new Error(`defineRelations() builder has no ${kind}.${target}`);
  }

  return factory({ from: source, to: destination });
};

/**
 * The relations an application on this drizzle would declare: `defineRelations()` on 1.0 (unless
 * QPA_DRIZZLE_RELATIONS=legacy), `relations()` before it. `relationsSchema` is what the
 * application passes as the adapter's `schema` option. The mapper names some relations and leaves
 * others to the tables' foreign keys, so the corpus proves both ways a relation is inferred.
 */
function withRelations<T extends AdversarialSchema>(tables: T) {
  const { resources, parents, inners, tags, categories, subCategories, labels } = tables;
  const { defineRelations } = loadRelationsModule("drizzle-orm");

  if (defineRelations !== undefined && process.env["QPA_DRIZZLE_RELATIONS"] !== "legacy") {
    const relationsSchema = defineRelations(
      { resources, parents, inners, tags, categories, subCategories, labels },
      (r) => ({
        resources: {
          parent: relationV2(r, "one", "parents", ["resources", "id"], ["parents", "resourceId"]),
          stepParent: relationV2(r, "one", "parents", ["resources", "stepParentId"], ["parents", "id"]),
          peer: relationV2(r, "one", "resources", ["resources", "peerId"], ["resources", "id"]),
          tags: relationV2(r, "many", "tags", ["resources", "id"], ["tags", "resourceId"]),
          categories: relationV2(r, "many", "categories", ["resources", "id"], ["categories", "resourceId"]),
        },
        parents: {
          inner: relationV2(r, "one", "inners", ["parents", "id"], ["inners", "parentId"]),
        },
        categories: {
          subCategories: relationV2(r, "many", "subCategories", ["categories", "id"], ["subCategories", "categoryId"]),
        },
        subCategories: {
          labels: relationV2(r, "many", "labels", ["subCategories", "id"], ["labels", "subCategoryId"]),
        },
      }),
    );

    return { ...tables, relationsSchema };
  }

  const relations =
    loadRelationsModule("drizzle-orm").relations ?? loadRelationsModule("drizzle-orm/_relations").relations;

  if (relations === undefined) throw new Error("this drizzle-orm exports no relations()");

  const relationsSchema: RelationsSchema = {
    resources,
    parents,
    inners,
    tags,
    categories,
    subCategories,
    labels,
    resourcesRelations: relations(resources, ({ one, many }) => ({
      parent: one(parents),
      stepParent: one(parents, {
        fields: [resources.stepParentId],
        references: [parents.id],
      }),
      peer: one(resources, { fields: [resources.peerId], references: [resources.id] }),
      tags: many(tags),
      categories: many(categories),
    })),
    parentsRelations: relations(parents, ({ one }) => ({
      resource: one(resources, { fields: [parents.resourceId], references: [resources.id] }),
      inner: one(inners),
    })),
    innersRelations: relations(inners, ({ one }) => ({
      parent: one(parents, { fields: [inners.parentId], references: [parents.id] }),
    })),
    tagsRelations: relations(tags, ({ one }) => ({
      resource: one(resources, { fields: [tags.resourceId], references: [resources.id] }),
    })),
    categoriesRelations: relations(categories, ({ one, many }) => ({
      resource: one(resources, { fields: [categories.resourceId], references: [resources.id] }),
      subCategories: many(subCategories),
    })),
    subCategoriesRelations: relations(subCategories, ({ one, many }) => ({
      category: one(categories, { fields: [subCategories.categoryId], references: [categories.id] }),
      labels: many(labels),
    })),
    labelsRelations: relations(labels, ({ one }) => ({
      subCategory: one(subCategories, { fields: [labels.subCategoryId], references: [subCategories.id] }),
    })),
  };

  return { ...tables, relationsSchema };
}

/** The SQLite tables the `sqlite` store seeds. */
export function sqliteSchema() {
  const resources = sqliteTable("adversarial_resources", {
    id: text("id").primaryKey(),
    aBool: integer("a_bool", { mode: "boolean" }),
    aString: text("a_string"),
    aNumber: integer("a_number"),
    aDouble: real("a_double"),
    aOptionalString: text("a_optional_string"),
    createdBy: text("created_by").notNull(),
    scope: text("scope"),
    createdAt: text("created_at"),
    updatedAt: text("updated_at"),
    tagNamesJson: text("tag_names_json", { mode: "json" }).$type<(string | null)[]>(),
    aNumberListJson: text("a_number_list_json", { mode: "json" }).$type<(number | null)[]>(),
    aBoolListJson: text("a_bool_list_json", { mode: "json" }).$type<(boolean | null)[]>(),
    peerId: text("peer_id").references((): AnySQLiteColumn => resources.id),
    stepParentId: text("step_parent_id").unique().references((): AnySQLiteColumn => parents.id),
  });

  // The corpus's one real to-one chain, one owned row per level and per resource.
  const parents = sqliteTable("adversarial_parents", {
    id: text("id").primaryKey(),
    aBool: integer("a_bool", { mode: "boolean" }),
    aString: text("a_string"),
    aNumber: integer("a_number"),
    aOptionalString: text("a_optional_string"),
    resourceId: text("resource_id").unique().references(() => resources.id),
  });

  const inners = sqliteTable("adversarial_inners", {
    id: text("id").primaryKey(),
    aBool: integer("a_bool", { mode: "boolean" }),
    aString: text("a_string"),
    aNumber: integer("a_number"),
    aOptionalString: text("a_optional_string"),
    parentId: text("parent_id").notNull().unique().references(() => parents.id),
  });

  const tags = sqliteTable("adversarial_tags", {
    tagId: text("tag_id").primaryKey(),
    // NULLABLE on purpose: a NULL tag name is a missing element attribute on the check
    // side (a CEL error → deny) and must stay UNKNOWN — never FALSE — in SQL.
    name: text("name"),
    position: integer("position").notNull(),
    resourceId: text("resource_id").notNull().references(() => resources.id),
  });

  const categories = sqliteTable("adversarial_categories", {
    id: text("id").primaryKey(),
    name: text("name").notNull(),
    resourceId: text("resource_id").notNull().references(() => resources.id),
  });

  const subCategories = sqliteTable("adversarial_sub_categories", {
    id: text("id").primaryKey(),
    name: text("name").notNull(),
    categoryId: text("category_id").notNull().references(() => categories.id),
  });

  const labels = sqliteTable("adversarial_labels", {
    id: text("id").primaryKey(),
    name: text("name"),
    subCategoryId: text("sub_category_id").notNull().references(() => subCategories.id),
  });

  return withRelations({ resources, parents, inners, tags, categories, subCategories, labels });
}

/**
 * The PostgreSQL tables the `postgres` store seeds.
 *
 * The column types are the point: `boolean` and `timestamptz` exercise the typed paths SQLite
 * cannot reach — on SQLite a boolean is an integer and a timestamp is text the adapter rewrites into
 * its own string form, so a CASE arm yielding `1` instead of `true`, or a timestamp bound in a
 * layout PostgreSQL cannot parse, passes there and fails here.
 */
export function postgresSchema() {
  const resources = pgTable("adversarial_resources", {
    id: pgText("id").primaryKey(),
    aBool: boolean("a_bool"),
    aString: pgText("a_string"),
    aNumber: pgInteger("a_number"),
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
    peerId: pgText("peer_id").references((): AnyPgColumn => resources.id),
    stepParentId: pgText("step_parent_id").unique().references((): AnyPgColumn => parents.id),
  });

  // The corpus's one real to-one chain, one owned row per level and per resource.
  const parents = pgTable("adversarial_parents", {
    id: pgText("id").primaryKey(),
    aBool: boolean("a_bool"),
    aString: pgText("a_string"),
    aNumber: pgInteger("a_number"),
    aOptionalString: pgText("a_optional_string"),
    resourceId: pgText("resource_id").unique().references(() => resources.id),
  });

  const inners = pgTable("adversarial_inners", {
    id: pgText("id").primaryKey(),
    aBool: boolean("a_bool"),
    aString: pgText("a_string"),
    aNumber: pgInteger("a_number"),
    aOptionalString: pgText("a_optional_string"),
    parentId: pgText("parent_id").notNull().unique().references(() => parents.id),
  });

  const tags = pgTable("adversarial_tags", {
    tagId: pgText("tag_id").primaryKey(),
    // NULLABLE on purpose: a NULL tag name is a missing element attribute on the check
    // side (a CEL error → deny) and must stay UNKNOWN — never FALSE — in SQL.
    name: pgText("name"),
    position: pgInteger("position").notNull(),
    resourceId: pgText("resource_id").notNull().references(() => resources.id),
  });

  const categories = pgTable("adversarial_categories", {
    id: pgText("id").primaryKey(),
    name: pgText("name").notNull(),
    resourceId: pgText("resource_id").notNull().references(() => resources.id),
  });

  const subCategories = pgTable("adversarial_sub_categories", {
    id: pgText("id").primaryKey(),
    name: pgText("name").notNull(),
    categoryId: pgText("category_id").notNull().references(() => categories.id),
  });

  const labels = pgTable("adversarial_labels", {
    id: pgText("id").primaryKey(),
    name: pgText("name"),
    subCategoryId: pgText("sub_category_id").notNull().references(() => subCategories.id),
  });

  return withRelations({ resources, parents, inners, tags, categories, subCategories, labels });
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
  const resources = mysqlTable("adversarial_resources", {
    id: varchar("id", { length: 64 }).primaryKey(),
    aBool: mysqlBoolean("a_bool"),
    aString: varchar("a_string", { length: 255 }),
    aNumber: mysqlInt("a_number"),
    aDouble: double("a_double"),
    aOptionalString: varchar("a_optional_string", { length: 255 }),
    createdBy: varchar("created_by", { length: 64 }).notNull(),
    scope: varchar("scope", { length: 255 }),
    createdAt: datetime("created_at", { mode: "string", fsp: 6 }),
    updatedAt: datetime("updated_at", { mode: "string", fsp: 6 }),
    tagNamesJson: mysqlJson("tag_names_json").$type<(string | null)[]>(),
    aNumberListJson: mysqlJson("a_number_list_json").$type<(number | null)[]>(),
    aBoolListJson: mysqlJson("a_bool_list_json").$type<(boolean | null)[]>(),
    peerId: varchar("peer_id", { length: 64 }).references((): AnyMySqlColumn => resources.id),
    stepParentId: varchar("step_parent_id", { length: 64 }).unique().references((): AnyMySqlColumn => parents.id),
  });

  // The corpus's one real to-one chain, one owned row per level and per resource.
  const parents = mysqlTable("adversarial_parents", {
    id: varchar("id", { length: 64 }).primaryKey(),
    aBool: mysqlBoolean("a_bool"),
    aString: varchar("a_string", { length: 255 }),
    aNumber: mysqlInt("a_number"),
    aOptionalString: varchar("a_optional_string", { length: 255 }),
    resourceId: varchar("resource_id", { length: 64 }).unique().references(() => resources.id),
  });

  const inners = mysqlTable("adversarial_inners", {
    id: varchar("id", { length: 64 }).primaryKey(),
    aBool: mysqlBoolean("a_bool"),
    aString: varchar("a_string", { length: 255 }),
    aNumber: mysqlInt("a_number"),
    aOptionalString: varchar("a_optional_string", { length: 255 }),
    parentId: varchar("parent_id", { length: 64 }).notNull().unique().references(() => parents.id),
  });

  const tags = mysqlTable("adversarial_tags", {
    tagId: varchar("tag_id", { length: 64 }).primaryKey(),
    // NULLABLE on purpose: a NULL tag name is a missing element attribute on the check
    // side (a CEL error → deny) and must stay UNKNOWN — never FALSE — in SQL.
    name: varchar("name", { length: 255 }),
    position: mysqlInt("position").notNull(),
    resourceId: varchar("resource_id", { length: 64 }).notNull().references(() => resources.id),
  });

  const categories = mysqlTable("adversarial_categories", {
    id: varchar("id", { length: 64 }).primaryKey(),
    name: varchar("name", { length: 255 }).notNull(),
    resourceId: varchar("resource_id", { length: 64 }).notNull().references(() => resources.id),
  });

  const subCategories = mysqlTable("adversarial_sub_categories", {
    id: varchar("id", { length: 64 }).primaryKey(),
    name: varchar("name", { length: 255 }).notNull(),
    categoryId: varchar("category_id", { length: 64 }).notNull().references(() => categories.id),
  });

  const labels = mysqlTable("adversarial_labels", {
    id: varchar("id", { length: 64 }).primaryKey(),
    name: varchar("name", { length: 255 }),
    subCategoryId: varchar("sub_category_id", { length: 64 }).notNull().references(() => subCategories.id),
  });

  return withRelations({ resources, parents, inners, tags, categories, subCategories, labels });
}

export interface ConformanceMapper {
  [reference: string]: MapperEntry;
}

/**
 * How the corpus mapper declares its relations. `inferred` names each one, by `relations()` name or
 * by target table, and leaves cardinality and join columns to the schema; `declared` spells every
 * relation out in full and is translated with no `table` or `schema` option. Each harness replays
 * every case under both.
 */
export type MappingMode = "inferred" | "declared";

const SCHEMA_ONLY_REFERENCES = [
  "request.resource.id",
  "request.resource.attr.createdBy",
];

export function buildMapper(
  schema: AdversarialSchema,
  mode: MappingMode = "inferred",
): ConformanceMapper {
  const join = (
    inferred: { name: string } | { table: Table },
    type: "one" | "many",
    table: Table,
    sourceColumn: AnyColumn,
    targetColumn: AnyColumn,
  ): DeclaredRelation =>
    mode === "inferred" ? inferred : { type, table, sourceColumn, targetColumn };

  const labelsRelation: DeclaredRelation = {
    ...join({ name: "labels" }, "many", schema.labels, schema.subCategories.id, schema.labels.subCategoryId),
    field: schema.labels.name,
    fields: {
      name: schema.labels.name,
    },
  };

  const subCategoriesRelation: DeclaredRelation = {
    ...join({ table: schema.subCategories }, "many", schema.subCategories, schema.categories.id, schema.subCategories.categoryId),
    field: schema.subCategories.name,
    fields: {
      name: schema.subCategories.name,
      labels: { relation: labelsRelation },
    },
  };

  const mapper: ConformanceMapper = {
    // The primary key, reached as `request.resource.id` rather than through `attr` (the `identifier/*`
    // cases). It is a mapping like any other here, which is the point: an adapter that resolves
    // references by stripping a `request.resource.attr.` prefix never sees this name.
    "request.resource.id": schema.resources.id,
    // Every attribute resources.json omits when its column is NULL is declared on the omitted
    // convention: aBool, aString and aNumber (NULL on j3, j1, j2), aOptionalString, aDouble, scope,
    // createdAt, updatedAt, obj.inner and every `parent` field. A null literal against one is then a
    // missing-attribute error CEL denies, so it is refused rather than rendered as IS NULL (#528).
    "request.resource.attr.aBool": {
      column: schema.resources.aBool,
      nullAttributeRepresentation: "omitted",
    },
    "request.resource.attr.aString": {
      column: schema.resources.aString,
      nullAttributeRepresentation: "omitted",
    },
    "request.resource.attr.aNumber": {
      column: schema.resources.aNumber,
      nullAttributeRepresentation: "omitted",
    },
    "request.resource.attr.aDouble": {
      column: schema.resources.aDouble,
      nullAttributeRepresentation: "omitted",
    },
    // The corpus's default NULL convention: a NULL column sends no attribute (resources.json omits
    // it), so `== null` is a missing-attribute error in CEL, never true. Declaring it is what makes
    // the adapter read a NULL column here as UNKNOWN instead of emitting an over-granting IS NULL.
    "request.resource.attr.aOptionalString": {
      column: schema.resources.aOptionalString,
      nullAttributeRepresentation: "omitted",
    },
    "request.resource.attr.createdBy": schema.resources.createdBy,
    "request.resource.attr.scope": {
      column: schema.resources.scope,
      nullAttributeRepresentation: "omitted",
    },
    "request.resource.attr.createdAt": {
      column: schema.resources.createdAt,
      valueType: "timestamp",
      nullAttributeRepresentation: "omitted",
    },
    "request.resource.attr.updatedAt": {
      column: schema.resources.updatedAt,
      valueType: "timestamp",
      nullAttributeRepresentation: "omitted",
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
    "request.resource.attr.obj.inner": {
      column: schema.resources.aString,
      nullAttributeRepresentation: "omitted",
    },
    // The corpus's one REAL to-one chain (the `relation/*` cases). `type: "one"` (inferred from
    // the unique `resourceId`) is what tells the adapter this hop can be ABSENT, which is what the negated shapes discriminate: an absent
    // parent sends no attribute, so CEL raises a missing-path error and the PDP denies, while an
    // unguarded `NOT EXISTS` over the join is TRUE for exactly those rows. `inner` nests the same
    // declaration one level further out.
    "request.resource.attr.parent": {
      relation: {
        ...join({ name: "parent" }, "one", schema.parents, schema.resources.id, schema.parents.resourceId),
        fields: {
          aBool: { column: schema.parents.aBool, nullAttributeRepresentation: "omitted" },
          aString: { column: schema.parents.aString, nullAttributeRepresentation: "omitted" },
          aNumber: { column: schema.parents.aNumber, nullAttributeRepresentation: "omitted" },
          aOptionalString: { column: schema.parents.aOptionalString, nullAttributeRepresentation: "omitted" },
          inner: {
            relation: {
              ...join({ name: "inner" }, "one", schema.inners, schema.parents.id, schema.inners.parentId),
              fields: {
                aBool: { column: schema.inners.aBool, nullAttributeRepresentation: "omitted" },
                aString: { column: schema.inners.aString, nullAttributeRepresentation: "omitted" },
                aNumber: { column: schema.inners.aNumber, nullAttributeRepresentation: "omitted" },
                aOptionalString: { column: schema.inners.aOptionalString, nullAttributeRepresentation: "omitted" },
              },
            },
          },
        },
      },
    },
    // Two more to-one relations for joins that reach a table twice: `peer` is a resource row (a
    // foreign key from the resource table to itself) and `stepParent` a second row in the parent table.
    "request.resource.attr.peer": {
      relation: {
        ...join({ name: "peer" }, "one", schema.resources, schema.resources.peerId, schema.resources.id),
        fields: {
          aBool: { column: schema.resources.aBool, nullAttributeRepresentation: "omitted" },
          aString: { column: schema.resources.aString, nullAttributeRepresentation: "omitted" },
          aNumber: { column: schema.resources.aNumber, nullAttributeRepresentation: "omitted" },
          aOptionalString: { column: schema.resources.aOptionalString, nullAttributeRepresentation: "omitted" },
        },
      },
    },
    "request.resource.attr.stepParent": {
      relation: {
        ...join({ name: "stepParent" }, "one", schema.parents, schema.resources.stepParentId, schema.parents.id),
        fields: {
          aBool: { column: schema.parents.aBool, nullAttributeRepresentation: "omitted" },
          aString: { column: schema.parents.aString, nullAttributeRepresentation: "omitted" },
          aNumber: { column: schema.parents.aNumber, nullAttributeRepresentation: "omitted" },
          aOptionalString: { column: schema.parents.aOptionalString, nullAttributeRepresentation: "omitted" },
        },
      },
    },
    "request.resource.attr.tags": {
      relation: {
        ...join({ name: "tags" }, "many", schema.tags, schema.resources.id, schema.tags.resourceId),
        position: schema.tags.position,
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
        ...join({ table: schema.tags }, "many", schema.tags, schema.resources.id, schema.tags.resourceId),
        position: schema.tags.position,
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
        ...join({ name: "categories" }, "many", schema.categories, schema.resources.id, schema.categories.resourceId),
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
        ...join({ table: schema.categories }, "many", schema.categories, schema.resources.id, schema.categories.resourceId),
        fields: {
          name: schema.categories.name,
          subCategories: { relation: subCategoriesRelation },
          subNames: { relation: subCategoriesRelation },
        },
      },
    },
  };

  // What the schema already says, left to the adapter in the inferred mode: the primary key and a
  // column read under the call's NULL convention. Every other entry overrides something the schema
  // cannot say: a NULL convention, a list column, an order, or a default field.
  if (mode === "inferred") {
    for (const reference of SCHEMA_ONLY_REFERENCES) delete mapper[reference];
  }

  return mapper;
}
