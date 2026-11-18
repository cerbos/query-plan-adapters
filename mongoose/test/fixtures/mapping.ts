import type { MapperConfig } from "../../src";

/**
 * The mapping the conformance corpus's resource is translated through: shared by the unit suite
 * (`../translator.test.ts`) and the conformance harness (`conformance/harnesses/mongoose/`).
 *
 * Test-only: outside `src/`, so it is never built or published.
 */

// -- the mapper ----------------------------------------------------------------------------------

const labelsMapping: MapperConfig = {
  relation: {
    name: "labels",
    type: "many",
    fields: {
      name: { field: "name", nullable: true },
    },
  },
};

const subCategoriesMapping: MapperConfig = {
  relation: {
    name: "subCategories",
    type: "many",
    field: "name",
    fields: {
      name: { field: "name" },
      labels: labelsMapping,
    },
  },
};

export interface ConformanceMapper {
  [reference: string]: MapperConfig;
}

// What only the application knows, which MAPPER and SCHEMA_MAPPER share.
//
// The primary key, reached as `request.resource.id` rather than through `attr` (the
// `identifier/*` cases). An adapter that resolves references by stripping a `request.resource.attr.` prefix
// never sees this name.
//
// It maps to `resourceId`, the string field the harness carries the corpus id in, NOT to an
// ObjectId. That is a deliberate limit on what the corpus can prove here: the ObjectId coercion
// is a caller-supplied `valueParser` on the mapper entry, and three `identifier/*` cases
// compare the key against a STRING column (field-to-field, its negation, concatenation), so a single key
// mapping cannot be an ObjectId and satisfy them. The coercion is pinned where it belongs
// instead — against the `identifier/equals/literal` golden plan in `translator.test.ts`.
//
// The `nullable` columns are those seeds j1, j2 and j3 store a null in, which the corpus sends as a
// missing attribute (conformance/README.md, "The dataset").
//
// `coOwner` aliases the `scope` field under the explicit-null convention: the oracle sends a
// real null attribute for it rather than omitting it. Mongoose stores an explicit null, and
// its query semantics already treat null as a value, so no `nullable` flag applies here — the
// flag means the opposite (a stored null IS a missing attribute).
//
// obj.inner is not a real nested path — it mirrors aString. `parent.inner` below is the
// opposite: a real two-level to-one chain. The two are kept side by side on purpose.
//
// MAPPER also declares each scalar column's schema type, as the README tells a caller to: Mongoose
// casts a query literal to it (`"true"` to `true`, `"5"` to `5`, `0` to `"0"`), and the declaration
// is what lets the adapter answer a literal of another type as CEL does instead (the
// `type-mismatch/*` scalar cases).
//
// Each shared attribute: what the application knows, then what only MAPPER spells out.
const SHARED_ATTRIBUTES: ReadonlyArray<readonly [string, MapperConfig, MapperConfig]> = [
  ["request.resource.id", { field: "resourceId" }, {}],
  ["request.resource.attr.aBool", { nullable: true }, { field: "aBool", valueType: "boolean" }],
  ["request.resource.attr.aString", { nullable: true }, { field: "aString", valueType: "string" }],
  ["request.resource.attr.aNumber", { nullable: true }, { field: "aNumber", valueType: "number" }],
  ["request.resource.attr.aDouble", { nullable: true }, { field: "aDouble", valueType: "number" }],
  ["request.resource.attr.aOptionalString", { nullable: true }, { field: "aOptionalString" }],
  ["request.resource.attr.scope", { nullable: true }, { field: "scope" }],
  ["request.resource.attr.createdAt", { nullable: true }, { field: "createdAt", valueType: "dateTime" }],
  ["request.resource.attr.updatedAt", { nullable: true }, { field: "updatedAt", valueType: "dateTime" }],
  ["request.resource.attr.owner", { field: "aOptionalString" }, {}],
  ["request.resource.attr.coOwner", { field: "scope" }, {}],
  ["request.resource.attr.obj.inner", { field: "aString", nullable: true }, {}],
];

export const MAPPER: ConformanceMapper = {
  ...Object.fromEntries(
    SHARED_ATTRIBUTES.map(([reference, application, declared]) => [reference, { ...declared, ...application }]),
  ),
  "request.resource.attr.createdBy": { field: "createdBy" },
  // The corpus's one REAL to-one chain (the `relation/*` cases), stored as an embedded subdocument
  // per level rather than a joined collection. `type: "one"` flattens the path to `parent.aBool`
  // AND declares the level as absent-able, which is what makes the adapter require it outside
  // any `$nor`: a document with `parent: null` has no `parent.aBool` path at all, and an
  // unguarded negation matches exactly those documents.
  "request.resource.attr.parent": {
    relation: {
      name: "parent",
      type: "one",
      fields: {
        aBool: { field: "aBool", nullable: true },
        aString: { field: "aString", nullable: true },
        aNumber: { field: "aNumber", nullable: true },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
  "request.resource.attr.parent.inner": {
    relation: {
      name: "parent.inner",
      type: "one",
      fields: {
        aBool: { field: "aBool", nullable: true },
        aString: { field: "aString", nullable: true },
        aNumber: { field: "aNumber", nullable: true },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
  "request.resource.attr.peer": {
    relation: {
      name: "peer",
      type: "one",
      fields: {
        aBool: { field: "aBool", nullable: true },
        aString: { field: "aString", nullable: true },
        aNumber: { field: "aNumber", nullable: true },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
  "request.resource.attr.stepParent": {
    relation: {
      name: "stepParent",
      type: "one",
      fields: {
        aBool: { field: "aBool", nullable: true },
        aString: { field: "aString", nullable: true },
        aNumber: { field: "aNumber", nullable: true },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: {
        id: { field: "id" },
        // Typed for the same reason as the scalar columns: Mongoose casts inside `$elemMatch`
        // over a typed subdocument field too (`type-mismatch/exists/element-field-against-number-literal`).
        name: { field: "name", nullable: true, valueType: "string" },
      },
    },
  },
  "request.resource.attr.tagNames": {
    relation: {
      name: "tags",
      type: "many",
      field: "name",
      fields: { name: { field: "name", valueType: "string" } },
    },
  },
  // Homogeneous scalar lists stored as NATIVE arrays on the document — a plain field, not a
  // relation, because an element is a scalar with no field of its own (`tagNames`, the other list
  // the corpus indexes, is a projection of the `tags` objects). `$arrayElemAt` keeps each
  // element's BSON type, so `true` is never `1` here and a null element is a null value, as it is
  // to CEL. No `valueType`: an indexed read is compared inside `$expr`, which never consults it
  // (see the `index` builder in aggregation.ts for why the literal also stays uncast by Mongoose there).
  // Membership (`membership/in/literal-in-resource-number-list` and the list `type-mismatch/in/*` cases) needs no `valueType` either:
  // a plain field is answered inside `$expr` too (`emitUncastListMembership` in filter.ts).
  "request.resource.attr.aNumberList": { field: "aNumberList" },
  "request.resource.attr.aBoolList": { field: "aBoolList" },
  "request.resource.attr.categories": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name" },
        subCategories: subCategoriesMapping,
      },
    },
  },
  "request.resource.attr.mainCategory": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name" },
        subCategories: subCategoriesMapping,
        subNames: subCategoriesMapping,
      },
    },
  },
  // mainCategory is a to-ONE parent on the check side: a seed with no subCategoryNames
  // sends NO mainCategory attribute, so CEL raises a missing-path error and check()
  // denies. The flattened `categories.subCategories` path cannot see that on its own —
  // an absent parent and a childless parent both give an empty array — so the mapping
  // declares the parent and the adapter makes the count UNKNOWN when it is missing
  // (cerbos/query-plan-adapters#309).
  "request.resource.attr.mainCategory.subCategories": {
    relation: {
      name: "categories.subCategories",
      type: "many",
      requiresParent: "categories",
      fields: { name: { field: "name" } },
    },
  },
  "request.resource.attr.mainCategory.subNames": {
    relation: {
      name: "categories.subCategories",
      type: "many",
      field: "name",
      requiresParent: "categories",
      fields: { name: { field: "name" } },
    },
  },
};

const nullableLevel = {
  aBool: { nullable: true },
  aString: { nullable: true },
  aNumber: { nullable: true },
  aOptionalString: { nullable: true },
};

const schemaSubCategories: MapperConfig = {
  relation: {
    name: "subCategories",
    field: "name",
    fields: { labels: { relation: { name: "labels", fields: { name: { nullable: true } } } } },
  },
};

/**
 * `MAPPER` as a caller who passes the Model's schema writes it: only what the schema cannot say.
 * That is a resource attribute stored under another name, a stored null that means a missing
 * attribute, an element field a relation is read through, and a to-one on the `check()` side that
 * is stored as an array. Every relation type, element field and scalar `valueType` is read from
 * the schema, and the harness replays every case through both mappers.
 */
export const SCHEMA_MAPPER: ConformanceMapper = {
  ...Object.fromEntries(SHARED_ATTRIBUTES.map(([reference, application]) => [reference, application])),
  "request.resource.attr.parent": { relation: { name: "parent", fields: nullableLevel } },
  "request.resource.attr.parent.inner": { relation: { name: "parent.inner", fields: nullableLevel } },
  "request.resource.attr.peer": { relation: { name: "peer", fields: nullableLevel } },
  "request.resource.attr.stepParent": { relation: { name: "stepParent", fields: nullableLevel } },
  "request.resource.attr.tags": { relation: { name: "tags", fields: { name: { nullable: true } } } },
  "request.resource.attr.tagNames": { relation: { name: "tags", field: "name" } },
  "request.resource.attr.categories": {
    relation: { name: "categories", fields: { subCategories: schemaSubCategories } },
  },
  "request.resource.attr.mainCategory": {
    relation: {
      name: "categories",
      fields: { subCategories: schemaSubCategories, subNames: schemaSubCategories },
    },
  },
  "request.resource.attr.mainCategory.subCategories": {
    relation: { name: "categories.subCategories", requiresParent: "categories" },
  },
  "request.resource.attr.mainCategory.subNames": {
    relation: { name: "categories.subCategories", field: "name", requiresParent: "categories" },
  },
};
