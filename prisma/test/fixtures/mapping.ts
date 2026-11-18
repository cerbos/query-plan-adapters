import type { MapperConfig } from "../../src";

/**
 * The mapping the conformance corpus's resource is translated through: shared by the unit suite
 * (`../translator.test.ts`) and the conformance harness (`conformance/harnesses/prisma/`).
 *
 * Test-only: outside `src/`, so it is never built or published.
 */

/** The Prisma model the corpus's resource kind maps onto. */
export const MODEL = "AdversarialResource";

// -- the mapper ----------------------------------------------------------------------------------

interface ConformanceMapper {
  [reference: string]: MapperConfig;
}

// What only the application knows, which both mappers share.
//
// Every attribute resources.json OMITS when its column is NULL follows the omitted convention:
// aBool, aString and aNumber (NULL on j3, j1, j2), aOptionalString, aDouble, scope, createdAt,
// updatedAt, obj.inner and every `parent` field. A null literal against one is then a
// missing-attribute error the PDP denies, never an IS NULL match (#528).
//
// `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under the
// OTHER null convention: the oracle sends a real null attribute for them rather than omitting
// it. Declaring that here is what makes the equality family definite for these two attributes
// and leaves it untouched for every other mapping (cerbos/query-plan-adapters#308).
//
// obj.inner is not a real nested column — mirrors aString, same trick the spring-data reference
// harness uses for the comparison/equals/nested-map-member case. `parent.inner` below is the
// opposite: a real two-level join. The two are kept side by side on purpose.
const OMITTED = { nullAttributeRepresentation: "omitted" } as const;

// Each shared attribute: what the application knows, then what the schema says about its column,
// which only DECLARED_MAPPER spells out.
const SHARED_ATTRIBUTES: ReadonlyArray<readonly [string, MapperConfig, MapperConfig]> = [
  ["request.resource.attr.aBool", OMITTED, { field: "aBool", valueType: "boolean", nullable: true }],
  ["request.resource.attr.aString", OMITTED, { field: "aString", valueType: "string", nullable: true }],
  ["request.resource.attr.aNumber", OMITTED, { field: "aNumber", valueType: "number", nullable: true }],
  ["request.resource.attr.aDouble", OMITTED, { field: "aDouble", valueType: "number" }],
  [
    "request.resource.attr.aOptionalString",
    OMITTED,
    { field: "aOptionalString", valueType: "string", nullable: true },
  ],
  ["request.resource.attr.scope", OMITTED, { field: "scope", valueType: "string", nullable: true }],
  ["request.resource.attr.createdAt", OMITTED, { field: "createdAt", valueType: "dateTime" }],
  ["request.resource.attr.updatedAt", OMITTED, { field: "updatedAt", valueType: "dateTime" }],
  [
    "request.resource.attr.owner",
    { field: "aOptionalString", nullAttributeRepresentation: "explicit" },
    { valueType: "string", nullable: true },
  ],
  [
    "request.resource.attr.coOwner",
    { field: "scope", nullAttributeRepresentation: "explicit" },
    { valueType: "string", nullable: true },
  ],
  ["request.resource.attr.obj.inner", { field: "aString", ...OMITTED }, { valueType: "string", nullable: true }],
];

// Everything the Prisma schema already says is left to the `datamodel` option: an attribute named
// like its column (`request.resource.id` is the `@id` field, so `identifier/*` needs nothing),
// whether a relation is to-one or to-many, the model it reaches, segments named like their field,
// and each column's type and nullability. What is left is what only the application knows: the
// NULL convention, aliases, projections, a list's element type and a to-one relation's keys.
export const INFERRED_MAPPER: ConformanceMapper = {
  ...Object.fromEntries(SHARED_ATTRIBUTES.map(([reference, application]) => [reference, application])),
  // The corpus's one REAL to-one chain (the `relation/*` cases), `parent` and `parent.inner`, both
  // read from the schema. A column reached through a relation takes the relation entry's null
  // convention, so the one declaration here covers every `parent.*` and `parent.inner.*` field.
  // The keys listed under `fields` are the attributes the application sends for the related row,
  // which `"aString" in R.attr.parent` tests; the schema cannot say which columns those are.
  "request.resource.attr.parent": {
    nullAttributeRepresentation: "omitted",
    relation: {
      name: "parent",
      fields: { aBool: {}, aString: {}, aNumber: {}, aOptionalString: {}, inner: {} },
    },
  },
  // Two more to-one relations for joins that reach a table twice: `peer` is another resource row
  // (a relation from the resource model to itself), `stepParent` a second row in the parent table.
  "request.resource.attr.peer": {
    nullAttributeRepresentation: "omitted",
    relation: { name: "peer", fields: { aBool: {}, aString: {}, aNumber: {}, aOptionalString: {} } },
  },
  "request.resource.attr.stepParent": {
    nullAttributeRepresentation: "omitted",
    relation: { name: "stepParent", fields: { aBool: {}, aString: {}, aNumber: {}, aOptionalString: {} } },
  },
  "request.resource.attr.tags": {
    relation: { name: "tags", fields: { id: { field: "tagId" } } },
  },
  "request.resource.attr.tagNames": { relation: { name: "tags", field: "name" } },
  // The two homogeneous scalar lists, one owned row per element, projected the way `tagNames`
  // is. `valueType` sits on the list mapping itself because that is where a membership test reads
  // it: a literal of another type (`"2" in aNumberList`) is refused rather than bound, since CEL's
  // heterogeneous equality answers it false and a store that coerces would not. It is not read
  // from the projected column, because `size()` reads the same key as the type of the attribute.
  "request.resource.attr.aNumberList": {
    valueType: "number",
    relation: { name: "numberList", field: "value" },
  },
  "request.resource.attr.aBoolList": {
    valueType: "boolean",
    relation: { name: "boolList", field: "value" },
  },
  // The multi-hop chain (the `relation/*/*-to-one-chain` cases): mainCategory mirrors the SAME
  // categories/subCategories relation as a single-object dotted chain on the check side (every
  // seed holds at most one category), pinning that the adapter joins through every intermediate
  // hop, never off the root. subNames is the same 2-hop chain with a bare `field`, so plain `in`
  // membership compares the flattened tail's name column directly.
  "request.resource.attr.mainCategory": {
    relation: {
      name: "categories",
      fields: { subNames: { relation: { name: "subCategories", field: "name" } } },
    },
  },
};

// Every mapping spelled out, the way a caller without the `datamodel` option writes it. The harness
// replays the corpus under both mappers, so the two must agree on every case.
export const DECLARED_MAPPER: ConformanceMapper = {
  // The primary key, reached as `request.resource.id` rather than through `attr` (the
  // `identifier/*` cases). It is a mapping like any other here, which is the point: an adapter that resolves
  // references by stripping a `request.resource.attr.` prefix never sees this name.
  "request.resource.id": { field: "id", valueType: "string" },
  ...Object.fromEntries(
    SHARED_ATTRIBUTES.map(([reference, application, column]) => [reference, { ...column, ...application }])
  ),
  "request.resource.attr.createdBy": { field: "createdBy", valueType: "string" },
  // The corpus's one REAL to-one chain (the `relation/*` cases). `type: "one"` is what makes the
  // adapter emit `is:` rather than `some:`, and `is:` on an optional relation is what requires
  // the hop to exist — the absent-parent guard the negated shapes discriminate. `inner` nests
  // the same declaration one level further out; two levels is where alias scoping breaks.
  // A column reached through a relation takes the relation entry's null convention, so the one
  // declaration here covers every `parent.*` and `parent.inner.*` field.
  "request.resource.attr.parent": {
    nullAttributeRepresentation: "omitted",
    relation: {
      name: "parent",
      type: "one",
      model: "AdversarialParent",
      fields: {
        aBool: { field: "aBool", valueType: "boolean", nullable: true },
        aString: { field: "aString", valueType: "string", nullable: true },
        aNumber: { field: "aNumber", valueType: "number", nullable: true },
        aOptionalString: { field: "aOptionalString", valueType: "string", nullable: true  },
        inner: {
          relation: {
            name: "inner",
            type: "one",
            model: "AdversarialInner",
            fields: {
              aBool: { field: "aBool", valueType: "boolean", nullable: true },
              aString: { field: "aString", valueType: "string", nullable: true },
              aNumber: { field: "aNumber", valueType: "number", nullable: true },
              aOptionalString: { field: "aOptionalString", valueType: "string", nullable: true  },
            },
          },
        },
      },
    },
  },
  "request.resource.attr.peer": {
    nullAttributeRepresentation: "omitted",
    relation: {
      name: "peer",
      type: "one",
      model: "AdversarialResource",
      fields: {
        aBool: { field: "aBool", valueType: "boolean", nullable: true },
        aString: { field: "aString", valueType: "string", nullable: true },
        aNumber: { field: "aNumber", valueType: "number", nullable: true },
        aOptionalString: { field: "aOptionalString", valueType: "string", nullable: true },
      },
    },
  },
  "request.resource.attr.stepParent": {
    nullAttributeRepresentation: "omitted",
    relation: {
      name: "stepParent",
      type: "one",
      model: "AdversarialParent",
      fields: {
        aBool: { field: "aBool", valueType: "boolean", nullable: true },
        aString: { field: "aString", valueType: "string", nullable: true },
        aNumber: { field: "aNumber", valueType: "number", nullable: true },
        aOptionalString: { field: "aOptionalString", valueType: "string", nullable: true },
      },
    },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      // Model name enables field-to-field comparisons between tag columns. `name` is NULLable
      // in the schema, so it keeps the adapter's three-valued-logic guards for collection
      // macros over elements whose name column is NULL (a missing attribute — a CEL error,
      // hence deny — on the check side). Those guards are the default; every REQUIRED element
      // column below says `nullable: false`, because Prisma rejects `{ name: null }` on one.
      model: "AdversarialTag",
      fields: {
        id: { field: "tagId", nullable: false },
        name: { field: "name", valueType: "string", nullable: true  },
      },
    },
  },
  "request.resource.attr.tagNames": {
    relation: {
      name: "tags",
      type: "many",
      field: "name",
      fields: { name: { field: "name", valueType: "string", nullable: true  } },
    },
  },
  // The two homogeneous scalar lists, one owned row per element, projected the way `tagNames`
  // is. `valueType` sits on the list mapping itself because that is where a membership test reads
  // it: a literal of another type (`"2" in aNumberList`) is refused rather than bound, since CEL's
  // heterogeneous equality answers it false and a store that coerces would not. The element column
  // is nullable because a6 and a4 hold a null element, which is a VALUE in CEL.
  "request.resource.attr.aNumberList": {
    valueType: "number",
    relation: {
      name: "numberList",
      type: "many",
      field: "value",
      fields: { value: { field: "value", valueType: "number", nullable: true } },
    },
  },
  "request.resource.attr.aBoolList": {
    valueType: "boolean",
    relation: {
      name: "boolList",
      type: "many",
      field: "value",
      fields: { value: { field: "value", valueType: "boolean", nullable: true } },
    },
  },
  "request.resource.attr.categories": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name", valueType: "string", nullable: false },
        subCategories: {
          relation: {
            name: "subCategories",
            type: "many",
            fields: {
              name: { field: "name", valueType: "string", nullable: false },
              labels: {
                relation: {
                  name: "labels",
                  type: "many",
                  fields: {
                    name: { field: "name", valueType: "string", nullable: true  },
                  },
                },
              },
            },
          },
        },
      },
    },
  },
  // The multi-hop chain (the `relation/*/*-to-one-chain` cases): mainCategory mirrors the SAME
  // categories/subCategories relation as a single-object dotted chain on the check side (every
  // seed holds at most one category), pinning that the adapter joins through every intermediate
  // hop, never off the root.
  "request.resource.attr.mainCategory": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name", valueType: "string", nullable: false },
        subCategories: {
          relation: {
            name: "subCategories",
            type: "many",
            fields: { name: { field: "name", valueType: "string", nullable: false } },
          },
        },
        // subNames: the same 2-hop chain but with a bare `field`, so plain `in` membership
        // compares the flattened tail's name column directly.
        subNames: {
          relation: {
            name: "subCategories",
            type: "many",
            field: "name",
          },
        },
      },
    },
  },
};
