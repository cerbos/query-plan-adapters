import type { FieldNameMapperConfig } from "../../src";

/**
 * The mapping the conformance corpus's resource is translated through: shared by the unit suite
 * (`../translator.test.ts`) and the conformance harness (`conformance/harnesses/langchain-chromadb/`).
 *
 * Test-only: outside `src/`, so it is never built or published.
 */

// -- the metadata mapping, as both suites see it --------------------------------------------------

/**
 * The corpus mapping from Cerbos attribute paths onto Chroma metadata keys.
 *
 * Fields are optional unless declared otherwise, so `$ne`/`$nin` are rejected by default.
 * `required: true` is asserted only for the metadata keys the harness's `metadataFor` writes for
 * every seed in `conformance/seeds.json`: the id alone. Every scalar attribute is NULL for some
 * seed — a missing attribute in `resources.json`, and no metadata key here — so each stays
 * optional. An inequality over one is refused unless the key's declared type (`valueType:
 * "boolean"`, `numericType: "integer"`) spells it without `$ne`, as it does for the booleans and
 * the integers. The attributes no scalar key can hold (`owner`, `coOwner`, the lists, `categories`,
 * `mainCategory`, and `parent` and `obj` read whole) are stored as JSON text and declared
 * `valueType: "json"`, so only the post-filter reads them. The mapping names every attribute the
 * corpus reads: the post-filter refuses any reference the mapping does not declare.
 */
export interface ConformanceMapper {
  [reference: string]: string | FieldNameMapperConfig;
}

export const FIELD_NAME_MAPPER: ConformanceMapper =
  {
    // The primary key, reached as `request.resource.id` rather than through `attr` (the
    // `identifier/*` cases). Chroma's `where` filters metadata only — the document id is addressed by the
    // separate `ids` argument to `get()` — so `metadataFor` mirrors the id into a metadata key and
    // this maps onto that.
    "request.resource.id": { field: "id", required: true },
    // NULL for one seed each (j3, j1, j2: no metadata key), so `required: false`. The type
    // declarations (`valueType`, `numericType`) let an inequality over aBool and aNumber be spelled
    // without `$ne`; aString has no such spelling.
    "request.resource.attr.aBool": {
      field: "aBool",
      valueType: "boolean",
      required: false,
    },
    "request.resource.attr.aString": { field: "aString", required: false },
    "request.resource.attr.aNumber": {
      field: "aNumber",
      numericType: "integer",
      required: false,
    },
    "request.resource.attr.aOptionalString": {
      field: "aOptionalString",
      required: false,
    },
    // NULL for some seeds (no metadata key), so `required: false`. i1, i2, i5 and g1 are whole-valued,
    // so the client stores them as integer metadata.
    "request.resource.attr.aDouble": { field: "aDouble", numericType: "float", required: false },
    // RFC 3339 strings from derived-fields.json, NULL for some seeds (no metadata key). No `Where`
    // clause reads them — Chroma's `$lt`/`$gt` reject a string — but the post-filter compares them
    // through `timestamp()` and as strings.
    "request.resource.attr.createdAt": { field: "createdAt" },
    "request.resource.attr.updatedAt": { field: "updatedAt" },
    // Strings from derived-fields.json: createdBy on every seed, scope NULL (no key) on some.
    "request.resource.attr.createdBy": { field: "createdBy" },
    "request.resource.attr.scope": { field: "scope" },
    // `obj.inner` mirrors aString in the corpus resource, so it is missing where aString is.
    "request.resource.attr.obj.inner": { field: "obj.inner", required: false },
    // The corpus's one REAL to-one chain (the `relation/*` cases), flattened onto dotted metadata keys
    // by `metadataFor`. EVERY level stays `required: false` — the whole point of the relation is
    // that a level can be absent — so a `$ne` over these keys stays fail-closed. A metadata key
    // Chroma cannot prove is present cannot answer `$ne` the way CEL's missing-attribute error does
    // (cerbos/query-plan-adapters#375). The booleans and integers declare their type, which spells
    // their inequalities without `$ne`.
    "request.resource.attr.parent.aBool": {
      field: "parent.aBool",
      valueType: "boolean",
    },
    "request.resource.attr.parent.aString": { field: "parent.aString" },
    "request.resource.attr.parent.aNumber": {
      field: "parent.aNumber",
      numericType: "integer",
    },
    "request.resource.attr.parent.aOptionalString": {
      field: "parent.aOptionalString",
    },
    "request.resource.attr.parent.inner.aBool": {
      field: "parent.inner.aBool",
      valueType: "boolean",
    },
    "request.resource.attr.parent.inner.aString": {
      field: "parent.inner.aString",
    },
    "request.resource.attr.parent.inner.aNumber": {
      field: "parent.inner.aNumber",
      numericType: "integer",
    },
    "request.resource.attr.parent.inner.aOptionalString": {
      field: "parent.inner.aOptionalString",
    },
    "request.resource.attr.peer.aBool": {
      field: "peer.aBool",
      valueType: "boolean",
    },
    "request.resource.attr.peer.aString": { field: "peer.aString" },
    "request.resource.attr.peer.aNumber": {
      field: "peer.aNumber",
      numericType: "integer",
    },
    "request.resource.attr.peer.aOptionalString": {
      field: "peer.aOptionalString",
    },
    "request.resource.attr.stepParent.aBool": {
      field: "stepParent.aBool",
      valueType: "boolean",
    },
    "request.resource.attr.stepParent.aString": { field: "stepParent.aString" },
    "request.resource.attr.stepParent.aNumber": {
      field: "stepParent.aNumber",
      numericType: "integer",
    },
    "request.resource.attr.stepParent.aOptionalString": {
      field: "stepParent.aOptionalString",
    },
    // The attributes Chroma metadata cannot hold as the PDP sees them — lists (empty, with null
    // elements, of objects), maps, and the explicit nulls `owner`, `coOwner` and the lists send —
    // stored as JSON text under one key each. No `Where` clause reads them; the post-filter parses
    // them, and a reference below one (`mainCategory.subCategories`) resolves through its prefix.
    // `obj` is also read whole, by the map macro cases, beside its flattened `obj.inner`.
    "request.resource.attr.owner": { field: "owner", valueType: "json" },
    "request.resource.attr.coOwner": { field: "coOwner", valueType: "json" },
    // The scalar lists also carry their non-null elements as a native list key, absent when there
    // are none. aNumberList is null on j4, so only tagNames is required.
    "request.resource.attr.tagNames": {
      field: "tagNames",
      valueType: "json",
      required: true,
      listElements: { field: "tagNames.elements", type: "string" },
    },
    "request.resource.attr.aNumberList": {
      field: "aNumberList",
      valueType: "json",
      listElements: { field: "aNumberList.elements", type: "integer" },
    },
    "request.resource.attr.aBoolList": { field: "aBoolList", valueType: "json" },
    "request.resource.attr.tags": { field: "tags", valueType: "json" },
    "request.resource.attr.categories": { field: "categories", valueType: "json" },
    "request.resource.attr.mainCategory": { field: "mainCategory", valueType: "json" },
    "request.resource.attr.parent": { field: "parent", valueType: "json" },
    "request.resource.attr.peer": { field: "peer", valueType: "json" },
    "request.resource.attr.stepParent": { field: "stepParent", valueType: "json" },
    "request.resource.attr.obj": { field: "obj", valueType: "json" },
  };

/**
 * The metadata keys the corpus mapping asserts is present on every document — the only keys `$ne`
 * and `$nin` are sound over, since Chroma's inequalities MATCH a document missing the key.
 *
 * A bare string carries no presence assertion, so it is optional here exactly as it is in the
 * adapter: the default lives in one place and both readings of it agree.
 */
export function requiredMetadataKeys(): string[] {
  return Object.values(FIELD_NAME_MAPPER)
    .filter(
      (entry): entry is FieldNameMapperConfig =>
        typeof entry !== "string" && entry.required === true,
    )
    .flatMap((entry) =>
      entry.listElements ? [entry.field, entry.listElements.field] : [entry.field],
    )
    .sort();
}
