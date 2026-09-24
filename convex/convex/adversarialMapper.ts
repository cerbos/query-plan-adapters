import type { MapperConfig } from "../src/index";

/**
 * The one mapper every conformance case is translated through.
 *
 * It lives here rather than in `adversarial.ts` because two readers need it and they do not have
 * the same dependencies:
 *
 * - `adversarial.ts`, the Convex backend that executes the translated query;
 * - `src/translator.test.ts`, the offline unit suite, which must run with nothing installed but
 *   node.
 *
 * `adversarial.ts` imports `./_generated/server`, which `npx convex codegen` produces against a
 * live backend and `.gitignore` excludes — so importing the mapper from there would make the
 * offline suite need a Convex deployment. This file imports types only, from `../src/index`.
 *
 * `nullable: true` means "this path may be absent from the document", which is CEL's
 * missing-attribute case and the corpus's omitted-null convention: `conformance/resources.json`
 * omits those attributes for a NULL column, and so does the stored document. `canPushToDb` keeps
 * such a field off Convex's filter engine, and the adapter's in-memory post-filter answers it with
 * CEL's three-valued semantics. The explicit-null attributes (`owner`, `coOwner`, `tagNames` and
 * the two scalar lists) are always present and are not `nullable`.
 */

// Typed as the record arm of `Mapper`, so the unit suite can index it to build a function mapper.
export const MAPPER: Record<string, MapperConfig> = {
  // The primary key, reached as `request.resource.id` rather than through `attr` (the
  // `identifier/*` cases). An adapter that resolves references by stripping a `request.resource.attr.` prefix
  // never sees this name. It maps to the corpus id field rather than Convex's own `_id`, which
  // holds a generated document handle unrelated to the corpus.
  "request.resource.id": { field: "id" },
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aDouble": { field: "aDouble", nullable: true },
  "request.resource.attr.aOptionalString": {
    field: "aOptionalString",
    nullable: true,
  },
  "request.resource.attr.createdBy": { field: "createdBy" },
  "request.resource.attr.createdAt": { field: "createdAt", nullable: true },
  "request.resource.attr.updatedAt": { field: "updatedAt", nullable: true },
  "request.resource.attr.scope": { field: "scope", nullable: true },
  // `owner` and `coOwner` alias `aOptionalString` and `scope` under the explicit-null convention:
  // the key is always present and a NULL column is stored as a null VALUE, so neither is
  // `nullable` and Convex's engine answers their comparisons with its own `q.eq(field, null)`.
  "request.resource.attr.owner": { field: "owner" },
  "request.resource.attr.coOwner": { field: "coOwner" },
  "request.resource.attr.tagNames": { field: "tagNames" },
  // Every seed carries both lists, most of them empty, so neither path is ever absent.
  "request.resource.attr.aNumberList": { field: "aNumberList" },
  "request.resource.attr.aBoolList": { field: "aBoolList" },
  "request.resource.attr.obj.inner": { field: "obj.inner" },
  "request.resource.attr.tags": { field: "tags" },
  "request.resource.attr.categories": { field: "categories" },
  "request.resource.attr.mainCategory": {
    field: "mainCategory",
    nullable: true,
  },
  "request.resource.attr.mainCategory.subCategories": {
    field: "mainCategory.subCategories",
    nullable: true,
  },
  "request.resource.attr.mainCategory.subNames": {
    field: "mainCategory.subNames",
    nullable: true,
  },
  // The corpus's one REAL to-one chain (the `relation/*` cases), stored as nested objects rather
  // than a joined table. EVERY level is `nullable: true`, which here means "this path may be
  // absent from the document": a row with no parent carries no `parent` key at all, and one whose
  // parent has no parent of its own carries no `parent.inner`. That is precisely the CEL
  // missing-attribute case, so `canPushToDb` keeps these off the Convex filter engine and the
  // adapter's in-memory post-filter answers them with the right three-valued semantics
  // (cerbos/query-plan-adapters#375). Each level is declared explicitly, as mainCategory is.
  "request.resource.attr.parent": { field: "parent", nullable: true },
  "request.resource.attr.parent.aBool": {
    field: "parent.aBool",
    nullable: true,
  },
  "request.resource.attr.parent.aString": {
    field: "parent.aString",
    nullable: true,
  },
  "request.resource.attr.parent.aNumber": {
    field: "parent.aNumber",
    nullable: true,
  },
  "request.resource.attr.parent.aOptionalString": {
    field: "parent.aOptionalString",
    nullable: true,
  },
  "request.resource.attr.parent.inner": {
    field: "parent.inner",
    nullable: true,
  },
  "request.resource.attr.parent.inner.aBool": {
    field: "parent.inner.aBool",
    nullable: true,
  },
  "request.resource.attr.parent.inner.aString": {
    field: "parent.inner.aString",
    nullable: true,
  },
  "request.resource.attr.parent.inner.aNumber": {
    field: "parent.inner.aNumber",
    nullable: true,
  },
  "request.resource.attr.parent.inner.aOptionalString": {
    field: "parent.inner.aOptionalString",
    nullable: true,
  },
};
