import type { Mapper, MapperConfig } from "../src/index";

/**
 * The mapper the adversarial corpus is proved against, and the pushdown variant of it.
 *
 * It lives here rather than in `adversarial.ts` because THREE readers need it and they do not all
 * have the same dependencies:
 *
 * - `adversarial.ts`, the Convex backend that executes the translated query;
 * - `src/adversarial.test.ts`, the differential harness that drives that backend;
 * - `src/translator.test.ts`, the offline translator unit test, which must run with nothing
 *   installed but node.
 *
 * `adversarial.ts` imports `./_generated/server`, which `npx convex codegen` produces against a
 * live backend and `.gitignore` excludes — so importing the mapper from there would make the
 * offline suite need a Convex deployment to assert a filter no Convex ever sees. This file imports
 * types only, from `../src/index`, exactly as `planExecution.ts` does.
 *
 * One copy, not three: the unit test pins the filter this adapter emits for a mapping, and the
 * harness proves that same filter returns the documents the PDP allows. Two copies that drifted
 * would leave the pinned filter describing a mapping nothing executes.
 */

// Typed as the record arm of `Mapper` rather than as `Mapper` itself, so `PUSHDOWN_MAPPER` below
// can derive from it without asserting away the function arm.
export const MAPPER: Record<string, MapperConfig> = {
  // The primary key, reached as `request.resource.id` rather than through `attr` (the `id-*`
  // actions). An adapter that resolves references by stripping a `request.resource.attr.` prefix
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
  "request.resource.attr.scope": { field: "scope", nullable: true },
  "request.resource.attr.owner": { field: "owner", nullable: true },
  // `coOwner` is the explicit-null alias of the `scope` field, the second half of
  // `null-value-f2f`: `scope` itself is omitted when NULL, so the corpus carries the same
  // value under both conventions (cerbos/query-plan-adapters#308).
  "request.resource.attr.coOwner": { field: "coOwner", nullable: true },
  "request.resource.attr.tagNames": { field: "tagNames" },
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
  // The corpus's one REAL to-one chain (the `rel-*` actions), stored as nested objects rather
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

/**
 * Fields `MAPPER` declares `nullable` that the seeded documents nonetheless ALWAYS carry.
 *
 * `nullable: true` means "this path may be absent from the document", and `canPushToDb` refuses to
 * push any comparison touching such a field: an absent path has CEL missing-attribute semantics
 * that a Convex comparison cannot reproduce. `owner` is the one nullable field whose value can be
 * NULL while the key is still present — the table declares it `v.union(v.string(), v.null())`, not
 * `v.optional(...)` — so the flag is buying nothing there and costs the whole null-comparison
 * family its push-down.
 *
 * Demoting it is a statement about the DOCUMENT SHAPE, not about the plan, so the harness asserts
 * the key is present on every seeded document before trusting this list
 * (cerbos/query-plan-adapters#327).
 */
export const PUSHDOWN_DEMOTED_FIELDS = ["owner"] as const;

/**
 * The same mapper with `nullable` cleared on {@link PUSHDOWN_DEMOTED_FIELDS}, so the comparison
 * shapes over those fields are decided by Convex's filter engine instead of the adapter's
 * in-memory post-filter. That moves the null-comparison family across the boundary, which is
 * where Convex's own `q.eq(field, null)` semantics live.
 *
 * How much of the corpus each mapper actually hands to the engine is pinned by `translator.test.ts`
 * and quoted in the adapter README; it is deliberately not restated here.
 */
export const PUSHDOWN_MAPPER: Record<string, MapperConfig> = Object.fromEntries(
  Object.entries(MAPPER).map(([path, config]) =>
    PUSHDOWN_DEMOTED_FIELDS.some((field) => field === config.field)
      ? [path, { ...config, nullable: false }]
      : [path, config],
  ),
);

/** Which mapper `executePlan` should translate with. */
export type MapperVariant = "default" | "pushdown";

export const MAPPERS: Record<MapperVariant, Mapper> = {
  default: MAPPER,
  pushdown: PUSHDOWN_MAPPER,
};
