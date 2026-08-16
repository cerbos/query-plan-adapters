/**
 * The trusted-backend half of the example: the Convex functions the CLI in ../src/main.ts calls.
 *
 * Convex is the one adapter whose filter cannot be applied by the caller. `queryPlanToConvex`
 * returns a FUNCTION of Convex's own `FilterBuilder`, and the only place that builder exists is
 * inside a Convex query — so the plan crosses the wire and the adapter runs in here, beside
 * `ctx.db`. That is also what makes this file the packaging proof: `npx convex deploy` bundles
 * `@cerbos/orm-convex` out of node_modules, where run.sh installed the artifact `npm publish`
 * would upload, resolving it through the published `exports` map and `files` allowlist.
 *
 * The plan arrives as `v.any()` and is cast, exactly as the adapter's README's "Trusted usage
 * pattern" shows: this argument must be the response Cerbos returned. Note what the crossing
 * costs — @cerbos/core builds a plan out of `PlanExpression` class instances, and Convex's
 * argument encoding keeps the fields and drops the prototypes. The adapter classifies operands by
 * shape rather than with `instanceof`, which is why that survives (cerbos/query-plan-adapters#419).
 *
 * These are PUBLIC queries because the example's client is an outside process. A real application
 * makes them `internalQuery` and calls them from trusted backend code — an untrusted caller who
 * can hand you a plan can hand you an ALWAYS_ALLOWED one. See the adapter README's "Trusted usage
 * pattern".
 */
import type { PlanResourcesResponse } from "@cerbos/core";
import { PlanKind, queryPlanToConvex, type Mapper } from "@cerbos/orm-convex";
import {
  paginationOptsValidator,
  type Expression,
  type FilterBuilder,
  type Query,
} from "convex/server";
import { v, type Infer } from "convex/values";

import type { DataModel } from "./_generated/dataModel";
import { mutation, query } from "./_generated/server";
import { documentFields } from "./schema";

type Documents = DataModel["documents"];
/** What `queryPlanToConvex` hands back, and what `.filter()` takes: a function of the builder. */
type Predicate = (q: FilterBuilder<Documents>) => Expression<boolean>;

/**
 * The one piece of configuration a Convex consumer must not skip. A Cerbos plan names its
 * operands `request.resource.attr.ownerId`; a Convex document field is `ownerId`, and Convex reads
 * a dotted field name as a path INTO the document. Without this mapper the filter would ask each
 * document for `request.resource.attr.ownerId`, find nothing there, and return no rows at all —
 * quietly, because an absent path is not an error to the filter engine.
 *
 * `region` and `archived` are deliberately absent: they are the application's own fields, never
 * referenced by demo/policies/document.yaml, and composing them with the adapter's filter is
 * shape 5.
 */
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "public" },
};

/** The application's OWN predicate, passed in by the application. Declared in demo/seeds.json. */
const applicationFilterValidator = v.object({
  archived: v.boolean(),
  region: v.string(),
});
type ApplicationFilter = Infer<typeof applicationFilterValidator>;

/**
 * `allowPostFilter` is left at its default of `false` on purpose. Every shape in
 * demo/policies/document.yaml is a flat comparison Convex's filter engine evaluates itself, so a
 * postFilter appearing here would mean the demo domain had grown a shape the engine cannot
 * express — and the adapter throwing is how that should arrive, rather than as documents read
 * before the whole authorization predicate has run.
 */
const translate = (queryPlan: unknown) =>
  queryPlanToConvex<FilterBuilder<Documents>, Expression<boolean>>({
    queryPlan: queryPlan as PlanResourcesResponse,
    mapper: MAPPER,
  });

/**
 * Usage shape 5, and the reason this example is worth running: the adapter's filter and the
 * application's own predicate, ANDed into the single `.filter()` call Convex allows.
 *
 * Both halves are optional and the two absences mean different things. No adapter filter is
 * KIND_ALWAYS_ALLOWED — nothing to AND with, so the application's predicate stands alone. No
 * application predicate is shape 1, the plain filtered list. Neither is KIND_ALWAYS_DENIED, which
 * never reaches here: `list` returns before building a predicate, because a denial ANDed with
 * anything is still a denial and it must not be reachable through this function at all.
 */
const compose = (
  filter: Predicate | undefined,
  application: ApplicationFilter | undefined,
): Predicate | undefined => {
  if (!filter && !application) return undefined;
  return (q) => {
    const clauses: Expression<boolean>[] = [];
    if (filter) clauses.push(filter(q));
    if (application) {
      clauses.push(q.eq(q.field("archived"), application.archived));
      clauses.push(q.eq(q.field("region"), application.region));
    }
    const [only] = clauses;
    return clauses.length === 1 && only ? only : q.and(...clauses);
  };
};

/**
 * `.filter()` if there is a predicate, and the unfiltered query if there is not.
 * `KIND_ALWAYS_ALLOWED` is the second case and it must return every document, not none.
 */
const withPredicate = (
  documents: Query<Documents>,
  predicate: Predicate | undefined,
): Query<Documents> => (predicate ? documents.filter(predicate) : documents);

/** Replaces the table contents with demo/seeds.json. The client owns the rows; this stores them. */
export const seed = mutation({
  args: { documents: v.array(v.object(documentFields)) },
  handler: async (ctx, args) => {
    for (const stored of await ctx.db.query("documents").collect()) {
      await ctx.db.delete(stored._id);
    }
    for (const document of args.documents) {
      await ctx.db.insert("documents", document);
    }
  },
});

/**
 * Usage shapes 1, 2, 3 and 5: the whole answer in one call. `applicationFilter` is what separates
 * shape 5 from shape 1 — the same plan, composed with a predicate the policy never mentions.
 *
 * The plan `kind` is reported from the TRANSLATED result rather than from the plan the client
 * already holds, so the ids and the kind come from the same call and an example that never
 * reached the adapter cannot report one of them.
 */
export const list = query({
  args: {
    queryPlan: v.any(),
    applicationFilter: v.optional(applicationFilterValidator),
  },
  handler: async (ctx, args) => {
    const { kind, filter } = translate(args.queryPlan);
    // Short-circuited before any predicate is built: the application's predicate must not be able
    // to turn a denial into a query that returns rows.
    if (kind === PlanKind.ALWAYS_DENIED) return { kind, ids: [] as string[] };

    const documents = await withPredicate(
      ctx.db.query("documents"),
      compose(filter, args.applicationFilter),
    ).collect();
    return { kind, ids: documents.map((document) => document.id).sort() };
  },
});

/**
 * Usage shape 4: one page of the filtered answer. Convex has no filtered count, which is why
 * count is not one of the five shapes and why this is `.paginate()`.
 *
 * `.filter()` is applied by the query engine as it walks the table, so a page holds `numItems`
 * documents the adapter ALLOWED rather than `numItems` documents of which some are then dropped.
 * The client walks the cursor and asserts page sizes plus the sorted union.
 */
export const page = query({
  args: { queryPlan: v.any(), paginationOpts: paginationOptsValidator },
  handler: async (ctx, args) => {
    const { kind, filter } = translate(args.queryPlan);
    if (kind === PlanKind.ALWAYS_DENIED) {
      return { kind, ids: [] as string[], isDone: true, cursor: null };
    }

    // The adapter's filter alone: shape 4 pages over the adapter's answer, not over a composed
    // one. `compose` is not called here, because with nothing to compose it would only re-wrap
    // this same predicate.
    const result = await withPredicate(
      ctx.db.query("documents"),
      filter,
    ).paginate(args.paginationOpts);
    return {
      kind,
      ids: result.page.map((document) => document.id).sort(),
      isDone: result.isDone,
      cursor: result.continueCursor,
    };
  },
});
