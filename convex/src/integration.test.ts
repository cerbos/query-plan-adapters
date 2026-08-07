import { beforeAll, afterAll, test, expect, describe } from "@jest/globals";
import type { Resource as CerbosResource, Value } from "@cerbos/core";
import { GRPC as Cerbos } from "@cerbos/grpc";
import { ConvexHttpClient } from "convex/browser";

import { api } from "../convex/_generated/api.js";

// What this suite is for, and what it deliberately leaves alone.
//
// `index.test.ts` proves the SHAPE of the filter the adapter builds, but it runs that filter
// against a hand-rolled mock of Convex's `FilterBuilder`. Nothing in that suite can catch the mock
// lying about what Convex's real filter engine does with the expression tree it is handed. That is
// the gap this suite closes, so every case here is one the adapter PUSHES DOWN — plus the one that
// splits, which carries a `postFilter` alongside the pushed-down half.
//
// It used to assert `result.kind === CONDITIONAL` and then run a hand-written query built from a
// `filterType`/`filterField`/`filterValue` switch, so the translated filter was discarded and the
// suite proved Convex's filter API worked (cerbos/query-plan-adapters#327).
//
// Post-filter-only shapes are NOT rehearsed here. `src/adversarial.test.ts` already executes 126
// of them inside a real Convex backend against per-document `check()` decisions; adding a second,
// weaker copy over the non-hostile `/policies` suite would buy runtime, not coverage.
//
// Expectations are per-document `checkResource` decisions, not hand-written key lists — the same
// oracle recipe the shared corpus uses (conformance/README.md).

const CONVEX_URL = process.env["CONVEX_URL"] || "http://127.0.0.1:3210";
const convex = new ConvexHttpClient(CONVEX_URL);
const cerbos = new Cerbos("127.0.0.1:3593", { tls: false });

const RESOURCE_KIND = "resource";
const PRINCIPAL = { id: "user1", roles: ["USER"] };

interface Fixture {
  key: string;
  aBool: boolean;
  aNumber: number;
  aString: string;
  aOptionalString?: string;
  nested: {
    aBool: boolean;
    aNumber: number;
    aString: string;
  };
}

/**
 * Chosen so every action below allows some fixtures and denies others — a suite whose oracle is
 * empty or total passes without discriminating anything, which is the failure mode the corpus
 * calls a degenerate oracle. The `oracle is not degenerate` test enforces it.
 */
const fixtures: Fixture[] = [
  {
    key: "a",
    aBool: true,
    aNumber: 1,
    aString: "string",
    aOptionalString: "string",
    nested: { aBool: true, aNumber: 1, aString: "string" },
  },
  {
    key: "b",
    aBool: false,
    aNumber: 2,
    aString: "string2",
    nested: { aBool: true, aNumber: 1, aString: "string" },
  },
  {
    key: "c",
    aBool: false,
    aNumber: 3,
    aString: "string3",
    nested: { aBool: false, aNumber: 2, aString: "testing" },
  },
  {
    key: "d",
    aBool: true,
    aNumber: 4,
    aString: "other",
    aOptionalString: "set",
    nested: { aBool: true, aNumber: 3, aString: "test123" },
  },
  {
    key: "e",
    aBool: false,
    aNumber: 0,
    aString: "string",
    nested: { aBool: false, aNumber: 0, aString: "none" },
  },
];

/**
 * The attributes `check()` sees must be the ones the stored document carries, or the two sides of
 * the differential are answering questions about different data. A fixture with no
 * `aOptionalString` sends no attribute, matching the document that omits the field.
 */
function checkResource(fixture: Fixture): CerbosResource {
  const attr: Record<string, Value> = {
    aBool: fixture.aBool,
    aNumber: fixture.aNumber,
    aString: fixture.aString,
    nested: {
      aBool: fixture.nested.aBool,
      aNumber: fixture.nested.aNumber,
      aString: fixture.nested.aString,
    },
  };
  if (fixture.aOptionalString !== undefined) {
    attr["aOptionalString"] = fixture.aOptionalString;
  }
  return { kind: RESOURCE_KIND, id: fixture.key, attr };
}

async function oracleAllowedKeys(action: string): Promise<string[]> {
  const keys: string[] = [];
  for (const fixture of fixtures) {
    const result = await cerbos.checkResource({
      principal: PRINCIPAL,
      resource: checkResource(fixture),
      actions: [action],
    });
    if (result.isAllowed(action)) keys.push(fixture.key);
  }
  return keys.sort();
}

/**
 * Plans, translates, and executes the ADAPTER'S filter against the real Convex backend, reporting
 * the keys it selected and which half of the adapter's output selected them. The path is the
 * backend's own answer — re-deriving it here would re-run the translation rather than observe the
 * one that ran, and both halves return the same keys.
 */
async function adapterRun(
  action: string,
): Promise<{ keys: string[]; execution: string }> {
  const queryPlan = await cerbos.planResources({
    principal: PRINCIPAL,
    resource: { kind: RESOURCE_KIND },
    action,
  });
  return convex.query(api.resources.executePlan, {
    queryPlan: JSON.parse(JSON.stringify(queryPlan)),
  });
}

/**
 * The one action that splits: `and(eq(aBool, true), contains(nested.aString, "test"))`. The
 * pushed-down half runs in Convex's filter engine and the `contains` half in the `postFilter`,
 * both inside the same Convex query function.
 */
const SPLIT_ACTION = "combined-and";

/**
 * Actions the adapter pushes into Convex's filter engine, chosen so each `q.*` builder method the
 * adapter can emit is executed at least once: `eq`, `neq`, `lt`, `lte`, `gt`, `gte`, `and`, `or`,
 * `not`, `field`, and the `or`-of-`eq` composition `in` translates to. Every oracle is non-empty
 * and non-total over the fixtures above, asserted below.
 *
 * A post-filter-only shape does not belong in this list — its per-action test would fail, since
 * every entry here is asserted to be answered by the engine. That is deliberate: those shapes are
 * the adversarial harness's job, and a second copy of them over the non-hostile `/policies` suite
 * would buy runtime, not coverage.
 */
const DISCRIMINATING_ACTIONS = [
  // Pushed to Convex's filter engine in full.
  "and",
  "bare-bool",
  "bare-bool-negated",
  "bare-bool-nested",
  "bare-bool-nested-negated",
  "equal",
  "equal-bool-false",
  "equal-nested",
  "explicit-deny",
  "gt",
  "gte",
  "in",
  "in-number",
  "lt",
  "lte",
  "nand",
  "ne",
  "nor",
  "or",
  "relation-eq-number",
  "relation-gt-number",
  SPLIT_ACTION,
];

/** Total and empty BY CONSTRUCTION, so they are excluded from the degeneracy guard. */
const UNCONDITIONAL_ACTIONS = ["always-allow", "always-deny"];

beforeAll(async () => {
  await convex.mutation(api.resources.deleteAll, {});
  for (const fixture of fixtures) {
    await convex.mutation(api.resources.insert, fixture);
  }
});

afterAll(async () => {
  await convex.mutation(api.resources.deleteAll, {});
});

describe("Integration: Convex + Cerbos", () => {
  test.each(DISCRIMINATING_ACTIONS)(
    "%s: Convex's filter engine selects the documents check() allows",
    async (action) => {
      const [oracle, run] = await Promise.all([
        oracleAllowedKeys(action),
        adapterRun(action),
      ]);
      // The path is asserted with the keys: a case that stopped pushing down would still return
      // the right keys, via the post-filter, and prove nothing about Convex.
      expect({ keys: run.keys, pushedDown: run.execution }).toEqual({
        keys: oracle,
        pushedDown: action === SPLIT_ACTION ? "split" : "db",
      });
    },
  );

  test.each(UNCONDITIONAL_ACTIONS)(
    "%s: the unconditional plan selects the documents check() allows",
    async (action) => {
      const [oracle, run] = await Promise.all([
        oracleAllowedKeys(action),
        adapterRun(action),
      ]);
      expect({ keys: run.keys, execution: run.execution }).toEqual({
        keys: oracle,
        execution: "unconditional",
      });
    },
  );

  test("oracle is not degenerate", async () => {
    const degenerate: string[] = [];
    for (const action of DISCRIMINATING_ACTIONS) {
      const allowed = await oracleAllowedKeys(action);
      if (allowed.length === 0 || allowed.length === fixtures.length) {
        degenerate.push(`${action} (${allowed.length}/${fixtures.length})`);
      }
    }
    expect(degenerate).toEqual([]);

    // The unconditional pair is the complement: it must stay total and empty, otherwise it has
    // drifted into the guarded set without anyone noticing.
    expect({
      allow: await oracleAllowedKeys("always-allow"),
      deny: await oracleAllowedKeys("always-deny"),
    }).toEqual({ allow: fixtures.map((f) => f.key).sort(), deny: [] });
  });

});
