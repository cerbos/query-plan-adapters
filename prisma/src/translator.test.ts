import { describe, expect, test } from "@jest/globals";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";

import { queryPlanToPrisma, PlanKind } from ".";
import type {
  Mapper,
  NullAttributeRepresentation,
  PrismaFilter,
  QueryPlanToPrismaResult,
} from ".";
import {
  MAPPER,
  MODEL,
  planFromWireFixture,
  wireFixtureActions,
} from "./corpus";
import { loadActionControlPlane, requireOutcomeMessage } from "./controlPlane";

/**
 * Translator unit test: for every action in the shared `../conformance/` corpus, the filter this
 * adapter emits. Offline — no Cerbos sidecar, no database, no generated Prisma client.
 *
 * A per-adapter suite used to braid four assertions into every test. Three of them are somebody
 * else's job now, and this file makes only the fourth:
 *
 * | assertion | who owns it |
 * | --- | --- |
 * | the plan the PDP produces for a policy | `conformance/wire-fixtures/`, replanned and diffed by the `Conformance Corpus` workflow |
 * | which shapes this adapter must refuse, and with what message | `prisma/adapterctl.json` — read below, not restated |
 * | the rows a filter returns | `adversarial.test.ts`, against a real store with `check()` as the oracle |
 * | **the filter this adapter emits for a plan** | **here** |
 *
 * **The plans are read, not written.** A hand-built plan is a *belief* about what the planner
 * emits, and this repository keeps golden fixtures because that belief has been wrong before: a
 * planner change used to fail fixture regeneration and silently leave every adapter's hand-written
 * plans describing a wire contract that no longer existed. Sourcing from fixtures inverts that —
 * the drift check now protects the plans this file asserts against. See
 * [ADR 0006](../../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md).
 *
 * **What a pinned filter buys over the harness.** The harness proves the filter returns the right
 * rows *against the canonical check resources*. Two different filters can agree on all of them and
 * disagree on the row a consumer has, so a rewrite that quietly changes the emitted SQL passes
 * there and shows up here as a diff a reviewer reads. It is also the only place a
 * `nullAttributeRepresentation` boundary, a timestamp literal, or a mapping the corpus cannot
 * express (`subqueryFilter`) can be pinned at all.
 *
 * **Adding a corpus action fails this file.** Every wire fixture must be classified here exactly
 * once — expected filter, expected plan kind, or expected throw — and the guard at the bottom is
 * what makes a new action land as a failure rather than as silence.
 */

const controlPlane = loadActionControlPlane({
  adapter: "prisma",
  selectedAction: undefined,
});

test("ADAPTERCTL_ACTION selects one direct outcome", () => {
  const previous = process.env["ADAPTERCTL_ACTION"];
  process.env["ADAPTERCTL_ACTION"] = "vf-le";
  try {
    const focused = loadActionControlPlane({
      adapter: "prisma",
      selectedAction: process.env["ADAPTERCTL_ACTION"],
    });
    expect(focused.selectedActions).toEqual(["vf-le"]);
    expect(focused.oracleActions).toEqual(["vf-le"]);
    expect(focused.throwingActions).toEqual([]);
    expect(focused.upstreamBlockedActions).toEqual([]);
    expect(focused.unassessedActions).toEqual([]);
  } finally {
    if (previous === undefined) {
      delete process.env["ADAPTERCTL_ACTION"];
    } else {
      process.env["ADAPTERCTL_ACTION"] = previous;
    }
  }
});

/**
 * The direct outcomes in `adapterctl.json` say which shapes this adapter must refuse and include
 * the message it must refuse
 * them with. Identical to the direct outcomes `adversarial.test.ts` asserts against a live PDP;
 * asserting it here as well is what lets the completeness guard below be total, and it costs a
 * millisecond rather than a container.
 */
const THROWING_ACTIONS = controlPlane.throwingActions.filter(
  ({ action }) => action !== "null-eq-missing",
);

function translate(
  action: string,
  options: {
    mapper?: Mapper;
    nullAttributeRepresentation?: NullAttributeRepresentation;
  } = {},
): QueryPlanToPrismaResult {
  return queryPlanToPrisma({
    queryPlan: planFromWireFixture(action),
    mapper: options.mapper ?? MAPPER,
    model: MODEL,
    ...(options.nullAttributeRepresentation
      ? { nullAttributeRepresentation: options.nullAttributeRepresentation }
      : {}),
  });
}

/**
 * The plan kind for the two corpus actions the planner resolves without a condition.
 *
 * `p-has` is `upstream-blocked` direct outcomes for every adapter — the planner folds `has(unknown attr)` to
 * ALWAYS_ALLOWED, so the harness cannot compare it against the oracle. Translation is still
 * defined, and pinning it here is the only assertion the corpus makes about the action.
 */
const EXPECTED_KINDS: Record<
  string,
  PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED
> = {
  "in-empty": PlanKind.ALWAYS_DENIED,
  "p-has": PlanKind.ALWAYS_ALLOWED,
};

/**
 * The `where` input this adapter emits for every corpus action it can translate, under `MAPPER`
 * and the default (`"explicit"`) null representation.
 *
 * Alphabetical by action, so the diff of a translator change reads as a list of the shapes it
 * moved. Comments are on the entries where the emitted shape is the whole point of the action;
 * the rest are pins.
 */
const EXPECTED_FILTERS: Record<string, PrismaFilter> = {
  // The three-valued-logic guard. `every` collapses UNKNOWN to false at the EXISTS boundary, so
  // an element whose `name` is NULL would be silently satisfied; CEL raises a missing-attribute
  // error and check() denies. The second conjunct is what keeps the two aligned, and it is only
  // emitted because the mapper declares `name` nullable.
  "all-on-empty": {
    AND: [
      { tags: { every: { name: { equals: "public" } } } },
      { tags: { none: { name: null } } },
    ],
  },
  "arith-add": { aNumber: { gt: 1 } },
  "arith-div": { aNumber: { equals: 2 } },
  "arith-div-frac": { aNumber: { gte: 3 } },
  "arith-mult-neg": {
    OR: [{ aNumber: { gt: -1.5 } }, { aNumber: { gte: -1 } }],
  },
  "arith-sub": { aNumber: { lte: 3 } },
  // Value-first: the plan reads `K < add(aNumber, K)`, and the mirrored operator is the whole
  // assertion. Emitting `lt` here would invert the comparison — this repository's canonical bug
  // class, and the reason `vf-*` and `arith-vf` pin the same filter as their column-first twins.
  "arith-vf": { aNumber: { gt: 1 } },
  // A constant receiver with a column needle: `K.contains(aString)` cannot become a LIKE, so the
  // adapter enumerates every substring of the constant and tests membership. The enumeration is
  // long, and pinning it verbatim is the point — an off-by-one at either end of the window is
  // invisible in a row comparison over 22 seeds and obvious in this list.
  "cr-contains": {
    aString: {
      in: [
        "",
        "s",
        "s1",
        "s10",
        "s100",
        "s100X",
        "s100Xd",
        "s100Xdo",
        "s100Xdon",
        "s100Xdone",
        "s100Xdone-",
        "s100Xdone-t",
        "s100Xdone-ta",
        "s100Xdone-tai",
        "s100Xdone-tail",
        "s100Xdone-tail\\",
        "s100Xdone-tail\\o",
        "s100Xdone-tail\\on",
        "s100Xdone-tail\\one",
        "s100Xdone-tail\\one-",
        "s100Xdone-tail\\one-e",
        "s100Xdone-tail\\one-en",
        "s100Xdone-tail\\one-end",
        "1",
        "10",
        "100",
        "100X",
        "100Xd",
        "100Xdo",
        "100Xdon",
        "100Xdone",
        "100Xdone-",
        "100Xdone-t",
        "100Xdone-ta",
        "100Xdone-tai",
        "100Xdone-tail",
        "100Xdone-tail\\",
        "100Xdone-tail\\o",
        "100Xdone-tail\\on",
        "100Xdone-tail\\one",
        "100Xdone-tail\\one-",
        "100Xdone-tail\\one-e",
        "100Xdone-tail\\one-en",
        "100Xdone-tail\\one-end",
        "0",
        "00",
        "00X",
        "00Xd",
        "00Xdo",
        "00Xdon",
        "00Xdone",
        "00Xdone-",
        "00Xdone-t",
        "00Xdone-ta",
        "00Xdone-tai",
        "00Xdone-tail",
        "00Xdone-tail\\",
        "00Xdone-tail\\o",
        "00Xdone-tail\\on",
        "00Xdone-tail\\one",
        "00Xdone-tail\\one-",
        "00Xdone-tail\\one-e",
        "00Xdone-tail\\one-en",
        "00Xdone-tail\\one-end",
        "0X",
        "0Xd",
        "0Xdo",
        "0Xdon",
        "0Xdone",
        "0Xdone-",
        "0Xdone-t",
        "0Xdone-ta",
        "0Xdone-tai",
        "0Xdone-tail",
        "0Xdone-tail\\",
        "0Xdone-tail\\o",
        "0Xdone-tail\\on",
        "0Xdone-tail\\one",
        "0Xdone-tail\\one-",
        "0Xdone-tail\\one-e",
        "0Xdone-tail\\one-en",
        "0Xdone-tail\\one-end",
        "X",
        "Xd",
        "Xdo",
        "Xdon",
        "Xdone",
        "Xdone-",
        "Xdone-t",
        "Xdone-ta",
        "Xdone-tai",
        "Xdone-tail",
        "Xdone-tail\\",
        "Xdone-tail\\o",
        "Xdone-tail\\on",
        "Xdone-tail\\one",
        "Xdone-tail\\one-",
        "Xdone-tail\\one-e",
        "Xdone-tail\\one-en",
        "Xdone-tail\\one-end",
        "d",
        "do",
        "don",
        "done",
        "done-",
        "done-t",
        "done-ta",
        "done-tai",
        "done-tail",
        "done-tail\\",
        "done-tail\\o",
        "done-tail\\on",
        "done-tail\\one",
        "done-tail\\one-",
        "done-tail\\one-e",
        "done-tail\\one-en",
        "done-tail\\one-end",
        "o",
        "on",
        "one",
        "one-",
        "one-t",
        "one-ta",
        "one-tai",
        "one-tail",
        "one-tail\\",
        "one-tail\\o",
        "one-tail\\on",
        "one-tail\\one",
        "one-tail\\one-",
        "one-tail\\one-e",
        "one-tail\\one-en",
        "one-tail\\one-end",
        "n",
        "ne",
        "ne-",
        "ne-t",
        "ne-ta",
        "ne-tai",
        "ne-tail",
        "ne-tail\\",
        "ne-tail\\o",
        "ne-tail\\on",
        "ne-tail\\one",
        "ne-tail\\one-",
        "ne-tail\\one-e",
        "ne-tail\\one-en",
        "ne-tail\\one-end",
        "e",
        "e-",
        "e-t",
        "e-ta",
        "e-tai",
        "e-tail",
        "e-tail\\",
        "e-tail\\o",
        "e-tail\\on",
        "e-tail\\one",
        "e-tail\\one-",
        "e-tail\\one-e",
        "e-tail\\one-en",
        "e-tail\\one-end",
        "-",
        "-t",
        "-ta",
        "-tai",
        "-tail",
        "-tail\\",
        "-tail\\o",
        "-tail\\on",
        "-tail\\one",
        "-tail\\one-",
        "-tail\\one-e",
        "-tail\\one-en",
        "-tail\\one-end",
        "t",
        "ta",
        "tai",
        "tail",
        "tail\\",
        "tail\\o",
        "tail\\on",
        "tail\\one",
        "tail\\one-",
        "tail\\one-e",
        "tail\\one-en",
        "tail\\one-end",
        "a",
        "ai",
        "ail",
        "ail\\",
        "ail\\o",
        "ail\\on",
        "ail\\one",
        "ail\\one-",
        "ail\\one-e",
        "ail\\one-en",
        "ail\\one-end",
        "i",
        "il",
        "il\\",
        "il\\o",
        "il\\on",
        "il\\one",
        "il\\one-",
        "il\\one-e",
        "il\\one-en",
        "il\\one-end",
        "l",
        "l\\",
        "l\\o",
        "l\\on",
        "l\\one",
        "l\\one-",
        "l\\one-e",
        "l\\one-en",
        "l\\one-end",
        "\\",
        "\\o",
        "\\on",
        "\\one",
        "\\one-",
        "\\one-e",
        "\\one-en",
        "\\one-end",
        "one-e",
        "one-en",
        "one-end",
        "ne-e",
        "ne-en",
        "ne-end",
        "e-e",
        "e-en",
        "e-end",
        "-e",
        "-en",
        "-end",
        "en",
        "end",
        "nd",
      ],
    },
  },
  "cr-endswith": {
    aString: {
      in: [
        "",
        "prefix-xaXby",
        "refix-xaXby",
        "efix-xaXby",
        "fix-xaXby",
        "ix-xaXby",
        "x-xaXby",
        "-xaXby",
        "xaXby",
        "aXby",
        "Xby",
        "by",
        "y",
      ],
    },
  },
  "cr-startswith": {
    aString: {
      in: [
        "",
        "x",
        "xa",
        "xaX",
        "xaXb",
        "xaXby",
        "xaXby-",
        "xaXby-t",
        "xaXby-ta",
        "xaXby-tai",
        "xaXby-tail",
      ],
    },
  },
  "cr-startswith-concat": {
    aString: {
      in: [
        "",
        "x",
        "xa",
        "xaX",
        "xaXb",
        "xaXby",
        "xaXby-",
        "xaXby-t",
        "xaXby-ta",
        "xaXby-tai",
        "xaXby-tail",
      ],
    },
  },
  "cs-contains": { aString: { contains: "one" } },
  "cs-endswith": { aString: { endsWith: "one" } },
  "cs-eq": { aString: { equals: "one" } },
  "cs-startswith": { aString: { startsWith: "one" } },
  "double-negation": { NOT: { aBool: { equals: false } } },
  "double-threshold": {
    AND: [{ aNumber: { gte: 1.5 } }, { aNumber: { gt: 1 } }],
  },
  "empty-string-eq": { aString: { equals: "" } },
  "exists-on-empty": { tags: { some: { name: { equals: "public" } } } },
  "field-to-field": {
    aString: {
      equals: { _ref: "aOptionalString", _container: "AdversarialResource" },
    },
  },
  "gt-bare": { aNumber: { gt: 1 } },
  "hier-ancestor-cf": { scope: { startsWith: "dept.eng." } },
  "hier-ancestor-ff": { scope: { in: ["dept", "dept.eng"] } },
  "hier-descendent-cf": { scope: { in: ["dept", "dept.eng"] } },
  "hier-descendent-ff": { scope: { startsWith: "dept.eng." } },
  "hier-list-id": { id: { equals: "f1" } },
  "hier-meta-in": { scope: { in: ["50%", "50%.a_b"] } },
  "hier-overlaps-cf": {
    OR: [
      { scope: { in: ["dept"] } },
      { scope: { equals: "dept.eng" } },
      { scope: { startsWith: "dept.eng." } },
    ],
  },
  "hier-overlaps-ff": {
    OR: [
      { scope: { in: ["dept"] } },
      { scope: { equals: "dept.eng" } },
      { scope: { startsWith: "dept.eng." } },
    ],
  },
  "id-concat-vf": { id: { equals: "f1" } },
  // The primary key, which arrives as the variable `request.resource.id` rather than through
  // `attr` — the planner leaves the resource's own identity symbolic because PlanResources is
  // asked about a kind, not a row. An adapter that resolves references by stripping a
  // `request.resource.attr.` prefix never reaches this name, and fails silently when it does not.
  "id-eq-const": { id: { equals: "f1" } },
  // `_ref`/`_container` is Prisma's field-reference encoding: comparing two columns needs the
  // model name, which is why `queryPlanToPrisma` takes `model`.
  "id-f2f": {
    aString: { equals: { _ref: "id", _container: "AdversarialResource" } },
  },
  "id-f2f-ne": {
    NOT: {
      aString: { equals: { _ref: "id", _container: "AdversarialResource" } },
    },
  },
  "in-null-elem-hasint": {
    OR: [
      { tags: { some: { name: "public" } } },
      { tags: { some: { name: null } } },
    ],
  },
  "in-null-elem-mixed": {
    OR: [
      { aOptionalString: { in: ["x", "one_two"] } },
      { aOptionalString: null },
    ],
  },
  "in-null-elem-neg": {
    NOT: {
      OR: [
        { aOptionalString: { in: ["x", "one_two"] } },
        { aOptionalString: null },
      ],
    },
  },
  "in-null-elem-only": { aOptionalString: { equals: null } },
  "in-null-elem-only-neg": { NOT: { aOptionalString: { equals: null } } },
  "in-null-elem-rel": { tags: { some: { name: null } } },
  "in-null-elem-rel-neg": { NOT: { tags: { some: { name: null } } } },
  "in-single": { aString: { equals: "one" } },
  "lambda-in-principal": {
    tags: { some: { name: { in: ["public", "special"] } } },
  },
  "le-bare": { aNumber: { lte: 2 } },
  "like-bracket": { aString: { startsWith: "[SEC]" } },
  "macro-depth3-all": {
    categories: {
      some: {
        subCategories: {
          every: { labels: { some: { name: { equals: "gold" } } } },
        },
      },
    },
  },
  "macro-depth3-exists": {
    categories: {
      some: {
        subCategories: {
          some: { labels: { some: { name: { equals: "gold" } } } },
        },
      },
    },
  },
  "macro-depth3-not-exists": {
    AND: [
      {
        NOT: {
          categories: {
            some: {
              subCategories: {
                some: { labels: { some: { name: { equals: "gold" } } } },
              },
            },
          },
        },
      },
      {
        NOT: {
          categories: {
            some: {
              AND: [
                {
                  NOT: {
                    subCategories: {
                      some: { labels: { some: { name: { equals: "gold" } } } },
                    },
                  },
                },
                {
                  subCategories: {
                    some: {
                      AND: [
                        {
                          NOT: {
                            labels: { some: { name: { equals: "gold" } } },
                          },
                        },
                        { labels: { some: { name: null } } },
                      ],
                    },
                  },
                },
              ],
            },
          },
        },
      },
    ],
  },
  "n-all-mixed-null": {
    AND: [
      { tags: { every: { name: { not: "x" } } } },
      { tags: { none: { name: null } } },
    ],
  },
  "n-not-all-absorb": { tags: { some: { NOT: { name: { not: "public" } } } } },
  "n-not-all-null": { tags: { some: { NOT: { name: { equals: "public" } } } } },
  "nan-ord-inf": {
    OR: [
      { aBool: { equals: true } },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "nan-ord-le": {
    OR: [
      {
        AND: [
          { aBool: { equals: true } },
          {
            OR: [
              {
                AND: [
                  { aBool: { equals: true } },
                  { aBool: { equals: false } },
                ],
              },
            ],
          },
        ],
      },
      {
        AND: [
          { aBool: { equals: false } },
          {
            OR: [
              { aBool: { equals: true } },
              {
                AND: [
                  { aBool: { equals: true } },
                  { aBool: { equals: false } },
                ],
              },
            ],
          },
        ],
      },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "nan-ord-ternary": {
    OR: [
      { aBool: { equals: true } },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "nan-ord-ternary-vf": {
    OR: [
      { aBool: { equals: true } },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "nary-and": {
    AND: [
      { aBool: { equals: true } },
      { aNumber: { gte: 0 } },
      { aString: { not: "one" } },
    ],
  },
  "neg-number": { aNumber: { lt: -1 } },
  // The De Morgan branch. `NOT` over a conjunction was unreachable from the corpus until #387,
  // and it is the same wire shape a DENY-composed `nand` rule produces — byte-identical
  // `filter.condition` JSON, which is why the DENY spelling ported as a delete.
  "not-and": {
    NOT: { AND: [{ aBool: { equals: true } }, { aString: { not: "one" } }] },
  },
  "not-empty": { NOT: { tags: { none: {} } } },
  "not-exists": {
    AND: [
      { NOT: { tags: { some: { name: { equals: "private" } } } } },
      { NOT: { tags: { some: { name: null } } } },
    ],
  },
  "not-gt": { NOT: { aNumber: { gt: 1 } } },
  "not-lt": { NOT: { aNumber: { lt: 2 } } },
  "null-eq": { aOptionalString: { equals: null } },
  "null-eq-missing": { aOptionalString: { equals: null } },
  "null-ne": { aOptionalString: { not: null } },
  "null-not-eq": { NOT: { aOptionalString: { equals: null } } },
  "null-value-f2f": {
    OR: [
      { AND: [{ aOptionalString: null }, { scope: null }] },
      {
        AND: [
          { aOptionalString: { not: null } },
          { scope: { not: null } },
          {
            aOptionalString: {
              equals: { _ref: "scope", _container: "AdversarialResource" },
            },
          },
        ],
      },
    ],
  },
  "null-value-ne-const": {
    NOT: {
      AND: [
        { aOptionalString: { not: null } },
        { aOptionalString: { equals: "x" } },
      ],
    },
  },
  // Under the explicit-null convention CEL holds a null VALUE, so `null != "x"` is TRUE and
  // `null == "x"` is FALSE — both definite. Prisma's bare `{ not: "x" }` drops the row under both
  // polarities, so the equality family is rendered so it can never depend on SQL's UNKNOWN. The
  // `not: null` conjunct is that rendering, and it is what the `owner`/`coOwner` declarations in
  // the mapper buy (cerbos/query-plan-adapters#308).
  "null-value-not-eq-const": {
    NOT: {
      AND: [
        { aOptionalString: { not: null } },
        { aOptionalString: { equals: "x" } },
      ],
    },
  },
  "null-value-not-in-const": {
    NOT: {
      AND: [
        { aOptionalString: { not: null } },
        { aOptionalString: { in: ["x", "one_two"] } },
      ],
    },
  },
  "null-value-pv-not-exists": {
    AND: [
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "set" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "same" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "%_o" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "X" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "Y" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "MIRROR" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "filler-1" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "filler-2" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "filler-3" } },
          ],
        },
      },
      {
        NOT: {
          AND: [
            { aOptionalString: { not: null } },
            { aOptionalString: { equals: "filler-4" } },
          ],
        },
      },
    ],
  },
  "optional-ne": { aOptionalString: { not: "x" } },
  "or-eq-exists": {
    OR: [
      { aBool: { equals: true } },
      { tags: { some: { name: { equals: "public" } } } },
    ],
  },
  "or-eq-in": {
    OR: [{ aBool: { equals: true } }, { tags: { some: { name: "public" } } }],
  },
  "outer-attr-depth2": {
    AND: [
      {
        categories: {
          some: { subCategories: { some: { name: { equals: "finance" } } } },
        },
      },
      { aBool: { equals: true } },
    ],
  },
  "p-arith-in-lambda": {
    AND: [
      { tags: { some: { name: { equals: "public" } } } },
      { aNumber: { gt: 1 } },
    ],
  },
  "p-double-frac": {
    AND: [
      { aNumber: { equals: 2.9999999999999996 } },
      { aNumber: { gt: 2 } },
      { aNumber: { lt: 3 } },
    ],
  },
  "p-hasintersection-map": {
    AND: [
      {
        tags: {
          some: {
            name: { in: ["public", "h\u00e9llo\ud83d\ude80", "100%_x"] },
          },
        },
      },
      { NOT: { tags: { some: { name: null } } } },
    ],
  },
  "p-in-null-multi": { aOptionalString: { in: ["x", "one_two"] } },
  "p-in-null-single": { aOptionalString: { equals: "x" } },
  "p-lambda-inner-f2f": {
    tags: {
      some: {
        tagId: { equals: { _ref: "name", _container: "AdversarialTag" } },
      },
    },
  },
  "p-not-exists-empty": {
    AND: [
      { NOT: { tags: { some: { name: { equals: "public" } } } } },
      { NOT: { tags: { some: { name: null } } } },
    ],
  },
  "p-not-ternary-null": {
    NOT: {
      OR: [
        { AND: [{ aOptionalString: { not: "x" } }, { aNumber: { gt: 1 } }] },
        {
          AND: [
            { aOptionalString: { not: "x" } },
            { NOT: { aOptionalString: { not: "x" } } },
          ],
        },
      ],
    },
  },
  "p-struct": { aString: { equals: "one" } },
  "p-ternary-in-exists": {
    OR: [
      {
        AND: [
          { aBool: { equals: true } },
          { tags: { some: { name: { equals: "public" } } } },
        ],
      },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "p-ternary-of-ternaries": {
    OR: [
      {
        AND: [
          { aBool: { equals: true } },
          {
            OR: [
              {
                AND: [
                  { NOT: { aString: { equals: "" } } },
                  { aNumber: { gt: 1 } },
                ],
              },
              {
                AND: [
                  { aString: { equals: "" } },
                  { NOT: { aString: { equals: "" } } },
                ],
              },
            ],
          },
        ],
      },
      {
        AND: [
          { aBool: { equals: false } },
          {
            OR: [
              { NOT: { aNumber: { lt: 0 } } },
              {
                AND: [{ aNumber: { lt: 0 } }, { NOT: { aNumber: { lt: 0 } } }],
              },
            ],
          },
        ],
      },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "p-ternary-under-all": {
    OR: [
      {
        AND: [
          { aBool: { equals: true } },
          {
            AND: [
              { tags: { every: { name: { not: "private" } } } },
              { tags: { none: { name: null } } },
            ],
          },
        ],
      },
      { aBool: { equals: false } },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "p-ternary-vs-ternary": {
    OR: [
      {
        AND: [
          { aBool: { equals: true } },
          {
            OR: [
              { AND: [{ aString: { equals: "" } }, { aNumber: { gt: 1 } }] },
              {
                AND: [
                  { NOT: { aString: { equals: "" } } },
                  { aNumber: { gt: 2 } },
                ],
              },
              {
                AND: [
                  { aString: { equals: "" } },
                  { NOT: { aString: { equals: "" } } },
                ],
              },
            ],
          },
        ],
      },
      {
        AND: [
          { aBool: { equals: false } },
          {
            OR: [
              {
                AND: [
                  { aString: { equals: "" } },
                  { NOT: { aString: { equals: "" } } },
                ],
              },
            ],
          },
        ],
      },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "pv-all": {
    AND: [
      { aOptionalString: { not: "set" } },
      { aOptionalString: { not: "same" } },
      { aOptionalString: { not: "" } },
      { aOptionalString: { not: "%_o" } },
      { aOptionalString: { not: "X" } },
      { aOptionalString: { not: "Y" } },
      { aOptionalString: { not: "MIRROR" } },
      { aOptionalString: { not: "filler-1" } },
      { aOptionalString: { not: "filler-2" } },
      { aOptionalString: { not: "filler-3" } },
      { aOptionalString: { not: "filler-4" } },
    ],
  },
  "pv-all-unrolled": {
    AND: [
      { aOptionalString: { not: "set" } },
      {
        AND: [
          { aOptionalString: { not: "" } },
          { aOptionalString: { not: "%_o" } },
        ],
      },
    ],
  },
  "pv-exists": {
    OR: [
      { aOptionalString: { equals: "set" } },
      { aOptionalString: { equals: "same" } },
      { aOptionalString: { equals: "" } },
      { aOptionalString: { equals: "%_o" } },
      { aOptionalString: { equals: "X" } },
      { aOptionalString: { equals: "Y" } },
      { aOptionalString: { equals: "MIRROR" } },
      { aOptionalString: { equals: "filler-1" } },
      { aOptionalString: { equals: "filler-2" } },
      { aOptionalString: { equals: "filler-3" } },
      { aOptionalString: { equals: "filler-4" } },
    ],
  },
  "pv-exists-unrolled": {
    OR: [
      { aOptionalString: { equals: "set" } },
      {
        OR: [
          { aOptionalString: { equals: "" } },
          { aOptionalString: { equals: "%_o" } },
        ],
      },
    ],
  },
  "rel-bool-hop": { parent: { is: { aBool: { equals: true } } } },
  "rel-bool-hop2": {
    parent: { is: { inner: { is: { aBool: { equals: true } } } } },
  },
  "rel-contains-hop": { parent: { is: { aString: { contains: "done" } } } },
  "rel-eq-hop": { parent: { is: { aString: { equals: "One" } } } },
  "rel-eq-num-hop": { parent: { is: { aNumber: { equals: 2 } } } },
  "rel-ge-hop": { parent: { is: { aNumber: { gte: 2 } } } },
  "rel-gt-hop": { parent: { is: { aNumber: { gt: 2 } } } },
  "rel-hop-and-root": {
    AND: [
      { aBool: { equals: true } },
      { parent: { is: { aString: { contains: "re" } } } },
    ],
  },
  "rel-hop2-or-exists": {
    OR: [
      { parent: { is: { inner: { is: { aBool: { equals: true } } } } } },
      { categories: { some: { name: { equals: "business" } } } },
    ],
  },
  "rel-le-hop": { parent: { is: { aNumber: { lte: 2 } } } },
  "rel-lt-hop": { parent: { is: { aNumber: { lt: 2 } } } },
  "rel-ne-null-hop": { parent: { is: { aOptionalString: { not: null } } } },
  // The absent-parent guard. `NOT { parent: { is: P } }` is satisfied by a row with no parent at
  // all, while CEL raises a missing-attribute error on the hop and check() denies. The first
  // conjunct — an existence test on the hop itself — is what keeps a negated shape over an
  // OPTIONAL to-one relation from over-granting. The positive shapes cannot discriminate it,
  // which is why the corpus carries the negated ones (ADR 0005).
  "rel-not-bool-hop": {
    AND: [
      { parent: { is: {} } },
      { NOT: { parent: { is: { aBool: { equals: true } } } } },
    ],
  },
  "rel-range-hop": {
    AND: [
      { parent: { is: { aNumber: { gt: 2 } } } },
      { parent: { is: { aNumber: { lt: 12 } } } },
    ],
  },
  "rel-startswith-hop2": {
    parent: { is: { inner: { is: { aString: { startsWith: "100" } } } } },
  },
  "root-bare-bool": { aBool: { equals: true } },
  "root-or": { OR: [{ aBool: { equals: true } }, { aNumber: { lt: 0 } }] },
  "ternary-bare": {
    OR: [
      { AND: [{ aBool: { equals: true } }, { aString: { equals: "one" } }] },
      { AND: [{ aBool: { equals: false } }, { aNumber: { lt: 0 } }] },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "ternary-cmp": {
    OR: [
      { AND: [{ aBool: { equals: true } }, { aNumber: { gt: 1 } }] },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "ternary-expr-cond": {
    OR: [
      { AND: [{ aString: { startsWith: "100" } }, { aNumber: { gte: 0 } }] },
      {
        AND: [
          { aString: { startsWith: "100" } },
          { NOT: { aString: { startsWith: "100" } } },
        ],
      },
    ],
  },
  "ternary-negated": {
    NOT: {
      OR: [
        { AND: [{ aBool: { equals: true } }, { aNumber: { gt: 1 } }] },
        { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
      ],
    },
  },
  "ternary-nested": {
    OR: [
      {
        AND: [
          { aBool: { equals: true } },
          {
            OR: [
              {
                AND: [
                  { NOT: { aString: { equals: "" } } },
                  { aNumber: { gte: 2 } },
                ],
              },
              {
                AND: [
                  { aString: { equals: "" } },
                  { NOT: { aString: { equals: "" } } },
                ],
              },
            ],
          },
        ],
      },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "ternary-null-cond": {
    OR: [
      { AND: [{ aOptionalString: { not: "x" } }, { aNumber: { gt: 1 } }] },
      {
        AND: [
          { aOptionalString: { not: "x" } },
          { NOT: { aOptionalString: { not: "x" } } },
        ],
      },
    ],
  },
  "ternary-value-first": {
    OR: [
      { AND: [{ aBool: { equals: true } }, { aNumber: { gt: 0 } }] },
      { AND: [{ aBool: { equals: true } }, { aBool: { equals: false } }] },
    ],
  },
  "triple-negation": { NOT: { NOT: { aBool: { equals: false } } } },
  "ts-eq": { createdAt: { equals: "2024-06-01T00:00:00.000Z" } },
  "ts-eq-offset": { createdAt: { equals: "2024-06-01T00:00:00.000Z" } },
  "ts-ne": { createdAt: { not: "2024-06-01T00:00:00.000Z" } },
  "unicode-eq": { aString: { equals: "h\u00e9llo\ud83d\ude80" } },
  "vf-ge": { aNumber: { lte: 2 } },
  "vf-hasint": { tags: { some: { name: { in: ["public", "other"] } } } },
  "vf-le": { aNumber: { gte: 3 } },
  "vf-lt": { aNumber: { gt: 1 } },
  "vf-ne": { aString: { not: "one" } },
  "vf-null-ne": { aOptionalString: { not: null } },
  "vf-size": { tags: { some: {} } },
  "w1-exists-chain": {
    categories: {
      some: { subCategories: { some: { name: { equals: "finance" } } } },
    },
  },
  "w1-in-chain": {
    categories: { some: { subCategories: { some: { name: "finance" } } } },
  },
  // The same absent-hop guard as `rel-not-bool-hop`, one level up a multi-hop chain: the negation
  // is joined through every intermediate relation, never off the root, and the leading existence
  // test is what stops a row with no category at all from satisfying the negation.
  "w1-not-exists-chain": {
    AND: [
      { categories: { some: {} } },
      {
        NOT: {
          categories: {
            some: { subCategories: { some: { name: { equals: "finance" } } } },
          },
        },
      },
    ],
  },
  "w1-not-hasint-chain": {
    AND: [
      { categories: { some: {} } },
      {
        NOT: {
          categories: {
            some: { subCategories: { some: { name: "finance" } } },
          },
        },
      },
    ],
  },
  "w1-not-in-chain": {
    AND: [
      { categories: { some: {} } },
      {
        NOT: {
          categories: {
            some: { subCategories: { some: { name: "finance" } } },
          },
        },
      },
    ],
  },
  "w1-not-size-chain": {
    AND: [
      { categories: { some: {} } },
      { NOT: { categories: { some: { subCategories: { some: {} } } } } },
    ],
  },
  "w1-size-chain": { categories: { some: { subCategories: { some: {} } } } },
  "w1-size-zero-chain": {
    categories: { some: { subCategories: { none: {} } } },
  },
  "w1-ternary-chain-cond": {
    OR: [
      {
        AND: [
          {
            categories: {
              some: { subCategories: { some: { name: "finance" } } },
            },
          },
          { aBool: { equals: true } },
        ],
      },
      {
        AND: [
          {
            AND: [
              { categories: { some: {} } },
              {
                NOT: {
                  categories: {
                    some: { subCategories: { some: { name: "finance" } } },
                  },
                },
              },
            ],
          },
          { aBool: { equals: false } },
        ],
      },
      {
        AND: [
          {
            categories: {
              some: { subCategories: { some: { name: "finance" } } },
            },
          },
          {
            AND: [
              { categories: { some: {} } },
              {
                NOT: {
                  categories: {
                    some: { subCategories: { some: { name: "finance" } } },
                  },
                },
              },
            ],
          },
        ],
      },
    ],
  },
};

describe("corpus shapes", () => {
  test.each(Object.entries(EXPECTED_FILTERS))(
    "%s emits the pinned filter",
    (action, filters) => {
      expect(translate(action)).toStrictEqual({
        kind: PlanKind.CONDITIONAL,
        filters,
      });
    },
  );

  test.each(Object.entries(EXPECTED_KINDS))(
    "%s resolves without a condition",
    (action, kind) => {
      expect(translate(action)).toStrictEqual({ kind });
    },
  );

  // The message, not just the throw: a mapper typo or an unrelated validation satisfies a bare
  // `toThrow()` just as well as the limitation the corpus documents (#326). The harness makes the
  // same assertion against a live PDP; here it costs a millisecond and covers the whole roster,
  // which is what lets the completeness guard below be total.
  test.each(THROWING_ACTIONS)(
    "$action is refused with the message adapterctl.json pins ($reason)",
    ({ action, message }) => {
      expect(() => translate(action)).toThrow(message);
    },
  );

  test("every corpus action is classified here exactly once", () => {
    const filters = Object.keys(EXPECTED_FILTERS);
    const kinds = Object.keys(EXPECTED_KINDS);
    const throwing = THROWING_ACTIONS.map(({ action }) => action);
    const classified = [...filters, ...kinds, ...throwing].sort();

    // Total: a corpus action with no entry here lands as a failure rather than as silence. This
    // is the assertion that makes the file self-maintaining — adding a hostile shape to the
    // corpus forces someone to look at the filter this adapter emits for it.
    expect(classified).toEqual(wireFixtureActions());
    // Disjoint: an action pinned as a filter AND declared unsupported would satisfy the union
    // above while asserting two contradictory things.
    expect(classified).toEqual([...new Set(classified)].sort());
    // The table is alphabetical, so a translator change reads as the list of shapes it moved.
    expect(filters).toEqual([...filters].sort());

    expect(classified).toEqual(controlPlane.allActions);
  });
});

describe("nullAttributeRepresentation", () => {
  // `null-eq-missing` is the corpus's omitted-NULL probe: `== null` against an
  // attribute the caller OMITS when the column is NULL. The two conventions are indistinguishable
  // on the wire — the planner emits the same `eq(attr, null)` either way — so the adapter has to
  // be told, and the whole behaviour is a translator property with no store in it.
  test("explicit: a null operand becomes an IS NULL filter", () => {
    expect(
      translate("null-eq-missing", { nullAttributeRepresentation: "explicit" }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aOptionalString: { equals: null } },
    });
  });

  test("omitted: the same plan is refused rather than translated", () => {
    // A NULL column sends no attribute, so check() denies on a missing-attribute error while the
    // filter above would return exactly those rows (#302).
    expect(() =>
      translate("null-eq-missing", { nullAttributeRepresentation: "omitted" }),
    ).toThrow(
      requireOutcomeMessage({ controlPlane, action: "null-eq-missing" }),
    );
  });
});

describe("timestamp literals", () => {
  // `regenerate-wire-fixtures.sh` rewrites the folded `now() - duration("24h")` literal in
  // `ts-window` to a placeholder, because it differs on every capture. That makes this the one
  // fixture whose value the reader chooses — so it is also the one place the whole timestamp
  // boundary can be walked, by substituting the instant and asking what the adapter does with it.
  const at = (plannedAt: string) =>
    queryPlanToPrisma({
      queryPlan: planFromWireFixture("ts-window", plannedAt),
      mapper: MAPPER,
      model: MODEL,
    });

  test("a nanosecond instant — what the PDP actually folds — is refused", () => {
    // This, and nothing else, is why `ts-window` and `ts-vf` have `rejected` direct outcomes. A tidy
    // millisecond substitution in the loader would translate cleanly and quietly contradict
    // adapterctl.json.
    expect(() => translate("ts-window")).toThrow(
      "Timestamp value exceeds millisecond precision",
    );
  });

  test("the same plan at millisecond precision translates", () => {
    expect(at("2026-08-11T09:13:39.123Z")).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { createdAt: { lt: "2026-08-11T09:13:39.123Z" } },
    });
  });

  test("excess fractional digits are accepted only when they are zero", () => {
    expect(at("2026-08-11T09:13:39.123000Z")).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { createdAt: { lt: "2026-08-11T09:13:39.123Z" } },
    });
  });

  // Each of these is refused rather than coerced: a `Date` parsed from a lenient string would
  // compare against the column as some other instant, which is a filter that returns rows the PDP
  // denies rather than an error the caller can see.
  test.each([
    ["a date with no time part", "2024-01-01"],
    ["a year outside CEL's instant range", "0000-01-01T00:00:00Z"],
    ["a day that does not exist", "2024-02-30T00:00:00Z"],
    ["sub-millisecond precision", "2024-01-01T00:00:00.1234Z"],
    [
      "an offset that pushes past the maximum instant",
      "9999-12-31T23:00:00-02:00",
    ],
  ])("%s fails closed", (_label, value) => {
    expect(() => at(value)).toThrow(/RFC 3339|millisecond|instant range/);
  });
});

describe("relation subqueryFilter", () => {
  /**
   * The one mapping hazard the corpus cannot express with a policy action, because the policy is
   * irrelevant to it: the rows the adapter's subquery sees must equal the rows the application
   * put into the resource attributes. Prisma has no schema-level filtered relation — a `where`
   * injected by a client extension rewrites the top-level query, never the nested
   * `some`/`every`/`none` this adapter generates — so a narrowing the application applies to its
   * own reads does not reach the subquery unless the caller declares it. See "Mapping hazards" in
   * conformance/README.md and cerbos/query-plan-adapters#314.
   */
  const VISIBLE_ONLY: PrismaFilter = { name: { not: "hidden" } };

  const mapperFor = (subqueryFilter?: PrismaFilter): Mapper => ({
    "request.resource.attr.tags": {
      relation: {
        name: "tags",
        type: "many",
        fields: { name: { field: "name" } },
        ...(subqueryFilter ? { subqueryFilter } : {}),
      },
    },
  });

  const filtersFor = (action: string, subqueryFilter?: PrismaFilter) => {
    const result = translate(action, { mapper: mapperFor(subqueryFilter) });
    if (result.kind !== PlanKind.CONDITIONAL) {
      throw new Error(`Expected CONDITIONAL result for ${action}`);
    }
    return result.filters;
  };

  test("declared: exists() examines only the records the application serialised", () => {
    expect(filtersFor("exists-on-empty", VISIBLE_ONLY)).toStrictEqual({
      tags: {
        some: {
          AND: [{ name: { not: "hidden" } }, { name: { equals: "public" } }],
        },
      },
    });
  });

  test("declared: all() narrows the records examined, not the records required", () => {
    // `every: AND(visible, P)` would REQUIRE every record to be visible, dropping any row that
    // holds a hidden tag. The rewrite to `none: AND(visible, NOT P)` is what makes the
    // declaration mean "ignore what the application hides" — including the empty-collection case,
    // where CEL's all() is vacuously true and check() agrees because the application sent an
    // empty list for the same reason.
    expect(filtersFor("all-on-empty", VISIBLE_ONLY)).toStrictEqual({
      tags: {
        none: {
          AND: [
            { name: { not: "hidden" } },
            { NOT: { name: { equals: "public" } } },
          ],
        },
      },
    });
  });

  test("declared: an emptiness check counts only the visible records", () => {
    expect(filtersFor("not-empty", VISIBLE_ONLY)).toStrictEqual({
      NOT: { tags: { none: { name: { not: "hidden" } } } },
    });
  });

  test("undeclared: the emitted filter is what it was before the field existed", () => {
    // The non-breaking guarantee. Silence must not add a clause, and must not warn.
    expect(filtersFor("exists-on-empty")).toStrictEqual({
      tags: { some: { name: { equals: "public" } } },
    });
    expect(filtersFor("all-on-empty")).toStrictEqual({
      tags: { every: { name: { equals: "public" } } },
    });
    expect(filtersFor("not-empty")).toStrictEqual({
      NOT: { tags: { none: {} } },
    });
  });
});

describe("the mapper contract", () => {
  // A mapper is caller-supplied, so these are caller errors rather than policy shapes: no corpus
  // action can produce them, because the corpus fixes one mapper per adapter.

  test("a function mapper resolves a scalar reference", () => {
    expect(
      translate("cs-eq", {
        mapper: (key: string) => ({
          field: key.replace("request.resource.attr.", ""),
        }),
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aString: { equals: "one" } },
    });
  });

  test("a function mapper resolves a relation", () => {
    expect(
      translate("rel-bool-hop", {
        mapper: () => ({
          relation: {
            name: "parent",
            type: "one",
            fields: { aBool: { field: "aBool" } },
          },
        }),
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { parent: { is: { aBool: { equals: true } } } },
    });
  });

  test("size() against a scalar mapping is refused rather than guessed", () => {
    expect(() =>
      translate("not-empty", {
        mapper: { "request.resource.attr.tags": { field: "tags" } },
      }),
    ).toThrow("size operator requires a relation mapping");
  });
});

describe("plans the planner cannot produce", () => {
  // Input validation on a public function, not policy shapes. Every other assertion in this file
  // reads its plan from a fixture precisely because a typed plan is a belief about the planner —
  // but these are malformed by construction, so there is no fixture to read and nothing to
  // believe. They exist so a caller who hands the adapter a hand-rolled or half-decoded plan gets
  // an error rather than a filter.
  //
  // The test for a shape CEL *can* express does not belong here, whatever its plan looks like:
  // it belongs in the corpus, where every adapter is asked about it. Two such shapes were
  // pinned here by hand-built plan before #377 and are now
  // [#394](https://github.com/cerbos/query-plan-adapters/issues/394).

  const plan = (condition: unknown): PlanResourcesResponse =>
    ({
      kind: PlanKind.CONDITIONAL,
      condition: condition as PlanExpressionOperand,
      cerbosCallId: "",
      requestId: "",
      validationErrors: [],
      metadata: undefined,
    }) as PlanResourcesResponse;

  test("an unrecognised plan kind", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        mapper: {},
      }),
    ).toThrow("Invalid query plan.");
  });

  test("a condition with neither operator nor operands", () => {
    expect(() =>
      queryPlanToPrisma({ queryPlan: plan({}), mapper: {} }),
    ).toThrow("Invalid Cerbos expression structure");
  });

  test("an operator this adapter has never heard of", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({ operator: "unsupported", operands: [] }),
        mapper: {},
      }),
    ).toThrow("Unsupported operator: unsupported");
  });

  test("an operand that is neither a variable nor a value", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({ operator: "eq", operands: [{}, { value: "test" }] }),
        mapper: {},
      }),
    ).toThrow("No valid left operand found");
  });

  test("a ternary with the wrong arity", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "if",
          operands: [{ name: "request.resource.attr.aBool" }, { value: true }],
        }),
        mapper: MAPPER,
      }),
    ).toThrow(
      "if (ternary) requires exactly 3 operands (condition, then, else), got 2",
    );
  });

  test("a ternary whose condition is not a boolean expression", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "if",
          operands: [{ value: null }, { value: true }, { value: false }],
        }),
        mapper: MAPPER,
      }),
    ).toThrow("if (ternary) condition must be a boolean expression");
  });

  test("a comparison against a ternary with three operands", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "eq",
          operands: [
            {
              operator: "if",
              operands: [
                { name: "request.resource.attr.aBool" },
                { value: true },
                { value: false },
              ],
            },
            { value: true },
            { value: false },
          ],
        }),
        mapper: MAPPER,
      }),
    ).toThrow("eq with a ternary requires exactly 2 operands, got 3");
  });

  test("a macro over a collection value that is not a list", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "exists",
          operands: [
            { value: "not-a-list" },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "eq",
                  operands: [
                    { name: "request.resource.attr.aString" },
                    { name: "t" },
                  ],
                },
                { name: "t" },
              ],
            },
          ],
        }),
        mapper: MAPPER,
      }),
    ).toThrow("exists over a literal collection requires a list value");
  });

  test("a constant-false predicate the planner should have folded", () => {
    expect(() =>
      queryPlanToPrisma({
        queryPlan: plan({
          operator: "if",
          operands: [{ value: true }, { value: false }, { value: true }],
        }),
        mapper: MAPPER,
      }),
    ).toThrow(
      "A constant-false conditional predicate must be folded by the Cerbos planner",
    );
  });
});
