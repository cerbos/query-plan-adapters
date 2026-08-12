import { describe, expect, test } from "@jest/globals";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
} from "@cerbos/core";
import { Types } from "mongoose";

import { PlanKind, queryPlanToMongoose } from ".";
import type {
  Mapper,
  MongooseFilter,
  NullAttributeRepresentation,
  QueryPlanToMongooseResult,
} from ".";
import {
  MAPPER,
  classifyActionsForAdapter,
  parseActionsFile,
  planFromWireFixture,
  readJson,
  wireFixtureActions,
} from "./corpus";

/**
 * Translator unit test: for every action in the shared `../conformance/` corpus, the filter this
 * adapter emits. Offline — no Cerbos sidecar, no MongoDB.
 *
 * The per-adapter suite this replaced braided four assertions into every test. Three of them are
 * somebody else's job now, and this file makes only the fourth:
 *
 * | assertion | who owns it |
 * | --- | --- |
 * | the plan the PDP produces for a policy | `conformance/wire-fixtures/`, replanned and diffed by the `Conformance Corpus` workflow |
 * | which shapes this adapter must refuse, and with what message | `conformance/actions.json` — read below, not restated |
 * | the documents a filter returns | `adversarial.test.ts`, against a real MongoDB with `check()` as the oracle |
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
 * documents *against the 21 it seeds*. Two different filters can agree on all of them and disagree
 * on the document a consumer has, so a rewrite that quietly changes the emitted query passes there
 * and shows up here as a diff a reviewer reads. It is also the only place a
 * `nullAttributeRepresentation` boundary, a timestamp literal, or a caller-supplied `valueParser`
 * can be pinned at all.
 *
 * **Adding a corpus action fails this file.** Every wire fixture must be classified here exactly
 * once — expected filter, expected plan kind, or expected throw — and the guard at the bottom is
 * what makes a new action land as a failure rather than as silence.
 */

const actionsFile = parseActionsFile(readJson("actions.json"));

/**
 * The shapes `actions.json` says this adapter must refuse, each with the message it must refuse
 * them with. Identical to the classification `adversarial.test.ts` asserts against a live PDP;
 * asserting it here as well is what lets the completeness guard below be total, and it costs a
 * millisecond rather than a container.
 */
const { throwingActions: THROWING_ACTIONS } = classifyActionsForAdapter(
  actionsFile,
  "mongoose",
);

function translate(
  action: string,
  options: {
    mapper?: Mapper;
    nullAttributeRepresentation?: NullAttributeRepresentation;
  } = {},
): QueryPlanToMongooseResult {
  return queryPlanToMongoose({
    queryPlan: planFromWireFixture(action),
    mapper: options.mapper ?? MAPPER,
    ...(options.nullAttributeRepresentation
      ? { nullAttributeRepresentation: options.nullAttributeRepresentation }
      : {}),
  });
}

/**
 * The plan kind for the two corpus actions the planner resolves without a condition.
 *
 * `p-has` is `knownDivergences` for every adapter — the planner folds `has(unknown attr)` to
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
 * The filter this adapter emits for every corpus action it can translate, under `MAPPER` and the
 * default (`"explicit"`) null representation.
 *
 * Alphabetical by action, so the diff of a translator change reads as a list of the shapes it
 * moved. Comments are on the entries where the emitted shape is the whole point of the action;
 * the rest are pins.
 */
const EXPECTED_FILTERS: Record<string, MongooseFilter> = {
  // The three-valued-logic guard. `$not/$elemMatch` is the only universal quantifier a query
  // filter has, and it collapses UNKNOWN to false: an element whose `name` is NULL would satisfy
  // the negated inner predicate silently, while CEL raises a missing-attribute error and check()
  // denies. The `name: {$ne: null}` conjunct inside the `$nor` is what keeps the two aligned, and
  // it is only emitted because the mapper declares `name` nullable.
  "all-on-empty": {
    tags: {
      $type: "array",
      $not: {
        $elemMatch: {
          $nor: [
            {
              $and: [
                {
                  name: {
                    $ne: null,
                  },
                },
                {
                  name: {
                    $eq: "public",
                  },
                },
              ],
            },
          ],
        },
      },
    },
  },
  "arith-add": {
    $expr: {
      $gt: [
        {
          $add: ["$aNumber", 1],
        },
        2,
      ],
    },
  },
  "arith-add-eq-frac": {
    $and: [
      {
        aDouble: {
          $ne: null,
        },
      },
      {
        $expr: {
          $eq: [
            {
              $add: ["$aDouble", 0.7],
            },
            0.1,
          ],
        },
      },
    ],
  },
  "arith-add-eq-frac-exact": {
    $and: [
      {
        aDouble: {
          $ne: null,
        },
      },
      {
        $expr: {
          $eq: [
            {
              $add: ["$aDouble", 0.5],
            },
            0.75,
          ],
        },
      },
    ],
  },
  "arith-add-ne-frac": {
    $and: [
      {
        aDouble: {
          $ne: null,
        },
      },
      {
        $expr: {
          $ne: [
            {
              $add: ["$aDouble", 0.7],
            },
            0.1,
          ],
        },
      },
    ],
  },
  "arith-both": {
    $expr: {
      $gt: [
        {
          $add: ["$aNumber", 1],
        },
        {
          $multiply: ["$aNumber", 2],
        },
      ],
    },
  },
  "arith-div": {
    $expr: {
      $eq: [
        {
          $divide: ["$aNumber", 2],
        },
        1,
      ],
    },
  },
  "arith-div-frac": {
    $expr: {
      $gte: [
        {
          $divide: ["$aNumber", 2],
        },
        1.5,
      ],
    },
  },
  "arith-mult-neg": {
    $expr: {
      $lt: [
        {
          $multiply: ["$aNumber", -2],
        },
        3,
      ],
    },
  },
  "arith-sub": {
    $expr: {
      $lte: [
        {
          $subtract: ["$aNumber", 3],
        },
        0,
      ],
    },
  },
  // Value-first: the plan reads `K < add(aNumber, K)`, and preserving the operand order under
  // `$expr` is the whole assertion. Mirroring the operator without mirroring the operands would
  // invert the comparison — this repository's canonical bug class, and the reason every `vf-*`
  // action pins the same rows as its column-first twin.
  "arith-vf": {
    $expr: {
      $lt: [
        2,
        {
          $add: ["$aNumber", 1],
        },
      ],
    },
  },
  "cast-string-bool": {
    $and: [
      {
        $expr: {
          $ne: [
            {
              $cond: {
                if: {
                  $in: [
                    {
                      $type: "$aBool",
                    },
                    ["string", "bool", "int", "long", "double", "decimal"],
                  ],
                },
                then: {
                  $convert: {
                    input: "$aBool",
                    to: "string",
                    onError: null,
                    onNull: null,
                  },
                },
                else: null,
              },
            },
            null,
          ],
        },
      },
      {
        $expr: {
          $eq: [
            {
              $cond: {
                if: {
                  $in: [
                    {
                      $type: "$aBool",
                    },
                    ["string", "bool", "int", "long", "double", "decimal"],
                  ],
                },
                then: {
                  $convert: {
                    input: "$aBool",
                    to: "string",
                    onError: null,
                    onNull: null,
                  },
                },
                else: null,
              },
            },
            "true",
          ],
        },
      },
    ],
  },
  "cast-string-double": {
    $and: [
      {
        $expr: {
          $ne: [
            {
              $cond: {
                if: {
                  $in: [
                    {
                      $type: "$aDouble",
                    },
                    ["string", "bool", "int", "long", "double", "decimal"],
                  ],
                },
                then: {
                  $convert: {
                    input: "$aDouble",
                    to: "string",
                    onError: null,
                    onNull: null,
                  },
                },
                else: null,
              },
            },
            null,
          ],
        },
      },
      {
        $and: [
          {
            aDouble: {
              $ne: null,
            },
          },
          {
            $expr: {
              $eq: [
                {
                  $cond: {
                    if: {
                      $in: [
                        {
                          $type: "$aDouble",
                        },
                        ["string", "bool", "int", "long", "double", "decimal"],
                      ],
                    },
                    then: {
                      $convert: {
                        input: "$aDouble",
                        to: "string",
                        onError: null,
                        onNull: null,
                      },
                    },
                    else: null,
                  },
                },
                "-0.6",
              ],
            },
          },
        ],
      },
    ],
  },
  // A constant receiver with a column needle: `K.contains(aString)` cannot become a regex over the
  // column, so it becomes `$indexOfCP` with the constant as the haystack. The SQL adapters have to
  // enumerate every substring of the constant here; an aggregation expression can say it directly.
  "cr-contains": {
    $expr: {
      $gte: [
        {
          $indexOfCP: ["s100Xdone-tail\\one-end", "$aString"],
        },
        0,
      ],
    },
  },
  "cr-endswith": {
    $expr: {
      $cond: {
        if: {
          $gte: [
            {
              $strLenCP: "prefix-xaXby",
            },
            {
              $strLenCP: "$aString",
            },
          ],
        },
        then: {
          $eq: [
            {
              $substrCP: [
                "prefix-xaXby",
                {
                  $subtract: [
                    {
                      $strLenCP: "prefix-xaXby",
                    },
                    {
                      $strLenCP: "$aString",
                    },
                  ],
                },
                {
                  $strLenCP: "$aString",
                },
              ],
            },
            "$aString",
          ],
        },
        else: false,
      },
    },
  },
  "cr-size-frac-ge": {
    $expr: {
      $gte: [
        {
          $cond: [
            {
              $isArray: "$tags",
            },
            {
              $size: "$tags",
            },
            {
              $strLenCP: "$tags",
            },
          ],
        },
        1.5,
      ],
    },
  },
  "cr-startswith": {
    $expr: {
      $eq: [
        {
          $indexOfCP: ["xaXby-tail", "$aString"],
        },
        0,
      ],
    },
  },
  "cr-startswith-concat": {
    $expr: {
      $eq: [
        {
          $indexOfCP: ["xaXby-tail", "$aString"],
        },
        0,
      ],
    },
  },
  // The regex form of the same question. The needle is escaped, not interpreted, and not folded.
  "cs-contains": {
    aString: {
      $regex: "one",
    },
  },
  "cs-endswith": {
    aString: {
      $regex: "one\\z",
    },
  },
  // Case sensitivity in equality: no collation, no `$options: "i"`. `cs-contains` below is the
  // separate mechanism — a regex needle, where a stray `i` flag would be just as invisible.
  "cs-eq": {
    aString: {
      $eq: "one",
    },
  },
  "cs-startswith": {
    aString: {
      $regex: "^one",
    },
  },
  "double-negation": {
    $nor: [
      {
        $nor: [
          {
            aBool: {
              $eq: true,
            },
          },
        ],
      },
    ],
  },
  "double-threshold": {
    aNumber: {
      $gte: 1.5,
    },
  },
  "empty-string-eq": {
    aString: {
      $eq: "",
    },
  },
  "exists-on-empty": {
    tags: {
      $elemMatch: {
        $and: [
          {
            name: {
              $ne: null,
            },
          },
          {
            name: {
              $eq: "public",
            },
          },
        ],
      },
    },
  },
  "f2f-contains": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        $expr: {
          $gte: [
            {
              $indexOfCP: ["$aString", "$aOptionalString"],
            },
            0,
          ],
        },
      },
    ],
  },
  "f2f-endswith": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        $expr: {
          $cond: {
            if: {
              $gte: [
                {
                  $strLenCP: "$aString",
                },
                {
                  $strLenCP: "$aOptionalString",
                },
              ],
            },
            then: {
              $eq: [
                {
                  $substrCP: [
                    "$aString",
                    {
                      $subtract: [
                        {
                          $strLenCP: "$aString",
                        },
                        {
                          $strLenCP: "$aOptionalString",
                        },
                      ],
                    },
                    {
                      $strLenCP: "$aOptionalString",
                    },
                  ],
                },
                "$aOptionalString",
              ],
            },
            else: false,
          },
        },
      },
    ],
  },
  "f2f-startswith": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        $expr: {
          $eq: [
            {
              $indexOfCP: ["$aString", "$aOptionalString"],
            },
            0,
          ],
        },
      },
    ],
  },
  "field-to-field": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        $expr: {
          $eq: ["$aString", "$aOptionalString"],
        },
      },
    ],
  },
  "gt-bare": {
    aNumber: {
      $gt: 1,
    },
  },
  "hier-ancestor-cf": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        scope: {
          $regex: "^dept\\.eng\\.",
        },
      },
    ],
  },
  "hier-ancestor-ff": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        scope: {
          $in: ["dept", "dept.eng"],
        },
      },
    ],
  },
  "hier-bracket": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        scope: {
          $regex: "^\\[env\\]:prod:",
        },
      },
    ],
  },
  "hier-descendent-cf": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        scope: {
          $in: ["dept", "dept.eng"],
        },
      },
    ],
  },
  "hier-descendent-ff": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        scope: {
          $regex: "^dept\\.eng\\.",
        },
      },
    ],
  },
  "hier-meta-in": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        scope: {
          $in: ["50%", "50%.a_b"],
        },
      },
    ],
  },
  "hier-meta-like": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        scope: {
          $regex: "^50%:a_b:",
        },
      },
    ],
  },
  "hier-overlaps-cf": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        $or: [
          {
            scope: {
              $in: ["dept", "dept.eng"],
            },
          },
          {
            scope: {
              $regex: "^dept\\.eng\\.",
            },
          },
        ],
      },
    ],
  },
  "hier-overlaps-ff": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        $or: [
          {
            scope: {
              $in: ["dept", "dept.eng"],
            },
          },
          {
            scope: {
              $regex: "^dept\\.eng\\.",
            },
          },
        ],
      },
    ],
  },
  "hier-overlaps-meta": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        $or: [
          {
            scope: {
              $in: ["50%", "50%:a_b"],
            },
          },
          {
            scope: {
              $regex: "^50%:a_b:",
            },
          },
        ],
      },
    ],
  },
  "id-concat": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        $expr: {
          $eq: [
            "$aOptionalString",
            {
              $concat: ["prefix:", "$resourceId"],
            },
          ],
        },
      },
    ],
  },
  "id-concat-vf": {
    $expr: {
      $eq: [
        "projects:f1",
        {
          $concat: ["projects:", "$resourceId"],
        },
      ],
    },
  },
  "id-eq-const": {
    resourceId: {
      $eq: "f1",
    },
  },
  "id-f2f": {
    $expr: {
      $eq: ["$aString", "$resourceId"],
    },
  },
  "id-f2f-ne": {
    $expr: {
      $ne: ["$aString", "$resourceId"],
    },
  },
  "in-null-elem-hasint": {
    tags: {
      $elemMatch: {
        $and: [
          {
            name: {
              $exists: true,
            },
          },
          {
            name: {
              $in: ["public", null],
            },
          },
        ],
      },
    },
  },
  "in-null-elem-mixed": {
    $and: [
      {
        aOptionalString: {
          $exists: true,
        },
      },
      {
        aOptionalString: {
          $in: ["x", "one_two", null],
        },
      },
    ],
  },
  "in-null-elem-neg": {
    $nor: [
      {
        $and: [
          {
            aOptionalString: {
              $exists: true,
            },
          },
          {
            aOptionalString: {
              $in: ["x", "one_two", null],
            },
          },
        ],
      },
    ],
  },
  "in-null-elem-only": {
    $and: [
      {
        aOptionalString: {
          $exists: true,
        },
      },
      {
        aOptionalString: {
          $eq: null,
        },
      },
    ],
  },
  "in-null-elem-only-neg": {
    $nor: [
      {
        $and: [
          {
            aOptionalString: {
              $exists: true,
            },
          },
          {
            aOptionalString: {
              $eq: null,
            },
          },
        ],
      },
    ],
  },
  "in-null-elem-rel": {
    tags: {
      $elemMatch: {
        $and: [
          {
            name: {
              $exists: true,
            },
          },
          {
            name: {
              $eq: null,
            },
          },
        ],
      },
    },
  },
  "in-null-elem-rel-neg": {
    $nor: [
      {
        tags: {
          $elemMatch: {
            $and: [
              {
                name: {
                  $exists: true,
                },
              },
              {
                name: {
                  $eq: null,
                },
              },
            ],
          },
        },
      },
    ],
  },
  "in-single": {
    aString: {
      $eq: "one",
    },
  },
  // Positional read of a scalar list. Mongoose is the one SQL-shaped adapter that can express it
  // ($arrayElemAt), and the bounds guard is why: `$arrayElemAt` past the end yields MISSING, which
  // compares equal to nothing but also raises nothing, so the emptiness check in front of it is
  // what turns an out-of-range index into a denial rather than a silent false.
  "index-scalar-list": {
    $and: [
      {
        $expr: {
          $cond: {
            if: {
              $isArray: "$tags.name",
            },
            then: {
              $gt: [
                {
                  $size: "$tags.name",
                },
                0,
              ],
            },
            else: false,
          },
        },
      },
      {
        $expr: {
          $eq: [
            {
              $arrayElemAt: ["$tags.name", 0],
            },
            "public",
          ],
        },
      },
    ],
  },
  "lambda-in-principal": {
    tags: {
      $elemMatch: {
        $and: [
          {
            name: {
              $ne: null,
            },
          },
          {
            name: {
              $in: ["public", "special"],
            },
          },
        ],
      },
    },
  },
  "le-bare": {
    aNumber: {
      $lte: 2,
    },
  },
  // The LIKE-metacharacter family, in this adapter's regex form. A needle is escaped for the regex
  // engine, and the trailing `\z` (not `$`) is what keeps RE2's absolute-end-of-text semantics:
  // PCRE2's `$` also matches before a final newline, so a document ending "…\\\n" would match
  // `endsWith("\\")` in Mongo and not in CEL.
  "like-backslash": {
    aString: {
      $regex: "\\\\\\z",
    },
  },
  "like-bracket": {
    aString: {
      $regex: "^\\[SEC\\]",
    },
  },
  "like-percent": {
    aString: {
      $regex: "^100%",
    },
  },
  "like-underscore": {
    aString: {
      $regex: "a_b",
    },
  },
  "macro-depth3-all": {
    categories: {
      $elemMatch: {
        subCategories: {
          $type: "array",
          $not: {
            $elemMatch: {
              $nor: [
                {
                  labels: {
                    $elemMatch: {
                      $and: [
                        {
                          name: {
                            $ne: null,
                          },
                        },
                        {
                          name: {
                            $eq: "gold",
                          },
                        },
                      ],
                    },
                  },
                },
              ],
            },
          },
        },
      },
    },
  },
  "macro-depth3-exists": {
    categories: {
      $elemMatch: {
        subCategories: {
          $elemMatch: {
            labels: {
              $elemMatch: {
                $and: [
                  {
                    name: {
                      $ne: null,
                    },
                  },
                  {
                    name: {
                      $eq: "gold",
                    },
                  },
                ],
              },
            },
          },
        },
      },
    },
  },
  "n-all-mixed-null": {
    tags: {
      $type: "array",
      $not: {
        $elemMatch: {
          $nor: [
            {
              $and: [
                {
                  name: {
                    $ne: null,
                  },
                },
                {
                  name: {
                    $ne: "x",
                  },
                },
              ],
            },
          ],
        },
      },
    },
  },
  "nary-and": {
    $and: [
      {
        aBool: {
          $eq: true,
        },
      },
      {
        aNumber: {
          $gte: 0,
        },
      },
      {
        aString: {
          $ne: "one",
        },
      },
    ],
  },
  "neg-number": {
    aNumber: {
      $lt: -1,
    },
  },
  // The De Morgan branch: `$nor` over the conjunction rather than a disjunction of the negated
  // leaves. Both are correct in two-valued logic; only the first stays correct when a leaf is
  // UNKNOWN, which is why the rewrite is not performed.
  "not-and": {
    $nor: [
      {
        $and: [
          {
            aBool: {
              $eq: true,
            },
          },
          {
            aString: {
              $ne: "one",
            },
          },
        ],
      },
    ],
  },
  "not-empty": {
    $nor: [
      {
        $expr: {
          $eq: [
            {
              $cond: [
                {
                  $isArray: "$tags",
                },
                {
                  $size: "$tags",
                },
                {
                  $strLenCP: "$tags",
                },
              ],
            },
            0,
          ],
        },
      },
    ],
  },
  "not-gt": {
    $nor: [
      {
        aNumber: {
          $gt: 1,
        },
      },
    ],
  },
  "not-lt": {
    $nor: [
      {
        aNumber: {
          $lt: 2,
        },
      },
    ],
  },
  // The explicit-null convention: `owner` maps to the same column as `aOptionalString` WITHOUT
  // `nullable`, so a stored null is a value and `== null` selects it. `null-eq-missing` below is
  // the same wire node against the nullable mapping, and the pair is what proves the mapper flag —
  // not the corpus — does the discriminating.
  "null-eq": {
    $and: [
      {
        aOptionalString: {
          $exists: true,
        },
      },
      {
        aOptionalString: {
          $eq: null,
        },
      },
    ],
  },
  // The `nullable` mapping of the same column: a stored null IS a missing attribute, so the
  // adapter emits `$exists` AND `$ne: null` AND `$eq: null` — deliberately contradictory, which is
  // the empty set check() produces when the attribute is absent (#302). `nullAttributeRepresentation`
  // below walks the other side of this boundary.
  "null-eq-missing": {
    $and: [
      {
        aOptionalString: {
          $exists: true,
        },
      },
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        aOptionalString: {
          $eq: null,
        },
      },
    ],
  },
  "null-ne": {
    $and: [
      {
        aOptionalString: {
          $exists: true,
        },
      },
      {
        aOptionalString: {
          $ne: null,
        },
      },
    ],
  },
  "null-not-eq": {
    $nor: [
      {
        $and: [
          {
            aOptionalString: {
              $exists: true,
            },
          },
          {
            aOptionalString: {
              $eq: null,
            },
          },
        ],
      },
    ],
  },
  "null-value-f2f": {
    $expr: {
      $eq: ["$aOptionalString", "$scope"],
    },
  },
  "null-value-f2f-mixed": {
    $and: [
      {
        scope: {
          $ne: null,
        },
      },
      {
        $expr: {
          $ne: ["$aOptionalString", "$scope"],
        },
      },
    ],
  },
  "null-value-ne-const": {
    aOptionalString: {
      $ne: "x",
    },
  },
  "null-value-not-eq-const": {
    $nor: [
      {
        aOptionalString: {
          $eq: "x",
        },
      },
    ],
  },
  "null-value-not-in-const": {
    $nor: [
      {
        aOptionalString: {
          $in: ["x", "one_two"],
        },
      },
    ],
  },
  "optional-ne": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        aOptionalString: {
          $ne: "x",
        },
      },
    ],
  },
  "or-eq-exists": {
    $or: [
      {
        aBool: {
          $eq: true,
        },
      },
      {
        tags: {
          $elemMatch: {
            $and: [
              {
                name: {
                  $ne: null,
                },
              },
              {
                name: {
                  $eq: "public",
                },
              },
            ],
          },
        },
      },
    ],
  },
  "or-eq-in": {
    $or: [
      {
        aBool: {
          $eq: true,
        },
      },
      {
        tags: {
          $elemMatch: {
            name: {
              $eq: "public",
            },
          },
        },
      },
    ],
  },
  "p-double-frac": {
    $expr: {
      $eq: [
        {
          $multiply: ["$aNumber", 0.1],
        },
        0.3,
      ],
    },
  },
  "p-hasintersection-map": {
    $and: [
      {
        tags: {
          $not: {
            $elemMatch: {
              name: {
                $eq: null,
              },
            },
          },
        },
      },
      {
        tags: {
          $elemMatch: {
            name: {
              $in: ["public", "héllo🚀", "100%_x"],
            },
          },
        },
      },
    ],
  },
  "p-in-null-multi": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        aOptionalString: {
          $in: ["x", "one_two"],
        },
      },
    ],
  },
  "p-in-null-single": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        aOptionalString: {
          $eq: "x",
        },
      },
    ],
  },
  "p-index": {
    $and: [
      {
        $expr: {
          $cond: {
            if: {
              $isArray: "$tags",
            },
            then: {
              $gt: [
                {
                  $size: "$tags",
                },
                0,
              ],
            },
            else: false,
          },
        },
      },
      {
        $expr: {
          $eq: [
            {
              $getField: {
                field: "name",
                input: {
                  $arrayElemAt: ["$tags", 0],
                },
              },
            },
            "public",
          ],
        },
      },
    ],
  },
  "p-matches": {
    aString: {
      $regex: "^h",
    },
  },
  "p-startswith-concat": {
    aString: {
      $regex: "^100%",
    },
  },
  "p-struct": {
    aString: {
      $eq: "one",
    },
  },
  "p-ternary-of-ternaries": {
    $expr: {
      $gt: [
        {
          $cond: {
            if: "$aBool",
            then: {
              $cond: {
                if: {
                  $eq: ["$aString", ""],
                },
                then: 0,
                else: "$aNumber",
              },
            },
            else: {
              $cond: {
                if: {
                  $lt: ["$aNumber", 0],
                },
                then: -1,
                else: 5,
              },
            },
          },
        },
        1,
      ],
    },
  },
  "p-timestamp": {
    $and: [
      {
        $expr: {
          $ne: [
            {
              $let: {
                vars: {
                  converted: {
                    $cond: {
                      if: {
                        $eq: [
                          {
                            $type: "$createdBy",
                          },
                          "date",
                        ],
                      },
                      then: "$createdBy",
                      else: {
                        $cond: {
                          if: {
                            $cond: {
                              if: {
                                $eq: [
                                  {
                                    $type: "$createdBy",
                                  },
                                  "string",
                                ],
                              },
                              then: {
                                $regexMatch: {
                                  input: "$createdBy",
                                  regex:
                                    "^((?!0000)\\d{4})-(\\d{2})-(\\d{2})[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,3})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)\\z",
                                },
                              },
                              else: false,
                            },
                          },
                          then: {
                            $convert: {
                              input: "$createdBy",
                              to: "date",
                              onError: null,
                              onNull: null,
                            },
                          },
                          else: null,
                        },
                      },
                    },
                  },
                },
                in: {
                  $cond: {
                    if: {
                      $and: [
                        {
                          $ne: ["$$converted", null],
                        },
                        {
                          $gte: [
                            "$$converted",
                            new Date("0001-01-01T00:00:00.000Z"),
                          ],
                        },
                        {
                          $lte: [
                            "$$converted",
                            new Date("9999-12-31T23:59:59.999Z"),
                          ],
                        },
                      ],
                    },
                    then: "$$converted",
                    else: null,
                  },
                },
              },
            },
            null,
          ],
        },
      },
      {
        $expr: {
          $lt: [
            {
              $let: {
                vars: {
                  converted: {
                    $cond: {
                      if: {
                        $eq: [
                          {
                            $type: "$createdBy",
                          },
                          "date",
                        ],
                      },
                      then: "$createdBy",
                      else: {
                        $cond: {
                          if: {
                            $cond: {
                              if: {
                                $eq: [
                                  {
                                    $type: "$createdBy",
                                  },
                                  "string",
                                ],
                              },
                              then: {
                                $regexMatch: {
                                  input: "$createdBy",
                                  regex:
                                    "^((?!0000)\\d{4})-(\\d{2})-(\\d{2})[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,3})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)\\z",
                                },
                              },
                              else: false,
                            },
                          },
                          then: {
                            $convert: {
                              input: "$createdBy",
                              to: "date",
                              onError: null,
                              onNull: null,
                            },
                          },
                          else: null,
                        },
                      },
                    },
                  },
                },
                in: {
                  $cond: {
                    if: {
                      $and: [
                        {
                          $ne: ["$$converted", null],
                        },
                        {
                          $gte: [
                            "$$converted",
                            new Date("0001-01-01T00:00:00.000Z"),
                          ],
                        },
                        {
                          $lte: [
                            "$$converted",
                            new Date("9999-12-31T23:59:59.999Z"),
                          ],
                        },
                      ],
                    },
                    then: "$$converted",
                    else: null,
                  },
                },
              },
            },
            new Date("2025-01-01T00:00:00.000Z"),
          ],
        },
      },
    ],
  },
  "pv-all": {
    $and: [
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "set",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "same",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "%_o",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "X",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "Y",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "MIRROR",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "filler-1",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "filler-2",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "filler-3",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "filler-4",
            },
          },
        ],
      },
    ],
  },
  "pv-all-unrolled": {
    $and: [
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $ne: "set",
            },
          },
        ],
      },
      {
        $and: [
          {
            $and: [
              {
                aOptionalString: {
                  $ne: null,
                },
              },
              {
                aOptionalString: {
                  $ne: "",
                },
              },
            ],
          },
          {
            $and: [
              {
                aOptionalString: {
                  $ne: null,
                },
              },
              {
                aOptionalString: {
                  $ne: "%_o",
                },
              },
            ],
          },
        ],
      },
    ],
  },
  "pv-exists": {
    $or: [
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "set",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "same",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "%_o",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "X",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "Y",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "MIRROR",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "filler-1",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "filler-2",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "filler-3",
            },
          },
        ],
      },
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "filler-4",
            },
          },
        ],
      },
    ],
  },
  // The BELOW-cliff unroll: at <= 10 elements the planner emits a nested `or` chain rather than a
  // macro over a literal value list, so the same policy produces a different wire shape depending
  // on how many teams the principal carries (#387). `pv-exists` is the above-cliff twin.
  "pv-exists-unrolled": {
    $or: [
      {
        $and: [
          {
            aOptionalString: {
              $ne: null,
            },
          },
          {
            aOptionalString: {
              $eq: "set",
            },
          },
        ],
      },
      {
        $or: [
          {
            $and: [
              {
                aOptionalString: {
                  $ne: null,
                },
              },
              {
                aOptionalString: {
                  $eq: "",
                },
              },
            ],
          },
          {
            $and: [
              {
                aOptionalString: {
                  $ne: null,
                },
              },
              {
                aOptionalString: {
                  $eq: "%_o",
                },
              },
            ],
          },
        ],
      },
    ],
  },
  "rel-bool-hop": {
    "parent.aBool": {
      $eq: true,
    },
  },
  "rel-bool-hop2": {
    "parent.inner.aBool": {
      $eq: true,
    },
  },
  "rel-contains-hop": {
    "parent.aString": {
      $regex: "done",
    },
  },
  "rel-eq-hop": {
    "parent.aString": {
      $eq: "One",
    },
  },
  "rel-eq-num-hop": {
    "parent.aNumber": {
      $eq: 2,
    },
  },
  "rel-ge-hop": {
    "parent.aNumber": {
      $gte: 2,
    },
  },
  "rel-gt-hop": {
    "parent.aNumber": {
      $gt: 2,
    },
  },
  "rel-hop-and-root": {
    $and: [
      {
        aBool: {
          $eq: true,
        },
      },
      {
        "parent.aString": {
          $regex: "re",
        },
      },
    ],
  },
  "rel-hop2-or-exists": {
    $or: [
      {
        "parent.inner.aBool": {
          $eq: true,
        },
      },
      {
        categories: {
          $elemMatch: {
            name: {
              $eq: "business",
            },
          },
        },
      },
    ],
  },
  "rel-le-hop": {
    "parent.aNumber": {
      $lte: 2,
    },
  },
  "rel-lt-hop": {
    "parent.aNumber": {
      $lt: 2,
    },
  },
  "rel-ne-null-hop": {
    $and: [
      {
        "parent.aOptionalString": {
          $exists: true,
        },
      },
      {
        "parent.aOptionalString": {
          $ne: null,
        },
      },
      {
        "parent.aOptionalString": {
          $ne: null,
        },
      },
    ],
  },
  // The absent-parent guard (#309/#375). A document with no `parent` subdocument has no
  // `parent.aBool` path at all, and an unguarded `$nor` matches exactly those documents — while
  // check() sees a missing attribute and denies. The `parent: {$ne: null}` conjunct outside the
  // negation is the guard; the positive hop shapes cannot discriminate it.
  "rel-not-bool-hop": {
    $and: [
      {
        parent: {
          $ne: null,
        },
      },
      {
        $nor: [
          {
            "parent.aBool": {
              $eq: true,
            },
          },
        ],
      },
    ],
  },
  "rel-range-hop": {
    $and: [
      {
        "parent.aNumber": {
          $gt: 2,
        },
      },
      {
        "parent.aNumber": {
          $lt: 12,
        },
      },
    ],
  },
  "rel-startswith-hop2": {
    "parent.inner.aString": {
      $regex: "^100",
    },
  },
  // A bare boolean column at the ROOT of the condition, with no comparison wrapped around it.
  "root-bare-bool": {
    aBool: {
      $eq: true,
    },
  },
  "root-or": {
    $or: [
      {
        aBool: {
          $eq: true,
        },
      },
      {
        aNumber: {
          $lt: 0,
        },
      },
    ],
  },
  "size-huge-gt": {
    $expr: {
      $gt: [
        {
          $cond: [
            {
              $isArray: "$aString",
            },
            {
              $size: "$aString",
            },
            {
              $strLenCP: "$aString",
            },
          ],
        },
        4294967296,
      ],
    },
  },
  "size-huge-lt": {
    $expr: {
      $lt: [
        {
          $cond: [
            {
              $isArray: "$aString",
            },
            {
              $size: "$aString",
            },
            {
              $strLenCP: "$aString",
            },
          ],
        },
        4294967296,
      ],
    },
  },
  "size-threshold": {
    $expr: {
      $gt: [
        {
          $cond: [
            {
              $isArray: "$tags",
            },
            {
              $size: "$tags",
            },
            {
              $strLenCP: "$tags",
            },
          ],
        },
        1,
      ],
    },
  },
  "string-size": {
    $expr: {
      $gt: [
        {
          $cond: [
            {
              $isArray: "$aString",
            },
            {
              $size: "$aString",
            },
            {
              $strLenCP: "$aString",
            },
          ],
        },
        4,
      ],
    },
  },
  "ternary-bare": {
    $expr: {
      $cond: {
        if: "$aBool",
        then: {
          $eq: ["$aString", "one"],
        },
        else: {
          $lt: ["$aNumber", 0],
        },
      },
    },
  },
  "ternary-cmp": {
    $expr: {
      $gt: [
        {
          $cond: {
            if: "$aBool",
            then: "$aNumber",
            else: 0,
          },
        },
        1,
      ],
    },
  },
  "ternary-expr-cond": {
    $expr: {
      $gte: [
        {
          $cond: {
            if: {
              $eq: [
                {
                  $indexOfCP: ["$aString", "100"],
                },
                0,
              ],
            },
            then: "$aNumber",
            else: -1,
          },
        },
        0,
      ],
    },
  },
  "ternary-negated": {
    $nor: [
      {
        $expr: {
          $gt: [
            {
              $cond: {
                if: "$aBool",
                then: "$aNumber",
                else: 0,
              },
            },
            1,
          ],
        },
      },
    ],
  },
  "ternary-nested": {
    $expr: {
      $gte: [
        {
          $cond: {
            if: "$aBool",
            then: {
              $cond: {
                if: {
                  $eq: ["$aString", ""],
                },
                then: 0,
                else: "$aNumber",
              },
            },
            else: -1,
          },
        },
        2,
      ],
    },
  },
  "ternary-null-cond": {
    $and: [
      {
        aOptionalString: {
          $ne: null,
        },
      },
      {
        $expr: {
          $gt: [
            {
              $cond: {
                if: {
                  $ne: ["$aOptionalString", "x"],
                },
                then: "$aNumber",
                else: 0,
              },
            },
            1,
          ],
        },
      },
    ],
  },
  "ternary-value-first": {
    $expr: {
      $lt: [
        0,
        {
          $cond: {
            if: "$aBool",
            then: "$aNumber",
            else: -1,
          },
        },
      ],
    },
  },
  "triple-negation": {
    $nor: [
      {
        $nor: [
          {
            $nor: [
              {
                aBool: {
                  $eq: true,
                },
              },
            ],
          },
        ],
      },
    ],
  },
  "ts-eq": {
    $and: [
      {
        $expr: {
          $ne: [
            {
              $let: {
                vars: {
                  converted: {
                    $cond: {
                      if: {
                        $eq: [
                          {
                            $type: "$createdAt",
                          },
                          "date",
                        ],
                      },
                      then: "$createdAt",
                      else: {
                        $cond: {
                          if: {
                            $cond: {
                              if: {
                                $eq: [
                                  {
                                    $type: "$createdAt",
                                  },
                                  "string",
                                ],
                              },
                              then: {
                                $regexMatch: {
                                  input: "$createdAt",
                                  regex:
                                    "^((?!0000)\\d{4})-(\\d{2})-(\\d{2})[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,3})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)\\z",
                                },
                              },
                              else: false,
                            },
                          },
                          then: {
                            $convert: {
                              input: "$createdAt",
                              to: "date",
                              onError: null,
                              onNull: null,
                            },
                          },
                          else: null,
                        },
                      },
                    },
                  },
                },
                in: {
                  $cond: {
                    if: {
                      $and: [
                        {
                          $ne: ["$$converted", null],
                        },
                        {
                          $gte: [
                            "$$converted",
                            new Date("0001-01-01T00:00:00.000Z"),
                          ],
                        },
                        {
                          $lte: [
                            "$$converted",
                            new Date("9999-12-31T23:59:59.999Z"),
                          ],
                        },
                      ],
                    },
                    then: "$$converted",
                    else: null,
                  },
                },
              },
            },
            null,
          ],
        },
      },
      {
        $and: [
          {
            createdAt: {
              $ne: null,
            },
          },
          {
            $expr: {
              $eq: [
                {
                  $let: {
                    vars: {
                      converted: {
                        $cond: {
                          if: {
                            $eq: [
                              {
                                $type: "$createdAt",
                              },
                              "date",
                            ],
                          },
                          then: "$createdAt",
                          else: {
                            $cond: {
                              if: {
                                $cond: {
                                  if: {
                                    $eq: [
                                      {
                                        $type: "$createdAt",
                                      },
                                      "string",
                                    ],
                                  },
                                  then: {
                                    $regexMatch: {
                                      input: "$createdAt",
                                      regex:
                                        "^((?!0000)\\d{4})-(\\d{2})-(\\d{2})[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,3})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)\\z",
                                    },
                                  },
                                  else: false,
                                },
                              },
                              then: {
                                $convert: {
                                  input: "$createdAt",
                                  to: "date",
                                  onError: null,
                                  onNull: null,
                                },
                              },
                              else: null,
                            },
                          },
                        },
                      },
                    },
                    in: {
                      $cond: {
                        if: {
                          $and: [
                            {
                              $ne: ["$$converted", null],
                            },
                            {
                              $gte: [
                                "$$converted",
                                new Date("0001-01-01T00:00:00.000Z"),
                              ],
                            },
                            {
                              $lte: [
                                "$$converted",
                                new Date("9999-12-31T23:59:59.999Z"),
                              ],
                            },
                          ],
                        },
                        then: "$$converted",
                        else: null,
                      },
                    },
                  },
                },
                new Date("2024-06-01T00:00:00.000Z"),
              ],
            },
          },
        ],
      },
    ],
  },
  "ts-eq-offset": {
    $and: [
      {
        $expr: {
          $ne: [
            {
              $let: {
                vars: {
                  converted: {
                    $cond: {
                      if: {
                        $eq: [
                          {
                            $type: "$createdAt",
                          },
                          "date",
                        ],
                      },
                      then: "$createdAt",
                      else: {
                        $cond: {
                          if: {
                            $cond: {
                              if: {
                                $eq: [
                                  {
                                    $type: "$createdAt",
                                  },
                                  "string",
                                ],
                              },
                              then: {
                                $regexMatch: {
                                  input: "$createdAt",
                                  regex:
                                    "^((?!0000)\\d{4})-(\\d{2})-(\\d{2})[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,3})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)\\z",
                                },
                              },
                              else: false,
                            },
                          },
                          then: {
                            $convert: {
                              input: "$createdAt",
                              to: "date",
                              onError: null,
                              onNull: null,
                            },
                          },
                          else: null,
                        },
                      },
                    },
                  },
                },
                in: {
                  $cond: {
                    if: {
                      $and: [
                        {
                          $ne: ["$$converted", null],
                        },
                        {
                          $gte: [
                            "$$converted",
                            new Date("0001-01-01T00:00:00.000Z"),
                          ],
                        },
                        {
                          $lte: [
                            "$$converted",
                            new Date("9999-12-31T23:59:59.999Z"),
                          ],
                        },
                      ],
                    },
                    then: "$$converted",
                    else: null,
                  },
                },
              },
            },
            null,
          ],
        },
      },
      {
        $and: [
          {
            createdAt: {
              $ne: null,
            },
          },
          {
            $expr: {
              $eq: [
                {
                  $let: {
                    vars: {
                      converted: {
                        $cond: {
                          if: {
                            $eq: [
                              {
                                $type: "$createdAt",
                              },
                              "date",
                            ],
                          },
                          then: "$createdAt",
                          else: {
                            $cond: {
                              if: {
                                $cond: {
                                  if: {
                                    $eq: [
                                      {
                                        $type: "$createdAt",
                                      },
                                      "string",
                                    ],
                                  },
                                  then: {
                                    $regexMatch: {
                                      input: "$createdAt",
                                      regex:
                                        "^((?!0000)\\d{4})-(\\d{2})-(\\d{2})[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,3})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)\\z",
                                    },
                                  },
                                  else: false,
                                },
                              },
                              then: {
                                $convert: {
                                  input: "$createdAt",
                                  to: "date",
                                  onError: null,
                                  onNull: null,
                                },
                              },
                              else: null,
                            },
                          },
                        },
                      },
                    },
                    in: {
                      $cond: {
                        if: {
                          $and: [
                            {
                              $ne: ["$$converted", null],
                            },
                            {
                              $gte: [
                                "$$converted",
                                new Date("0001-01-01T00:00:00.000Z"),
                              ],
                            },
                            {
                              $lte: [
                                "$$converted",
                                new Date("9999-12-31T23:59:59.999Z"),
                              ],
                            },
                          ],
                        },
                        then: "$$converted",
                        else: null,
                      },
                    },
                  },
                },
                new Date("2024-06-01T00:00:00.000Z"),
              ],
            },
          },
        ],
      },
    ],
  },
  "ts-ne": {
    $and: [
      {
        $expr: {
          $ne: [
            {
              $let: {
                vars: {
                  converted: {
                    $cond: {
                      if: {
                        $eq: [
                          {
                            $type: "$createdAt",
                          },
                          "date",
                        ],
                      },
                      then: "$createdAt",
                      else: {
                        $cond: {
                          if: {
                            $cond: {
                              if: {
                                $eq: [
                                  {
                                    $type: "$createdAt",
                                  },
                                  "string",
                                ],
                              },
                              then: {
                                $regexMatch: {
                                  input: "$createdAt",
                                  regex:
                                    "^((?!0000)\\d{4})-(\\d{2})-(\\d{2})[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,3})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)\\z",
                                },
                              },
                              else: false,
                            },
                          },
                          then: {
                            $convert: {
                              input: "$createdAt",
                              to: "date",
                              onError: null,
                              onNull: null,
                            },
                          },
                          else: null,
                        },
                      },
                    },
                  },
                },
                in: {
                  $cond: {
                    if: {
                      $and: [
                        {
                          $ne: ["$$converted", null],
                        },
                        {
                          $gte: [
                            "$$converted",
                            new Date("0001-01-01T00:00:00.000Z"),
                          ],
                        },
                        {
                          $lte: [
                            "$$converted",
                            new Date("9999-12-31T23:59:59.999Z"),
                          ],
                        },
                      ],
                    },
                    then: "$$converted",
                    else: null,
                  },
                },
              },
            },
            null,
          ],
        },
      },
      {
        $and: [
          {
            createdAt: {
              $ne: null,
            },
          },
          {
            $expr: {
              $ne: [
                {
                  $let: {
                    vars: {
                      converted: {
                        $cond: {
                          if: {
                            $eq: [
                              {
                                $type: "$createdAt",
                              },
                              "date",
                            ],
                          },
                          then: "$createdAt",
                          else: {
                            $cond: {
                              if: {
                                $cond: {
                                  if: {
                                    $eq: [
                                      {
                                        $type: "$createdAt",
                                      },
                                      "string",
                                    ],
                                  },
                                  then: {
                                    $regexMatch: {
                                      input: "$createdAt",
                                      regex:
                                        "^((?!0000)\\d{4})-(\\d{2})-(\\d{2})[Tt](?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,3})?(?:[Zz]|[+-](?:[01]\\d|2[0-3]):[0-5]\\d)\\z",
                                    },
                                  },
                                  else: false,
                                },
                              },
                              then: {
                                $convert: {
                                  input: "$createdAt",
                                  to: "date",
                                  onError: null,
                                  onNull: null,
                                },
                              },
                              else: null,
                            },
                          },
                        },
                      },
                    },
                    in: {
                      $cond: {
                        if: {
                          $and: [
                            {
                              $ne: ["$$converted", null],
                            },
                            {
                              $gte: [
                                "$$converted",
                                new Date("0001-01-01T00:00:00.000Z"),
                              ],
                            },
                            {
                              $lte: [
                                "$$converted",
                                new Date("9999-12-31T23:59:59.999Z"),
                              ],
                            },
                          ],
                        },
                        then: "$$converted",
                        else: null,
                      },
                    },
                  },
                },
                new Date("2024-06-01T00:00:00.000Z"),
              ],
            },
          },
        ],
      },
    ],
  },
  "unicode-eq": {
    aString: {
      $eq: "héllo🚀",
    },
  },
  "vf-ge": {
    aNumber: {
      $lte: 2,
    },
  },
  // Value-first `hasIntersection(K[], V)`: the constant list is the RECEIVER and the collection is
  // the argument. Reading the operands positionally rather than by kind is what used to make this
  // shape unsupported here.
  "vf-hasint": {
    tags: {
      $elemMatch: {
        name: {
          $in: ["public", "other"],
        },
      },
    },
  },
  "vf-le": {
    aNumber: {
      $gte: 3,
    },
  },
  "vf-lt": {
    aNumber: {
      $gt: 1,
    },
  },
  "vf-ne": {
    aString: {
      $ne: "one",
    },
  },
  "vf-null-ne": {
    $and: [
      {
        aOptionalString: {
          $exists: true,
        },
      },
      {
        aOptionalString: {
          $ne: null,
        },
      },
    ],
  },
  "vf-size": {
    $expr: {
      $lt: [
        0,
        {
          $cond: [
            {
              $isArray: "$tags",
            },
            {
              $size: "$tags",
            },
            {
              $strLenCP: "$tags",
            },
          ],
        },
      ],
    },
  },
  "w1-all-chain": {
    "categories.subCategories": {
      $type: "array",
      $not: {
        $elemMatch: {
          $nor: [
            {
              name: {
                $eq: "finance",
              },
            },
          ],
        },
      },
    },
  },
  "w1-exists-chain": {
    "categories.subCategories": {
      $elemMatch: {
        name: {
          $eq: "finance",
        },
      },
    },
  },
  "w1-in-chain": {
    "categories.subCategories": {
      $elemMatch: {
        name: {
          $eq: "finance",
        },
      },
    },
  },
  "w1-not-hasint-chain": {
    $and: [
      {
        "categories.0": {
          $exists: true,
        },
      },
      {
        $nor: [
          {
            "categories.subCategories": {
              $elemMatch: {
                name: {
                  $in: ["finance"],
                },
              },
            },
          },
        ],
      },
    ],
  },
  "w1-not-in-chain": {
    $and: [
      {
        "categories.0": {
          $exists: true,
        },
      },
      {
        $nor: [
          {
            "categories.subCategories": {
              $elemMatch: {
                name: {
                  $eq: "finance",
                },
              },
            },
          },
        ],
      },
    ],
  },
  "w1-not-size-chain": {
    $and: [
      {
        "categories.0": {
          $exists: true,
        },
      },
      {
        $nor: [
          {
            $and: [
              {
                "categories.0": {
                  $exists: true,
                },
              },
              {
                $expr: {
                  $gt: [
                    {
                      $cond: [
                        {
                          $gt: [
                            {
                              $size: {
                                $ifNull: ["$categories", []],
                              },
                            },
                            0,
                          ],
                        },
                        {
                          $cond: [
                            {
                              $isArray: "$categories.subCategories",
                            },
                            {
                              $size: "$categories.subCategories",
                            },
                            {
                              $strLenCP: "$categories.subCategories",
                            },
                          ],
                        },
                        null,
                      ],
                    },
                    0,
                  ],
                },
              },
            ],
          },
        ],
      },
    ],
  },
  "w1-size-chain": {
    $and: [
      {
        "categories.0": {
          $exists: true,
        },
      },
      {
        $expr: {
          $gt: [
            {
              $cond: [
                {
                  $gt: [
                    {
                      $size: {
                        $ifNull: ["$categories", []],
                      },
                    },
                    0,
                  ],
                },
                {
                  $cond: [
                    {
                      $isArray: "$categories.subCategories",
                    },
                    {
                      $size: "$categories.subCategories",
                    },
                    {
                      $strLenCP: "$categories.subCategories",
                    },
                  ],
                },
                null,
              ],
            },
            0,
          ],
        },
      },
    ],
  },
  "w1-size-frac-chain": {
    $and: [
      {
        "categories.0": {
          $exists: true,
        },
      },
      {
        $expr: {
          $gte: [
            {
              $cond: [
                {
                  $gt: [
                    {
                      $size: {
                        $ifNull: ["$categories", []],
                      },
                    },
                    0,
                  ],
                },
                {
                  $cond: [
                    {
                      $isArray: "$categories.subCategories",
                    },
                    {
                      $size: "$categories.subCategories",
                    },
                    {
                      $strLenCP: "$categories.subCategories",
                    },
                  ],
                },
                null,
              ],
            },
            1.5,
          ],
        },
      },
    ],
  },
  "w1-size-frac-le-chain": {
    $and: [
      {
        "categories.0": {
          $exists: true,
        },
      },
      {
        $expr: {
          $lte: [
            {
              $cond: [
                {
                  $gt: [
                    {
                      $size: {
                        $ifNull: ["$categories", []],
                      },
                    },
                    0,
                  ],
                },
                {
                  $cond: [
                    {
                      $isArray: "$categories.subCategories",
                    },
                    {
                      $size: "$categories.subCategories",
                    },
                    {
                      $strLenCP: "$categories.subCategories",
                    },
                  ],
                },
                null,
              ],
            },
            1.5,
          ],
        },
      },
    ],
  },
  // The multi-hop chain with an optional to-one parent (`requiresParent`). `size(chain) >= 0` is
  // true for every document a flattened path can see, including the ones whose parent is absent —
  // which check() denies on a missing-path error. The `categories.0: {$exists: true}` conjunct and
  // the `$cond` that yields null without it are what exclude them (#309/#316).
  "w1-size-nonneg-chain": {
    $and: [
      {
        "categories.0": {
          $exists: true,
        },
      },
      {
        $expr: {
          $gte: [
            {
              $cond: [
                {
                  $gt: [
                    {
                      $size: {
                        $ifNull: ["$categories", []],
                      },
                    },
                    0,
                  ],
                },
                {
                  $cond: [
                    {
                      $isArray: "$categories.subCategories",
                    },
                    {
                      $size: "$categories.subCategories",
                    },
                    {
                      $strLenCP: "$categories.subCategories",
                    },
                  ],
                },
                null,
              ],
            },
            0,
          ],
        },
      },
    ],
  },
  "w1-size-zero-chain": {
    $and: [
      {
        "categories.0": {
          $exists: true,
        },
      },
      {
        $expr: {
          $eq: [
            {
              $cond: [
                {
                  $gt: [
                    {
                      $size: {
                        $ifNull: ["$categories", []],
                      },
                    },
                    0,
                  ],
                },
                {
                  $cond: [
                    {
                      $isArray: "$categories.subCategories",
                    },
                    {
                      $size: "$categories.subCategories",
                    },
                    {
                      $strLenCP: "$categories.subCategories",
                    },
                  ],
                },
                null,
              ],
            },
            0,
          ],
        },
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
    "$action is refused with the message actions.json pins ($reason)",
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

    // Tripwires. Bump them deliberately: a count that moves without anyone noticing is how a
    // shape gets dropped from a table nobody reads end to end.
    expect({
      filters: filters.length,
      kinds: kinds.length,
      throwing: throwing.length,
    }).toEqual({ filters: 147, kinds: 2, throwing: 50 });
  });

  // The mapping-hazard contract in README.md rests on one structural fact: this adapter builds no
  // subquery, because a relation is a path inside the same document. `adversarial.test.ts` guards
  // it by scanning the source and every emitted filter for `$lookup`; that guard needs a live PDP
  // only for the plans, so the cheap half of it is repeated here over the fixtures.
  //
  // It walks what the translator EMITS, not the table above: a guard over the pinned literals
  // could only fail once someone had already typed `$lookup` into them, which is the wrong end of
  // the change to catch it at.
  test("no corpus action reaches a second collection", () => {
    const forbidden = /\$lookup|\$graphLookup/;
    for (const action of Object.keys(EXPECTED_FILTERS)) {
      expect([action, JSON.stringify(translate(action).filters)]).toEqual([
        action,
        expect.not.stringMatching(forbidden),
      ]);
    }
  });
});

describe("nullAttributeRepresentation", () => {
  // `null-eq-missing` is the corpus's `nullRepresentationOmitted` probe: `== null` against an
  // attribute the caller OMITS when the field is NULL. The two conventions are indistinguishable
  // on the wire — the planner emits the same `eq(attr, null)` either way — so the adapter has to
  // be told, and the whole behaviour is a translator property with no store in it.
  //
  // Mongoose expresses it twice over: per attribute with the `nullable` mapper flag (the pinned
  // filters above), and globally with this switch, which is the fail-closed backstop for a caller
  // who omits attributes without declaring `nullable` on every affected entry.
  test("explicit: the null operand is translated", () => {
    expect(
      translate("null-eq-missing", { nullAttributeRepresentation: "explicit" }),
    ).toStrictEqual(translate("null-eq-missing"));
  });

  test("omitted: the same plan is refused rather than translated", () => {
    // A NULL field sends no attribute, so check() denies on a missing-attribute error while a
    // null-selecting filter would return exactly those documents (#302).
    expect(() =>
      translate("null-eq-missing", { nullAttributeRepresentation: "omitted" }),
    ).toThrow("missing-attribute error");
  });

  // The rejection keys off the null OPERAND, not off a list of operators, so a value list carrying
  // one is refused as well. `adversarial.test.ts` proves that over every corpus action against a
  // live PDP; this is the same claim on one fixture, offline.
  test("omitted: a null element inside a value list is refused too", () => {
    expect(() =>
      translate("in-null-elem-only", {
        nullAttributeRepresentation: "omitted",
      }),
    ).toThrow("missing-attribute error");
  });

  // The negative control, and the reason the two assertions above are not vacuous: a guard that
  // rejected EVERY plan under `omitted` would satisfy both while breaking every caller who set
  // the option. The switch narrows what translates; it does not turn the adapter off.
  test("omitted: a null-free comparison is untouched", () => {
    expect(
      translate("cs-eq", { nullAttributeRepresentation: "omitted" }),
    ).toStrictEqual(translate("cs-eq"));
  });
});

describe("timestamp literals", () => {
  // `regenerate-wire-fixtures.sh` rewrites the folded `now() - duration("24h")` literal in
  // `ts-window` to a placeholder, because it differs on every capture. That makes this the one
  // fixture whose value the reader chooses — so it is also the one place the whole timestamp
  // boundary can be walked, by substituting the instant and asking what the adapter does with it.
  const at = (plannedAt: string) =>
    queryPlanToMongoose({
      queryPlan: planFromWireFixture("ts-window", plannedAt),
      mapper: MAPPER,
    });

  test("a nanosecond instant — what the PDP actually folds — is refused", () => {
    // This, and nothing else, is why `ts-window` and `ts-vf` are `adapterUnsupported`. A tidy
    // millisecond substitution in the loader would translate cleanly and quietly contradict
    // actions.json.
    expect(() => translate("ts-window")).toThrow(
      "timestamp value must be a millisecond-exact RFC 3339 instant in the CEL range",
    );
  });

  test("the same plan at millisecond precision translates", () => {
    const result = at("2026-08-11T09:13:39.123Z");
    expect(result.kind).toBe(PlanKind.CONDITIONAL);
    // The comparison operand is the instant, and the guard around the field is the one the
    // `ts-*` entries above pin: a field that is not a date and not an RFC 3339 string converts to
    // null, and null loses every comparison rather than matching one.
    expect(JSON.stringify(result.filters)).toContain(
      '"2026-08-11T09:13:39.123Z"',
    );
  });

  // Each of these is refused rather than coerced: a BSON Date built from a lenient string would
  // compare against the field as some other instant, which is a filter that returns documents the
  // PDP denies rather than an error the caller can see. A BSON Date holds milliseconds, so the
  // sub-millisecond cases are refused even when the extra digits are zeros — the value would be
  // silently truncated, and truncation is what makes `ts-window` unsupported in the first place.
  test.each([
    ["a date with no time part", "2024-01-01"],
    ["a year outside CEL's instant range", "0000-01-01T00:00:00Z"],
    ["a day that does not exist", "2024-02-30T00:00:00Z"],
    ["sub-millisecond precision", "2024-01-01T00:00:00.1234Z"],
    ["excess fractional digits, even when zero", "2026-08-11T09:13:39.123000Z"],
    [
      "an offset that pushes past the maximum instant",
      "9999-12-31T23:00:00-02:00",
    ],
  ])("%s fails closed", (_label, value) => {
    expect(() => at(value)).toThrow(
      "timestamp value must be a millisecond-exact RFC 3339 instant in the CEL range",
    );
  });
});

describe("the mapper contract", () => {
  // A mapper is caller-supplied, so these are caller shapes rather than policy shapes: no corpus
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
      filters: { aString: { $eq: "one" } },
    });
  });

  test("a function mapper resolves a relation", () => {
    expect(
      translate("rel-bool-hop", {
        mapper: (key: string) =>
          key === "request.resource.attr.parent"
            ? {
                relation: {
                  name: "parent",
                  type: "one",
                  fields: { aBool: { field: "aBool" } },
                },
              }
            : { field: key.replace("request.resource.attr.", "") },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "parent.aBool": { $eq: true } },
    });
  });

  // `valueParser` is how a caller reconciles the type Cerbos sends with the type the collection
  // stores. The corpus cannot exercise it: its mapper deliberately carries none, because the
  // harness compares document ids against `check()` and a parser that changed a value would change
  // both sides at once.
  test("a valueParser rewrites the constant of an equality", () => {
    expect(
      translate("cs-eq", {
        mapper: {
          "request.resource.attr.aString": {
            field: "aString",
            valueParser: (value: unknown) => String(value).toUpperCase(),
          },
        },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aString: { $eq: "ONE" } },
    });
  });

  test("a valueParser rewrites every element of a membership list", () => {
    expect(
      translate("p-in-null-multi", {
        mapper: {
          "request.resource.attr.aOptionalString": {
            field: "aOptionalString",
            valueParser: (value: unknown) => String(value).toUpperCase(),
          },
        },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { aOptionalString: { $in: ["X", "ONE_TWO"] } },
    });
  });

  // The reason the corpus mapper maps the primary key to a string field rather than to `_id`: an
  // ObjectId collection key is the common real deployment, three of the six `id-*` actions compare
  // the key against a string field, and one mapping cannot be both. So the coercion is pinned
  // here, against the same wire fixture, rather than left to a README example nothing runs.
  test("a valueParser coerces the primary key to an ObjectId", () => {
    expect(
      translate("id-eq-const", {
        mapper: {
          "request.resource.id": {
            field: "_id",
            valueParser: (value: unknown) =>
              new Types.ObjectId(String(value).padEnd(24, "0")),
          },
        },
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { _id: { $eq: new Types.ObjectId("f10000000000000000000000") } },
    });
  });

  test("a valueParser declared on a relation field applies through the hop", () => {
    const relation = (valueParser?: (value: unknown) => unknown): Mapper => ({
      "request.resource.attr.parent": {
        relation: {
          name: "parent",
          type: "one",
          fields: {
            aString: {
              field: "aString",
              ...(valueParser ? { valueParser } : {}),
            },
          },
        },
      },
    });

    expect(
      translate("rel-eq-hop", {
        mapper: relation((value) => String(value).toUpperCase()),
      }),
    ).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "parent.aString": { $eq: "ONE" } },
    });
    // And the same mapping without one, so the assertion above is the parser talking rather than
    // some other normalisation of the constant.
    expect(translate("rel-eq-hop", { mapper: relation() })).toStrictEqual({
      kind: PlanKind.CONDITIONAL,
      filters: { "parent.aString": { $eq: "One" } },
    });
  });
});

describe("plans the planner cannot produce", () => {
  // Input validation on a public function, not policy shapes. Every other assertion in this file
  // reads its plan from a fixture precisely because a typed plan is a belief about the planner —
  // but these are malformed by construction, so there is no fixture to read and nothing to
  // believe. They exist so a caller who hands the adapter a hand-rolled or half-decoded plan gets
  // an error rather than a filter.
  //
  // A test for a shape CEL *can* express does not belong here, whatever its plan looks like: it
  // belongs in the corpus, where all ten adapters are asked about it. The shapes the retired suite
  // pinned that way are
  // [#394](https://github.com/cerbos/query-plan-adapters/issues/394) and
  // [#396](https://github.com/cerbos/query-plan-adapters/issues/396).

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
      queryPlanToMongoose({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        mapper: {},
      }),
    ).toThrow("Invalid query plan.");
  });

  test("a condition with neither operator nor operands", () => {
    expect(() =>
      queryPlanToMongoose({ queryPlan: plan({}), mapper: {} }),
    ).toThrow("Invalid Cerbos expression structure");
  });

  test("an operator this adapter has never heard of", () => {
    expect(() =>
      queryPlanToMongoose({
        queryPlan: plan({ operator: "unsupported", operands: [] }),
        mapper: {},
      }),
    ).toThrow("Unsupported operator: unsupported");
  });

  test("a macro over a collection value that is not a list", () => {
    expect(() =>
      queryPlanToMongoose({
        queryPlan: plan({
          operator: "exists",
          operands: [
            { value: { not: "a list" } },
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
});
