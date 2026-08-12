import * as fs from "fs";
import * as path from "path";

import { describe, expect, test } from "@jest/globals";
import type { PlanExpressionOperand, PlanResourcesResponse } from "@cerbos/core";
import { eq, ne, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";
import { MySqlDialect } from "drizzle-orm/mysql-core/dialect";
import { PgDialect } from "drizzle-orm/pg-core/dialect";
import { SQLiteSyncDialect } from "drizzle-orm/sqlite-core/dialect";

import { PlanKind, queryPlanToDrizzle } from ".";
import type {
  Mapper,
  MapperEntry,
  NullAttributeRepresentation,
  QueryPlanToDrizzleResult,
} from ".";
import {
  ADAPTER,
  GOLDEN_REGENERATE_COMMAND,
  GOLDEN_STORES,
  buildMapper,
  classifyActionsForAdapter,
  planFromWireFixture,
  postgresSchema,
  readCorpusJson,
  readGoldenExpectations,
  requireMessage,
  sqliteSchema,
  wireFixtureActions,
  writeGoldenExpectations,
} from "./corpus";
import type {
  ActionsFile,
  GoldenExpectation,
  GoldenStore,
  RenderedFilter,
} from "./corpus";

/**
 * Translator unit test: for every action in the shared `../conformance/` corpus, the SQL this
 * adapter emits. Offline — no Cerbos sidecar, no database, no containers.
 *
 * A per-adapter suite used to braid four assertions into every test. Three of them are somebody
 * else's job now, and this file makes only the fourth:
 *
 * | assertion | who owns it |
 * | --- | --- |
 * | the plan the PDP produces for a policy | `conformance/wire-fixtures/`, replanned and diffed by the `Conformance Corpus` workflow |
 * | which shapes this adapter must refuse, and with what message | `conformance/actions.json` — read below, not restated |
 * | the rows a filter returns | `adversarial.test.ts`, against real SQLite and PostgreSQL with `check()` as the oracle |
 * | **the SQL this adapter emits for a plan** | **here** |
 *
 * **The plans are read, not written.** A hand-built plan is a *belief* about what the planner
 * emits, and this repository keeps golden fixtures because that belief has been wrong before: a
 * planner change used to fail fixture regeneration and silently leave every adapter's hand-written
 * plans describing a wire contract that no longer existed. See
 * [ADR 0006](../../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md).
 *
 * **The expectations are data, not literals.** The SQL this adapter is pinned to emit lives in
 * `golden/expectations.json`, a **golden expectation** file this adapter owns — never under
 * `conformance/`, where eleven workflows trigger and one adapter re-pinning one filter would
 * re-run the other ten. The file is regenerated with `npm run golden:update` and reviewed as a
 * diff, exactly like the wire fixtures it is asserted against. See
 * [ADR 0007](../../docs/adr/0007-adapters-share-data-not-code.md) and the "Golden expectations"
 * section of `conformance/README.md`.
 *
 * **What a pinned filter buys over the harness.** The harness proves the filter returns the right
 * rows *against the rows it seeds*. Two different filters can agree on all of them and disagree on
 * the row a consumer has, so a rewrite that quietly changes the emitted SQL passes there and shows
 * up here as a diff a reviewer reads. It is also the only place a `nullAttributeRepresentation`
 * boundary, a timestamp literal, a `subqueryFilter`, or MySQL — claimed by the peer range and
 * executed by nothing in this repository — can be asserted at all.
 *
 * **Adding a corpus action fails this file.** Every wire fixture must be accounted for here
 * exactly once — a golden expectation (an emitted filter or an unconditional plan kind) or a throw
 * carrying the message `actions.json` pins — and the completeness guard below is what makes a new
 * action land as a failure rather than as silence.
 */

const actionsFile = readCorpusJson("actions.json") as ActionsFile;

/**
 * The shapes `actions.json` says this adapter must refuse, each with the message it must refuse
 * them with. Identical to the classification `adversarial.test.ts` asserts against a live PDP;
 * asserting it here as well is what lets the completeness guard below be total, and it costs a
 * millisecond rather than two containers.
 *
 * A throwing action needs no golden expectation of its own: the message is already corpus data,
 * pinned once in `actions.json` and read by every adapter. Writing it into this adapter's asset
 * too would create two places to change one string with nothing to say which is authoritative.
 */
const { throwingActions: THROWING_ACTIONS } = classifyActionsForAdapter(
  actionsFile,
  ADAPTER,
);
const THROWING = new Set(THROWING_ACTIONS.map(([action]) => action));

// -- the stores, their mappers, and the dialects ---------------------------------------------

/**
 * One schema and mapper per store the adversarial harness executes, both built by the same
 * `buildMapper` the harness calls. The pinned SQL therefore describes a mapping that is actually
 * proved against rows somewhere; a second mapper written for this file could drift and leave the
 * expectations describing a mapping nothing executes.
 */
const MAPPERS: Record<GoldenStore, Record<string, MapperEntry>> = {
  sqlite: buildMapper(sqliteSchema()),
  postgresql: buildMapper(postgresSchema()),
};

const DIALECTS = {
  postgresql: new PgDialect(),
  sqlite: new SQLiteSyncDialect(),
  mysql: new MySqlDialect(),
};

/** Every dialect the README and the `drizzle-orm` peer range claim. */
const CLAIMED_DIALECTS = ["postgresql", "mysql", "sqlite"] as const;

function translate(
  store: GoldenStore,
  action: string,
  options: {
    mapper?: Mapper;
    nullAttributeRepresentation?: NullAttributeRepresentation;
    plannedAt?: string;
  } = {},
): QueryPlanToDrizzleResult {
  return queryPlanToDrizzle({
    queryPlan: planFromWireFixture(action, options.plannedAt),
    mapper: options.mapper ?? MAPPERS[store],
    ...(options.nullAttributeRepresentation
      ? { nullAttributeRepresentation: options.nullAttributeRepresentation }
      : {}),
  });
}

function filterFor(
  store: GoldenStore,
  action: string,
  options: Parameters<typeof translate>[2] = {},
): SQL {
  const result = translate(store, action, options);
  if (result.kind !== PlanKind.CONDITIONAL) {
    throw new Error(`${action} translated to ${result.kind}, not a filter`);
  }
  return result.filter;
}

/**
 * A bound parameter has to survive a JSON round trip, or the golden file records something other
 * than what the driver is handed. A `Date` is the case that matters: it serialises to a string,
 * reads back as a string, and would pin an instant the driver never sees.
 *
 * Negative zero is the one value that survives the trip only in spirit — `JSON.stringify(-0)` is
 * `"0"` — so it is normalised on both sides here and pinned separately, by the "binds a negative
 * zero" assertion below. That assertion is the format's answer to a value the asset cannot hold:
 * normalise it in the data, and name the actions it applies to in code.
 */
function jsonParams(label: string, params: unknown[]): unknown[] {
  return params.map((param, index) => {
    const type = typeof param;
    if (
      param !== null &&
      type !== "string" &&
      type !== "number" &&
      type !== "boolean"
    ) {
      throw new Error(
        `${label} binds a parameter at index ${index} that is not a JSON scalar (${type}); the golden file cannot record it faithfully`,
      );
    }
    return Object.is(param, -0) ? 0 : param;
  });
}

function render(
  dialect: keyof typeof DIALECTS,
  label: string,
  filter: SQL,
): RenderedFilter {
  const query = DIALECTS[dialect].sqlToQuery(filter);
  return {
    sql: query.sql,
    params: jsonParams(`${label} (${dialect})`, query.params),
  };
}

/** The parameters as the driver actually receives them — negative zero included. */
function rawParams(store: GoldenStore, action: string): unknown[] {
  return DIALECTS[store].sqlToQuery(filterFor(store, action)).params;
}

/** The whole translator output for one corpus action, in the shape the golden file records. */
function expectationFor(action: string): GoldenExpectation {
  const results = GOLDEN_STORES.map(
    (store) => [store, translate(store, action)] as const,
  );
  const kinds = new Set(results.map(([, result]) => result.kind));
  if (kinds.size !== 1) {
    throw new Error(
      `${action} translates to different plan kinds per store: ${[...kinds].join(", ")}`,
    );
  }
  const rendered: Partial<Record<GoldenStore, RenderedFilter>> = {};
  for (const [store, result] of results) {
    if (result.kind !== PlanKind.CONDITIONAL) {
      return { kind: result.kind };
    }
    rendered[store] = render(store, action, result.filter);
  }
  return {
    kind: PlanKind.CONDITIONAL,
    rendered: rendered as Record<GoldenStore, RenderedFilter>,
  };
}

// -- the golden expectations -------------------------------------------------------------------
//
// `npm run golden:update` rewrites the file from what the translator emits today and preserves
// every `note`. That is the same deliberate act as regenerating the wire fixtures, and the safety
// is identical: the diff is what a reviewer reads. CI never sets the variable, so a translator
// change that moves the emitted SQL fails there whatever anyone ran locally.

if (process.env["GOLDEN_UPDATE"] === "1") {
  const regenerated = new Map<string, GoldenExpectation>();
  for (const action of wireFixtureActions()) {
    // A throwing action gets no entry: its message is corpus data. Skipping it here is also what
    // keeps regeneration from papering over a misclassification — an action moved into
    // `adapterUnsupported` that this adapter still translates fails the throw suite, and one moved
    // out of it that this adapter still refuses fails regeneration itself.
    if (THROWING.has(action)) {
      continue;
    }
    regenerated.set(action, expectationFor(action));
  }
  writeGoldenExpectations(regenerated);
}

const RECORDED = readGoldenExpectations();

const RECORDED_ACTIONS = [...RECORDED.keys()];

const CONDITIONAL_ACTIONS = RECORDED_ACTIONS.filter(
  (action) => RECORDED.get(action)!.expectation.kind === PlanKind.CONDITIONAL,
);

describe("corpus shapes", () => {
  test.each(RECORDED_ACTIONS)("%s emits the golden expectation", (action) => {
    expect(expectationFor(action)).toEqual(RECORDED.get(action)!.expectation);
  });

  // The message, not just the throw: a mapper typo or an unrelated validation satisfies a bare
  // `toThrow()` just as well as the limitation the corpus documents (#326). The harness makes the
  // same assertion against a live PDP; here it costs a millisecond and covers the whole roster,
  // which is what lets the completeness guard below be total.
  test.each(THROWING_ACTIONS)(
    "%s is refused with the message actions.json pins (%s)",
    (action, _reason, message) => {
      for (const store of GOLDEN_STORES) {
        expect(() => translate(store, action)).toThrow(message);
      }
    },
  );

  // Adding a throwing action without pinning its message must fail this suite rather than
  // silently degrade the throw assertions to a bare "it threw" (#326).
  test("a throwing action with no pinned message fails classification", () => {
    expect(() => requireMessage("synthetic-entry", undefined)).toThrow(
      /pins no throw message/,
    );
    expect(() => requireMessage("synthetic-entry", "")).toThrow(
      /pins no throw message/,
    );
  });

  test("every corpus action is accounted for here exactly once", () => {
    const throwing = THROWING_ACTIONS.map(([action]) => action);
    const classified = [...RECORDED_ACTIONS, ...throwing].sort();

    // Total: a corpus action with no golden expectation and no pinned throw lands as a failure
    // rather than as silence. This is the assertion that makes the asset self-maintaining —
    // adding a hostile shape to the corpus forces someone to look at the SQL this adapter emits
    // for it, and `npm run golden:update` refuses to invent one for a shape that throws.
    expect(classified).toEqual(wireFixtureActions());
    // Disjoint: an action carrying a golden expectation AND declared unsupported would satisfy
    // the union above while asserting two contradictory things.
    expect(classified).toEqual([...new Set(classified)].sort());
    // The asset is written sorted, so a translator change reads as the list of shapes it moved.
    expect(RECORDED_ACTIONS).toEqual([...RECORDED_ACTIONS].sort());

    // Tripwires. Bump them deliberately: a count that moves without anyone noticing is how a
    // shape gets dropped from an asset nobody reads end to end.
    expect({
      conditional: CONDITIONAL_ACTIONS.length,
      unconditional: RECORDED_ACTIONS.length - CONDITIONAL_ACTIONS.length,
      throwing: throwing.length,
    }).toEqual({ conditional: 177, unconditional: 2, throwing: 20 });
  });

  /**
   * The two stores share one translation and differ only in what the column types tell Drizzle to
   * bind. The one difference that exists is the boolean: PostgreSQL has a real `boolean`, SQLite
   * stores 1/0. Stating that as a rule rather than as a list of actions means a NEW divergence —
   * a timestamp bound as a `Date` on one store and a string on the other, say — fails here instead
   * of arriving as an unexplained diff in the asset.
   */
  test("the stores differ only in how a boolean binds", () => {
    const unexplained: { action: string; index: number }[] = [];
    let booleanDifferences = 0;

    for (const action of CONDITIONAL_ACTIONS) {
      const expectation = RECORDED.get(action)!.expectation;
      if (expectation.kind !== PlanKind.CONDITIONAL) continue;
      const { sqlite, postgresql } = expectation.rendered;
      expect({ action, length: sqlite.params.length }).toEqual({
        action,
        length: postgresql.params.length,
      });
      postgresql.params.forEach((expected, index) => {
        const actual = sqlite.params[index];
        if (Object.is(actual, expected)) return;
        if (typeof expected === "boolean" && actual === Number(expected)) {
          booleanDifferences += 1;
          return;
        }
        unexplained.push({ action, index });
      });
    }

    expect(unexplained).toEqual([]);
    // Anti-vacuity: the rule above is satisfied by two stores that never differ at all, which
    // would mean the SQLite mapper had quietly stopped using a boolean column.
    expect(booleanDifferences).toBeGreaterThan(0);
  });

  /**
   * JSON has no negative zero, so `golden/expectations.json` records `0` where the adapter binds
   * `-0`. The distinction is real — CEL's `x / -0.0` is `-Infinity` where `x / 0.0` is `+Infinity`
   * — and the guarded CASE arms the adapter emits are what keep either from reaching the database.
   * Pinning the list here is what stops the normalisation from hiding a change.
   */
  test("the actions that bind a negative zero, which the asset cannot record", () => {
    const negativeZero = CONDITIONAL_ACTIONS.filter((action) =>
      GOLDEN_STORES.some((store) =>
        rawParams(store, action).some((param) => Object.is(param, -0)),
      ),
    );

    expect(negativeZero).toEqual(["cr-div-neg-zero"]);
  });
});

// -- the dialects this adapter claims but nothing executes -------------------------------------

describe("rendering across the claimed dialects", () => {
  /**
   * MySQL is claimed by the README and by the `drizzle-orm` peer range, and no MySQL server runs
   * anywhere in this repository. Rendering the whole corpus through `MySqlDialect` is the only
   * coverage it has. It proves the adapter emits no dialect-exclusive construct — not that MySQL
   * evaluates the result the way CEL does.
   *
   * These are rules rather than pinned bytes on purpose. A rule holds for a corpus action nobody
   * has added yet; a third pinned rendering would triple the asset for a dialect no oracle
   * compares, and a reviewer would read the same change three times.
   */
  const mysqlRendered = (action: string): string =>
    render("mysql", action, filterFor("postgresql", action)).sql;

  test.each(CONDITIONAL_ACTIONS)("%s uses no SQLite-only string function", (action) => {
    // instr() exists on SQLite and MySQL but not PostgreSQL; replace/substr/length are common to
    // all three. PostgreSQL's own evaluation of the string operators is proved by the corpus's
    // cr-contains, cs-*, f2f-* and hier-* actions on the executed PostgreSQL leg.
    for (const dialect of CLAIMED_DIALECTS) {
      const rendered = render(dialect, action, filterFor("postgresql", action));
      expect({ dialect, usesInstr: rendered.sql.includes("instr(") }).toEqual({
        dialect,
        usesInstr: false,
      });
    }
  });

  test.each(CONDITIONAL_ACTIONS)(
    "%s casts to 53-bit floating point, never to single precision",
    (action) => {
      // `real` is 4 bytes on PostgreSQL: it would round a CEL double on the way through a
      // division. Executing the corpus cannot pin this — whether a seed's value survives single
      // precision is an accident of the seeds, not of the translation.
      const rendered = mysqlRendered(action);
      expect({
        action,
        usesSinglePrecision: rendered.includes(" as real"),
      }).toEqual({ action, usesSinglePrecision: false });
    },
  );

  /**
   * `size(filter(...))` is a COUNT, and `sum(case when p then 1 else 0 end)` is how it is
   * counted — the only place an integer CASE arm is the right answer. Everywhere else an integer
   * arm is a tri-state expression rendered as a number: PostgreSQL rejects `case ... then 1 end`
   * where a boolean is required, so the executed PostgreSQL leg catches it, but MySQL accepts the
   * integer form silently and returns the wrong rows.
   */
  const COUNTING_ACTIONS = ["size-filter-count"];

  test.each(CONDITIONAL_ACTIONS)(
    "%s yields booleans from tri-state arms, not integers",
    (action) => {
      const rendered = mysqlRendered(action);
      expect({
        action,
        usesIntegerArms: /\bthen [01]\b|\belse [01]\b/.test(rendered),
      }).toEqual({
        action,
        usesIntegerArms: COUNTING_ACTIONS.includes(action),
      });
    },
  );

  /**
   * A fold that collapses a NULL column to FALSE excludes the row under BOTH polarities, so the
   * row the PDP allows never comes back. The adapter's guard is a `... is null then null` arm, and
   * a regeneration that quietly dropped it would still produce a valid-looking asset — which is
   * why this is a rule rather than something left to the pinned bytes.
   */
  test.each(CONDITIONAL_ACTIONS)(
    "%s never collapses a NULL operand to false",
    (action) => {
      expect({
        action,
        collapsesNullToFalse: mysqlRendered(action).includes(
          "is null then false",
        ),
      }).toEqual({ action, collapsesNullToFalse: false });
    },
  );

  // Anti-vacuity: every rule above is satisfied by an empty string, so a corpus that stopped
  // producing the constructs they police would leave them passing and proving nothing.
  test("the corpus actually exercises the constructs those rules police", () => {
    const all = CONDITIONAL_ACTIONS.map(mysqlRendered).join("\n");
    expect({
      substringMatching: all.includes("substr("),
      doubleCast: all.includes("as float(53)"),
      booleanArms: all.includes("then true") && all.includes("else false"),
      nullPreservingFold: all.includes("is null then null"),
      countingAggregate: all.includes("sum("),
    }).toEqual({
      substringMatching: true,
      doubleCast: true,
      booleanArms: true,
      nullPreservingFold: true,
      countingAggregate: true,
    });
  });
});

describe("what the driver is asked to bind", () => {
  // PostgreSQL parses 'NaN' and 'Infinity' as double precision inputs and every comparison against
  // them is false — the same rows a folded translation returns. So an executed leg agrees either
  // way, and only the parameter list distinguishes them. The corpus drives the folds through
  // nan-ord-inf, nan-ord-le, nan-ord-ternary and nan-ord-ternary-vf.
  test.each(GOLDEN_STORES)(
    "no corpus action binds a non-finite number (%s)",
    (store) => {
      const offenders = CONDITIONAL_ACTIONS.filter((action) =>
        render(store, action, filterFor(store, action)).params.some(
          (param) => typeof param === "number" && !Number.isFinite(param),
        ),
      );
      expect(offenders).toEqual([]);
    },
  );

  // Anti-vacuity: the rule above is satisfied by a corpus with no non-finite arithmetic in it at
  // all. These are the actions that produce one — four ternaries the planner folds to constant
  // arms, and the divisions that reach a nullable column — and every one of them has to still be
  // translated here for the rule to be saying anything.
  test("the corpus still drives the folds the rule polices", () => {
    for (const action of [
      "nan-ord-inf",
      "nan-ord-le",
      "nan-ord-ternary",
      "nan-ord-ternary-vf",
      "cr-div-zero",
      "cr-div-neg-zero",
    ]) {
      expect(CONDITIONAL_ACTIONS).toContain(action);
    }
  });
});

// -- the mapper contract, which no policy can reach ---------------------------------------------

describe("mapper forms", () => {
  // A deep relation shape, so the equivalence covers references resolved through several hops
  // rather than one lookup off the root.
  const DEEP_ACTION = "macro-depth3-exists";

  test("a function mapper resolves the same references as a record mapper", () => {
    const record = MAPPERS.postgresql;
    const asFunction: Mapper = (reference) => record[reference];

    expect(
      render(
        "postgresql",
        `${DEEP_ACTION} (function mapper)`,
        filterFor("postgresql", DEEP_ACTION, { mapper: asFunction }),
      ),
    ).toEqual(
      render(
        "postgresql",
        DEEP_ACTION,
        filterFor("postgresql", DEEP_ACTION),
      ),
    );
  });

  test("a transform replaces the comparison the adapter would have built", () => {
    const schema = postgresSchema();
    const lowered: Mapper = {
      ...MAPPERS.postgresql,
      "request.resource.attr.aString": {
        column: schema.resources.aString,
        transform: ({ value }) =>
          eq(
            sql`lower(${schema.resources.aString})`,
            String(value).toLowerCase(),
          ),
      },
    };

    const rendered = render(
      "postgresql",
      "cs-eq (transform)",
      filterFor("postgresql", "cs-eq", { mapper: lowered }),
    );
    const pinned = RECORDED.get("cs-eq")!.expectation;
    expect(pinned.kind).toEqual(PlanKind.CONDITIONAL);

    // The transform owns the whole comparison, not just the column: the SQL is not the pinned one,
    // and the value it binds is the one the transform produced rather than the plan's literal.
    expect(rendered.sql).toContain("lower(");
    expect(rendered.params).toEqual(
      (pinned as { rendered: Record<GoldenStore, RenderedFilter> }).rendered
        .postgresql.params.map((param) => String(param).toLowerCase()),
    );
  });

  test("an unmapped reference is refused rather than dropped", () => {
    // Dropping it would emit a filter that answers a different question from the policy.
    expect(() =>
      translate("postgresql", "cs-eq", { mapper: {} }),
    ).toThrow(/No mapping/);
  });
});

/**
 * The one mapping hazard the corpus cannot express with a policy action, because the policy is
 * irrelevant to it: the rows the adapter's subquery sees must equal the rows the application put
 * into the resource attributes. The adapter reads the relation table directly, so a soft-delete
 * flag, tenant column or subtype discriminator the application applies to its own reads does NOT
 * reach the generated EXISTS unless the caller declares it. See "Mapping hazards" in
 * conformance/README.md and cerbos/query-plan-adapters#314.
 */
describe("relation subqueryFilter", () => {
  const schema = postgresSchema();
  /** A predicate on a column nothing else in these two translations mentions. */
  const VISIBLE_ONLY = ne(schema.tags.tagId, "hidden");
  const DECLARATION = '"tag_id" <> ';

  const mapperFor = (subqueryFilter?: SQL): Mapper => ({
    "request.resource.attr.tags": {
      relation: {
        type: "many",
        table: schema.tags,
        sourceColumn: schema.resources.id,
        targetColumn: schema.tags.resourceId,
        field: schema.tags.name,
        fields: { name: schema.tags.name, id: schema.tags.tagId },
        ...(subqueryFilter ? { subqueryFilter } : {}),
      },
    },
  });

  const sqlFor = (action: string, subqueryFilter?: SQL): string =>
    render(
      "postgresql",
      `${action} (subqueryFilter)`,
      filterFor("postgresql", action, { mapper: mapperFor(subqueryFilter) }),
    ).sql;

  const occurrences = (haystack: string, needle: string): number =>
    haystack.split(needle).length - 1;

  test("declared: exists() examines only the records the application serialised", () => {
    // Two correlated subqueries — the witness and the UNKNOWN probe — and the declaration has to
    // narrow both, or the shape the application hides still decides the row.
    const declared = sqlFor("exists-on-empty", VISIBLE_ONLY);
    expect(occurrences(declared, "exists (select 1")).toEqual(2);
    expect(occurrences(declared, DECLARATION)).toEqual(2);
  });

  test("declared: all() narrows the records examined, not the records required", () => {
    // all() compiles to "no record falsifies it", so a declaration applied around the subquery
    // instead of inside it would leave a hidden record denying the row.
    const declared = sqlFor("all-on-empty", VISIBLE_ONLY);
    expect(occurrences(declared, "exists (select 1")).toEqual(2);
    expect(occurrences(declared, DECLARATION)).toEqual(2);
  });

  test("declared: a count sees only the records the application serialised", () => {
    expect(sqlFor("size-threshold", VISIBLE_ONLY)).toContain(DECLARATION);
  });

  test("undeclared: the emitted SQL is byte-identical to before the field existed", () => {
    // The non-breaking guarantee. Silence must not add a clause, and must not warn.
    expect(sqlFor("exists-on-empty", undefined)).toEqual(
      sqlFor("exists-on-empty"),
    );
    expect(sqlFor("exists-on-empty")).not.toContain(DECLARATION);
  });
});

describe("nullAttributeRepresentation", () => {
  // `null-eq-missing` is the corpus's `nullRepresentationOmitted` probe: `== null` against an
  // attribute the caller OMITS when the column is NULL. The two conventions are indistinguishable
  // on the wire — the planner emits the same `eq(attr, null)` either way — so the adapter has to
  // be told, and the whole behaviour is a translator property with no store in it.
  const OMITTED_MESSAGE = requireMessage(
    "nullRepresentationOmitted.null-eq-missing.messages.drizzle",
    actionsFile.nullRepresentationOmitted.find(
      (entry) => entry.action === "null-eq-missing",
    )?.messages?.[ADAPTER],
  );

  test("explicit: a null operand becomes an IS NULL filter", () => {
    expect(
      render(
        "postgresql",
        "null-eq-missing (explicit)",
        filterFor("postgresql", "null-eq-missing", {
          nullAttributeRepresentation: "explicit",
        }),
      ).sql,
    ).toContain('"a_optional_string" is null');
  });

  test("omitted: the same plan is refused rather than translated", () => {
    // A NULL column sends no attribute, so check() denies on a missing-attribute error while the
    // filter above would return exactly those rows (#302).
    expect(() =>
      translate("postgresql", "null-eq-missing", {
        nullAttributeRepresentation: "omitted",
      }),
    ).toThrow(OMITTED_MESSAGE);
  });

  // #308. The per-attribute declaration overrides the call-level option in both directions, which
  // is the whole point: one call, two conventions. `owner` declares "explicit" in the shared
  // mapper, so `null-eq` — which probes it — must translate even under a call-level "omitted".
  test("a per-attribute declaration overrides the call-level option", () => {
    expect(
      render(
        "postgresql",
        "null-eq (per-attribute explicit)",
        filterFor("postgresql", "null-eq", {
          nullAttributeRepresentation: "omitted",
        }),
      ),
    ).toEqual(
      render(
        "postgresql",
        "null-eq (default)",
        filterFor("postgresql", "null-eq"),
      ),
    );

    // Strip the declaration and the same action under the same option is rejected, so the
    // override above is doing work rather than being quietly equivalent to the default.
    const undeclared: Mapper = {
      ...MAPPERS.postgresql,
      "request.resource.attr.owner": postgresSchema().resources.aOptionalString,
    };
    expect(() =>
      translate("postgresql", "null-eq", {
        mapper: undeclared,
        nullAttributeRepresentation: "omitted",
      }),
    ).toThrow(OMITTED_MESSAGE);
  });

  // The equality family only. An ordering comparison against a null receiver is a no-overload
  // error in CEL, which denies under both polarities — exactly what UNKNOWN already does — so the
  // orderings must keep propagating it rather than being made definite.
  test("an ordering comparison keeps propagating UNKNOWN", () => {
    expect(
      render(
        "postgresql",
        "vf-le",
        filterFor("postgresql", "vf-le"),
      ).sql,
    ).not.toContain("is null");
  });
});

describe("timestamp literals", () => {
  // `regenerate-wire-fixtures.sh` rewrites the folded `now() - duration("24h")` literal in
  // `ts-window` to a placeholder, because it differs on every capture. That makes this the one
  // fixture whose value the reader chooses — so it is also the one place the whole timestamp
  // boundary can be walked, by substituting the instant and asking what the adapter does with it.
  const at = (plannedAt: string) =>
    filterFor("postgresql", "ts-window", { plannedAt });

  test("a nanosecond instant — what the PDP actually folds — is refused", () => {
    // This, and nothing else, is why `ts-window` and `ts-vf` are `adapterUnsupported`. A tidy
    // millisecond substitution in the loader would translate cleanly and quietly contradict
    // actions.json.
    expect(() => translate("postgresql", "ts-window")).toThrow(
      "Timestamp value exceeds millisecond precision",
    );
  });

  test("the same plan at millisecond precision translates", () => {
    expect(
      render("postgresql", "ts-window (ms)", at("2026-08-11T09:13:39.123Z"))
        .params,
    ).toEqual(["2026-08-11T09:13:39.123Z"]);
  });

  test("excess fractional digits are accepted only when they are zero", () => {
    expect(
      render(
        "postgresql",
        "ts-window (padded ms)",
        at("2026-08-11T09:13:39.123000Z"),
      ).params,
    ).toEqual(["2026-08-11T09:13:39.123Z"]);
  });

  // Each of these is refused rather than coerced: a Date parsed from a lenient string would
  // compare against the column as some other instant, which is a filter that returns rows the PDP
  // denies rather than an error the caller can see.
  test.each([
    ["a date with no time part", "2024-01-01"],
    ["a year outside CEL's instant range", "0000-01-01T00:00:00Z"],
    ["a day that does not exist", "2024-02-30T00:00:00Z"],
    ["sub-millisecond precision", "2024-01-01T00:00:00.1234Z"],
    ["an offset that pushes past the maximum instant", "9999-12-31T23:00:00-02:00"],
  ])("%s fails closed", (_label, value) => {
    expect(() => at(value)).toThrow(/RFC-3339|millisecond|instant range/);
  });
});

describe("plans the planner cannot produce", () => {
  // Input validation on a public function, not policy shapes. Every other assertion in this file
  // reads its plan from a fixture precisely because a typed plan is a belief about the planner —
  // but these are malformed by construction, so there is no fixture to read and nothing to
  // believe. They exist so a caller who hands the adapter a hand-rolled or half-decoded plan gets
  // an error rather than a filter.
  //
  // A shape CEL *can* express does not belong here, whatever its plan looks like: it belongs in
  // the corpus, where all ten adapters are asked about it.

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
      queryPlanToDrizzle({
        queryPlan: { kind: "INVALID_KIND" } as unknown as PlanResourcesResponse,
        mapper: MAPPERS.postgresql,
      }),
    ).toThrow(/Invalid plan kind/);
  });

  test("an operator this adapter has never heard of", () => {
    expect(() =>
      queryPlanToDrizzle({
        queryPlan: plan({ operator: "unsupported", operands: [] }),
        mapper: MAPPERS.postgresql,
      }),
    ).toThrow(/Unsupported operator: unsupported/);
  });

  test("a collection macro over a value that is not a list", () => {
    expect(() =>
      queryPlanToDrizzle({
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
        mapper: MAPPERS.postgresql,
      }),
    ).toThrow("'exists' over a literal collection requires a list value");
  });

  test("a lambda reading a path no element carries", () => {
    expect(() =>
      queryPlanToDrizzle({
        queryPlan: plan({
          operator: "exists",
          operands: [
            { value: [{ name: "alpha" }] },
            {
              operator: "lambda",
              operands: [
                {
                  operator: "eq",
                  operands: [
                    { name: "request.resource.attr.aString" },
                    { name: "t.missing" },
                  ],
                },
                { name: "t" },
              ],
            },
          ],
        }),
        mapper: MAPPERS.postgresql,
      }),
    ).toThrow('Cannot resolve "t.missing"');
  });
});

describe("the golden asset", () => {
  // The asset carries the command that rewrites it, so a reader who opens the file after a failing
  // assertion is told how to look at the difference. That is only useful while the command exists.
  test("names a command this package actually defines", () => {
    const manifest = JSON.parse(
      fs.readFileSync(path.join(__dirname, "..", "package.json"), "utf8"),
    ) as { scripts: Record<string, string> };
    const [runner, run, script] = GOLDEN_REGENERATE_COMMAND.split(" ");

    expect({ runner, run }).toEqual({ runner: "npm", run: "run" });
    expect(Object.keys(manifest.scripts)).toContain(script);
  });
});
