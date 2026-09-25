import * as fs from "fs";
import * as path from "path";

import {
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";
import type {
  PlanExpressionOperand,
  PlanResourcesResponse,
  Value,
} from "@cerbos/core";

import { PlanKind, MapperConfig } from ".";

/**
 * What both of this adapter's suites read from the shared `../conformance/` corpus: the recorded
 * golden plans and the one mapper every case is translated through. `adversarial.test.ts` replays
 * the goldens against a real store; `translator.test.ts` uses the same mapper for the caller-option
 * tests the corpus cannot vary.
 *
 * Duplicated across adapters on purpose — adapters share data, not code (ADR 0007).
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

export function readCorpusJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

/** The Prisma model the corpus's resource kind maps onto. */
export const MODEL = "AdversarialResource";

// -- the golden files ----------------------------------------------------------------------------

interface WireOperand {
  expression?: { operator: string; operands: WireOperand[] };
  variable?: string;
  value?: unknown;
}

export interface Golden {
  id: string;
  pdp: string;
  tier: "core" | "extended" | "adversarial";
  plan: { kind: string; condition?: WireOperand };
  allowed: string[];
  plannerDivergence: { pdp?: string[]; issue: string; reason?: string } | null;
}

/** The PDP tags the goldens were recorded against: current first, then previous. */
export function pdpTags(): string[] {
  const versions = readCorpusJson("pdp-versions.json") as Record<
    "current" | "previous",
    { tag: string }
  >;
  return [versions.current.tag, versions.previous.tag];
}

/** Every golden file recorded against `tag`, sorted by case id. */
export function readGoldens(tag: string): Golden[] {
  const root = path.join(CONFORMANCE_DIR, "golden", tag);
  const files = (fs.readdirSync(root, { recursive: true }) as string[])
    .filter((file) => file.endsWith(".json"))
    .map((file) => JSON.parse(fs.readFileSync(path.join(root, file), "utf8")) as Golden);
  return files.sort((a, b) => a.id.localeCompare(b.id));
}

export function readGolden(tag: string, id: string): Golden {
  return JSON.parse(
    fs.readFileSync(path.join(CONFORMANCE_DIR, "golden", tag, `${id}.json`), "utf8")
  ) as Golden;
}

/**
 * The instant substituted for `__NOW_MINUS_24H__`, the literal the planner folds
 * `now() - duration("24h")` into. The PDP folds its clock at nanosecond precision, so the
 * substitute carries sub-millisecond digits too: a tidy millisecond instant would translate here
 * while the same case refuses in production (`Timestamp value exceeds millisecond precision`).
 */
export function nowMinus24h(): string {
  const ms = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  return `${ms.slice(0, -1)}456789Z`;
}

function operandFromWire(node: WireOperand, now: string): PlanExpressionOperand {
  if (node.expression) {
    return new PlanExpression(
      node.expression.operator,
      node.expression.operands.map((child) => operandFromWire(child, now))
    );
  }
  if (node.variable !== undefined) {
    return new PlanExpressionVariable(node.variable);
  }
  // The golden is JSON the PDP produced, so its leaves are already the shapes `Value` admits.
  return new PlanExpressionValue(
    (node.value === "__NOW_MINUS_24H__" ? now : node.value) as Value
  );
}

/** A golden's plan decoded the way `@cerbos/http` decodes a PlanResources response. */
export function planOf(
  golden: Golden,
  now: string = nowMinus24h()
): PlanResourcesResponse {
  const base = { cerbosCallId: "", requestId: "", validationErrors: [], metadata: undefined };
  const { kind, condition } = golden.plan;
  if (kind === PlanKind.CONDITIONAL && condition) {
    return { ...base, kind: PlanKind.CONDITIONAL, condition: operandFromWire(condition, now) };
  }
  if (kind === PlanKind.ALWAYS_ALLOWED || kind === PlanKind.ALWAYS_DENIED) {
    return { ...base, kind };
  }
  throw new Error(`${golden.id}: unrecognised plan ${JSON.stringify(golden.plan)}`);
}

// -- the mapper ----------------------------------------------------------------------------------

export const MAPPER: Record<string, MapperConfig> = {
  // The primary key, reached as `request.resource.id` rather than through `attr` (the
  // `identifier/*` cases). It is a mapping like any other here, which is the point: an adapter that resolves
  // references by stripping a `request.resource.attr.` prefix never sees this name.
  "request.resource.id": { field: "id", valueType: "string" },
  // resources.json OMITS aBool, aString and aNumber on the row where the column is NULL (j3, j1,
  // j2), so each follows the omitted convention like aOptionalString below.
  "request.resource.attr.aBool": {
    field: "aBool",
    valueType: "boolean",
    nullable: true,
    nullAttributeRepresentation: "omitted",
  },
  "request.resource.attr.aString": {
    field: "aString",
    valueType: "string",
    nullable: true,
    nullAttributeRepresentation: "omitted",
  },
  "request.resource.attr.aNumber": {
    field: "aNumber",
    valueType: "number",
    nullable: true,
    nullAttributeRepresentation: "omitted",
  },
  "request.resource.attr.aDouble": { field: "aDouble", valueType: "number" },
  // resources.json OMITS aOptionalString when the column is NULL, so `== null` against it is a
  // missing-attribute error the PDP denies, never an IS NULL match.
  "request.resource.attr.aOptionalString": {
    field: "aOptionalString",
    valueType: "string",
    nullable: true,
    nullAttributeRepresentation: "omitted",
  },
  "request.resource.attr.createdBy": { field: "createdBy", valueType: "string" },
  "request.resource.attr.scope": { field: "scope", valueType: "string", nullable: true },
  "request.resource.attr.createdAt": {
    field: "createdAt",
    valueType: "dateTime",
  },
  "request.resource.attr.updatedAt": {
    field: "updatedAt",
    valueType: "dateTime",
  },
  // `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under the
  // OTHER null convention: the oracle sends a real null attribute for them rather than omitting
  // it. Declaring that here is what makes the equality family definite for these two attributes
  // and leaves it untouched for every other mapping (cerbos/query-plan-adapters#308).
  "request.resource.attr.owner": {
    field: "aOptionalString",
    valueType: "string",
    nullable: true,
    nullAttributeRepresentation: "explicit",
  },
  "request.resource.attr.coOwner": {
    field: "scope",
    valueType: "string",
    nullable: true,
    nullAttributeRepresentation: "explicit",
  },
  // obj.inner is not a real nested column — mirrors aString, same trick the spring-data reference
  // harness uses for the comparison/equals/nested-map-member case. `parent.inner` below is the
  // opposite: a real two-level join. The two are kept side by side on purpose.
  "request.resource.attr.obj.inner": {
    field: "aString",
    valueType: "string",
    nullable: true,
    nullAttributeRepresentation: "omitted",
  },
  // The corpus's one REAL to-one chain (the `relation/*` cases). `type: "one"` is what makes the
  // adapter emit `is:` rather than `some:`, and `is:` on an optional relation is what requires
  // the hop to exist — the absent-parent guard the negated shapes discriminate. `inner` nests
  // the same declaration one level further out; two levels is where alias scoping breaks.
  "request.resource.attr.parent": {
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
