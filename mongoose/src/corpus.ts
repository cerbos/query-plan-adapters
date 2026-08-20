import * as fs from "node:fs";
import * as path from "node:path";

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

import { PlanKind } from ".";
import type { Mapper, MapperConfig } from ".";

/**
 * The parts of the shared `../conformance/` corpus that both of this adapter's suites read.
 *
 * `adversarial.test.ts` plans against a real PDP and executes the translated query;
 * `translator.test.ts` reads the same actions off the golden wire fixtures and asserts nothing
 * but the emitted filter. They must agree on the mapper or they prove less than they appear to:
 *
 * - **the mapper.** The unit test pins the filter this adapter emits for a mapping; the harness
 *   proves that same filter returns the rows the PDP allows. Two copies that drifted would leave
 *   the pinned filters describing a mapping no harness ever executes.
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

const WIRE_FIXTURES_DIR = path.join(CONFORMANCE_DIR, "wire-fixtures");

export function readJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

// -- corpus JSON validators ----------------------------------------------------------------------
//
// Shared with the harness, which parses seeds.json and derived-fields.json with the same
// primitives. A corpus file is machine-generated and diffed by validate-corpus.sh, so these are
// not there to catch a malformed file: they are what stops a field this repository renamed from
// arriving as `undefined` and being read as "absent" by a suite that would then assert nothing.

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function expectRecord(
  value: unknown,
  label: string,
): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value;
}

export function expectString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new Error(`${label} must be a string`);
  }
  return value;
}

export function expectBoolean(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`${label} must be a boolean`);
  }
  return value;
}

export function expectNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${label} must be a finite number`);
  }
  return value;
}

export function expectStringArray(value: unknown, label: string): string[] {
  if (
    !Array.isArray(value) ||
    !value.every((entry): entry is string => typeof entry === "string")
  ) {
    throw new Error(`${label} must be an array of strings`);
  }
  return value;
}

export function isValue(value: unknown): value is Value {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    return value.every(isValue);
  }
  return isRecord(value) && Object.values(value).every(isValue);
}

export function assertKeys(
  label: string,
  got: string[],
  want: readonly string[],
  optional: readonly string[] = [],
): void {
  const allowed = new Set<string>([...want, ...optional]);
  for (const key of got) {
    if (!allowed.has(key)) {
      throw new Error(
        `${label} carries "${key}", which this harness does not consume: an unconsumed corpus field is dropped from the stored document and the check() oracle at once`,
      );
    }
  }
  const present = new Set(got);
  for (const key of want) {
    if (!present.has(key)) {
      throw new Error(
        `${label} is missing "${key}", which this harness consumes`,
      );
    }
  }
}

// -- the golden wire fixtures --------------------------------------------------------------------

/**
 * The instant `regenerate-wire-fixtures.sh` substitutes for the one operand it cannot pin.
 *
 * `ts-window` and `ts-vf` compare against `now() - duration("24h")`, which the planner folds to a
 * literal timestamp: a different value on every capture, so the script rewrites it to
 * `__NOW_MINUS_24H__` to keep the drift check deterministic. Reading the fixture back therefore
 * means choosing a value, and the choice is load-bearing rather than arbitrary — Cerbos emits the
 * PDP's clock at nanosecond precision, which is exactly why both actions have `rejected` direct outcomes
 * for this adapter (a BSON Date holds milliseconds). A tidy millisecond instant here would
 * translate cleanly and quietly contradict `adapterctl.json`, so the fraction carries the nine digits
 * a real plan carries. `translator.test.ts` pins both sides of that boundary, which is what the
 * `plannedAt` override on `planFromWireFixture` is for.
 */
const PLANNED_AT = "2026-08-11T09:13:39.123456789Z";

interface WireOperand {
  expression?: { operator: string; operands: WireOperand[] };
  variable?: string;
  value?: unknown;
}

interface WireFixture {
  action: string;
  resourceKind: string;
  filter: { kind: string; condition?: WireOperand };
}

function operandFromWire(
  node: WireOperand,
  plannedAt: string,
): PlanExpressionOperand {
  if (node.expression) {
    return new PlanExpression(
      node.expression.operator,
      node.expression.operands.map((child) =>
        operandFromWire(child, plannedAt),
      ),
    );
  }
  if (node.variable !== undefined) {
    return new PlanExpressionVariable(node.variable);
  }
  if (!("value" in node)) {
    throw new Error(
      `Wire fixture operand is neither an expression, a variable nor a value: ${JSON.stringify(node)}`,
    );
  }
  // The one cast in this file. A fixture is JSON the PDP produced, so its leaves are already
  // exactly the JSON shapes `Value` admits — but `JSON.parse` cannot say so, and re-validating a
  // file the corpus workflow regenerates and diffs would assert nothing new.
  return new PlanExpressionValue(
    (node.value === "__NOW_MINUS_24H__" ? plannedAt : node.value) as Value,
  );
}

/** Every action the corpus has a golden wire fixture for, sorted. */
export function wireFixtureActions(): string[] {
  return fs
    .readdirSync(WIRE_FIXTURES_DIR)
    .filter((name) => name.endsWith(".json"))
    .map((name) => name.slice(0, -".json".length))
    .sort();
}

/**
 * The plan the pinned PDP produced for `action`, decoded into the shape the SDK hands callers.
 *
 * The fixture is the PDP's HTTP response, so the decoding here is the one `@cerbos/http` performs
 * — `{expression|variable|value}` nodes into `PlanExpression` / `PlanExpressionVariable` /
 * `PlanExpressionValue`. It is deliberately not a hand-built plan: a plan somebody typed is a
 * belief about what the planner emits, and this repository keeps fixtures precisely because that
 * belief has been wrong before. See docs/adr/0006.
 */
export function planFromWireFixture(
  action: string,
  plannedAt: string = PLANNED_AT,
): PlanResourcesResponse {
  const fixture: WireFixture = JSON.parse(
    fs.readFileSync(path.join(WIRE_FIXTURES_DIR, `${action}.json`), "utf8"),
  );
  const base = {
    cerbosCallId: "",
    requestId: "",
    validationErrors: [],
    metadata: undefined,
  };
  switch (fixture.filter.kind) {
    case PlanKind.CONDITIONAL:
      if (!fixture.filter.condition) {
        throw new Error(
          `Wire fixture ${action} is conditional with no condition`,
        );
      }
      return {
        ...base,
        kind: PlanKind.CONDITIONAL,
        condition: operandFromWire(fixture.filter.condition, plannedAt),
      };
    case PlanKind.ALWAYS_ALLOWED:
    case PlanKind.ALWAYS_DENIED:
      return { ...base, kind: fixture.filter.kind };
    default:
      throw new Error(
        `Wire fixture ${action} has an unrecognised filter kind ${fixture.filter.kind}`,
      );
  }
}

// -- the mapper ----------------------------------------------------------------------------------

const labelsMapping: MapperConfig = {
  relation: {
    name: "labels",
    type: "many",
    fields: {
      name: { field: "name", nullable: true },
    },
  },
};
const subCategoriesMapping: MapperConfig = {
  relation: {
    name: "subCategories",
    type: "many",
    field: "name",
    fields: {
      name: { field: "name" },
      labels: labelsMapping,
    },
  },
};

export const MAPPER: Mapper = {
  // The primary key, reached as `request.resource.id` rather than through `attr` (the `id-*`
  // actions). An adapter that resolves references by stripping a `request.resource.attr.` prefix
  // never sees this name.
  //
  // It maps to `resourceId`, the string field the harness carries the corpus id in, NOT to an
  // ObjectId. That is a deliberate limit on what the corpus can prove here: the ObjectId coercion
  // is a caller-supplied `valueParser` on the mapper entry, and three of the six id-* actions
  // compare the key against a STRING column (id-f2f, id-f2f-ne, id-concat), so a single key
  // mapping cannot be an ObjectId and satisfy them. The coercion is pinned where it belongs
  // instead — against the `id-eq-const` wire fixture in `translator.test.ts`.
  "request.resource.id": { field: "resourceId" },
  "request.resource.attr.aBool": { field: "aBool" },
  "request.resource.attr.aString": { field: "aString" },
  "request.resource.attr.aNumber": { field: "aNumber" },
  "request.resource.attr.aDouble": { field: "aDouble", nullable: true },
  "request.resource.attr.aOptionalString": {
    field: "aOptionalString",
    nullable: true,
  },
  "request.resource.attr.createdBy": { field: "createdBy" },
  "request.resource.attr.scope": { field: "scope", nullable: true },
  "request.resource.attr.createdAt": {
    field: "createdAt",
    nullable: true,
  },
  "request.resource.attr.owner": { field: "aOptionalString" },
  // `coOwner` aliases the `scope` field under the explicit-null convention: the oracle sends a
  // real null attribute for it rather than omitting it. Mongoose stores an explicit null, and
  // its query semantics already treat null as a value, so no `nullable` flag applies here — the
  // flag means the opposite (a stored null IS a missing attribute).
  "request.resource.attr.coOwner": { field: "scope" },
  // obj.inner is not a real nested path — it mirrors aString. `parent.inner` below is the
  // opposite: a real two-level to-one chain. The two are kept side by side on purpose.
  "request.resource.attr.obj.inner": { field: "aString" },
  // The corpus's one REAL to-one chain (the `rel-*` actions), stored as an embedded subdocument
  // per level rather than a joined collection. `type: "one"` flattens the path to `parent.aBool`
  // AND declares the level as absent-able, which is what makes the adapter require it outside
  // any `$nor`: a document with `parent: null` has no `parent.aBool` path at all, and an
  // unguarded negation matches exactly those documents.
  "request.resource.attr.parent": {
    relation: {
      name: "parent",
      type: "one",
      fields: {
        aBool: { field: "aBool" },
        aString: { field: "aString" },
        aNumber: { field: "aNumber" },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
  "request.resource.attr.parent.inner": {
    relation: {
      name: "parent.inner",
      type: "one",
      fields: {
        aBool: { field: "aBool" },
        aString: { field: "aString" },
        aNumber: { field: "aNumber" },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
  "request.resource.attr.tags": {
    relation: {
      name: "tags",
      type: "many",
      fields: {
        id: { field: "id" },
        name: { field: "name", nullable: true },
      },
    },
  },
  "request.resource.attr.tagNames": {
    relation: {
      name: "tags",
      type: "many",
      field: "name",
      fields: { name: { field: "name" } },
    },
  },
  "request.resource.attr.categories": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name" },
        subCategories: subCategoriesMapping,
      },
    },
  },
  "request.resource.attr.mainCategory": {
    relation: {
      name: "categories",
      type: "many",
      fields: {
        name: { field: "name" },
        subCategories: subCategoriesMapping,
        subNames: subCategoriesMapping,
      },
    },
  },
  // mainCategory is a to-ONE parent on the check side: a seed with no subCategoryNames
  // sends NO mainCategory attribute, so CEL raises a missing-path error and check()
  // denies. The flattened `categories.subCategories` path cannot see that on its own —
  // an absent parent and a childless parent both give an empty array — so the mapping
  // declares the parent and the adapter makes the count UNKNOWN when it is missing
  // (cerbos/query-plan-adapters#309).
  "request.resource.attr.mainCategory.subCategories": {
    relation: {
      name: "categories.subCategories",
      type: "many",
      requiresParent: "categories",
      fields: { name: { field: "name" } },
    },
  },
  "request.resource.attr.mainCategory.subNames": {
    relation: {
      name: "categories.subCategories",
      type: "many",
      field: "name",
      requiresParent: "categories",
      fields: { name: { field: "name" } },
    },
  },
};
