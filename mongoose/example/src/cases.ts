import { readFileSync } from "node:fs";
import { resolve } from "node:path";

export type ConsumerOperation =
  | "filtered"
  | "alwaysAllowed"
  | "alwaysDenied"
  | "paginated"
  | "composed";

type ExpectedPlanKind =
  | "KIND_CONDITIONAL"
  | "KIND_ALWAYS_ALLOWED"
  | "KIND_ALWAYS_DENIED";

interface ExpectedResult {
  kind: ExpectedPlanKind;
  ids: string[];
}

interface BaseConsumerCase {
  id: string;
  principal: string;
  action: string;
  expected: ExpectedResult;
}

export interface UnpaginatedConsumerCase extends BaseConsumerCase {
  operation: Exclude<ConsumerOperation, "paginated">;
  pagination: null;
}

export interface PaginatedConsumerCase extends BaseConsumerCase {
  operation: "paginated";
  pagination: {
    pageSize: number;
    pageSizes: number[];
  };
}

export type ConsumerCase = UnpaginatedConsumerCase | PaginatedConsumerCase;

export interface ConsumerResult {
  kind: string;
  ids: string[];
  pageSize?: number;
  pageSizes?: number[];
}

export type ConsumerShapes = Record<
  ConsumerOperation,
  Record<string, ConsumerResult>
>;

export interface DemoSeeds {
  principals: { id: string; roles: string[] }[];
  applicationFilter: {
    description: string;
    archived: boolean;
    region: string;
  };
  documents: {
    id: string;
    ownerId: string;
    public: boolean;
    region: string;
    archived: boolean;
  }[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value;
}

function requireString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value;
}

function requireInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
  return value;
}

function requireBoolean(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`${label} must be a boolean`);
  }
  return value;
}

function requireStringArray(value: unknown, label: string): string[] {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  return value.map((entry, index) =>
    requireString(entry, `${label}[${index}]`),
  );
}

function requireIntegerArray(value: unknown, label: string): number[] {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  return value.map((entry, index) =>
    requireInteger(entry, `${label}[${index}]`),
  );
}

function parseOperation(value: unknown, label: string): ConsumerOperation {
  switch (value) {
    case "filtered":
    case "alwaysAllowed":
    case "alwaysDenied":
    case "paginated":
    case "composed":
      return value;
    default:
      throw new Error(`${label} has an unsupported operation`);
  }
}

function parsePlanKind(value: unknown, label: string): ExpectedPlanKind {
  switch (value) {
    case "KIND_CONDITIONAL":
    case "KIND_ALWAYS_ALLOWED":
    case "KIND_ALWAYS_DENIED":
      return value;
    default:
      throw new Error(`${label} has an unsupported plan kind`);
  }
}

function parseCase(value: unknown, index: number): ConsumerCase {
  const label = `cases.json.cases[${index}]`;
  const record = requireRecord(value, label);
  const operation = parseOperation(record["operation"], `${label}.operation`);
  const principal = requireString(record["principal"], `${label}.principal`);
  const action = requireString(record["action"], `${label}.action`);
  const id = requireString(record["id"], `${label}.id`);
  if (id !== `${operation}/${principal}/${action}`) {
    throw new Error(
      `${label}.id must identify its operation, principal, and action`,
    );
  }

  const expectedRecord = requireRecord(record["expected"], `${label}.expected`);
  const expected: ExpectedResult = {
    kind: parsePlanKind(expectedRecord["kind"], `${label}.expected.kind`),
    ids: requireStringArray(expectedRecord["ids"], `${label}.expected.ids`),
  };

  if (operation === "paginated") {
    const pagination = requireRecord(
      record["pagination"],
      `${label}.pagination`,
    );
    return {
      id,
      operation,
      principal,
      action,
      pagination: {
        pageSize: requireInteger(
          pagination["pageSize"],
          `${label}.pagination.pageSize`,
        ),
        pageSizes: requireIntegerArray(
          pagination["pageSizes"],
          `${label}.pagination.pageSizes`,
        ),
      },
      expected,
    };
  }

  if (record["pagination"] !== null) {
    throw new Error(`${label}.pagination must be null`);
  }
  return { id, operation, principal, action, pagination: null, expected };
}

export function loadConsumerCases(demoDir: string): ConsumerCase[] {
  const value: unknown = JSON.parse(
    readFileSync(resolve(demoDir, "cases.json"), "utf8"),
  );
  const record = requireRecord(value, "cases.json");
  if (record["schemaVersion"] !== 1 || !Array.isArray(record["cases"])) {
    throw new Error("cases.json must be a v1 consumer case document");
  }
  const cases = record["cases"].map(parseCase);
  const ids = cases.map(({ id }) => id);
  if (new Set(ids).size !== ids.length) {
    throw new Error("cases.json contains duplicate case ids");
  }
  return cases;
}

export function loadDemoSeeds(demoDir: string): DemoSeeds {
  const value: unknown = JSON.parse(
    readFileSync(resolve(demoDir, "seeds.json"), "utf8"),
  );
  const record = requireRecord(value, "seeds.json");
  if (!Array.isArray(record["principals"])) {
    throw new Error("seeds.json.principals must be an array");
  }
  if (!Array.isArray(record["documents"])) {
    throw new Error("seeds.json.documents must be an array");
  }
  const principals = record["principals"].map((value, index) => {
    const label = `seeds.json.principals[${index}]`;
    const principal = requireRecord(value, label);
    return {
      id: requireString(principal["id"], `${label}.id`),
      roles: requireStringArray(principal["roles"], `${label}.roles`),
    };
  });
  const documents = record["documents"].map((value, index) => {
    const label = `seeds.json.documents[${index}]`;
    const document = requireRecord(value, label);
    return {
      id: requireString(document["id"], `${label}.id`),
      ownerId: requireString(document["ownerId"], `${label}.ownerId`),
      public: requireBoolean(document["public"], `${label}.public`),
      region: requireString(document["region"], `${label}.region`),
      archived: requireBoolean(document["archived"], `${label}.archived`),
    };
  });
  const applicationFilter = requireRecord(
    record["applicationFilter"],
    "seeds.json.applicationFilter",
  );
  return {
    principals,
    applicationFilter: {
      description: requireString(
        applicationFilter["description"],
        "seeds.json.applicationFilter.description",
      ),
      archived: requireBoolean(
        applicationFilter["archived"],
        "seeds.json.applicationFilter.archived",
      ),
      region: requireString(
        applicationFilter["region"],
        "seeds.json.applicationFilter.region",
      ),
    },
    documents,
  };
}

function assertEqual(actual: unknown, expected: unknown, label: string): void {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
    );
  }
}

function assertResult(testCase: ConsumerCase, result: ConsumerResult): void {
  assertEqual(result.kind, testCase.expected.kind, `${testCase.id} plan kind`);
  assertEqual(result.ids, testCase.expected.ids, `${testCase.id} ids`);
  if (testCase.operation === "paginated") {
    assertEqual(
      result.pageSize,
      testCase.pagination.pageSize,
      `${testCase.id} page size`,
    );
    assertEqual(
      result.pageSizes,
      testCase.pagination.pageSizes,
      `${testCase.id} page sizes`,
    );
  }
}

export async function runConsumerCases({
  cases,
  execute,
}: {
  cases: ConsumerCase[];
  execute: (testCase: ConsumerCase) => Promise<ConsumerResult>;
}): Promise<ConsumerShapes> {
  const shapes: ConsumerShapes = {
    filtered: {},
    alwaysAllowed: {},
    alwaysDenied: {},
    paginated: {},
    composed: {},
  };

  for (const testCase of cases) {
    const result = await execute(testCase);
    assertResult(testCase, result);
    shapes[testCase.operation][`${testCase.principal}/${testCase.action}`] =
      result;
  }
  return shapes;
}
