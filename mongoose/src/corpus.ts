import * as fs from "node:fs";
import * as path from "node:path";
import { isDeepStrictEqual } from "node:util";

import { fromBinary, fromJson, toBinary } from "@bufbuild/protobuf";
import type { JsonObject, JsonValue } from "@bufbuild/protobuf";
import type { PlanResourcesRequest } from "@cerbos/api/cerbos/request/v1/request_pb";
import type { PlanResourcesResponse as PlanResourcesResponseMessage } from "@cerbos/api/cerbos/response/v1/response_pb";
import { CerbosService } from "@cerbos/api/cerbos/svc/v1/svc_pb";
import type { PlanResourcesResponse } from "@cerbos/core";
import { GRPC } from "@cerbos/grpc";
import { HTTP } from "@cerbos/http";
import { Server, ServerCredentials } from "@grpc/grpc-js";
import type { sendUnaryData, ServerUnaryCall } from "@grpc/grpc-js";
import { afterAll } from "@jest/globals";

import type { Mapper, MapperConfig } from ".";

/**
 * What both of this adapter's suites read from the shared `../conformance/` corpus: the recorded
 * golden plans and the one mapper every case is translated through. `adversarial.test.ts` replays
 * the goldens against a real MongoDB; `translator.test.ts` uses the same mapper for the
 * caller-option tests the corpus cannot vary.
 *
 * Duplicated across adapters on purpose — adapters share data, not code (ADR 0007).
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

export function readCorpusJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

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
  plannerDivergence: { pdp?: string[]; reason: string } | null;
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
    .map(
      (file) =>
        JSON.parse(fs.readFileSync(path.join(root, file), "utf8")) as Golden,
    );
  return files.sort((a, b) => a.id.localeCompare(b.id));
}

export function readGolden(tag: string, id: string): Golden {
  return JSON.parse(
    fs.readFileSync(
      path.join(CONFORMANCE_DIR, "golden", tag, `${id}.json`),
      "utf8",
    ),
  ) as Golden;
}

/**
 * The instant substituted for `__NOW_MINUS_24H__`, the literal the planner folds
 * `now() - duration("24h")` into. The PDP folds its clock at nanosecond precision, so the
 * substitute carries sub-millisecond digits too — and that is load-bearing here: a BSON Date holds
 * milliseconds, so this adapter refuses the real instant, and a tidy millisecond substitute would
 * translate in the harness while the same case refuses in production.
 */
export function nowMinus24h(): string {
  const ms = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  return `${ms.slice(0, -1)}456789Z`;
}

// -- the plans, as the SDK returns them ----------------------------------------------------------

/*
 * A golden reaches the adapter through both official JS clients, `@cerbos/http` and
 * `@cerbos/grpc`, so it arrives as an application's `planResources` call returns it. Only the PDP
 * is replaced: each transport has a stub that answers PlanResources with the response registered
 * under the request's `requestId`, a token unique to one call. A plan the two clients decode
 * differently fails the case, so every case also proves the adapter is handed the same input
 * whichever transport the application uses.
 */

/**
 * The base URL of the stubbed HTTP PDP. `.invalid` never resolves, so a request that escapes the
 * stub fails rather than reaching a real server.
 */
const STUB_HTTP_PDP = "http://stub-pdp.invalid";

const planResources = CerbosService.method.planResources;

/** The response bodies waiting to be served, by the `requestId` of the call that will read them. */
const pendingResponses = new Map<string, JsonObject>();
let requestCount = 0;

function takeResponse(requestId: string): JsonObject {
  const body = pendingResponses.get(requestId);
  pendingResponses.delete(requestId);
  if (body === undefined) throw new Error(`stub PDP: no response for request ${requestId}`);
  return body;
}

function requestUrl(input: Parameters<typeof fetch>[0]): string {
  if (typeof input === "string") return input;
  return input instanceof URL ? input.href : input.url;
}

/**
 * An `@cerbos/http` client whose PlanResources call is answered here. Only requests to
 * `STUB_HTTP_PDP` are intercepted; every other `fetch` goes through untouched.
 */
function httpClient(): HTTP {
  const passthrough = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    if (!requestUrl(input).startsWith(STUB_HTTP_PDP)) return passthrough(input, init);
    const { requestId } = JSON.parse(String(init?.body)) as { requestId: string };
    return new Response(toProtoJson(takeResponse(requestId)), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };
  return new HTTP(STUB_HTTP_PDP);
}

/**
 * An `@cerbos/grpc` client connected to an in-process gRPC server on a loopback port, which
 * answers PlanResources with the registered response encoded as the protobuf a PDP sends.
 */
async function grpcClient(): Promise<{ client: GRPC; server: Server }> {
  const server = new Server();
  server.addService(
    {
      planResources: {
        path: `/${CerbosService.typeName}/${planResources.name}`,
        requestStream: false,
        responseStream: false,
        requestSerialize: (message: PlanResourcesRequest) =>
          Buffer.from(toBinary(planResources.input, message)),
        requestDeserialize: (bytes: Buffer) => fromBinary(planResources.input, bytes),
        responseSerialize: (message: PlanResourcesResponseMessage) =>
          Buffer.from(toBinary(planResources.output, message)),
        responseDeserialize: (bytes: Buffer) => fromBinary(planResources.output, bytes),
      },
    },
    {
      planResources: (
        call: ServerUnaryCall<PlanResourcesRequest, PlanResourcesResponseMessage>,
        respond: sendUnaryData<PlanResourcesResponseMessage>
      ) => {
        try {
          respond(null, fromJson(planResources.output, takeResponse(call.request.requestId)));
        } catch (error) {
          respond(error as Error, null);
        }
      },
    }
  );
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, bound) =>
      error ? reject(error) : resolve(bound)
    )
  );
  // The stub is on loopback; never route it through an ambient proxy.
  const client = new GRPC(`127.0.0.1:${port}`, {
    tls: false,
    channelOptions: { "grpc.enable_http_proxy": 0 },
  });
  return { client, server };
}

let stubClients: Promise<{ http: HTTP; grpc: GRPC; server: Server }> | undefined;

function sdkClients(): Promise<{ http: HTTP; grpc: GRPC; server: Server }> {
  stubClients ??= grpcClient().then(({ client, server }) => ({
    http: httpClient(),
    grpc: client,
    server,
  }));
  return stubClients;
}

afterAll(async () => {
  if (!stubClients) return;
  const { grpc, server } = await stubClients;
  grpc.close();
  server.forceShutdown();
});

/**
 * JSON as the PDP writes it. `JSON.stringify` writes -0 as `0`, where protojson keeps the sign, so
 * a stub serialising with it would hand the SDK a different number from the one the PDP sends.
 */
function toProtoJson(node: unknown): string {
  if (Object.is(node, -0)) return "-0";
  if (Array.isArray(node)) return `[${node.map(toProtoJson).join(",")}]`;
  if (node !== null && typeof node === "object") {
    const members = Object.entries(node).map(
      ([key, child]) => `${JSON.stringify(key)}:${toProtoJson(child)}`
    );
    return `{${members.join(",")}}`;
  }
  return JSON.stringify(node);
}

function substituteNow(node: unknown, now: string): JsonValue {
  if (node === "__NOW_MINUS_24H__") return now;
  if (Array.isArray(node)) return node.map((child) => substituteNow(child, now));
  if (node !== null && typeof node === "object") {
    return Object.fromEntries(
      Object.entries(node).map(([key, child]) => [key, substituteNow(child, now)])
    );
  }
  return node as JsonValue;
}

/** One PlanResources call through `client`, answered with `response`. */
function fetchPlan(
  client: HTTP | GRPC,
  golden: Golden,
  response: JsonObject
): Promise<PlanResourcesResponse> {
  const requestId = `${golden.id}#${++requestCount}`;
  pendingResponses.set(requestId, response);
  return client.planResources({
    requestId,
    principal: { id: "stub", roles: ["USER"] },
    resource: { kind: "conformance" },
    action: golden.id,
  });
}

/**
 * A golden's plan as the SDK returns it: the recorded `filter` is served as the response to a
 * stubbed PlanResources call over each transport, so the SDK's own decoding produces what the
 * adapter receives.
 */
export async function planOf(
  golden: Golden,
  now: string = nowMinus24h()
): Promise<PlanResourcesResponse> {
  const response: JsonObject = {
    requestId: golden.id,
    action: golden.id,
    resourceKind: "conformance",
    policyVersion: "default",
    filter: substituteNow(golden.plan, now),
  };
  const { http, grpc } = await sdkClients();
  const [overHttp, overGrpc] = await Promise.all([
    fetchPlan(http, golden, response),
    fetchPlan(grpc, golden, response),
  ]);
  if (!isDeepStrictEqual(overHttp, overGrpc)) {
    throw new Error(`${golden.id}: @cerbos/http and @cerbos/grpc decode the plan differently`);
  }
  return overGrpc;
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
  // The primary key, reached as `request.resource.id` rather than through `attr` (the
  // `identifier/*` cases). An adapter that resolves references by stripping a `request.resource.attr.` prefix
  // never sees this name.
  //
  // It maps to `resourceId`, the string field the harness carries the corpus id in, NOT to an
  // ObjectId. That is a deliberate limit on what the corpus can prove here: the ObjectId coercion
  // is a caller-supplied `valueParser` on the mapper entry, and three `identifier/*` cases
  // compare the key against a STRING column (field-to-field, its negation, concatenation), so a single key
  // mapping cannot be an ObjectId and satisfy them. The coercion is pinned where it belongs
  // instead — against the `identifier/equals/literal` golden plan in `translator.test.ts`.
  "request.resource.id": { field: "resourceId" },
  // The three scalar columns declare their schema type, as the README tells a caller to: Mongoose
  // casts a query literal to it (`"true"` to `true`, `"5"` to `5`, `0` to `"0"`), and the
  // declaration is what lets the adapter answer a literal of another type as CEL does instead
  // (the `type-mismatch/*` scalar cases).
  //
  // All three are `nullable` as well: seeds j1, j2 and j3 store a null in one of them, which the
  // corpus sends as a missing attribute (conformance/README.md, "The dataset").
  "request.resource.attr.aBool": {
    field: "aBool",
    valueType: "boolean",
    nullable: true,
  },
  "request.resource.attr.aString": {
    field: "aString",
    valueType: "string",
    nullable: true,
  },
  "request.resource.attr.aNumber": {
    field: "aNumber",
    valueType: "number",
    nullable: true,
  },
  "request.resource.attr.aDouble": {
    field: "aDouble",
    valueType: "number",
    nullable: true,
  },
  "request.resource.attr.aOptionalString": {
    field: "aOptionalString",
    nullable: true,
  },
  "request.resource.attr.createdBy": { field: "createdBy" },
  "request.resource.attr.scope": { field: "scope", nullable: true },
  "request.resource.attr.createdAt": {
    field: "createdAt",
    valueType: "dateTime",
    nullable: true,
  },
  "request.resource.attr.updatedAt": {
    field: "updatedAt",
    valueType: "dateTime",
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
  "request.resource.attr.obj.inner": { field: "aString", nullable: true },
  // The corpus's one REAL to-one chain (the `relation/*` cases), stored as an embedded subdocument
  // per level rather than a joined collection. `type: "one"` flattens the path to `parent.aBool`
  // AND declares the level as absent-able, which is what makes the adapter require it outside
  // any `$nor`: a document with `parent: null` has no `parent.aBool` path at all, and an
  // unguarded negation matches exactly those documents.
  "request.resource.attr.parent": {
    relation: {
      name: "parent",
      type: "one",
      fields: {
        aBool: { field: "aBool", nullable: true },
        aString: { field: "aString", nullable: true },
        aNumber: { field: "aNumber", nullable: true },
        aOptionalString: { field: "aOptionalString", nullable: true },
      },
    },
  },
  "request.resource.attr.parent.inner": {
    relation: {
      name: "parent.inner",
      type: "one",
      fields: {
        aBool: { field: "aBool", nullable: true },
        aString: { field: "aString", nullable: true },
        aNumber: { field: "aNumber", nullable: true },
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
        // Typed for the same reason as the scalar columns: Mongoose casts inside `$elemMatch`
        // over a typed subdocument field too (`type-mismatch/exists/element-field-against-number-literal`).
        name: { field: "name", nullable: true, valueType: "string" },
      },
    },
  },
  "request.resource.attr.tagNames": {
    relation: {
      name: "tags",
      type: "many",
      field: "name",
      fields: { name: { field: "name", valueType: "string" } },
    },
  },
  // Homogeneous scalar lists stored as NATIVE arrays on the document — a plain field, not a
  // relation, because an element is a scalar with no field of its own (`tagNames`, the other list
  // the corpus indexes, is a projection of the `tags` objects). `$arrayElemAt` keeps each
  // element's BSON type, so `true` is never `1` here and a null element is a null value, as it is
  // to CEL. No `valueType`: an indexed read is compared inside `$expr`, which never consults it
  // (see the `index` builder in aggregation.ts for why the literal also stays uncast by Mongoose there).
  // Membership (`membership/in/literal-in-resource-number-list` and the list `type-mismatch/in/*` cases) needs no `valueType` either:
  // a plain field is answered inside `$expr` too (`emitUncastListMembership` in filter.ts).
  "request.resource.attr.aNumberList": { field: "aNumberList" },
  "request.resource.attr.aBoolList": { field: "aBoolList" },
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
