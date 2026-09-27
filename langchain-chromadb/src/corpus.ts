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

import type { FieldNameMapperConfig } from ".";

/**
 * What both of this adapter's suites read from the shared `../conformance/` corpus: the recorded
 * golden plans and the one field name mapper every case is translated through.
 * `adversarial.test.ts` replays the goldens against a real ChromaDB collection; `translator.test.ts`
 * uses the same mapper for the caller-option tests the corpus cannot vary, so the two cannot drift
 * onto metadata keys nothing seeds.
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
  return (fs.readdirSync(root, { recursive: true }) as string[])
    .filter((file) => file.endsWith(".json"))
    .map(
      (file) =>
        JSON.parse(fs.readFileSync(path.join(root, file), "utf8")) as Golden,
    )
    .sort((a, b) => a.id.localeCompare(b.id));
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
 * `now() - duration("24h")` into, at the nanosecond precision the PDP emits. It is inert for this
 * adapter — every case that carries it compares a computed operand Chroma refuses whatever the
 * instant is — but a harness that substituted a tidier value would be reading a plan the PDP never
 * sends.
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

// -- the metadata mapping, as both suites see it --------------------------------------------------

/**
 * The corpus mapping from Cerbos attribute paths onto Chroma metadata keys.
 *
 * Fields are optional unless declared otherwise, so `$ne`/`$nin` are rejected by default.
 * `required: true` is asserted only for the metadata keys the harness's `metadataFor` writes for
 * every seed in `conformance/seeds.json`: the id alone. Every scalar attribute is NULL for some
 * seed — a missing attribute in `resources.json`, and no metadata key here — so each stays
 * optional. An inequality over one is refused unless the key's declared type (`valueType:
 * "boolean"`, `numericType: "integer"`) spells it without `$ne`, as it does for the booleans and
 * the integers. Attributes the mapping does not name (`owner`,
 * `coOwner`, the lists and relations) are not stored by the harness, so no case this adapter
 * translates reads them: the pushdown refuses every such shape, and the post-filter refuses any
 * reference the mapping does not declare.
 */
export const FIELD_NAME_MAPPER: Record<string, string | FieldNameMapperConfig> =
  {
    // The primary key, reached as `request.resource.id` rather than through `attr` (the
    // `identifier/*` cases). Chroma's `where` filters metadata only — the document id is addressed by the
    // separate `ids` argument to `get()` — so `metadataFor` mirrors the id into a metadata key and
    // this maps onto that.
    "request.resource.id": { field: "id", required: true },
    // NULL for one seed each (j3, j1, j2: no metadata key), so `required: false`. The type
    // declarations (`valueType`, `numericType`) let an inequality over aBool and aNumber be spelled
    // without `$ne`; aString has no such spelling.
    "request.resource.attr.aBool": {
      field: "aBool",
      valueType: "boolean",
      required: false,
    },
    "request.resource.attr.aString": { field: "aString", required: false },
    "request.resource.attr.aNumber": {
      field: "aNumber",
      numericType: "integer",
      required: false,
    },
    "request.resource.attr.aOptionalString": {
      field: "aOptionalString",
      required: false,
    },
    // NULL for some seeds (no metadata key), so `required: false`.
    "request.resource.attr.aDouble": { field: "aDouble", required: false },
    // RFC 3339 strings from derived-fields.json, NULL for some seeds (no metadata key). No `Where`
    // clause reads them — Chroma's `$lt`/`$gt` reject a string — but the post-filter compares them
    // through `timestamp()` and as strings.
    "request.resource.attr.createdAt": { field: "createdAt" },
    "request.resource.attr.updatedAt": { field: "updatedAt" },
    // Strings from derived-fields.json: createdBy on every seed, scope NULL (no key) on some.
    "request.resource.attr.createdBy": { field: "createdBy" },
    "request.resource.attr.scope": { field: "scope" },
    // `obj.inner` mirrors aString in the corpus resource, so it is missing where aString is.
    "request.resource.attr.obj.inner": { field: "obj.inner", required: false },
    // The corpus's one REAL to-one chain (the `relation/*` cases), flattened onto dotted metadata keys
    // by `metadataFor`. EVERY level stays `required: false` — the whole point of the relation is
    // that a level can be absent — so a `$ne` over these keys stays fail-closed. A metadata key
    // Chroma cannot prove is present cannot answer `$ne` the way CEL's missing-attribute error does
    // (cerbos/query-plan-adapters#375). The booleans and integers declare their type, which spells
    // their inequalities without `$ne`.
    "request.resource.attr.parent.aBool": {
      field: "parent.aBool",
      valueType: "boolean",
    },
    "request.resource.attr.parent.aString": { field: "parent.aString" },
    "request.resource.attr.parent.aNumber": {
      field: "parent.aNumber",
      numericType: "integer",
    },
    "request.resource.attr.parent.aOptionalString": {
      field: "parent.aOptionalString",
    },
    "request.resource.attr.parent.inner.aBool": {
      field: "parent.inner.aBool",
      valueType: "boolean",
    },
    "request.resource.attr.parent.inner.aString": {
      field: "parent.inner.aString",
    },
    "request.resource.attr.parent.inner.aNumber": {
      field: "parent.inner.aNumber",
      numericType: "integer",
    },
    "request.resource.attr.parent.inner.aOptionalString": {
      field: "parent.inner.aOptionalString",
    },
  };

/** Every metadata key the corpus mapping can produce, for the "no undeclared key" rule. */
export function mappedMetadataKeys(): string[] {
  return Object.values(FIELD_NAME_MAPPER)
    .map((entry) => (typeof entry === "string" ? entry : entry.field))
    .sort();
}

/**
 * The subset of those keys the mapping asserts is present on every document — the only keys `$ne`
 * and `$nin` are sound over, since Chroma's inequalities MATCH a document missing the key.
 *
 * A bare string carries no presence assertion, so it is optional here exactly as it is in the
 * adapter: the default lives in one place and both readings of it agree.
 */
export function requiredMetadataKeys(): string[] {
  return Object.values(FIELD_NAME_MAPPER)
    .filter(
      (entry): entry is FieldNameMapperConfig =>
        typeof entry !== "string" && entry.required === true,
    )
    .map((entry) => entry.field)
    .sort();
}
