import * as fs from "node:fs";
import * as path from "node:path";
import { isDeepStrictEqual } from "node:util";

import { fromBinary, fromJson, fromJsonString, toBinary } from "@bufbuild/protobuf";
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

import { isObject, isString } from "../src/narrowing";

/**
 * The recorded golden plans in `../conformance/golden/`, decoded as the SDK hands them to a caller.
 * The unit suite reads plans by case id from here; the conformance harness in
 * `../conformance/harnesses/langchain-chromadb/` replays all of them against a real store.
 *
 * Test-only: excluded from `tsc --build` and from the published files.
 */

export const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

/** A file the conformance generator wrote: its shape is the generator's contract with these types. */
function readJson<T>(file: string): T {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

export function readCorpusJson<T>(file: string): T {
  return readJson(path.join(CONFORMANCE_DIR, file));
}

// -- the golden files ----------------------------------------------------------------------------

export interface Golden {
  id: string;
  pdp: string;
  tier: "core" | "extended" | "adversarial";
  plan: JsonObject;
  allowed: string[];
  plannerDivergence: { pdp?: string[]; reason: string } | null;
}

/** The PDP tags the goldens were recorded against: current first, then previous. */
export function pdpTags(): string[] {
  const versions = readCorpusJson<Record<
    "current" | "previous",
    { tag: string }
  >>("pdp-versions.json");

  return [versions.current.tag, versions.previous.tag];
}

/** Every golden file recorded against `tag` under `dir`, sorted by case id. */
export function readGoldens(tag: string, dir: string = path.join(CONFORMANCE_DIR, "golden")): Golden[] {
  const root = path.join(dir, tag);

  return fs.readdirSync(root, { recursive: true, encoding: "utf8" })
    .filter((file) => file.endsWith(".json"))
    .map(
      (file) => readJson<Golden>(path.join(root, file))
    )
    .sort((a, b) => a.id.localeCompare(b.id));
}

export function readGolden(tag: string, id: string): Golden {
  return readJson(path.join(CONFORMANCE_DIR, "golden", tag, `${id}.json`));
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
  if (isString(input)) return input;

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
    const { requestId } = fromJsonString(planResources.input, String(init?.body), { ignoreUnknownFields: true });

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
          respond(error instanceof Error ? error : new Error(String(error)), null);
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
function toProtoJson(node: JsonValue): string {
  if (Object.is(node, -0)) return "-0";

  if (Array.isArray(node)) return `[${node.map(toProtoJson).join(",")}]`;

  if (isObject(node)) {
    const members = Object.entries(node).map(
      ([key, child]) => `${JSON.stringify(key)}:${toProtoJson(child)}`
    );

    return `{${members.join(",")}}`;
  }

  return JSON.stringify(node);
}

function substituteNow(node: JsonValue, now: string): JsonValue {
  if (node === "__NOW_MINUS_24H__") return now;

  if (Array.isArray(node)) return node.map((child) => substituteNow(child, now));

  if (isObject(node)) {
    return Object.fromEntries(
      Object.entries(node).map(([key, child]) => [key, substituteNow(child, now)])
    );
  }

  return node;
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
