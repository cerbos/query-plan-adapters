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

/**
 * What both of this adapter's suites read from the shared `../conformance/` corpus: the recorded
 * golden plans, each decoded by `@cerbos/http` from a stubbed PlanResources call, so the adapter
 * receives exactly what the SDK hands a caller. `adversarial.test.ts` replays them against a real
 * Convex backend; `translator.test.ts` reads a few by case id for the caller-option tests the
 * corpus cannot vary.
 * The one mapper both use lives in `../convex/adversarialMapper.ts`, where the backend reads it.
 *
 * Duplicated across adapters on purpose — adapters share data, not code (ADR 0007).
 * Test-only: excluded from `tsc --build` by `tsconfig.json`, so nothing here reaches `lib/`.
 */

export const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

export function readCorpusJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(path.join(CONFORMANCE_DIR, file), "utf8"));
}

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
  /** A planner bug no adapter can pass. Recorded only in the goldens of the tags it applies to. */
  plannerDivergence: { issue: string; pdp?: string[] } | null;
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
  const root = FUZZ_DIR ? path.join(FUZZ_DIR, tag) : path.join(CONFORMANCE_DIR, "golden", tag);
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

// -- fuzz mode ------------------------------------------------------------------------------------

/**
 * The directory a fuzz run was recorded into (conformance/README.md, "Fuzzing"). When it is set,
 * `readGoldens` reads the generated cases from it instead of the corpus, and the harness judges
 * each case by `fuzzVerdict` instead of the ledger: a refusal is always acceptable, since the
 * ledger has no entry for a shape nobody has written down yet.
 */
export const FUZZ_DIR = process.env["QPA_FUZZ_DIR"] || undefined;

export interface FuzzVerdict {
  pdp: string;
  id: string;
  verdict: "match" | "refused" | "over-grant" | "under-grant" | "error";
  expected?: string[];
  returned?: string[];
  detail?: string;
}

/** Judges the ids a translated filter returned against the ids the PDP allowed. */
export function fuzzVerdict(golden: Golden, expected: string[], returned: string[]): FuzzVerdict {
  const base = { pdp: golden.pdp, id: golden.id };
  // Compared element by element: under jest the arrays come from different realms, which
  // isDeepStrictEqual tells apart.
  if (returned.length === expected.length && returned.every((id, i) => id === expected[i])) {
    return { ...base, verdict: "match" };
  }
  const allowed = new Set(expected);
  const got = new Set(returned);
  const verdict = returned.some((id) => !allowed.has(id))
    ? "over-grant"
    : expected.some((id) => !got.has(id))
      ? "under-grant"
      : "error";
  const detail = verdict === "error" ? "the right rows, but repeated or out of order" : undefined;
  return { ...base, verdict, expected, returned, ...(detail ? { detail } : {}) };
}

/** A translation or query that threw, judged: the adapter's refusal type is the invariant working. */
export function fuzzThrown(golden: Golden, error: unknown, refusal: abstract new (...args: never[]) => Error): FuzzVerdict {
  const base = { pdp: golden.pdp, id: golden.id };
  if (error instanceof refusal) return { ...base, verdict: "refused" };
  return { ...base, verdict: "error", detail: error instanceof Error ? `${error.name}: ${error.message}` : String(error) };
}

/** Fails the case unless its verdict is a match or a refusal. */
export function assertFuzzVerdict(verdict: FuzzVerdict): void {
  if (verdict.verdict === "match" || verdict.verdict === "refused") return;
  throw new Error(
    `${verdict.verdict}: expected [${verdict.expected?.join(", ") ?? ""}], returned [${verdict.returned?.join(", ") ?? ""}]${verdict.detail ? `: ${verdict.detail}` : ""}`,
  );
}

/** Writes `<FUZZ_DIR>/results/<adapter>-<store>.json`, which `fuzz-report` reads. */
export function writeFuzzResults(adapter: string, store: string, results: FuzzVerdict[]): void {
  if (!FUZZ_DIR) return;
  const dir = path.join(FUZZ_DIR, "results");
  fs.mkdirSync(dir, { recursive: true });
  const sorted = [...results].sort((a, b) => a.id.localeCompare(b.id) || a.pdp.localeCompare(b.pdp));
  fs.writeFileSync(
    path.join(dir, `${adapter}-${store}.json`),
    `${JSON.stringify({ adapter, store, results: sorted }, null, 2)}\n`,
  );
}

/**
 * The instant substituted for `__NOW_MINUS_24H__`, the literal the planner folds
 * `now() - duration("24h")` into. The PDP folds its clock at nanosecond precision, so the
 * substitute carries sub-millisecond digits too, and the adapter sees the literal it would see in
 * production.
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
  if (body === undefined)
    throw new Error(`stub PDP: no response for request ${requestId}`);
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
    if (!requestUrl(input).startsWith(STUB_HTTP_PDP))
      return passthrough(input, init);
    const { requestId } = JSON.parse(String(init?.body)) as {
      requestId: string;
    };
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
        requestDeserialize: (bytes: Buffer) =>
          fromBinary(planResources.input, bytes),
        responseSerialize: (message: PlanResourcesResponseMessage) =>
          Buffer.from(toBinary(planResources.output, message)),
        responseDeserialize: (bytes: Buffer) =>
          fromBinary(planResources.output, bytes),
      },
    },
    {
      planResources: (
        call: ServerUnaryCall<
          PlanResourcesRequest,
          PlanResourcesResponseMessage
        >,
        respond: sendUnaryData<PlanResourcesResponseMessage>,
      ) => {
        try {
          respond(
            null,
            fromJson(
              planResources.output,
              takeResponse(call.request.requestId),
            ),
          );
        } catch (error) {
          respond(error as Error, null);
        }
      },
    },
  );
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync(
      "127.0.0.1:0",
      ServerCredentials.createInsecure(),
      (error, bound) => (error ? reject(error) : resolve(bound)),
    ),
  );
  // The stub is on loopback; never route it through an ambient proxy.
  const client = new GRPC(`127.0.0.1:${port}`, {
    tls: false,
    channelOptions: { "grpc.enable_http_proxy": 0 },
  });
  return { client, server };
}

let stubClients:
  | Promise<{ http: HTTP; grpc: GRPC; server: Server }>
  | undefined;

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
      ([key, child]) => `${JSON.stringify(key)}:${toProtoJson(child)}`,
    );
    return `{${members.join(",")}}`;
  }
  return JSON.stringify(node);
}

function substituteNow(node: unknown, now: string): JsonValue {
  if (node === "__NOW_MINUS_24H__") return now;
  if (Array.isArray(node))
    return node.map((child) => substituteNow(child, now));
  if (node !== null && typeof node === "object") {
    return Object.fromEntries(
      Object.entries(node).map(([key, child]) => [
        key,
        substituteNow(child, now),
      ]),
    );
  }
  return node as JsonValue;
}

/** One PlanResources call through `client`, answered with `response`. */
function fetchPlan(
  client: HTTP | GRPC,
  golden: Golden,
  response: JsonObject,
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
  now: string = nowMinus24h(),
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
    throw new Error(
      `${golden.id}: @cerbos/http and @cerbos/grpc decode the plan differently`,
    );
  }
  return overGrpc;
}
