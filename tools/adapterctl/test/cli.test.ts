import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { afterEach, test } from "node:test";

const cli = resolve(import.meta.dirname, "../../../adapterctl");
const temporaryRoots: string[] = [];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

afterEach(async () => {
  await Promise.all(temporaryRoots.splice(0).map(async (root) => rm(root, { recursive: true })));
});

async function createRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "adapterctl-test-"));
  temporaryRoots.push(root);
  return root;
}

async function writeJson(root: string, path: string, value: unknown): Promise<void> {
  const absolute = join(root, path);
  await mkdir(dirname(absolute), { recursive: true });
  await writeFile(absolute, `${JSON.stringify(value, null, 2)}\n`);
}

async function writeText(root: string, path: string, value: string): Promise<void> {
  const absolute = join(root, path);
  await mkdir(dirname(absolute), { recursive: true });
  await writeFile(absolute, value);
}

async function readJson(root: string, path: string): Promise<Record<string, unknown>> {
  const source = await import("node:fs/promises").then(async ({ readFile }) =>
    readFile(join(root, path), "utf8")
  );
  const value: unknown = JSON.parse(source);
  if (!isRecord(value)) {
    throw new Error(`${path}: expected object fixture`);
  }
  return value;
}

async function writeValidRepository(
  root: string,
  outcomes: Record<string, unknown> = { read: { status: "matched" } },
): Promise<void> {
  await writeJson(root, "alpha/adapterctl.json", {
    schemaVersion: 1,
    adapter: "alpha",
    package: { ecosystem: "test", name: "alpha" },
    workflow: ".github/workflows/alpha.yaml",
    commands: {
      test: [process.execPath, "-e", "process.exit(0)"],
      typecheck: null,
      golden: null,
      consumer: [process.execPath, "-e", "process.exit(0)"],
    },
    semanticEnvironments: [{
      name: "default",
      command: [process.execPath, "-e", "process.exit(0)"],
      env: {},
    }],
    consumer: { coverage: "artifact-install" },
    outcomes,
  });
  await writeJson(root, "conformance/catalog.json", {
    schemaVersion: 1,
    actions: [{ name: "read", oracleExpectation: { kind: "proper-subset" } }],
  });
  await writeText(root, "conformance/policies/adversarial.yaml", [
    "apiVersion: api.cerbos.dev/v1",
    "resourcePolicy:",
    "  version: default",
    "  resource: test",
    "  rules:",
    "    - actions: [read]",
    "      effect: EFFECT_ALLOW",
    "      roles: [USER]",
    "",
  ].join("\n"));
  await writeJson(root, "demo/cases.json", { schemaVersion: 1, cases: [] });
  await writeJson(root, "demo/expected.json", { shapes: {} });
  await writeJson(root, "conformance/actions.json", {
    adapters: ["alpha"],
    conformance: ["read"],
    adapterUnsupported: {},
    adapterSupportedExpected: {},
    expectedUnsupported: [],
    nullRepresentationOmitted: [],
    knownDivergences: [],
  });
  await writeJson(root, "conformance/seeds.json", {
    principal: { id: "u1", roles: ["USER"], attr: {} },
    resourceKind: "test",
    seeds: [{
      id: "r1",
      aBool: true,
      aString: "one",
      aNumber: 1,
      aOptionalString: "set",
      tags: [],
      subCategoryNames: [],
      parentSeedId: null,
    }],
  });
  await writeJson(root, "conformance/derived-fields.json", {
    fields: ["createdBy", "aDouble", "createdAt", "scope", "labels"],
    derived: {
      r1: {
        createdBy: "2024-06-01T00:00:00Z",
        aDouble: 1.3,
        createdAt: "2036-06-06T06:06:06Z",
        scope: null,
        labels: [],
      },
    },
  });
  await writeJson(root, "conformance/check-resources.json", {
    schemaVersion: 1,
    principal: { id: "u1", roles: ["USER"], attr: {} },
    resources: [{
      kind: "test",
      id: "r1",
      attr: {
        aBool: true,
        aString: "one",
        aNumber: 1,
        createdBy: "2024-06-01T00:00:00Z",
        owner: "set",
        coOwner: null,
        tagNames: [],
        obj: { inner: "one" },
        tags: [],
        categories: [],
        aOptionalString: "set",
        aDouble: 1.3,
        createdAt: "2036-06-06T06:06:06Z",
      },
    }],
  });
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    steps:",
    `      - run: ${process.execPath} -e process.exit(0)`,
    "",
  ].join("\n"));
}

async function runCli(root: string, arguments_: string[]): Promise<{
  code: number | null;
  stdout: string;
  stderr: string;
}> {
  return await new Promise((resolveRun, reject) => {
    const child = spawn(cli, arguments_, {
      env: { ...process.env, ADAPTERCTL_ROOT: root },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (code) => resolveRun({ code, stdout, stderr }));
  });
}

async function runProgram(root: string, executable: string, arguments_: string[]): Promise<number | null> {
  return await new Promise((resolveRun, reject) => {
    const child = spawn(executable, arguments_, { cwd: root, stdio: "ignore" });
    child.once("error", reject);
    child.once("close", resolveRun);
  });
}

test("list discovers manifests without a root roster", async () => {
  const root = await createRoot();
  await writeJson(root, "zeta/adapterctl.json", { adapter: "zeta" });
  await writeJson(root, "alpha/adapterctl.json", { adapter: "alpha" });
  await chmod(cli, 0o755);

  const result = await runCli(root, ["list"]);

  assert.equal(result.code, 0, result.stderr);
  assert.equal(result.stdout, "alpha\nzeta\n");
  assert.equal(result.stderr, "");
});

test("strict validation rejects missing outcomes while discovery reports them", async () => {
  const root = await createRoot();
  await writeValidRepository(root, {});

  const strict = await runCli(root, ["validate"]);
  const discovery = await runCli(root, ["validate", "--discovery"]);

  assert.equal(strict.code, 1);
  assert.match(strict.stderr, /alpha: action read is unassessed/);
  assert.equal(discovery.code, 0, discovery.stderr);
  assert.equal(discovery.stdout, "alpha: action read is unassessed\n");
});

test("discovery still rejects malformed exact-schema metadata", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  manifest["surprise"] = true;
  await writeJson(root, "alpha/adapterctl.json", manifest);

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /alpha\/adapterctl\.json: unknown key surprise/);
});

test("validation accepts an alternate valid YAML action layout", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeText(root, "conformance/policies/adversarial.yaml", [
    "apiVersion: api.cerbos.dev/v1",
    "resourcePolicy:",
    "  resource: test",
    "  rules:",
    "  - roles:",
    "    - USER",
    "    effect: EFFECT_ALLOW",
    "    actions:",
    "    - read",
    "  version: default",
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate"]);

  assert.equal(result.code, 0, result.stderr);
});

test("validation rejects canonical check-resource drift", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const resources = await readJson(root, "conformance/check-resources.json");
  const list = resources["resources"];
  if (!Array.isArray(list) || !isRecord(list[0])) {
    throw new Error("invalid test fixture resources");
  }
  const attr = list[0]["attr"];
  if (!isRecord(attr)) {
    throw new Error("invalid test fixture attr");
  }
  attr["aString"] = "drifted";
  await writeJson(root, "conformance/check-resources.json", resources);

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /resources\[r1\]: does not match canonical resource/);
});

test("validation rejects consumer case identity drift", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeJson(root, "demo/cases.json", {
    schemaVersion: 1,
    cases: [{
      id: "wrong",
      operation: "filtered",
      principal: "alice",
      action: "view",
      pagination: null,
      expected: { kind: "KIND_CONDITIONAL", ids: ["d1"] },
    }],
  });

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /cases\[0\]\.id: expected filtered\/alice\/view/);
});

test("validation rejects workflow trigger drift", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeText(root, ".github/workflows/alpha.yaml", "on: push\n");

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing path trigger alpha\/\*\*/);
});

test("workflow comments cannot satisfy certification evidence", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on: push",
    "# alpha/** conformance/** demo/**",
    `# ${process.execPath} -e process.exit(0)`,
    "# ADAPTER_TEST_DB=mysql",
    "jobs:",
    "  test:",
    "    steps:",
    "      - run: echo unrelated",
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing path trigger alpha\/\*\*/);
  assert.match(result.stderr, /missing native command marker/);
});

test("discovery rejects an assessed outcome that contradicts the legacy ledger", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const legacy = await readJson(root, "conformance/actions.json");
  legacy["conformance"] = [];
  await writeJson(root, "conformance/actions.json", legacy);

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /alpha\/read: manifest outcome matched is absent from legacy actions\.json/);
});

test("validation compares consumer cases with the legacy expected results", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeJson(root, "demo/cases.json", {
    schemaVersion: 1,
    cases: [{
      id: "filtered/alice/view",
      operation: "filtered",
      principal: "alice",
      action: "view",
      pagination: null,
      expected: { kind: "KIND_CONDITIONAL", ids: ["d1"] },
    }],
  });

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /demo\/cases\.json does not match demo\/expected\.json/);
});

test("validation checks semantic environment values against the workflow matrix", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  const environments = manifest["semanticEnvironments"];
  if (!Array.isArray(environments) || !isRecord(environments[0])) {
    throw new Error("invalid test fixture environment");
  }
  environments[0]["env"] = { ADAPTER_TEST_DB: "mysql" };
  await writeJson(root, "alpha/adapterctl.json", manifest);

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing environment marker ADAPTER_TEST_DB=mysql/);
});

test("validation rejects duplicate special-bucket legacy assignments", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const legacy = await readJson(root, "conformance/actions.json");
  legacy["adapterUnsupported"] = {
    alpha: [{ action: "read", reason: "unsupported", message: "no read" }],
  };
  legacy["knownDivergences"] = [{
    action: "read",
    adapters: ["alpha"],
    reason: "planner issue",
  }];
  await writeJson(root, "conformance/actions.json", legacy);

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /duplicate special classification for alpha\/read/);
});

test("report emits a deterministic machine-readable confidence profile", async () => {
  const root = await createRoot();
  await writeValidRepository(root);

  const result = await runCli(root, ["report", "--format", "json"]);

  assert.equal(result.code, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout), {
    schemaVersion: 1,
    actions: 1,
    adapters: [{
      adapter: "alpha",
      matched: 1,
      rejected: 0,
      upstreamBlocked: 0,
      unassessed: 0,
      safetyAccounted: 1,
      capabilitySupported: 1,
      consumerCoverage: "artifact-install",
      environments: ["default"],
    }],
  });
});

test("explain resolves adapter and action contracts without searching prose", async () => {
  const root = await createRoot();
  await writeValidRepository(root);

  const adapter = await runCli(root, ["explain", "adapter", "alpha"]);
  const action = await runCli(root, ["explain", "action", "read"]);

  assert.equal(adapter.code, 0, adapter.stderr);
  assert.match(adapter.stdout, /Adapter: alpha/);
  assert.match(adapter.stdout, /Consumer coverage: artifact-install/);
  assert.equal(action.code, 0, action.stderr);
  assert.match(action.stdout, /Action: read/);
  assert.match(action.stdout, /alpha: matched/);
});

test("docs writes and checks the deterministic certification document", async () => {
  const root = await createRoot();
  await writeValidRepository(root);

  const write = await runCli(root, ["docs", "--write"]);
  assert.equal(write.code, 0, write.stderr);
  const document = await import("node:fs/promises").then(async ({ readFile }) =>
    readFile(join(root, "docs/generated/adapter-certification.md"), "utf8")
  );
  assert.match(document, /Generated by `.\/adapterctl docs --write`/);
  assert.match(document, /# Adapter certification/);

  const check = await runCli(root, ["docs", "--check"]);
  assert.equal(check.code, 0, check.stderr);

  await writeText(root, "docs/generated/adapter-certification.md", "stale\n");
  const stale = await runCli(root, ["docs", "--check"]);
  assert.equal(stale.code, 1);
  assert.match(stale.stderr, /adapter certification documentation is out of date/);
});

test("run dry-run shows the selected native command, environment, and action", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  const environments = manifest["semanticEnvironments"];
  if (!Array.isArray(environments) || !isRecord(environments[0])) {
    throw new Error("invalid test fixture environment");
  }
  const code = "process.stdout.write(`${process.env.ADAPTER_TEST_DB}/${process.env.ADAPTERCTL_ACTION}`)";
  environments[0]["command"] = [process.execPath, "-e", code];
  environments[0]["env"] = { ADAPTER_TEST_DB: "mysql" };
  await writeJson(root, "alpha/adapterctl.json", manifest);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    env:",
    "      ADAPTER_TEST_DB: mysql",
    "    steps:",
    `      - run: ${process.execPath} -e ${code}`,
    `      - run: ${process.execPath} -e process.exit(0)`,
    "",
  ].join("\n"));

  const dryRun = await runCli(root, [
    "run", "--adapter", "alpha", "--profile", "conformance",
    "--environment", "default", "--action", "read", "--dry-run",
  ]);
  assert.equal(dryRun.code, 0, dryRun.stderr);
  assert.match(dryRun.stdout, /alpha\/default/);
  assert.match(dryRun.stdout, /ADAPTER_TEST_DB=mysql/);
  assert.match(dryRun.stdout, /ADAPTERCTL_ACTION=read/);

  const executed = await runCli(root, [
    "run", "--adapter", "alpha", "--profile", "conformance",
    "--environment", "default", "--action", "read",
  ]);
  assert.equal(executed.code, 0, executed.stderr);
  assert.match(executed.stdout, /mysql\/read/);
});

test("scaffold creates a safe unassessed manifest and refuses existing targets", async () => {
  const root = await createRoot();

  const dryRun = await runCli(root, [
    "scaffold", "beta", "--ecosystem", "npm", "--package", "@example/beta",
    "--usage-only", "--dry-run",
  ]);
  assert.equal(dryRun.code, 0, dryRun.stderr);
  assert.match(dryRun.stdout, /beta\/adapterctl\.json/);
  assert.match(dryRun.stdout, /native semantic environment/);
  await assert.rejects(import("node:fs/promises").then(async ({ stat }) => stat(join(root, "beta"))));

  const created = await runCli(root, [
    "scaffold", "beta", "--ecosystem", "npm", "--package", "@example/beta", "--usage-only",
  ]);
  assert.equal(created.code, 0, created.stderr);
  const manifest = await readJson(root, "beta/adapterctl.json");
  assert.deepEqual(manifest["semanticEnvironments"], []);
  assert.deepEqual(manifest["outcomes"], {});
  assert.deepEqual(manifest["consumer"], { coverage: "usage-only" });

  const duplicate = await runCli(root, [
    "scaffold", "beta", "--ecosystem", "npm", "--package", "@example/beta",
  ]);
  assert.equal(duplicate.code, 1);
  assert.match(duplicate.stderr, /target already exists: beta/);
});

test("full validation rejects stale legacy adapter roster entries", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const legacy = await readJson(root, "conformance/actions.json");
  legacy["adapters"] = ["alpha", "stale"];
  await writeJson(root, "conformance/actions.json", legacy);

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /legacy adapter has no discovered manifest: stale/);
});

test("strict validation cannot certify a manifest with no semantic environments", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  manifest["semanticEnvironments"] = [];
  await writeJson(root, "alpha/adapterctl.json", manifest);

  const discovery = await runCli(root, ["validate", "--discovery"]);
  const strict = await runCli(root, ["validate"]);

  assert.equal(discovery.code, 0, discovery.stderr);
  assert.equal(strict.code, 1);
  assert.match(strict.stderr, /alpha: strict certification requires a semantic environment/);
});

test("an explicit conformance run rejects an adapter with no semantic environments", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  manifest["semanticEnvironments"] = [];
  await writeJson(root, "alpha/adapterctl.json", manifest);

  const result = await runCli(root, ["run", "--adapter", "alpha", "--profile", "conformance"]);

  assert.equal(result.code, 64);
  assert.match(result.stderr, /alpha: conformance command is unavailable/);
});

test("validation checks available test and typecheck commands against the workflow", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  const commands = manifest["commands"];
  if (!isRecord(commands)) throw new Error("invalid commands fixture");
  commands["test"] = [process.execPath, "-e", "process.exit(17)"];
  await writeJson(root, "alpha/adapterctl.json", manifest);

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing native command marker .*process\.exit\(17\)/);
});

test("golden runs report adapter-local changed paths", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  assert.equal(await runProgram(root, "git", ["init", "-q"]), 0);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  const commands = manifest["commands"];
  if (!isRecord(commands)) throw new Error("invalid commands fixture");
  commands["golden"] = [
    process.execPath,
    "-e",
    "require('node:fs').mkdirSync('golden',{recursive:true});require('node:fs').writeFileSync('golden/expectations.json','{}')",
  ];
  await writeJson(root, "alpha/adapterctl.json", manifest);

  const result = await runCli(root, ["run", "--adapter", "alpha", "--profile", "golden"]);

  assert.equal(result.code, 0, result.stderr);
  assert.match(result.stdout, /alpha golden changes:/);
  assert.match(result.stdout, /alpha\/golden\//);
});

test("validation rejects unknown catalog references and legacy groups", async () => {
  const unknownActionRoot = await createRoot();
  await writeValidRepository(unknownActionRoot);
  const manifest = await readJson(unknownActionRoot, "alpha/adapterctl.json");
  const outcomes = manifest["outcomes"];
  if (!isRecord(outcomes)) throw new Error("invalid outcomes fixture");
  outcomes["ghost"] = { status: "matched" };
  await writeJson(unknownActionRoot, "alpha/adapterctl.json", manifest);
  const unknownAction = await runCli(unknownActionRoot, ["validate", "--discovery"]);
  assert.equal(unknownAction.code, 1);
  assert.match(unknownAction.stderr, /unknown action ghost/);

  const unknownGroupRoot = await createRoot();
  await writeValidRepository(unknownGroupRoot);
  const legacy = await readJson(unknownGroupRoot, "conformance/actions.json");
  legacy["mystery"] = [];
  await writeJson(unknownGroupRoot, "conformance/actions.json", legacy);
  const unknownGroup = await runCli(unknownGroupRoot, ["validate", "--discovery"]);
  assert.equal(unknownGroup.code, 1);
  assert.match(unknownGroup.stderr, /unknown group mystery/);
});
