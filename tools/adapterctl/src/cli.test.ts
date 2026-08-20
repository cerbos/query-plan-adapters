import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { afterEach, test } from "node:test";

import { isRecord } from "./records.ts";
import { renderHumanView } from "./ui.ts";

const cli = resolve(import.meta.dirname, "../../../adapterctl");
const temporaryRoots: string[] = [];

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
  await writeJson(root, "demo/cases.json", {
    schemaVersion: 1,
    cases: [{
      id: "filtered/user/read",
      operation: "filtered",
      principal: "user",
      action: "read",
      pagination: null,
      expected: { kind: "KIND_CONDITIONAL", ids: ["r1"] },
    }],
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
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
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

test("Ink adapter list view renders semantic adapter data", () => {
  assert.equal(renderHumanView({ kind: "adapter-list", adapters: ["alpha", "zeta"] }),
    "alpha\nzeta\n");
});

test("Ink markdown report view preserves the document without terminal decoration", () => {
  const rendered = renderHumanView({ kind: "markdown-report", markdown: "# Report\n\nReady\n" });
  assert.equal(rendered, "# Report\n\nReady\n");
  assert.doesNotMatch(rendered, /\u001b\[/);
});

test("Ink validation notices view renders notice data", () => {
  assert.equal(renderHumanView({ kind: "validation-notices", notices: [
    "alpha: read is unassessed",
    "beta: read is unassessed",
  ] }), "alpha: read is unassessed\nbeta: read is unassessed\n");
});

test("Ink explanation view renders explanation fields", () => {
  assert.equal(renderHumanView({
    kind: "adapter-explanation",
    explanation: {
      package: { ecosystem: "npm", name: "@example/alpha" },
      workflow: ".github/workflows/alpha.yaml",
      commands: {
        test: { kind: "command", arguments: ["npm", "test"] },
        typecheck: { kind: "command", arguments: ["npm", "run", "typecheck"] },
        golden: { kind: "unavailable" },
        consumer: { kind: "command", arguments: ["./example/run.sh"] },
      },
      semanticEnvironments: [{
        name: "sqlite",
        command: { kind: "command", arguments: ["npm", "run", "test:adversarial"] },
        env: { ADAPTER_TEST_DB: "sqlite" },
      }],
      summary: {
        adapter: "alpha",
        matched: 1,
        rejected: 2,
        upstreamBlocked: 3,
        unassessed: 4,
        safetyAccounted: 6,
        capabilitySupported: 1,
        consumerCoverage: "artifact-install",
        environments: ["sqlite"],
      },
    },
  }), [
    "Adapter: alpha",
    "Package: npm:@example/alpha",
    "Workflow: .github/workflows/alpha.yaml",
    "Consumer coverage: artifact-install",
    "Outcomes: 1 matched, 2 rejected, 3 upstream-blocked, 4 unassessed",
    "Commands:",
    "- test: npm test",
    "- typecheck: npm run typecheck",
    "- golden: unavailable",
    "- consumer: ./example/run.sh",
    "Semantic environments:",
    "- sqlite: npm run test:adversarial [ADAPTER_TEST_DB=sqlite]",
    "",
  ].join("\n"));
});

test("Ink run output view renders command progress", () => {
  assert.equal(renderHumanView({
    kind: "run-plan",
    executions: [{
      adapter: "alpha",
      environment: null,
      command: { kind: "command", arguments: ["npm", "test"] },
      env: {},
    }],
  }), "alpha: npm test\n");
  assert.equal(renderHumanView({
    kind: "run-plan",
    executions: [{
      adapter: "alpha",
      environment: "quoted",
      command: { kind: "command", arguments: ["node", "-e", "console.log('hello world')"] },
      env: { TOKEN: "two words" },
    }],
  }), "alpha/quoted: env 'TOKEN=two words' -- node -e 'console.log('\"'\"'hello world'\"'\"')'\n");
  assert.equal(renderHumanView({
    kind: "run-progress",
    event: {
      kind: "golden-changed",
      adapter: "alpha",
      changes: " M alpha/golden/expectations.json",
    },
  }), "alpha golden changes:\n M alpha/golden/expectations.json\n");
});

test("Ink docs confirmation view renders the generated path", () => {
  assert.equal(renderHumanView({
    kind: "docs-confirmation",
    path: "docs/generated/adapter-certification.md",
  }), "wrote docs/generated/adapter-certification.md\n");
});

test("Ink scaffold result view renders generated next steps", () => {
  assert.match(renderHumanView({
    kind: "scaffold-result",
    result: {
      status: "written",
      adapter: "beta",
      manifestPath: "beta/adapterctl.json",
      workflow: ".github/workflows/beta.yaml",
    },
  }), /^wrote beta\/adapterctl\.json\nNext steps:\n- set commands\.test/);
});

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

test("all v1 commands work when legacy files are absent", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const commands = [
    ["validate"],
    ["report", "--format", "json"],
    ["explain", "adapter", "alpha"],
    ["explain", "action", "read"],
    ["run", "--adapter", "alpha", "--profile", "test", "--dry-run"],
    ["docs", "--write"],
    ["docs", "--check"],
  ];
  for (const command of commands) {
    const result = await runCli(root, command);
    assert.equal(result.code, 0, `${command.join(" ")}: ${result.stderr}`);
  }
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

test("consumer case schema couples pagination to the operation", async () => {
  const paginatedRoot = await createRoot();
  await writeValidRepository(paginatedRoot);
  await writeJson(paginatedRoot, "demo/cases.json", {
    schemaVersion: 1,
    cases: [{
      id: "paginated/alice/view",
      operation: "paginated",
      principal: "alice",
      action: "view",
      pagination: null,
      expected: { kind: "KIND_CONDITIONAL", ids: ["d1"] },
    }],
  });

  const paginated = await runCli(paginatedRoot, ["validate", "--discovery"]);
  assert.equal(paginated.code, 1);
  assert.match(paginated.stderr, /demo\/cases\.json\/cases\/0/);

  const filteredRoot = await createRoot();
  await writeValidRepository(filteredRoot);
  await writeJson(filteredRoot, "demo/cases.json", {
    schemaVersion: 1,
    cases: [{
      id: "filtered/alice/view",
      operation: "filtered",
      principal: "alice",
      action: "view",
      pagination: { pageSize: 1, pageSizes: [1] },
      expected: { kind: "KIND_CONDITIONAL", ids: ["d1"] },
    }],
  });

  const filtered = await runCli(filteredRoot, ["validate", "--discovery"]);
  assert.equal(filtered.code, 1);
  assert.match(filtered.stderr, /demo\/cases\.json\/cases\/0/);
});

test("consumer case catalog cannot be empty", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeJson(root, "demo/cases.json", { schemaVersion: 1, cases: [] });

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /demo\/cases\.json.*must NOT have fewer than 1 items/);
});

test("action catalog cannot be empty", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeJson(root, "conformance/catalog.json", { schemaVersion: 1, actions: [] });

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /conformance\/catalog\.json.*must NOT have fewer than 1 items/);
});

test("strict repository certification requires at least one adapter", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await rm(join(root, "alpha"), { recursive: true });

  const strict = await runCli(root, ["validate"]);
  const discovery = await runCli(root, ["validate", "--discovery"]);

  assert.equal(strict.code, 1);
  assert.match(strict.stderr, /strict certification requires at least one adapter/);
  assert.equal(discovery.code, 0, discovery.stderr);
});

test("validation rejects workflow trigger drift", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeText(root, ".github/workflows/alpha.yaml", "on: push\n");

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing path trigger alpha\/\*\*/);
});

test("validation requires an adapter-scoped control-plane job", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
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

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing scoped adapterctl validation for alpha/);
});

test("matrix conditions cannot hide a scoped control-plane validation", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    strategy:",
    "      matrix:",
    "        node: ['22']",
    "    steps:",
    "      - if: matrix.node == 'never'",
    "        uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    `      - run: ${process.execPath} -e process.exit(0)`,
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing scoped adapterctl validation for alpha/);
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

test("workflow output cannot masquerade as a native command invocation", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    `      - run: echo ${process.execPath} -e process.exit(0)`,
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing native command marker/);
});

test("statically disabled workflow steps cannot satisfy command evidence", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    "      - if: ${{ false }}",
    `        run: ${process.execPath} -e process.exit(0)`,
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing native command marker/);
});

test("unreachable shell branches cannot satisfy command evidence", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    `      - run: false && ${process.execPath} -e process.exit(0)`,
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing native command marker/);
});

test("uninvoked package scripts cannot satisfy workflow command evidence", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  const commands = manifest["commands"];
  const environments = manifest["semanticEnvironments"];
  if (!isRecord(commands) || !Array.isArray(environments) || !isRecord(environments[0])) {
    throw new Error("invalid command fixture");
  }
  commands["test"] = ["npm", "run", "desired"];
  environments[0]["command"] = ["bash", "scripts/wrapper.sh", "true"];
  await writeJson(root, "alpha/adapterctl.json", manifest);
  await writeJson(root, "alpha/package.json", {
    scripts: {
      unrelated: "npm run desired",
    },
  });
  await writeText(root, "alpha/scripts/wrapper.sh", "#!/usr/bin/env bash\n\"$@\"\n");
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    "      - run: bash scripts/wrapper.sh true",
    `      - run: ${process.execPath} -e process.exit(0)`,
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing native command marker npm run desired/);
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

test("semantic environment evidence must be attached to its native command", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  const environments = manifest["semanticEnvironments"];
  if (!Array.isArray(environments) || !isRecord(environments[0])) {
    throw new Error("invalid environment fixture");
  }
  environments[0]["env"] = { ADAPTER_TEST_DB: "mysql" };
  await writeJson(root, "alpha/adapterctl.json", manifest);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    `      - run: ${process.execPath} -e process.exit(0)`,
    "      - run: echo unrelated",
    "        env:",
    "          ADAPTER_TEST_DB: mysql",
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing environment marker ADAPTER_TEST_DB=mysql/);
});

test("semantic environment keys must coexist in one concrete matrix row", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  const environments = manifest["semanticEnvironments"];
  if (!Array.isArray(environments) || !isRecord(environments[0])) {
    throw new Error("invalid environment fixture");
  }
  environments[0]["env"] = { RUBY_VERSION: "3.2", ACTIVERECORD_VERSION: "7.1" };
  await writeJson(root, "alpha/adapterctl.json", manifest);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    strategy:",
    "      matrix:",
    "        include:",
    "          - ruby: '3.2'",
    "            activerecord: '8.0'",
    "          - ruby: '3.4'",
    "            activerecord: '7.1'",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    `      - run: ${process.execPath} -e process.exit(0)`,
    "        env:",
    "          RUBY_VERSION: ${{ matrix.ruby }}",
    "          ACTIVERECORD_VERSION: ${{ matrix.activerecord }}",
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing environment marker/);
});

test("matrix conditions cannot certify rows they skip", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const manifest = await readJson(root, "alpha/adapterctl.json");
  const environments = manifest["semanticEnvironments"];
  if (!Array.isArray(environments) || !isRecord(environments[0])) {
    throw new Error("invalid environment fixture");
  }
  environments[0]["env"] = { ADAPTER_TEST_DB: "mysql" };
  await writeJson(root, "alpha/adapterctl.json", manifest);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    strategy:",
    "      matrix:",
    "        database: [postgres, mysql]",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    "      - if: matrix.database == 'postgres'",
    `        run: ${process.execPath} -e process.exit(0)`,
    "        env:",
    "          ADAPTER_TEST_DB: ${{ matrix.database }}",
    "",
  ].join("\n"));

  const result = await runCli(root, ["validate", "--discovery"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /missing environment marker ADAPTER_TEST_DB=mysql/);
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

test("an action-scoped run executes an unassessed discovery action", async () => {
  const root = await createRoot();
  await writeValidRepository(root, { read: { status: "unassessed" } });
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
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
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
  assert.match(
    dryRun.stdout,
    /env ADAPTER_TEST_DB=mysql ADAPTERCTL_ACTION=read --/,
  );

  const executed = await runCli(root, [
    "run", "--adapter", "alpha", "--profile", "conformance",
    "--environment", "default", "--action", "read",
  ]);
  assert.equal(executed.code, 0, executed.stderr);
  assert.match(executed.stdout, /mysql\/read/);
});

test("scaffold creates a safe unassessed manifest and refuses existing targets", async () => {
  const root = await createRoot();
  await writeJson(root, "conformance/catalog.json", {
    schemaVersion: 1,
    actions: [{ name: "read", oracleExpectation: { kind: "proper-subset" } }],
  });

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
  assert.deepEqual(manifest["outcomes"], { read: { status: "unassessed" } });
  assert.deepEqual(manifest["consumer"], { coverage: "usage-only" });

  const duplicate = await runCli(root, [
    "scaffold", "beta", "--ecosystem", "npm", "--package", "@example/beta",
  ]);
  assert.equal(duplicate.code, 1);
  assert.match(duplicate.stderr, /target already exists: beta/);
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

test("run completes the selected matrix before reporting command failures", async () => {
  const root = await createRoot();
  await writeValidRepository(root);
  const alpha = await readJson(root, "alpha/adapterctl.json");
  alpha["semanticEnvironments"] = [{
    name: "default",
    command: [process.execPath, "-e", "process.exit(7)"],
    env: {},
  }];
  await writeJson(root, "alpha/adapterctl.json", alpha);

  const beta = structuredClone(alpha);
  beta["adapter"] = "beta";
  beta["package"] = { ecosystem: "test", name: "beta" };
  beta["workflow"] = ".github/workflows/beta.yaml";
  beta["semanticEnvironments"] = [{
    name: "default",
    command: [
      process.execPath,
      "-e",
      "require('node:fs').writeFileSync('matrix-complete','yes')",
    ],
    env: {},
  }];
  await writeJson(root, "beta/adapterctl.json", beta);
  await writeText(root, ".github/workflows/alpha.yaml", [
    "on:",
    "  push:",
    "    paths: [alpha/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: alpha",
    `      - run: ${process.execPath} -e process.exit(0)`,
    `      - run: ${process.execPath} -e process.exit(7)`,
    "",
  ].join("\n"));
  await writeText(root, ".github/workflows/beta.yaml", [
    "on:",
    "  push:",
    "    paths: [beta/**, conformance/**, demo/**]",
    "jobs:",
    "  test:",
    "    steps:",
    "      - uses: ./.github/actions/validate-adapterctl",
    "        with:",
    "          adapter: beta",
    `      - run: ${process.execPath} -e process.exit(0)`,
    `      - run: ${process.execPath} -e require('node:fs').writeFileSync('matrix-complete','yes')`,
    "",
  ].join("\n"));

  const result = await runCli(root, ["run", "--profile", "conformance", "--action", "read"]);

  assert.equal(result.code, 1);
  assert.match(result.stderr, /alpha\/default: command exited with status 7/);
  assert.equal(await readFile(join(root, "beta/matrix-complete"), "utf8"), "yes");
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

test("validation rejects unknown catalog references", async () => {
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
});
