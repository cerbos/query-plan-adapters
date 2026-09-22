// Verify the responding PDP, not just the binary that the launcher found on PATH.
const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { GRPC } = require("@cerbos/grpc");

async function verify() {
  const address = process.env.CERBOS_GRPC;
  assert.ok(address?.startsWith("unix:"), "Expected a private CERBOS_GRPC socket from run-adversarial.sh");
  const strict = process.env.ADAPTER_TEST_STRICT_EVALUATION;
  assert.ok(strict === "true" || strict === "false", "Expected an explicit evaluation mode");
  const corpus = resolve(__dirname, "../../conformance");
  const expectedVersion = readFileSync(resolve(corpus, "CERBOS_VERSION"), "utf8").trim();
  const { hostile } = JSON.parse(readFileSync(resolve(corpus, "evaluation-modes/principals.json"), "utf8"));
  const cerbos = new GRPC(address, { tls: false });
  try {
    const options = { signal: AbortSignal.timeout(5000) };
    const info = await cerbos.serverInfo(options);
    assert.equal(info.version, expectedVersion, "PDP version does not match conformance/CERBOS_VERSION");
    // An erroring DENY overrides ALLOW only in strict mode. The unrelated action is a
    // positive control: a missing policy must not masquerade as strict evaluation.
    const result = await cerbos.checkResources({
      principal: hostile,
      resources: [{
        resource: { kind: "evaluation_probe", id: "startup" },
        actions: ["missing-deny", "unrelated"],
      }],
    }, options);
    assert.equal(result.isAllowed({ resource: { kind: "evaluation_probe", id: "startup" }, action: "unrelated" }), true,
      "PDP is not serving the conformance evaluation probe");
    assert.equal(result.isAllowed({ resource: { kind: "evaluation_probe", id: "startup" }, action: "missing-deny" }), strict === "false",
      `PDP evaluation mode does not match ADAPTER_TEST_STRICT_EVALUATION=${strict}`);
  } finally {
    cerbos.close();
  }
}

verify().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
