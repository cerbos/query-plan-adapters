import type { ControlPlane, Manifest, Outcome } from "./model.ts";

export type AdapterReport = {
  adapter: string;
  matched: number;
  rejected: number;
  upstreamBlocked: number;
  unassessed: number;
  safetyAccounted: number;
  capabilitySupported: number;
  consumerCoverage: "artifact-install" | "usage-only";
  environments: string[];
};

export type CertificationReport = {
  schemaVersion: 1;
  actions: number;
  adapters: AdapterReport[];
};

export function buildReport(controlPlane: ControlPlane): CertificationReport {
  const actions = controlPlane.catalog.actions.map((action) => action.name);
  return {
    schemaVersion: 1,
    actions: actions.length,
    adapters: controlPlane.manifests.map((manifest) => summarizeAdapter(manifest, actions)),
  };
}

function summarizeAdapter(manifest: Manifest, actions: string[]): AdapterReport {
  const counts = { matched: 0, rejected: 0, upstreamBlocked: 0, unassessed: 0 };
  for (const action of actions) {
    const outcome = manifest.outcomes.get(action) ?? { kind: "unassessed" };
    switch (outcome.kind) {
      case "matched":
        counts.matched += 1;
        break;
      case "rejected":
        counts.rejected += 1;
        break;
      case "upstream-blocked":
        counts.upstreamBlocked += 1;
        break;
      case "unassessed":
        counts.unassessed += 1;
        break;
      default: {
        const exhaustive: never = outcome;
        void exhaustive;
      }
    }
  }
  return {
    adapter: manifest.adapter,
    ...counts,
    safetyAccounted: actions.length - counts.unassessed,
    capabilitySupported: counts.matched,
    consumerCoverage: manifest.consumer.coverage,
    environments: manifest.semanticEnvironments.map((environment) => environment.name),
  };
}

export function reportMarkdown(controlPlane: ControlPlane): string {
  const report = buildReport(controlPlane);
  const lines = [
    "# Adapter certification",
    "",
    `Actions: ${report.actions}`,
    "",
    "| Adapter | Matched | Rejected | Upstream blocked | Unassessed | Safety accounted | Capability supported | Consumer coverage | Environments |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ...report.adapters.map((adapter) =>
      `| ${adapter.adapter} | ${adapter.matched} | ${adapter.rejected} | ${adapter.upstreamBlocked} | ${adapter.unassessed} | ${adapter.safetyAccounted} | ${adapter.capabilitySupported} | ${adapter.consumerCoverage} | ${adapter.environments.join(", ")} |`
    ),
  ];
  for (const manifest of controlPlane.manifests) {
    lines.push(
      "",
      `## ${manifest.adapter}`,
      "",
      `Package: ${manifest.package.ecosystem}:${manifest.package.name}`,
      `Workflow: ${manifest.workflow}`,
      "",
      "Commands:",
      `- test: ${formatCommand(manifest.commands.test)}`,
      `- typecheck: ${formatCommand(manifest.commands.typecheck)}`,
      `- consumer: ${formatCommand(manifest.commands.consumer)}`,
      `- golden: ${formatCommand(manifest.commands.golden)}`,
      "",
      "Semantic environments:",
      ...manifest.semanticEnvironments.map((environment) =>
        `- ${environment.name}: ${environment.command.arguments.join(" ")}${formatEnv(environment.env)}`
      ),
    );
    if (manifest.semanticEnvironments.length === 0) lines.push("- none");
  }
  return `${lines.join("\n")}\n`;
}

export function explainAdapter(controlPlane: ControlPlane, name: string): string[] {
  const manifest = controlPlane.manifests.find((candidate) => candidate.adapter === name);
  if (manifest === undefined) return [];
  const summary = summarizeAdapter(
    manifest,
    controlPlane.catalog.actions.map((action) => action.name),
  );
  return [
    `Adapter: ${manifest.adapter}`,
    `Package: ${manifest.package.ecosystem}:${manifest.package.name}`,
    `Workflow: ${manifest.workflow}`,
    `Consumer coverage: ${manifest.consumer.coverage}`,
    `Outcomes: ${summary.matched} matched, ${summary.rejected} rejected, ${summary.upstreamBlocked} upstream-blocked, ${summary.unassessed} unassessed`,
    `Environments: ${summary.environments.join(", ") || "none"}`,
  ];
}

export function explainAction(controlPlane: ControlPlane, name: string): string[] {
  const action = controlPlane.catalog.actions.find((candidate) => candidate.name === name);
  if (action === undefined) return [];
  const lines = [
    `Action: ${action.name}`,
    `Oracle expectation: ${action.oracleExpectation.kind}${"reason" in action.oracleExpectation ? ` — ${action.oracleExpectation.reason}` : ""}`,
    "Adapter outcomes:",
  ];
  for (const manifest of controlPlane.manifests) {
    const outcome = manifest.outcomes.get(name) ?? { kind: "unassessed" };
    lines.push(`- ${manifest.adapter}: ${formatOutcome(outcome)}`);
  }
  return lines;
}

function formatOutcome(outcome: Outcome): string {
  switch (outcome.kind) {
    case "matched":
    case "unassessed":
      return outcome.kind;
    case "rejected":
      return `rejected — ${outcome.reason} (message: ${outcome.message})`;
    case "upstream-blocked":
      return `upstream-blocked — ${outcome.reason}`;
    default: {
      const exhaustive: never = outcome;
      return exhaustive;
    }
  }
}

function formatCommand(command: Manifest["commands"]["test"]): string {
  return command.kind === "unavailable" ? "unavailable" : command.arguments.join(" ");
}

function formatEnv(env: Record<string, string>): string {
  const entries = Object.entries(env).sort(([left], [right]) => left.localeCompare(right));
  return entries.length === 0 ? "" : ` [${entries.map(([key, value]) => `${key}=${value}`).join(", ")}]`;
}
