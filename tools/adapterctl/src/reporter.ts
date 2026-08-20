import type { CatalogAction, ControlPlane, Manifest, Outcome } from "./model.ts";

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

export type AdapterExplanation = {
  package: Manifest["package"];
  workflow: string;
  commands: Manifest["commands"];
  semanticEnvironments: Manifest["semanticEnvironments"];
  summary: AdapterReport;
};

export type ActionExplanation = {
  action: CatalogAction;
  outcomes: Array<{ adapter: string; outcome: Outcome }>;
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

export function explainAdapter(args: {
  controlPlane: ControlPlane;
  name: string;
}): AdapterExplanation | null {
  const manifest = args.controlPlane.manifests.find((candidate) => candidate.adapter === args.name);
  if (manifest === undefined) return null;
  const summary = summarizeAdapter(
    manifest,
    args.controlPlane.catalog.actions.map((action) => action.name),
  );
  return {
    package: manifest.package,
    workflow: manifest.workflow,
    commands: manifest.commands,
    semanticEnvironments: manifest.semanticEnvironments,
    summary,
  };
}

export function explainAction(args: {
  controlPlane: ControlPlane;
  name: string;
}): ActionExplanation | null {
  const action = args.controlPlane.catalog.actions.find((candidate) => candidate.name === args.name);
  if (action === undefined) return null;
  return {
    action,
    outcomes: args.controlPlane.manifests.map((manifest) => ({
      adapter: manifest.adapter,
      outcome: manifest.outcomes.get(args.name) ?? { kind: "unassessed" },
    })),
  };
}

function formatCommand(command: Manifest["commands"]["test"]): string {
  return command.kind === "unavailable" ? "unavailable" : command.arguments.join(" ");
}

function formatEnv(env: Record<string, string>): string {
  const entries = Object.entries(env).sort(([left], [right]) => left.localeCompare(right));
  return entries.length === 0 ? "" : ` [${entries.map(([key, value]) => `${key}=${value}`).join(", ")}]`;
}
