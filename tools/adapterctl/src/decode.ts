import type { Command, Manifest, Outcome } from "./model.ts";
import type { ManifestDocument } from "./schemas.ts";

function decodeCommand(document: ManifestDocument["commands"]["test"]): Command {
  if (document === null) return { kind: "unavailable" };
  const [executable, ...arguments_] = document;
  if (executable === undefined) {
    throw new Error("validated command document must contain an executable");
  }
  return { kind: "command", arguments: [executable, ...arguments_] };
}

function decodeOutcome(document: ManifestDocument["outcomes"][string]): Outcome {
  switch (document.status) {
    case "matched":
      return { kind: "matched" };
    case "rejected":
      return { kind: "rejected", reason: document.reason, message: document.message };
    case "upstream-blocked":
      return { kind: "upstream-blocked", reason: document.reason };
    case "unassessed":
      return { kind: "unassessed" };
    default: {
      const exhaustive: never = document;
      return exhaustive;
    }
  }
}

export function decodeManifest(args: { document: ManifestDocument }): Manifest {
  const outcomes = new Map<string, Outcome>();
  for (const [action, document] of Object.entries(args.document.outcomes)) {
    outcomes.set(action, decodeOutcome(document));
  }
  return {
    schemaVersion: args.document.schemaVersion,
    adapter: args.document.adapter,
    package: args.document.package,
    workflow: args.document.workflow,
    commands: {
      test: decodeCommand(args.document.commands.test),
      typecheck: decodeCommand(args.document.commands.typecheck),
      golden: decodeCommand(args.document.commands.golden),
      consumer: decodeCommand(args.document.commands.consumer),
    },
    semanticEnvironments: args.document.semanticEnvironments.map((environment) => {
      const command = decodeCommand(environment.command);
      if (command.kind === "unavailable") {
        throw new Error("validated semantic environment command must be available");
      }
      return { ...environment, command };
    }),
    consumer: args.document.consumer,
    outcomes,
  };
}
