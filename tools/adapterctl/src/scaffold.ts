import { mkdir, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { isRecord } from "./decode.ts";
import { CliError } from "./errors.ts";

export type ScaffoldOptions = {
  name: string;
  ecosystem: string;
  packageName: string;
  coverage: "artifact-install" | "usage-only";
  dryRun: boolean;
};

export async function scaffold(root: string, options: ScaffoldOptions): Promise<string[]> {
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(options.name)) {
    throw new CliError("scaffold name must be a lowercase kebab-case adapter identifier", 64);
  }
  if (options.ecosystem.length === 0 || options.packageName.length === 0) {
    throw new CliError("scaffold ecosystem and package must be nonempty", 64);
  }
  const target = join(root, options.name);
  if (await exists(target)) throw new CliError(`target already exists: ${options.name}`);
  const manifest = {
    schemaVersion: 1,
    adapter: options.name,
    package: { ecosystem: options.ecosystem, name: options.packageName },
    workflow: `.github/workflows/${options.name}.yaml`,
    commands: { test: null, typecheck: null, golden: null, consumer: null },
    semanticEnvironments: [],
    consumer: { coverage: options.coverage },
    outcomes: {},
  };
  const path = `${options.name}/adapterctl.json`;
  if (!options.dryRun) {
    await mkdir(target);
    await writeFile(join(target, "adapterctl.json"), `${JSON.stringify(manifest, null, 2)}\n`, {
      flag: "wx",
    });
  }
  return [
    `${options.dryRun ? "would write" : "wrote"} ${path}`,
    "Next steps:",
    "- set commands.test to the native translator test command",
    "- add each native semantic environment and its array command",
    "- set commands.consumer to the native consumer smoke command",
    `- create ${manifest.workflow} with adapter, conformance, and demo path triggers`,
    "- run ./adapterctl validate --discovery --adapter " + options.name,
  ];
}

async function exists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error: unknown) {
    if (isRecord(error) && error["code"] === "ENOENT") return false;
    throw error;
  }
}
