import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { CliError } from "./errors.ts";
import { isRecord } from "./records.ts";
import { SchemaValidator } from "./schema-validator.ts";

export type ScaffoldOptions = {
  name: string;
  ecosystem: string;
  packageName: string;
  coverage: "artifact-install" | "usage-only";
  dryRun: boolean;
};

export type ScaffoldResult = {
  status: "planned" | "written";
  adapter: string;
  manifestPath: string;
  workflow: string;
};

export async function scaffold(args: {
  root: string;
  options: ScaffoldOptions;
}): Promise<ScaffoldResult> {
  const { options } = args;
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(options.name)) {
    throw new CliError({
      message: "scaffold name must be a lowercase kebab-case adapter identifier",
      exitCode: 64,
    });
  }
  if (options.ecosystem.length === 0 || options.packageName.length === 0) {
    throw new CliError({ message: "scaffold ecosystem and package must be nonempty", exitCode: 64 });
  }
  const target = join(args.root, options.name);
  if (await exists(target)) {
    throw new CliError({ message: `target already exists: ${options.name}` });
  }
  const catalogPath = join(args.root, "conformance/catalog.json");
  const catalogValue: unknown = JSON.parse(await readFile(catalogPath, "utf8"));
  const catalog = SchemaValidator.create().validate({
    name: "catalog",
    path: "conformance/catalog.json",
    value: catalogValue,
  });
  const outcomes = Object.fromEntries(
    catalog.actions.map((action) => [action.name, { status: "unassessed" }]),
  );
  const manifest = {
    schemaVersion: 1,
    adapter: options.name,
    package: { ecosystem: options.ecosystem, name: options.packageName },
    workflow: `.github/workflows/${options.name}.yaml`,
    commands: { test: null, typecheck: null, golden: null, consumer: null },
    semanticEnvironments: [],
    consumer: { coverage: options.coverage },
    outcomes,
  };
  const path = `${options.name}/adapterctl.json`;
  if (!options.dryRun) {
    await mkdir(target);
    await writeFile(join(target, "adapterctl.json"), `${JSON.stringify(manifest, null, 2)}\n`, {
      flag: "wx",
    });
  }
  return {
    status: options.dryRun ? "planned" : "written",
    adapter: options.name,
    manifestPath: path,
    workflow: manifest.workflow,
  };
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
