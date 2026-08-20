import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

import { decodeManifest } from "./decode.ts";
import { validateCanonicalResources } from "./canonical-resources.ts";
import { CliError, ValidationError } from "./errors.ts";
import type { ControlPlane, Manifest } from "./model.ts";
import { isRecord } from "./records.ts";
import { SchemaValidator } from "./schema-validator.ts";
import { validateSemantics } from "./validation.ts";

export type ValidationResult = { notices: string[]; controlPlane: ControlPlane };

type ValidationOptions = {
  discovery: boolean;
  adapter: string | null;
};

async function readJson(path: string): Promise<unknown> {
  const source = await readFile(path, "utf8");
  const parsed: unknown = JSON.parse(source);
  return parsed;
}

export class Repository {
  readonly root: string;

  constructor(root: string) {
    this.root = root;
  }

  async list(): Promise<string[]> {
    const paths = await this.manifestPaths();
    const names: string[] = [];
    for (const path of paths) {
      const document = await readJson(join(this.root, path));
      if (!isRecord(document) || typeof document["adapter"] !== "string") {
        throw new CliError({ message: `${path}: expected string adapter`, exitCode: 2 });
      }
      names.push(document["adapter"]);
    }
    return names.sort();
  }

  async load(adapter: string | null): Promise<ControlPlane> {
    const schema = SchemaValidator.create();
    const catalogValue = await readJson(join(this.root, "conformance/catalog.json"));
    const catalog = schema.validate({
      name: "catalog",
      path: "conformance/catalog.json",
      value: catalogValue,
    });
    const casesValue = await readJson(join(this.root, "demo/cases.json"));
    const cases = schema.validate({
      name: "consumer-cases",
      path: "demo/cases.json",
      value: casesValue,
    });
    const resourcesValue = await readJson(join(this.root, "conformance/check-resources.json"));
    const checkResources = schema.validate({
      name: "check-resources",
      path: "conformance/check-resources.json",
      value: resourcesValue,
    });
    const paths = await this.manifestPaths();
    const selectedPaths = adapter === null ? paths : paths.filter(
      (path) => path === `${adapter}/adapterctl.json`,
    );
    if (adapter !== null && selectedPaths.length === 0) {
      throw new CliError({ message: `unknown adapter: ${adapter}`, exitCode: 64 });
    }
    const manifests: Manifest[] = [];
    for (const path of selectedPaths) {
      const value = await readJson(join(this.root, path));
      const document = schema.validate({ name: "manifest", path, value });
      const manifest = decodeManifest({ document });
      const directory = path.split("/")[0];
      if (directory !== manifest.adapter) {
        throw new ValidationError([
          `${path}.adapter: expected ${directory ?? "adapter directory"}, got ${manifest.adapter}`,
        ]);
      }
      manifests.push(manifest);
    }
    manifests.sort((left, right) => left.adapter.localeCompare(right.adapter));
    return { catalog, cases, checkResources, manifests };
  }

  async validate(options: ValidationOptions): Promise<ValidationResult> {
    const controlPlane = await this.load(options.adapter);
    const errors = await validateSemantics({
      root: this.root,
      catalog: controlPlane.catalog,
      cases: controlPlane.cases,
      manifests: controlPlane.manifests,
    });
    if (!options.discovery && options.adapter === null && controlPlane.manifests.length === 0) {
      errors.push("strict certification requires at least one adapter");
    }
    const seeds = await readJson(join(this.root, "conformance/seeds.json"));
    const derived = await readJson(join(this.root, "conformance/derived-fields.json"));
    errors.push(...validateCanonicalResources({
      checkResources: controlPlane.checkResources,
      seedsValue: seeds,
      derivedValue: derived,
    }));
    const notices: string[] = [];
    const actionNames = controlPlane.catalog.actions.map((action) => action.name);
    for (const manifest of controlPlane.manifests) {
      if (!options.discovery && manifest.semanticEnvironments.length === 0) {
        errors.push(`${manifest.adapter}: strict certification requires a semantic environment`);
      }
      for (const action of actionNames) {
        const outcome = manifest.outcomes.get(action);
        if (outcome === undefined || outcome.kind === "unassessed") {
          notices.push(`${manifest.adapter}: action ${action} is unassessed`);
        }
      }
    }
    if (!options.discovery) errors.push(...notices);
    if (errors.length > 0) throw new ValidationError(errors);
    return { notices, controlPlane };
  }

  async rawJson(relativePath: string): Promise<unknown> {
    return await readJson(join(this.root, relativePath));
  }

  private async manifestPaths(): Promise<string[]> {
    const entries = await readdir(this.root, { withFileTypes: true });
    const paths: string[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const relative = `${entry.name}/adapterctl.json`;
      try {
        await readFile(join(this.root, relative), "utf8");
        paths.push(relative);
      } catch (error: unknown) {
        if (isRecord(error) && error["code"] === "ENOENT") continue;
        throw error;
      }
    }
    return paths.sort();
  }
}
