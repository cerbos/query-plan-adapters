import { readFile, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";

import {
  decodeCatalog,
  decodeCheckResources,
  decodeConsumerCases,
  decodeManifest,
  isRecord,
} from "./decode.ts";
import { validateCanonicalResources } from "./canonical-resources.ts";
import { compareConsumerLegacy } from "./consumer-legacy.ts";
import { CliError, ValidationError } from "./errors.ts";
import type { ControlPlane, Manifest } from "./model.ts";
import { compareLegacy } from "./legacy.ts";
import { SchemaValidator } from "./schema-validator.ts";
import { validateSemantics } from "./validation.ts";

export type ValidationResult = { notices: string[]; controlPlane: ControlPlane };

async function readJson(path: string): Promise<unknown> {
  const source = await readFile(path, "utf8");
  const parsed: unknown = JSON.parse(source);
  return parsed;
}

export class Repository {
  readonly root: string;
  readonly toolRoot: string;

  constructor(root: string) {
    this.root = root;
    this.toolRoot = resolve(import.meta.dirname, "..");
  }

  async list(): Promise<string[]> {
    const paths = await this.manifestPaths();
    const names: string[] = [];
    for (const path of paths) {
      const document = await readJson(join(this.root, path));
      if (!isRecord(document) || typeof document["adapter"] !== "string") {
        throw new CliError(`${path}: expected string adapter`, 2);
      }
      names.push(document["adapter"]);
    }
    return names.sort();
  }

  async load(adapter: string | null): Promise<ControlPlane> {
    const schema = await SchemaValidator.create(this.toolRoot);
    const catalogValue = await readJson(join(this.root, "conformance/catalog.json"));
    schema.validate("catalog", "conformance/catalog.json", catalogValue);
    const casesValue = await readJson(join(this.root, "demo/cases.json"));
    schema.validate("consumer-cases", "demo/cases.json", casesValue);
    const resourcesValue = await readJson(join(this.root, "conformance/check-resources.json"));
    schema.validate("check-resources", "conformance/check-resources.json", resourcesValue);
    const catalog = decodeCatalog(catalogValue, "conformance/catalog.json");
    const cases = decodeConsumerCases(casesValue, "demo/cases.json");
    const checkResources = decodeCheckResources(resourcesValue, "conformance/check-resources.json");
    const paths = await this.manifestPaths();
    const selectedPaths = adapter === null ? paths : paths.filter(
      (path) => path === `${adapter}/adapterctl.json`,
    );
    if (adapter !== null && selectedPaths.length === 0) {
      throw new CliError(`unknown adapter: ${adapter}`, 64);
    }
    const manifests: Manifest[] = [];
    for (const path of selectedPaths) {
      const value = await readJson(join(this.root, path));
      schema.validate("manifest", path, value);
      const manifest = decodeManifest(value, path);
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

  async validate(options: { discovery: boolean; adapter: string | null }): Promise<ValidationResult> {
    const controlPlane = await this.load(options.adapter);
    const errors = await validateSemantics({
      root: this.root,
      catalog: controlPlane.catalog,
      cases: controlPlane.cases,
      manifests: controlPlane.manifests,
    });
    const seeds = await readJson(join(this.root, "conformance/seeds.json"));
    const derived = await readJson(join(this.root, "conformance/derived-fields.json"));
    errors.push(...validateCanonicalResources(controlPlane.checkResources, seeds, derived));
    const legacy = await readJson(join(this.root, "conformance/actions.json"));
    errors.push(...compareLegacy({
      catalog: controlPlane.catalog,
      manifests: controlPlane.manifests,
      legacy,
      discovery: options.discovery,
      exactAdapterSet: options.adapter === null,
    }));
    const legacyCases = await readJson(join(this.root, "demo/expected.json"));
    errors.push(...compareConsumerLegacy(controlPlane.cases, legacyCases));
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
