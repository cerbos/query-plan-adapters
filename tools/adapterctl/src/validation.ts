import { readFile } from "node:fs/promises";
import { join, resolve, sep } from "node:path";
import { isDeepStrictEqual } from "node:util";

import { parseDocument } from "yaml";

import { isRecord } from "./decode.ts";
import type { Catalog, ConsumerCases, Manifest } from "./model.ts";

export async function validateSemantics(args: {
  root: string;
  catalog: Catalog;
  cases: ConsumerCases;
  manifests: Manifest[];
}): Promise<string[]> {
  const errors: string[] = [];
  validateCatalog(args.catalog, errors);
  await validatePolicyActions(args.root, args.catalog, errors);
  validateCases(args.cases, errors);
  for (const manifest of args.manifests) {
    validateManifestReferences(manifest, args.catalog, errors);
    await validateWorkflow(args.root, manifest, errors);
  }
  return errors;
}

function validateCatalog(catalog: Catalog, errors: string[]): void {
  const names = catalog.actions.map((action) => action.name);
  for (const duplicate of duplicates(names)) {
    errors.push(`conformance/catalog.json.actions: duplicate action ${duplicate}`);
  }
  if (!isDeepStrictEqual(names, [...names].sort())) {
    errors.push("conformance/catalog.json.actions: actions must be sorted by name");
  }
}

async function validatePolicyActions(root: string, catalog: Catalog, errors: string[]): Promise<void> {
  const source = await readFile(join(root, "conformance/policies/adversarial.yaml"), "utf8");
  const document = parseDocument(source, {
    strict: true,
    uniqueKeys: true,
    stringKeys: true,
    version: "1.2",
  });
  if (document.errors.length > 0) {
    errors.push(...document.errors.map((error) => `conformance/policies/adversarial.yaml: ${error.message}`));
    return;
  }
  const value: unknown = document.toJS();
  const actions = policyActions(value, errors);
  for (const duplicate of duplicates(actions)) {
    errors.push(`policy action appears more than once: ${duplicate}`);
  }
  const catalogActions = catalog.actions.map((action) => action.name);
  const policyOnly = difference(actions, catalogActions);
  const catalogOnly = difference(catalogActions, actions);
  if (policyOnly.length > 0) errors.push(`policy actions missing from catalog: ${policyOnly.join(", ")}`);
  if (catalogOnly.length > 0) errors.push(`catalog actions missing from policies: ${catalogOnly.join(", ")}`);
}

function policyActions(value: unknown, errors: string[]): string[] {
  if (!isRecord(value) || !isRecord(value["resourcePolicy"])) {
    errors.push("conformance/policies/adversarial.yaml: expected resourcePolicy object");
    return [];
  }
  const rules = value["resourcePolicy"]["rules"];
  if (!Array.isArray(rules)) {
    errors.push("conformance/policies/adversarial.yaml: expected resourcePolicy.rules array");
    return [];
  }
  const actions: string[] = [];
  for (const [index, rule] of rules.entries()) {
    if (!isRecord(rule) || !Array.isArray(rule["actions"]) ||
        !rule["actions"].every((action) => typeof action === "string" && action.length > 0)) {
      errors.push(`conformance/policies/adversarial.yaml.rules[${index}].actions: expected string array`);
      continue;
    }
    for (const action of rule["actions"]) {
      if (typeof action === "string") actions.push(action);
    }
  }
  return actions;
}

function validateCases(cases: ConsumerCases, errors: string[]): void {
  const ids: string[] = [];
  for (const [index, consumerCase] of cases.cases.entries()) {
    ids.push(consumerCase.id);
    const expectedId = `${consumerCase.operation}/${consumerCase.principal}/${consumerCase.action}`;
    if (consumerCase.id !== expectedId) {
      errors.push(`demo/cases.json.cases[${index}].id: expected ${expectedId}`);
    }
    if (consumerCase.operation === "paginated") {
      const pagination = consumerCase.pagination;
      if (pagination === null) {
        errors.push(`demo/cases.json.cases[${index}].pagination: required for paginated`);
      } else if (pagination.pageSizes.some((size) => size > pagination.pageSize)) {
        errors.push(`demo/cases.json.cases[${index}].pagination: page size exceeded`);
      }
    } else if (consumerCase.pagination !== null) {
      errors.push(`demo/cases.json.cases[${index}].pagination: must be null for ${consumerCase.operation}`);
    }
    const expectedIds = consumerCase.expected.ids;
    if (!isDeepStrictEqual(expectedIds, [...new Set(expectedIds)].sort())) {
      errors.push(`demo/cases.json.cases[${index}].expected.ids: expected sorted unique ids`);
    }
  }
  for (const duplicate of duplicates(ids)) errors.push(`demo/cases.json.cases: duplicate id ${duplicate}`);
}

function validateManifestReferences(manifest: Manifest, catalog: Catalog, errors: string[]): void {
  const actionNames = new Set(catalog.actions.map((action) => action.name));
  for (const action of manifest.outcomes.keys()) {
    if (!actionNames.has(action)) {
      errors.push(`${manifest.adapter}/adapterctl.json.outcomes: unknown action ${action}`);
    }
  }
  for (const duplicate of duplicates(manifest.semanticEnvironments.map((environment) => environment.name))) {
    errors.push(`${manifest.adapter}/adapterctl.json.semanticEnvironments: duplicate ${duplicate}`);
  }
}

async function validateWorkflow(root: string, manifest: Manifest, errors: string[]): Promise<void> {
  const workflowPath = resolve(root, manifest.workflow);
  if (!workflowPath.startsWith(`${resolve(root)}${sep}`)) {
    errors.push(`${manifest.adapter}/adapterctl.json.workflow: path escapes repository root`);
    return;
  }
  const workflowSource = await readOptionalFile(workflowPath);
  if (workflowSource === null) {
    errors.push(`${manifest.adapter}/adapterctl.json.workflow: file not found: ${manifest.workflow}`);
    return;
  }
  const evidence = parseWorkflowEvidence(workflowSource, manifest.workflow, errors);
  if (evidence === null) return;
  for (const marker of [`${manifest.adapter}/**`, "conformance/**", "demo/**"]) {
    if (!evidence.paths.includes(marker)) {
      errors.push(`${manifest.workflow}: missing path trigger ${marker}`);
    }
  }
  const declaredSources = await readDeclaredSources(root, manifest, errors);
  const directCommands = [
    manifest.commands.test,
    manifest.commands.typecheck,
    manifest.commands.consumer,
  ];
  for (const command of directCommands) {
    if (command.kind === "unavailable") continue;
    validateCommandMarker(evidence, declaredSources, command.arguments, manifest, errors);
  }
  for (const environment of manifest.semanticEnvironments) {
    validateCommandMarker(evidence, declaredSources, environment.command.arguments, manifest, errors);
  }
  for (const environment of manifest.semanticEnvironments) {
    for (const [key, value] of Object.entries(environment.env)) {
      if (value.length === 0) continue;
      if (!hasEnvironmentEvidence(evidence, key, value)) {
        errors.push(`${manifest.workflow}: missing environment marker ${key}=${value}`);
      }
    }
  }
}

type WorkflowEvidence = {
  paths: string[];
  runCommands: string[];
  expandedRunCommands: string[];
  matrix: Map<string, string[]>;
  environment: Map<string, string[]>;
};

type DeclaredSources = {
  packageScripts: string[];
  wrappers: Array<{ path: string; source: string }>;
};

function parseWorkflowEvidence(
  source: string,
  workflowPath: string,
  errors: string[],
): WorkflowEvidence | null {
  const document = parseDocument(source, {
    strict: true,
    uniqueKeys: true,
    stringKeys: true,
    version: "1.2",
  });
  if (document.errors.length > 0) {
    errors.push(...document.errors.map((error) => `${workflowPath}: ${error.message}`));
    return null;
  }
  const value: unknown = document.toJS();
  if (!isRecord(value)) {
    errors.push(`${workflowPath}: expected workflow object`);
    return null;
  }
  const paths = workflowPaths(value["on"]);
  const runCommands: string[] = [];
  const matrix = new Map<string, string[]>();
  const environment = new Map<string, string[]>();
  collectEnvironment(value["env"], environment);
  const jobs = value["jobs"];
  if (isRecord(jobs)) {
    for (const job of Object.values(jobs)) {
      if (!isRecord(job)) continue;
      collectEnvironment(job["env"], environment);
      if (isRecord(job["strategy"]) && isRecord(job["strategy"]["matrix"])) {
        collectMatrix(job["strategy"]["matrix"], "", matrix);
      }
      const steps = job["steps"];
      if (!Array.isArray(steps)) continue;
      for (const step of steps) {
        if (!isRecord(step)) continue;
        if (typeof step["run"] === "string") runCommands.push(step["run"]);
        collectEnvironment(step["env"], environment);
      }
    }
  }
  return {
    paths,
    runCommands,
    expandedRunCommands: expandMatrixCommands(runCommands, matrix),
    matrix,
    environment,
  };
}

function workflowPaths(triggers: unknown): string[] {
  if (!isRecord(triggers)) return [];
  const paths: string[] = [];
  for (const trigger of Object.values(triggers)) {
    if (!isRecord(trigger) || !Array.isArray(trigger["paths"])) continue;
    for (const path of trigger["paths"]) {
      if (typeof path === "string") paths.push(path);
    }
  }
  return paths;
}

function collectMatrix(value: unknown, prefix: string, matrix: Map<string, string[]>): void {
  if (Array.isArray(value)) {
    for (const item of value) collectMatrix(item, prefix, matrix);
    return;
  }
  if (isRecord(value)) {
    for (const [key, nested] of Object.entries(value)) {
      const path = prefix.length === 0 || prefix === "include" || prefix === "exclude" ? key : `${prefix}.${key}`;
      collectMatrix(nested, path, matrix);
    }
    return;
  }
  if (prefix.length === 0) return;
  const scalar = scalarString(value);
  if (scalar === null) return;
  const values = matrix.get(prefix) ?? [];
  if (!values.includes(scalar)) values.push(scalar);
  matrix.set(prefix, values);
}

function collectEnvironment(value: unknown, environment: Map<string, string[]>): void {
  if (!isRecord(value)) return;
  for (const [key, nested] of Object.entries(value)) {
    const scalar = scalarString(nested);
    if (scalar === null) continue;
    const values = environment.get(key) ?? [];
    if (!values.includes(scalar)) values.push(scalar);
    environment.set(key, values);
  }
}

function scalarString(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

function expandMatrixCommands(commands: string[], matrix: Map<string, string[]>): string[] {
  let expanded = [...commands];
  for (const [key, values] of matrix) {
    const expression = new RegExp(`\\$\\{\\{\\s*matrix\\.${escapeRegExp(key)}\\s*\\}\\}`, "g");
    const next: string[] = [];
    for (const command of expanded) {
      if (!expression.test(command)) {
        next.push(command);
        expression.lastIndex = 0;
        continue;
      }
      expression.lastIndex = 0;
      for (const value of values) next.push(command.replace(expression, value));
      expression.lastIndex = 0;
    }
    expanded = next;
  }
  return expanded;
}

function validateCommandMarker(
  evidence: WorkflowEvidence,
  declaredSources: DeclaredSources,
  arguments_: [string, ...string[]],
  manifest: Manifest,
  errors: string[],
): void {
  const full = arguments_.join(" ");
  const withoutParent = arguments_.map((argument, index) =>
    index === 0 ? argument.replace(/^\.\.\//, "") : argument
  ).join(" ");
  const npmAlias = arguments_[0] === "npm" && arguments_[1] === "test" ?
    "npm run test" : null;
  const wrapperInner = arguments_[0] === "bash" && arguments_.length > 2 ?
    arguments_.slice(2).join(" ") : null;
  const workflowCommands = evidence.expandedRunCommands.join("\n");
  if (workflowCommands.includes(full) || workflowCommands.includes(withoutParent) ||
      (npmAlias !== null && workflowCommands.includes(npmAlias)) ||
      hasMatrixCommandMarker(evidence, arguments_) ||
      hasTransitiveCommandMarker(
        evidence,
        declaredSources,
        [full, withoutParent, npmAlias, wrapperInner],
      )) return;
  errors.push(`${manifest.workflow}: missing native command marker ${full}`);
}

async function readDeclaredSources(
  root: string,
  manifest: Manifest,
  errors: string[],
): Promise<DeclaredSources> {
  const packageScripts: string[] = [];
  const packageSource = await readOptionalFile(join(root, manifest.adapter, "package.json"));
  if (packageSource !== null) {
    try {
      const packageValue: unknown = JSON.parse(packageSource);
      if (isRecord(packageValue) && isRecord(packageValue["scripts"])) {
        for (const script of Object.values(packageValue["scripts"])) {
          if (typeof script === "string") packageScripts.push(script);
        }
      }
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(`${manifest.adapter}/package.json: ${message}`);
    }
  }
  const wrappers: Array<{ path: string; source: string }> = [];
  for (const environment of manifest.semanticEnvironments) {
    const arguments_ = environment.command.arguments;
    if (arguments_[0] !== "bash") continue;
    const script = arguments_[1];
    if (script === undefined || script.startsWith("-") || script.includes("..")) continue;
    const source = await readOptionalFile(join(root, manifest.adapter, script));
    if (source !== null && !wrappers.some((wrapper) => wrapper.path === script)) {
      wrappers.push({ path: script, source });
    }
  }
  return { packageScripts, wrappers };
}

async function readOptionalFile(path: string): Promise<string | null> {
  try {
    return await readFile(path, "utf8");
  } catch (error: unknown) {
    if (isRecord(error) && error["code"] === "ENOENT") return null;
    throw error;
  }
}

function duplicates(values: string[]): string[] {
  const counts = new Map<string, number>();
  for (const value of values) counts.set(value, (counts.get(value) ?? 0) + 1);
  return [...counts.entries()].filter(([, count]) => count > 1).map(([value]) => value).sort();
}

function difference(left: string[], right: string[]): string[] {
  const rightSet = new Set(right);
  return [...new Set(left.filter((value) => !rightSet.has(value)))].sort();
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function hasMatrixCommandMarker(evidence: WorkflowEvidence, arguments_: [string, ...string[]]): boolean {
  const numericValues = arguments_.flatMap((argument) => argument.match(/\d+(?:\.\d+)*/g) ?? []);
  const generalized = arguments_.map((argument) => argument.replace(/\d+(?:\.\d+)*/g, ""));
  const significant = generalized.filter((argument) => argument.length > 0).join(" ");
  const matrixValues = [...evidence.matrix.values()].flat();
  return significant.length > 0 && evidence.runCommands.some((command) => command.includes(significant)) &&
    numericValues.every((value) => matrixValues.includes(value));
}

function hasTransitiveCommandMarker(
  evidence: WorkflowEvidence,
  declaredSources: DeclaredSources,
  variants: Array<string | null>,
): boolean {
  const invokedWrappers = declaredSources.wrappers.filter((wrapper) =>
    evidence.expandedRunCommands.some((command) => command.includes(wrapper.path))
  );
  if (invokedWrappers.length === 0) return false;
  const auxiliarySource = [
    ...declaredSources.packageScripts,
    ...invokedWrappers.map((wrapper) => wrapper.source),
  ].join("\n");
  return variants.some((variant) => variant !== null && auxiliarySource.includes(variant));
}

function hasEnvironmentEvidence(evidence: WorkflowEvidence, key: string, expected: string): boolean {
  const actualValues = evidence.environment.get(key) ?? [];
  return actualValues.some((actual) => {
    if (actual === expected) return true;
    for (const [matrixKey, matrixValues] of evidence.matrix) {
      if (actual.includes(`matrix.${matrixKey}`) && matrixValues.includes(expected)) return true;
    }
    return false;
  });
}
