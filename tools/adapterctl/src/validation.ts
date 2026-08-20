import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";

import { parseDocument } from "yaml";

import type { Catalog, ConsumerCases, Manifest } from "./model.ts";
import { isRecord } from "./records.ts";
import { validateWorkflow } from "./workflow-validation.ts";

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
    await validateWorkflow({ root: args.root, manifest, errors });
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
      if (consumerCase.pagination.pageSizes.some(
        (size) => size > consumerCase.pagination.pageSize,
      )) {
        errors.push(`demo/cases.json.cases[${index}].pagination: page size exceeded`);
      }
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

function duplicates(values: string[]): string[] {
  const counts = new Map<string, number>();
  for (const value of values) counts.set(value, (counts.get(value) ?? 0) + 1);
  return [...counts.entries()].filter(([, count]) => count > 1).map(([value]) => value).sort();
}

function difference(left: string[], right: string[]): string[] {
  const rightSet = new Set(right);
  return [...new Set(left.filter((value) => !rightSet.has(value)))].sort();
}
