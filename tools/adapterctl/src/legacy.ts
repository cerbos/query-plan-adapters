import { isDeepStrictEqual } from "node:util";

import { isRecord } from "./decode.ts";
import type { Catalog, Manifest, Outcome } from "./model.ts";

export type NormalizedOutcomes = Map<string, Map<string, Outcome>>;

export function compareLegacy(args: {
  catalog: Catalog;
  manifests: Manifest[];
  legacy: unknown;
  discovery: boolean;
  exactAdapterSet: boolean;
}): string[] {
  const errors: string[] = [];
  const legacy = normalizeLegacy(args.legacy, args.catalog, errors);
  if (args.exactAdapterSet) {
    const discovered = new Set(args.manifests.map((manifest) => manifest.adapter));
    for (const adapter of legacy.keys()) {
      if (!discovered.has(adapter)) {
        errors.push(`conformance/actions.json: legacy adapter has no discovered manifest: ${adapter}`);
      }
    }
  }
  for (const manifest of args.manifests) {
    const legacyOutcomes = legacy.get(manifest.adapter);
    if (legacyOutcomes === undefined) {
      errors.push(`${manifest.adapter}: missing from legacy actions.json adapters`);
      continue;
    }
    for (const action of args.catalog.actions) {
      const direct = manifest.outcomes.get(action.name) ?? { kind: "unassessed" };
      if (direct.kind === "unassessed" && args.discovery) continue;
      const previous = legacyOutcomes.get(action.name);
      if (previous === undefined) {
        errors.push(
          `${manifest.adapter}/${action.name}: manifest outcome ${direct.kind} is absent from legacy actions.json`,
        );
      } else if (!isDeepStrictEqual(direct, previous)) {
        errors.push(
          `${manifest.adapter}/${action.name}: manifest outcome ${formatOutcome(direct)} does not match legacy ${formatOutcome(previous)}`,
        );
      }
    }
  }
  return errors;
}

export function projectNormalizedOutcomes(manifests: Manifest[], catalog: Catalog): object {
  return {
    schemaVersion: 1,
    adapters: manifests.map((manifest) => ({
      adapter: manifest.adapter,
      outcomes: catalog.actions.map((action) => ({
        action: action.name,
        outcome: serializeOutcome(manifest.outcomes.get(action.name) ?? { kind: "unassessed" }),
      })),
    })),
  };
}

function normalizeLegacy(value: unknown, catalog: Catalog, errors: string[]): NormalizedOutcomes {
  if (!isRecord(value)) {
    errors.push("conformance/actions.json: expected object");
    return new Map();
  }
  const allowedKeys = new Set([
    "description", "adapters", "conformance", "adapterUnsupported", "adapterSupportedExpected",
    "expectedUnsupported", "nullRepresentationOmitted", "knownDivergences",
  ]);
  for (const key of Object.keys(value)) {
    if (!allowedKeys.has(key)) errors.push(`conformance/actions.json: unknown group ${key}`);
  }
  const adapters = strings(value["adapters"], "conformance/actions.json.adapters", errors);
  for (const duplicate of duplicateStrings(adapters)) {
    errors.push(`conformance/actions.json.adapters: duplicate adapter ${duplicate}`);
  }
  const normalized: NormalizedOutcomes = new Map(
    adapters.map((adapter) => [adapter, new Map<string, Outcome>()]),
  );
  const catalogNames = new Set(catalog.actions.map((action) => action.name));
  const assign = (adapter: string, action: string, outcome: Outcome): Map<string, Outcome> | undefined => {
    if (!catalogNames.has(action)) {
      errors.push(`conformance/actions.json: unknown action ${action}`);
      return undefined;
    }
    const outcomes = normalized.get(adapter);
    if (outcomes === undefined) {
      errors.push(`conformance/actions.json: stale adapter ${adapter}`);
      return undefined;
    }
    return outcomes;
  };
  const setBase = (adapter: string, action: string, outcome: Outcome): void => {
    assign(adapter, action, outcome)?.set(action, outcome);
  };
  const specialAssignments = new Set<string>();
  const setSpecial = (adapter: string, action: string, outcome: Outcome): void => {
    const outcomes = assign(adapter, action, outcome);
    if (outcomes === undefined) return;
    const key = `${adapter}/${action}`;
    if (specialAssignments.has(key)) {
      errors.push(`conformance/actions.json: duplicate special classification for ${key}`);
      return;
    }
    specialAssignments.add(key);
    outcomes.set(action, outcome);
  };
  const conformance = strings(value["conformance"], "conformance/actions.json.conformance", errors);
  for (const duplicate of duplicateStrings(conformance)) {
    errors.push(`conformance/actions.json.conformance: duplicate action ${duplicate}`);
  }
  for (const action of conformance) {
    for (const adapter of adapters) setBase(adapter, action, { kind: "matched" });
  }
  normalizeAdapterUnsupported(value["adapterUnsupported"], setSpecial, errors);
  const supported = normalizeSupportedExpected(
    value["adapterSupportedExpected"],
    new Set(adapters),
    catalogNames,
    errors,
  );
  normalizeExpectedUnsupported(value["expectedUnsupported"], adapters, supported, setSpecial, errors);
  normalizeNullOmitted(value["nullRepresentationOmitted"], adapters, setSpecial, errors);
  normalizeDivergences(value["knownDivergences"], setSpecial, errors);
  return normalized;
}

function normalizeAdapterUnsupported(
  value: unknown,
  set: (adapter: string, action: string, outcome: Outcome) => void,
  errors: string[],
): void {
  const groups = object(value, "conformance/actions.json.adapterUnsupported", errors);
  for (const [adapter, entriesValue] of Object.entries(groups)) {
    for (const entry of records(entriesValue, `adapterUnsupported.${adapter}`, errors)) {
      const action = requiredString(entry, "action", `adapterUnsupported.${adapter}`, errors);
      const reason = requiredString(entry, "reason", `adapterUnsupported.${adapter}.${action}`, errors);
      const message = requiredString(entry, "message", `adapterUnsupported.${adapter}.${action}`, errors);
      set(adapter, action, { kind: "rejected", reason, message });
    }
  }
}

function normalizeSupportedExpected(
  value: unknown,
  adapters: Set<string>,
  catalogNames: Set<string>,
  errors: string[],
): Map<string, Set<string>> {
  const groups = object(value, "conformance/actions.json.adapterSupportedExpected", errors);
  const supported = new Map<string, Set<string>>();
  for (const [adapter, entriesValue] of Object.entries(groups)) {
    if (!adapters.has(adapter)) {
      errors.push(`conformance/actions.json.adapterSupportedExpected: stale adapter ${adapter}`);
    }
    const actions = new Set<string>();
    for (const entry of records(entriesValue, `adapterSupportedExpected.${adapter}`, errors)) {
      const action = requiredString(entry, "action", `adapterSupportedExpected.${adapter}`, errors);
      if (!catalogNames.has(action)) {
        errors.push(`conformance/actions.json: unknown action ${action}`);
      }
      if (actions.has(action)) {
        errors.push(`adapterSupportedExpected.${adapter}: duplicate action ${action}`);
      }
      actions.add(action);
    }
    supported.set(adapter, actions);
  }
  return supported;
}

function normalizeExpectedUnsupported(
  value: unknown,
  adapters: string[],
  supported: Map<string, Set<string>>,
  set: (adapter: string, action: string, outcome: Outcome) => void,
  errors: string[],
): void {
  for (const entry of records(value, "conformance/actions.json.expectedUnsupported", errors)) {
    const action = requiredString(entry, "action", "expectedUnsupported", errors);
    const reasonValue = entry["reason"] ?? entry["shape"];
    const reason = typeof reasonValue === "string" && reasonValue.length > 0 ? reasonValue :
      `legacy expected-unsupported action ${action}`;
    const messages = object(entry["messages"], `expectedUnsupported.${action}.messages`, errors);
    for (const adapter of adapters) {
      if (supported.get(adapter)?.has(action) === true) {
        set(adapter, action, { kind: "matched" });
        continue;
      }
      const message = messages[adapter];
      if (typeof message !== "string" || message.length === 0) {
        errors.push(`expectedUnsupported.${action}.messages: missing adapter ${adapter}`);
        continue;
      }
      set(adapter, action, { kind: "rejected", reason, message });
    }
  }
}

function normalizeNullOmitted(
  value: unknown,
  adapters: string[],
  set: (adapter: string, action: string, outcome: Outcome) => void,
  errors: string[],
): void {
  for (const entry of records(value, "conformance/actions.json.nullRepresentationOmitted", errors)) {
    const action = requiredString(entry, "action", "nullRepresentationOmitted", errors);
    const reason = requiredString(entry, "reason", `nullRepresentationOmitted.${action}`, errors);
    const messages = object(entry["messages"], `nullRepresentationOmitted.${action}.messages`, errors);
    for (const adapter of adapters) {
      const message = messages[adapter];
      if (typeof message !== "string" || message.length === 0) {
        errors.push(`nullRepresentationOmitted.${action}.messages: missing adapter ${adapter}`);
        continue;
      }
      set(adapter, action, { kind: "rejected", reason, message });
    }
  }
}

function normalizeDivergences(
  value: unknown,
  set: (adapter: string, action: string, outcome: Outcome) => void,
  errors: string[],
): void {
  for (const entry of records(value, "conformance/actions.json.knownDivergences", errors)) {
    const action = requiredString(entry, "action", "knownDivergences", errors);
    const reason = requiredString(entry, "reason", `knownDivergences.${action}`, errors);
    for (const adapter of strings(entry["adapters"], `knownDivergences.${action}.adapters`, errors)) {
      set(adapter, action, { kind: "upstream-blocked", reason });
    }
  }
}

function serializeOutcome(outcome: Outcome): object {
  switch (outcome.kind) {
    case "matched":
    case "unassessed":
      return { status: outcome.kind };
    case "rejected":
      return { status: outcome.kind, reason: outcome.reason, message: outcome.message };
    case "upstream-blocked":
      return { status: outcome.kind, reason: outcome.reason };
    default: {
      const exhaustive: never = outcome;
      return exhaustive;
    }
  }
}

function formatOutcome(outcome: Outcome): string {
  return JSON.stringify(serializeOutcome(outcome));
}

function object(value: unknown, path: string, errors: string[]): Record<string, unknown> {
  if (isRecord(value)) return value;
  errors.push(`${path}: expected object`);
  return {};
}

function records(value: unknown, path: string, errors: string[]): Record<string, unknown>[] {
  if (!Array.isArray(value)) {
    errors.push(`${path}: expected array`);
    return [];
  }
  const result: Record<string, unknown>[] = [];
  for (const [index, item] of value.entries()) {
    if (isRecord(item)) result.push(item);
    else errors.push(`${path}[${index}]: expected object`);
  }
  return result;
}

function strings(value: unknown, path: string, errors: string[]): string[] {
  if (!Array.isArray(value)) {
    errors.push(`${path}: expected array`);
    return [];
  }
  const result: string[] = [];
  for (const [index, item] of value.entries()) {
    if (typeof item === "string") result.push(item);
    else errors.push(`${path}[${index}]: expected string`);
  }
  return result;
}

function requiredString(
  value: Record<string, unknown>,
  key: string,
  path: string,
  errors: string[],
): string {
  const item = value[key];
  if (typeof item === "string" && item.length > 0) return item;
  errors.push(`${path}.${key}: expected nonempty string`);
  return "<invalid>";
}

function duplicateStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) duplicates.add(value);
    seen.add(value);
  }
  return [...duplicates].sort();
}
