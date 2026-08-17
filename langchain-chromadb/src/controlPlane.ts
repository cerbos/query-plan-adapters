import * as fs from "fs";
import * as path from "path";

import type { Principal, Resource, Value } from "@cerbos/core";

const CONFORMANCE_DIR = path.join(__dirname, "..", "..", "conformance");

export type OracleExpectation =
  | { kind: "proper-subset" }
  | { kind: "empty"; reason: string }
  | { kind: "total"; reason: string };

export type DirectOutcome =
  | { status: "matched" }
  | { status: "rejected"; reason: string; message: string }
  | { status: "upstream-blocked"; reason: string }
  | { status: "unassessed" };

export interface ThrowingAction {
  action: string;
  reason: string;
  message: string;
}

export interface UpstreamBlockedAction {
  action: string;
  reason: string;
}

export interface ActionControlPlane {
  allActions: string[];
  selectedActions: string[];
  oracleActions: string[];
  throwingActions: ThrowingAction[];
  upstreamBlockedActions: UpstreamBlockedAction[];
  unassessedActions: string[];
  outcomes: Map<string, DirectOutcome>;
  oracleExpectations: Map<string, OracleExpectation>;
}

export interface CheckResources {
  principal: Principal;
  resources: Resource[];
}

function readJson(file: string): unknown {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  return value;
}

function requireString(value: unknown, label: string): string {
  if (typeof value !== "string" || value === "") {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value;
}

function requireSchemaVersion(
  value: Record<string, unknown>,
  label: string,
): void {
  if (value["schemaVersion"] !== 1) {
    throw new Error(`${label}.schemaVersion must be 1`);
  }
}

function requireStringArray(value: unknown, label: string): string[] {
  if (!Array.isArray(value)) throw new Error(`${label} must be an array`);
  return value.map((entry, index) =>
    requireString(entry, `${label}[${index}]`),
  );
}

function parseOracleExpectation(
  value: unknown,
  label: string,
): OracleExpectation {
  const record = requireRecord(value, label);
  switch (record["kind"]) {
    case "proper-subset":
      return { kind: "proper-subset" };
    case "empty":
      return {
        kind: "empty",
        reason: requireString(record["reason"], `${label}.reason`),
      };
    case "total":
      return {
        kind: "total",
        reason: requireString(record["reason"], `${label}.reason`),
      };
    default:
      throw new Error(`${label}.kind must be empty, total, or proper-subset`);
  }
}

function parseOutcome(value: unknown, label: string): DirectOutcome {
  const record = requireRecord(value, label);
  switch (record["status"]) {
    case "matched":
      return { status: "matched" };
    case "rejected":
      return {
        status: "rejected",
        reason: requireString(record["reason"], `${label}.reason`),
        message: requireString(record["message"], `${label}.message`),
      };
    case "upstream-blocked":
      return {
        status: "upstream-blocked",
        reason: requireString(record["reason"], `${label}.reason`),
      };
    case "unassessed":
      return { status: "unassessed" };
    default:
      throw new Error(
        `${label}.status must be matched, rejected, upstream-blocked, or unassessed`,
      );
  }
}

export function loadActionControlPlane({
  adapter,
  selectedAction,
  rootDirectory = path.join(CONFORMANCE_DIR, ".."),
}: {
  adapter: string;
  selectedAction: string | undefined;
  rootDirectory?: string;
}): ActionControlPlane {
  const catalog = requireRecord(
    readJson(path.join(rootDirectory, "conformance", "catalog.json")),
    "conformance/catalog.json",
  );
  requireSchemaVersion(catalog, "conformance/catalog.json");
  if (!Array.isArray(catalog["actions"])) {
    throw new Error("conformance/catalog.json.actions must be an array");
  }

  const oracleExpectations = new Map<string, OracleExpectation>();
  for (const [index, rawAction] of catalog["actions"].entries()) {
    const action = requireRecord(
      rawAction,
      `conformance/catalog.json.actions[${index}]`,
    );
    const name = requireString(
      action["name"],
      `conformance/catalog.json.actions[${index}].name`,
    );
    if (oracleExpectations.has(name)) {
      throw new Error(
        `conformance/catalog.json contains duplicate action ${name}`,
      );
    }
    oracleExpectations.set(
      name,
      parseOracleExpectation(
        action["oracleExpectation"],
        `conformance/catalog.json.actions[${index}].oracleExpectation`,
      ),
    );
  }

  const manifestLabel = `${adapter}/adapterctl.json`;
  const manifest = requireRecord(
    readJson(path.join(rootDirectory, adapter, "adapterctl.json")),
    manifestLabel,
  );
  requireSchemaVersion(manifest, manifestLabel);
  if (manifest["adapter"] !== adapter) {
    throw new Error(`${manifestLabel}.adapter must be ${adapter}`);
  }
  const rawOutcomes = requireRecord(
    manifest["outcomes"],
    `${manifestLabel}.outcomes`,
  );
  const outcomes = new Map<string, DirectOutcome>();
  for (const [action, value] of Object.entries(rawOutcomes)) {
    outcomes.set(
      action,
      parseOutcome(value, `${manifestLabel}.outcomes.${action}`),
    );
  }

  const allActions = [...oracleExpectations.keys()].sort();
  const outcomeActions = [...outcomes.keys()].sort();
  if (
    selectedAction === undefined &&
    JSON.stringify(outcomeActions) !== JSON.stringify(allActions)
  ) {
    throw new Error(
      `${manifestLabel}.outcomes must account for every catalog action exactly once`,
    );
  }
  if (selectedAction !== undefined && !oracleExpectations.has(selectedAction)) {
    throw new Error(`Unknown ADAPTERCTL_ACTION "${selectedAction}"`);
  }
  const selectedActions =
    selectedAction === undefined ? allActions : [selectedAction];
  const oracleActions: string[] = [];
  const throwingActions: ThrowingAction[] = [];
  const upstreamBlockedActions: UpstreamBlockedAction[] = [];
  const unassessedActions: string[] = [];
  for (const action of selectedActions) {
    const outcome = outcomes.get(action);
    if (outcome === undefined && selectedAction === undefined)
      throw new Error(`${manifestLabel} has no outcome for ${action}`);
    // A focused run is the discovery seam: catalog membership is authoritative while a missing
    // or explicitly unassessed adapter outcome is being measured. Full runs remain strict above.
    if (outcome === undefined || outcome.status === "unassessed") {
      if (selectedAction !== undefined) {
        oracleActions.push(action);
        continue;
      }
      throw new Error(`${manifestLabel} has an unassessed outcome for ${action}`);
    }
    switch (outcome.status) {
      case "matched":
        oracleActions.push(action);
        break;
      case "rejected":
        throwingActions.push({
          action,
          reason: outcome.reason,
          message: outcome.message,
        });
        break;
      case "upstream-blocked":
        upstreamBlockedActions.push({ action, reason: outcome.reason });
        break;
      default: {
        const exhaustive: never = outcome;
        throw exhaustive;
      }
    }
  }

  return {
    allActions,
    selectedActions,
    oracleActions,
    throwingActions,
    upstreamBlockedActions,
    unassessedActions,
    outcomes,
    oracleExpectations,
  };
}

function parseValue(value: unknown, label: string): Value {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry, index) => parseValue(entry, `${label}[${index}]`));
  }
  const record = requireRecord(value, label);
  return parseValueRecord(record, label);
}

function parseValueRecord(
  record: Record<string, unknown>,
  label: string,
): Record<string, Value> {
  const parsed: Record<string, Value> = {};
  for (const [key, value] of Object.entries(record)) {
    parsed[key] = parseValue(value, `${label}.${key}`);
  }
  return parsed;
}

export function loadCheckResources(): CheckResources {
  const label = "conformance/check-resources.json";
  const document = requireRecord(
    readJson(path.join(CONFORMANCE_DIR, "check-resources.json")),
    label,
  );
  requireSchemaVersion(document, label);
  const rawPrincipal = requireRecord(
    document["principal"],
    `${label}.principal`,
  );
  const principal: Principal = {
    id: requireString(rawPrincipal["id"], `${label}.principal.id`),
    roles: requireStringArray(
      rawPrincipal["roles"],
      `${label}.principal.roles`,
    ),
    attr: parseValueRecord(
      requireRecord(rawPrincipal["attr"], `${label}.principal.attr`),
      `${label}.principal.attr`,
    ),
  };
  if (!Array.isArray(document["resources"])) {
    throw new Error(`${label}.resources must be an array`);
  }
  const resources = document["resources"].map(
    (rawResource, index): Resource => {
      const resource = requireRecord(
        rawResource,
        `${label}.resources[${index}]`,
      );
      return {
        kind: requireString(
          resource["kind"],
          `${label}.resources[${index}].kind`,
        ),
        id: requireString(resource["id"], `${label}.resources[${index}].id`),
        attr: parseValueRecord(
          requireRecord(resource["attr"], `${label}.resources[${index}].attr`),
          `${label}.resources[${index}].attr`,
        ),
      };
    },
  );
  const ids = resources.map(({ id }) => id);
  if (new Set(ids).size !== ids.length) {
    throw new Error(`${label}.resources contains duplicate ids`);
  }
  return { principal, resources };
}

export function requireOutcomeMessage({
  controlPlane,
  action,
}: {
  controlPlane: ActionControlPlane;
  action: string;
}): string {
  const outcome = controlPlane.outcomes.get(action);
  if (outcome?.status !== "rejected") {
    throw new Error(
      `${action} must have a rejected outcome with a pinned message`,
    );
  }
  return outcome.message;
}
