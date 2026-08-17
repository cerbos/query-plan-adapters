import { CliError } from "./errors.ts";
import type {
  Catalog,
  CatalogAction,
  CheckResource,
  CheckResources,
  Command,
  ConsumerCase,
  ConsumerCases,
  Manifest,
  OracleExpectation,
  Outcome,
  SemanticEnvironment,
} from "./model.ts";

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function record(value: unknown, path: string): Record<string, unknown> {
  if (!isRecord(value)) throw new CliError(`${path}: expected object`, 2);
  return value;
}

function string(value: unknown, path: string): string {
  if (typeof value !== "string") throw new CliError(`${path}: expected string`, 2);
  return value;
}

function number(value: unknown, path: string): number {
  if (typeof value !== "number") throw new CliError(`${path}: expected number`, 2);
  return value;
}

function array(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) throw new CliError(`${path}: expected array`, 2);
  return value;
}

function stringArray(value: unknown, path: string): string[] {
  return array(value, path).map((item, index) => string(item, `${path}[${index}]`));
}

function command(value: unknown, path: string): Command {
  if (value === null) return { kind: "unavailable" };
  const arguments_ = stringArray(value, path);
  const [first, ...rest] = arguments_;
  if (first === undefined) throw new CliError(`${path}: expected nonempty command`, 2);
  return { kind: "command", arguments: [first, ...rest] };
}

function outcome(value: unknown, path: string): Outcome {
  const item = record(value, path);
  const status = string(item["status"], `${path}.status`);
  switch (status) {
    case "matched":
      return { kind: "matched" };
    case "rejected":
      return {
        kind: "rejected",
        reason: string(item["reason"], `${path}.reason`),
        message: string(item["message"], `${path}.message`),
      };
    case "upstream-blocked":
      return {
        kind: "upstream-blocked",
        reason: string(item["reason"], `${path}.reason`),
      };
    case "unassessed":
      return { kind: "unassessed" };
    default:
      throw new CliError(`${path}.status: unknown outcome ${status}`, 2);
  }
}

export function decodeManifest(value: unknown, path: string): Manifest {
  const item = record(value, path);
  const package_ = record(item["package"], `${path}.package`);
  const commands = record(item["commands"], `${path}.commands`);
  const consumer = record(item["consumer"], `${path}.consumer`);
  const environments = array(item["semanticEnvironments"], `${path}.semanticEnvironments`).map(
    (environment, index): SemanticEnvironment => {
      const environmentPath = `${path}.semanticEnvironments[${index}]`;
      const decoded = record(environment, environmentPath);
      const decodedCommand = command(decoded["command"], `${environmentPath}.command`);
      if (decodedCommand.kind === "unavailable") {
        throw new CliError(`${environmentPath}.command: expected command`, 2);
      }
      const env = record(decoded["env"], `${environmentPath}.env`);
      const decodedEnv: Record<string, string> = {};
      for (const [key, envValue] of Object.entries(env)) {
        decodedEnv[key] = string(envValue, `${environmentPath}.env.${key}`);
      }
      return {
        name: string(decoded["name"], `${environmentPath}.name`),
        command: decodedCommand,
        env: decodedEnv,
      };
    },
  );
  const rawOutcomes = record(item["outcomes"], `${path}.outcomes`);
  const outcomes = new Map<string, Outcome>();
  for (const [action, rawOutcome] of Object.entries(rawOutcomes)) {
    outcomes.set(action, outcome(rawOutcome, `${path}.outcomes.${action}`));
  }
  const coverage = string(consumer["coverage"], `${path}.consumer.coverage`);
  if (coverage !== "artifact-install" && coverage !== "usage-only") {
    throw new CliError(`${path}.consumer.coverage: unknown coverage ${coverage}`, 2);
  }
  return {
    schemaVersion: 1,
    adapter: string(item["adapter"], `${path}.adapter`),
    package: {
      ecosystem: string(package_["ecosystem"], `${path}.package.ecosystem`),
      name: string(package_["name"], `${path}.package.name`),
    },
    workflow: string(item["workflow"], `${path}.workflow`),
    commands: {
      test: command(commands["test"], `${path}.commands.test`),
      typecheck: command(commands["typecheck"], `${path}.commands.typecheck`),
      golden: command(commands["golden"], `${path}.commands.golden`),
      consumer: command(commands["consumer"], `${path}.commands.consumer`),
    },
    semanticEnvironments: environments,
    consumer: { coverage },
    outcomes,
  };
}

function oracleExpectation(value: unknown, path: string): OracleExpectation {
  const item = record(value, path);
  const kind = string(item["kind"], `${path}.kind`);
  switch (kind) {
    case "proper-subset":
      return { kind };
    case "empty":
    case "total":
      return { kind, reason: string(item["reason"], `${path}.reason`) };
    default:
      throw new CliError(`${path}.kind: unknown expectation ${kind}`, 2);
  }
}

export function decodeCatalog(value: unknown, path: string): Catalog {
  const item = record(value, path);
  const actions = array(item["actions"], `${path}.actions`).map((action, index): CatalogAction => {
    const actionPath = `${path}.actions[${index}]`;
    const decoded = record(action, actionPath);
    return {
      name: string(decoded["name"], `${actionPath}.name`),
      oracleExpectation: oracleExpectation(
        decoded["oracleExpectation"],
        `${actionPath}.oracleExpectation`,
      ),
    };
  });
  return { schemaVersion: 1, actions };
}

export function decodeConsumerCases(value: unknown, path: string): ConsumerCases {
  const item = record(value, path);
  const cases = array(item["cases"], `${path}.cases`).map((consumerCase, index): ConsumerCase => {
    const casePath = `${path}.cases[${index}]`;
    const decoded = record(consumerCase, casePath);
    const operation = string(decoded["operation"], `${casePath}.operation`);
    if (!isOperation(operation)) throw new CliError(`${casePath}.operation: unknown ${operation}`, 2);
    const paginationValue = decoded["pagination"];
    const pagination = paginationValue === null ? null : decodePagination(paginationValue, casePath);
    const expected = record(decoded["expected"], `${casePath}.expected`);
    const kind = string(expected["kind"], `${casePath}.expected.kind`);
    if (!isPlanKind(kind)) throw new CliError(`${casePath}.expected.kind: unknown ${kind}`, 2);
    return {
      id: string(decoded["id"], `${casePath}.id`),
      operation,
      principal: string(decoded["principal"], `${casePath}.principal`),
      action: string(decoded["action"], `${casePath}.action`),
      pagination,
      expected: { kind, ids: stringArray(expected["ids"], `${casePath}.expected.ids`) },
    };
  });
  return { schemaVersion: 1, cases };
}

function decodePagination(value: unknown, casePath: string): { pageSize: number; pageSizes: number[] } {
  const pagination = record(value, `${casePath}.pagination`);
  return {
    pageSize: number(pagination["pageSize"], `${casePath}.pagination.pageSize`),
    pageSizes: array(pagination["pageSizes"], `${casePath}.pagination.pageSizes`).map(
      (size, index) => number(size, `${casePath}.pagination.pageSizes[${index}]`),
    ),
  };
}

export function decodeCheckResources(value: unknown, path: string): CheckResources {
  const item = record(value, path);
  const resources = array(item["resources"], `${path}.resources`).map(
    (resource, index): CheckResource => {
      const resourcePath = `${path}.resources[${index}]`;
      const decoded = record(resource, resourcePath);
      return {
        kind: string(decoded["kind"], `${resourcePath}.kind`),
        id: string(decoded["id"], `${resourcePath}.id`),
        attr: record(decoded["attr"], `${resourcePath}.attr`),
      };
    },
  );
  return {
    schemaVersion: 1,
    principal: record(item["principal"], `${path}.principal`),
    resources,
  };
}

function isOperation(value: string): value is ConsumerCase["operation"] {
  return value === "filtered" || value === "alwaysAllowed" || value === "alwaysDenied" ||
    value === "paginated" || value === "composed";
}

function isPlanKind(value: string): value is ConsumerCase["expected"]["kind"] {
  return value === "KIND_CONDITIONAL" || value === "KIND_ALWAYS_ALLOWED" ||
    value === "KIND_ALWAYS_DENIED";
}
