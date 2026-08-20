import { readFile } from "node:fs/promises";
import { join, resolve, sep } from "node:path";

import { parseDocument } from "yaml";

import type { Manifest } from "./model.ts";
import { isRecord } from "./records.ts";

type WorkflowEvidence = {
  paths: string[];
  commands: Array<{ command: string; environment: Record<string, string> }>;
  scopedValidations: string[];
};

type DeclaredSources = {
  packageScripts: Map<string, string>;
  wrappers: Array<{ path: string; source: string }>;
};

export async function validateWorkflow(args: {
  root: string;
  manifest: Manifest;
  errors: string[];
}): Promise<void> {
  const workflowPath = resolve(args.root, args.manifest.workflow);
  if (!workflowPath.startsWith(`${resolve(args.root)}${sep}`)) {
    args.errors.push(`${args.manifest.adapter}/adapterctl.json.workflow: path escapes repository root`);
    return;
  }
  const workflowSource = await readOptionalFile(workflowPath);
  if (workflowSource === null) {
    args.errors.push(
      `${args.manifest.adapter}/adapterctl.json.workflow: file not found: ${args.manifest.workflow}`,
    );
    return;
  }
  const evidence = parseWorkflowEvidence(workflowSource, args.manifest.workflow, args.errors);
  if (evidence === null) return;
  for (const marker of [`${args.manifest.adapter}/**`, "conformance/**", "demo/**"]) {
    if (!evidence.paths.includes(marker)) {
      args.errors.push(`${args.manifest.workflow}: missing path trigger ${marker}`);
    }
  }
  if (!evidence.scopedValidations.includes(args.manifest.adapter)) {
    args.errors.push(
      `${args.manifest.workflow}: missing scoped adapterctl validation for ${args.manifest.adapter}`,
    );
  }
  const declaredSources = await readDeclaredSources(args.root, args.manifest, args.errors);
  const directCommands = [
    args.manifest.commands.test,
    args.manifest.commands.typecheck,
    args.manifest.commands.consumer,
  ];
  for (const command of directCommands) {
    if (command.kind === "unavailable") continue;
    validateCommandMarker(evidence, declaredSources, command.arguments, args.manifest, args.errors);
  }
  for (const environment of args.manifest.semanticEnvironments) {
    validateCommandMarker(
      evidence,
      declaredSources,
      environment.command.arguments,
      args.manifest,
      args.errors,
    );
  }
  for (const environment of args.manifest.semanticEnvironments) {
    const expectedEnvironment = Object.fromEntries(
      Object.entries(environment.env).filter(([, value]) => value.length > 0),
    );
    if (Object.keys(expectedEnvironment).length > 0 && !hasEnvironmentEvidence({
      evidence,
      declaredSources,
      command: environment.command.arguments,
      expectedEnvironment,
    })) {
      const markers = Object.entries(expectedEnvironment)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, value]) => `${key}=${value}`)
        .join(", ");
      args.errors.push(`${args.manifest.workflow}: missing environment marker ${markers}`);
    }
  }
}

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
  const commands: Array<{ command: string; environment: Record<string, string> }> = [];
  const scopedValidations: string[] = [];
  const workflowEnvironment = readEnvironment(value["env"]);
  const jobs = value["jobs"];
  if (isRecord(jobs)) {
    for (const job of Object.values(jobs)) {
      if (!isRecord(job) || isStaticallyDisabled(job["if"])) continue;
      const jobEnvironment = { ...workflowEnvironment, ...readEnvironment(job["env"]) };
      const rows = isRecord(job["strategy"]) ? matrixRows(job["strategy"]["matrix"]) : [{}];
      const steps = job["steps"];
      if (!Array.isArray(steps)) continue;
      for (const step of steps) {
        if (!isRecord(step) || isStaticallyDisabled(step["if"])) continue;
        const enabledRows = rows.filter((row) =>
          matrixConditionAllows({ condition: step["if"], row })
        );
        if (typeof step["run"] === "string") {
          const environment = { ...jobEnvironment, ...readEnvironment(step["env"]) };
          for (const row of enabledRows) {
            commands.push({
              command: expandMatrixValue({ value: step["run"], row }),
              environment: Object.fromEntries(Object.entries(environment).map(([key, value]) =>
                [key, expandMatrixValue({ value, row })]
              )),
            });
          }
        }
        if (enabledRows.length > 0 &&
            step["uses"] === "./.github/actions/validate-adapterctl" && isRecord(step["with"]) &&
            typeof step["with"]["adapter"] === "string") {
          scopedValidations.push(step["with"]["adapter"]);
        }
      }
    }
  }
  return {
    paths,
    commands,
    scopedValidations,
  };
}

function isStaticallyDisabled(condition: unknown): boolean {
  if (condition === false) return true;
  if (typeof condition !== "string") return false;
  const normalized = condition.trim();
  return normalized === "false" || /^\$\{\{\s*false\s*\}\}$/.test(normalized);
}

function matrixConditionAllows(args: { condition: unknown; row: MatrixRow }): boolean {
  if (args.condition === undefined || args.condition === true) return true;
  if (args.condition === false) return false;
  if (typeof args.condition !== "string") return true;
  const condition = args.condition.trim()
    .replace(/^\$\{\{\s*/, "")
    .replace(/\s*\}\}$/, "");
  const result = evaluateOrCondition({ condition, row: args.row });
  return result ?? true;
}

function evaluateOrCondition(args: { condition: string; row: MatrixRow }): boolean | null {
  const terms = args.condition.split(/\s*\|\|\s*/);
  const results = terms.map((condition) => evaluateAndCondition({ condition, row: args.row }));
  if (results.includes(true)) return true;
  if (results.every((result) => result === false)) return false;
  return null;
}

function evaluateAndCondition(args: { condition: string; row: MatrixRow }): boolean | null {
  const terms = args.condition.split(/\s*&&\s*/);
  const results = terms.map((condition) => evaluateMatrixCondition({ condition, row: args.row }));
  if (results.includes(false)) return false;
  if (results.every((result) => result === true)) return true;
  return null;
}

function evaluateMatrixCondition(args: { condition: string; row: MatrixRow }): boolean | null {
  const condition = args.condition.trim();
  if (condition === "true") return true;
  if (condition === "false") return false;
  const comparison = /^matrix\.([A-Za-z0-9_.-]+)\s*(==|!=)\s*(['"])(.*?)\3$/.exec(condition);
  if (comparison !== null) {
    const [, key, operator, , expected] = comparison;
    if (key === undefined || operator === undefined || expected === undefined) return null;
    const equal = args.row[key] === expected;
    return operator === "==" ? equal : !equal;
  }
  const truthy = /^(!)?matrix\.([A-Za-z0-9_.-]+)$/.exec(condition);
  if (truthy === null) return null;
  const value = truthy[2] === undefined ? undefined : args.row[truthy[2]];
  const result = value !== undefined && value !== "" && value !== "false" && value !== "0";
  return truthy[1] === "!" ? !result : result;
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

type MatrixRow = Record<string, string>;

function matrixRows(value: unknown): MatrixRow[] {
  if (!isRecord(value)) return [{}];
  const axes = Object.entries(value).filter(([key]) => key !== "include" && key !== "exclude");
  let rows: MatrixRow[] = [{}];
  for (const [key, options] of axes) {
    if (!Array.isArray(options) || options.length === 0) return [];
    const flattened = options.map((option) => flattenMatrixValue({ prefix: key, value: option }));
    if (flattened.some((option) => option === null)) return [];
    rows = rows.flatMap((row) => flattened.flatMap((option) =>
      option === null ? [] : [{ ...row, ...option }]
    ));
  }
  const excludes = matrixList(value["exclude"]);
  rows = rows.filter((row) => !excludes.some((excluded) => rowMatches({ row, values: excluded })));
  const includes = matrixList(value["include"]);
  if (axes.length === 0) return includes.length === 0 ? rows : includes;
  for (const included of includes) {
    const matchingIndexes = rows.flatMap((row, index) =>
      rowsCompatible({ row, values: included }) ? [index] : []
    );
    if (matchingIndexes.length === 0) {
      rows.push(included);
      continue;
    }
    for (const index of matchingIndexes) {
      const row = rows[index];
      if (row !== undefined) rows[index] = { ...row, ...included };
    }
  }
  return rows;
}

function matrixList(value: unknown): MatrixRow[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry) => {
    if (!isRecord(entry)) return [];
    const flattened = flattenMatrixValue({ prefix: "", value: entry });
    return flattened === null ? [] : [flattened];
  });
}

function flattenMatrixValue(args: { prefix: string; value: unknown }): MatrixRow | null {
  const scalar = scalarString(args.value);
  if (scalar !== null) return args.prefix.length === 0 ? null : { [args.prefix]: scalar };
  if (!isRecord(args.value)) return null;
  const flattened: MatrixRow = {};
  for (const [key, nested] of Object.entries(args.value)) {
    const prefix = args.prefix.length === 0 ? key : `${args.prefix}.${key}`;
    const child = flattenMatrixValue({ prefix, value: nested });
    if (child === null) return null;
    Object.assign(flattened, child);
  }
  return flattened;
}

function rowMatches(args: { row: MatrixRow; values: MatrixRow }): boolean {
  return Object.entries(args.values).every(([key, value]) => args.row[key] === value);
}

function rowsCompatible(args: { row: MatrixRow; values: MatrixRow }): boolean {
  return Object.entries(args.values).every(([key, value]) =>
    args.row[key] === undefined || args.row[key] === value
  );
}

function readEnvironment(value: unknown): Record<string, string> {
  if (!isRecord(value)) return {};
  const environment: Record<string, string> = {};
  for (const [key, nested] of Object.entries(value)) {
    const scalar = scalarString(nested);
    if (scalar === null) continue;
    environment[key] = scalar;
  }
  return environment;
}

function scalarString(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

function expandMatrixValue(args: { value: string; row: MatrixRow }): string {
  return args.value.replace(
    /\$\{\{\s*matrix\.([A-Za-z0-9_.-]+)(?:\s*\|\|\s*(?:'([^']*)'|"([^"]*)"))?\s*\}\}/g,
    (expression, key: string, singleQuoted: string | undefined, doubleQuoted: string | undefined) => {
      const value = args.row[key];
      if (value !== undefined) return value;
      const fallback = singleQuoted ?? doubleQuoted;
      return fallback === undefined ? expression : fallback;
    },
  );
}

function validateCommandMarker(
  evidence: WorkflowEvidence,
  declaredSources: DeclaredSources,
  arguments_: [string, ...string[]],
  manifest: Manifest,
  errors: string[],
): void {
  if (hasCommandMarker({
    workflowSources: evidence.commands.map(({ command }) => command),
    declaredSources,
    arguments_,
  })) return;
  errors.push(`${manifest.workflow}: missing native command marker ${arguments_.join(" ")}`);
}

function hasCommandMarker(args: {
  workflowSources: string[];
  declaredSources: DeclaredSources;
  arguments_: [string, ...string[]];
}): boolean {
  const { arguments_, declaredSources, workflowSources } = args;
  const full = arguments_.join(" ");
  const withoutParent = arguments_.map((argument, index) =>
    index === 0 ? argument.replace(/^\.\.\//, "") : argument
  ).join(" ");
  const npmAlias = arguments_[0] === "npm" && arguments_[1] === "test" ?
    "npm run test" : null;
  const variants = [full, withoutParent, npmAlias].filter(
    (variant): variant is string => variant !== null,
  );
  const reachableSources = collectReachableSources({
    initialSources: workflowSources,
    declaredSources,
  });
  if (sourcesInvoke({ sources: reachableSources, variants })) return true;
  if (arguments_[0] === "bash" && arguments_.length > 2) {
    const wrapperPath = arguments_[1];
    const wrapperInner = arguments_.slice(2).join(" ");
    if (wrapperPath !== undefined && wrapperInvokes({
      workflowSources,
      declaredSources,
      wrapperPath,
      innerCommand: wrapperInner,
    })) return true;
  }
  return false;
}

async function readDeclaredSources(
  root: string,
  manifest: Manifest,
  errors: string[],
): Promise<DeclaredSources> {
  const packageScripts = new Map<string, string>();
  const packageSource = await readOptionalFile(join(root, manifest.adapter, "package.json"));
  if (packageSource !== null) {
    try {
      const packageValue: unknown = JSON.parse(packageSource);
      if (isRecord(packageValue) && isRecord(packageValue["scripts"])) {
        for (const [name, script] of Object.entries(packageValue["scripts"])) {
          if (typeof script === "string") packageScripts.set(name, script);
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

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function collectReachableSources(args: {
  initialSources: string[];
  declaredSources: DeclaredSources;
}): string[] {
  const sources = [...args.initialSources];
  const seenPackageScripts = new Set<string>();
  const seenWrappers = new Set<string>();
  for (let index = 0; index < sources.length; index += 1) {
    const source = sources[index];
    if (source === undefined) continue;
    for (const invocation of shellInvocations(source)) {
      const packageScript = invokedPackageScript(invocation);
      if (packageScript !== null && !seenPackageScripts.has(packageScript)) {
        seenPackageScripts.add(packageScript);
        const script = args.declaredSources.packageScripts.get(packageScript);
        if (script !== undefined) sources.push(script);
      }
      for (const wrapper of args.declaredSources.wrappers) {
        if (seenWrappers.has(wrapper.path)) continue;
        if (wrapperRemainder({ invocation, wrapperPath: wrapper.path }) === null) continue;
        seenWrappers.add(wrapper.path);
        sources.push(wrapper.source);
      }
    }
  }
  return sources;
}

function wrapperInvokes(args: {
  workflowSources: string[];
  declaredSources: DeclaredSources;
  wrapperPath: string;
  innerCommand: string;
}): boolean {
  const wrapper = args.declaredSources.wrappers.find(
    (candidate) => candidate.path === args.wrapperPath,
  );
  if (wrapper === undefined) return false;
  const tails = args.workflowSources.flatMap((source) =>
    shellInvocations(source).flatMap((invocation) => {
      const remainder = wrapperRemainder({ invocation, wrapperPath: args.wrapperPath });
      return remainder === null || remainder.length === 0 ? [] : [remainder];
    })
  );
  if (tails.length === 0) return false;
  const reachableSources = collectReachableSources({
    initialSources: [...tails, wrapper.source],
    declaredSources: args.declaredSources,
  });
  return sourcesInvoke({ sources: reachableSources, variants: [args.innerCommand] });
}

function sourcesInvoke(args: { sources: string[]; variants: string[] }): boolean {
  return args.sources.some((source) => shellInvocations(source).some((invocation) =>
    args.variants.some((variant) => invocationMatches({ invocation, variant }))
  ));
}

function invocationMatches(args: { invocation: string; variant: string }): boolean {
  const invocation = normalizeInvocation(args.invocation);
  const variant = normalizeWhitespace(args.variant);
  return invocation === variant || invocation.startsWith(`${variant} `);
}

function normalizeInvocation(invocation: string): string {
  let value = normalizeWhitespace(invocation);
  let previous = "";
  while (value !== previous) {
    previous = value;
    value = value.replace(/^(?:(?:if|then|do|else|elif|while|until|!|command|exec)\s+)+/, "");
    value = value.replace(
      /^(?:[A-Za-z_][A-Za-z0-9_]*=(?:"(?:[^"\\]|\\.)*"|'[^']*'|[^\s]+)\s+)+/,
      "",
    );
  }
  return value;
}

function normalizeWhitespace(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function invokedPackageScript(invocation: string): string | null {
  const normalized = normalizeInvocation(invocation);
  if (normalized === "npm test" || normalized.startsWith("npm test ")) return "test";
  const match = /^npm run (?:--\s+)?([^\s]+)/.exec(normalized);
  return match?.[1] ?? null;
}

function wrapperRemainder(args: { invocation: string; wrapperPath: string }): string | null {
  const invocation = normalizeInvocation(args.invocation);
  const paths = [args.wrapperPath, `./${args.wrapperPath.replace(/^\.\//, "")}`];
  const prefixes = paths.flatMap((path) => [`bash ${path}`, path]);
  for (const prefix of prefixes) {
    if (invocation === prefix) return "";
    if (invocation.startsWith(`${prefix} `)) return invocation.slice(prefix.length + 1);
  }
  return null;
}

function shellInvocations(source: string): string[] {
  const invocations: string[] = [];
  let current = "";
  let quote: "'" | '"' | null = null;
  let escaped = false;
  let pendingOperator: "and" | "or" | "sequence" = "sequence";
  let previousStatus: boolean | null = null;
  const flush = () => {
    const invocation = current.trim();
    if (invocation.length > 0) {
      const reachable = pendingOperator === "sequence" || previousStatus === null ||
        (pendingOperator === "and" && previousStatus) ||
        (pendingOperator === "or" && !previousStatus);
      if (reachable) {
        invocations.push(invocation);
        previousStatus = staticShellStatus(invocation);
      }
    }
    current = "";
  };
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (character === undefined) continue;
    if (escaped) {
      if (character !== "\n") current += character;
      escaped = false;
      continue;
    }
    if (character === "\\" && quote !== "'") {
      if (source[index + 1] === "\n") {
        current += " ";
        index += 1;
        continue;
      }
      escaped = true;
      current += character;
      continue;
    }
    if (quote !== null) {
      current += character;
      if (character === quote) quote = null;
      continue;
    }
    if (character === "'" || character === '"') {
      quote = character;
      current += character;
      continue;
    }
    if (character === "#" && (current.length === 0 || /\s$/.test(current))) {
      flush();
      pendingOperator = "sequence";
      while (source[index + 1] !== undefined && source[index + 1] !== "\n") index += 1;
      continue;
    }
    if (character === "\n" || character === ";") {
      flush();
      pendingOperator = "sequence";
      continue;
    }
    if (character === "|") {
      flush();
      pendingOperator = source[index + 1] === "|" ? "or" : "sequence";
      if (source[index + 1] === "|") index += 1;
      continue;
    }
    if (character === "&" && source[index + 1] === "&") {
      flush();
      pendingOperator = "and";
      index += 1;
      continue;
    }
    current += character;
  }
  flush();
  return invocations;
}

function staticShellStatus(invocation: string): boolean | null {
  const normalized = normalizeWhitespace(invocation);
  if (normalized.startsWith("! ")) {
    const nested = staticShellStatus(normalized.slice(2));
    return nested === null ? null : !nested;
  }
  const command = normalizeInvocation(normalized).split(" ")[0];
  if (command === "false") return false;
  if (command === "true" || command === ":") return true;
  return null;
}

function hasEnvironmentEvidence(args: {
  evidence: WorkflowEvidence;
  declaredSources: DeclaredSources;
  command: [string, ...string[]];
  expectedEnvironment: Record<string, string>;
}): boolean {
  return args.evidence.commands.some((entry) => {
    if (!hasCommandMarker({
      workflowSources: [entry.command],
      declaredSources: args.declaredSources,
      arguments_: args.command,
    })) return false;
    return Object.entries(args.expectedEnvironment).every(([key, expected]) =>
      entry.environment[key] === expected
    );
  });
}
