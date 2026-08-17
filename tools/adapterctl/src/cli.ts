import type { Writable } from "node:stream";

import { CliError, ValidationError } from "./errors.ts";
import { checkDocs, writeDocs } from "./docs.ts";
import { Repository } from "./repository.ts";
import {
  buildReport,
  explainAction,
  explainAdapter,
  reportMarkdown,
} from "./reporter.ts";
import {
  describeExecution,
  executeRun,
  planRun,
  type RunOptions,
  type RunProfile,
} from "./runner.ts";
import { scaffold, type ScaffoldOptions } from "./scaffold.ts";
import { renderLines } from "./ui.ts";

export type CliDependencies = {
  root: string;
  stdout: Writable;
  stderr: Writable;
};

export async function runCli(arguments_: string[], dependencies: CliDependencies): Promise<number> {
  try {
    if (arguments_.length === 1 && arguments_[0] === "list") {
      const adapters = await new Repository(dependencies.root).list();
      dependencies.stdout.write(renderLines(adapters));
      return 0;
    }
    if (arguments_[0] === "validate") {
      const options = parseValidate(arguments_.slice(1));
      const result = await new Repository(dependencies.root).validate(options);
      dependencies.stdout.write(renderLines(result.notices));
      return 0;
    }
    if (arguments_[0] === "report") {
      const format = parseReport(arguments_.slice(1));
      const result = await new Repository(dependencies.root).validate({
        discovery: true,
        adapter: null,
      });
      if (format === "json") {
        dependencies.stdout.write(`${JSON.stringify(buildReport(result.controlPlane), null, 2)}\n`);
      } else {
        dependencies.stdout.write(renderLines(reportMarkdown(result.controlPlane).trimEnd().split("\n")));
      }
      return 0;
    }
    if (arguments_[0] === "explain") {
      if (arguments_.length !== 3) {
        throw new CliError("usage: adapterctl explain adapter|action <name>", 64);
      }
      const kind = arguments_[1];
      const name = arguments_[2];
      if (name === undefined) throw new CliError("explain requires a name", 64);
      const result = await new Repository(dependencies.root).validate({
        discovery: true,
        adapter: null,
      });
      const lines = kind === "adapter" ? explainAdapter(result.controlPlane, name) :
        kind === "action" ? explainAction(result.controlPlane, name) : [];
      if (lines.length === 0) throw new CliError(`unknown ${kind ?? "explain kind"}: ${name}`, 64);
      dependencies.stdout.write(renderLines(lines));
      return 0;
    }
    if (arguments_[0] === "docs") {
      const mode = arguments_.length === 2 ? arguments_[1] : undefined;
      if (mode !== "--check" && mode !== "--write") {
        throw new CliError("usage: adapterctl docs --check|--write", 64);
      }
      const result = await new Repository(dependencies.root).validate({
        discovery: true,
        adapter: null,
      });
      if (mode === "--write") {
        const path = await writeDocs(dependencies.root, result.controlPlane);
        dependencies.stdout.write(renderLines([`wrote ${path}`]));
      } else {
        await checkDocs(dependencies.root, result.controlPlane);
      }
      return 0;
    }
    if (arguments_[0] === "run") {
      const options = parseRun(arguments_.slice(1));
      const result = await new Repository(dependencies.root).validate({
        discovery: true,
        adapter: options.adapter,
      });
      const executions = planRun(result.controlPlane, options);
      if (options.dryRun) {
        dependencies.stdout.write(renderLines(executions.map(describeExecution)));
      } else {
        await executeRun({
          root: dependencies.root,
          executions,
          profile: options.profile,
          stdout: dependencies.stdout,
          stderr: dependencies.stderr,
          progress: (line) => dependencies.stdout.write(renderLines(line.split("\n"))),
        });
      }
      return 0;
    }
    if (arguments_[0] === "scaffold") {
      const options = parseScaffold(arguments_.slice(1));
      dependencies.stdout.write(renderLines(await scaffold(dependencies.root, options)));
      return 0;
    }
    throw new CliError("usage: adapterctl <command> [options]", 64);
  } catch (error: unknown) {
    if (error instanceof ValidationError) {
      for (const message of error.errors) dependencies.stderr.write(`adapterctl: ${message}\n`);
      return error.exitCode;
    }
    if (error instanceof CliError) {
      dependencies.stderr.write(`adapterctl: ${error.message}\n`);
      return error.exitCode;
    }
    const message = error instanceof Error ? error.message : String(error);
    dependencies.stderr.write(`adapterctl: ${message}\n`);
    return 2;
  }
}

function parseReport(arguments_: string[]): "json" | "markdown" {
  if (arguments_.length === 0) return "markdown";
  if (arguments_.length === 2 && arguments_[0] === "--format") {
    const format = arguments_[1];
    if (format === "json" || format === "markdown") return format;
  }
  throw new CliError("usage: adapterctl report [--format json|markdown]", 64);
}

function parseValidate(arguments_: string[]): { discovery: boolean; adapter: string | null } {
  let discovery = false;
  let adapter: string | null = null;
  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index];
    if (argument === "--discovery") {
      discovery = true;
      continue;
    }
    if (argument === "--adapter") {
      const value = arguments_[index + 1];
      if (value === undefined) throw new CliError("--adapter requires a name", 64);
      adapter = value;
      index += 1;
      continue;
    }
    throw new CliError(`unknown validate option: ${argument ?? ""}`, 64);
  }
  return { discovery, adapter };
}

function parseRun(arguments_: string[]): RunOptions {
  let profile: RunProfile | null = null;
  let adapter: string | null = null;
  let environment: string | null = null;
  let action: string | null = null;
  let dryRun = false;
  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index];
    if (argument === "--dry-run") {
      dryRun = true;
      continue;
    }
    const value = arguments_[index + 1];
    if (value === undefined) throw new CliError(`${argument ?? "option"} requires a value`, 64);
    switch (argument) {
      case "--profile":
        if (!isRunProfile(value)) throw new CliError(`unknown run profile: ${value}`, 64);
        profile = value;
        break;
      case "--adapter":
        adapter = value;
        break;
      case "--environment":
        environment = value;
        break;
      case "--action":
        action = value;
        break;
      default:
        throw new CliError(`unknown run option: ${argument ?? ""}`, 64);
    }
    index += 1;
  }
  if (profile === null) throw new CliError("run requires --profile", 64);
  return { profile, adapter, environment, action, dryRun };
}

function isRunProfile(value: string): value is RunProfile {
  return value === "test" || value === "typecheck" || value === "conformance" ||
    value === "consumer" || value === "golden";
}

function parseScaffold(arguments_: string[]): ScaffoldOptions {
  const name = arguments_[0];
  if (name === undefined || name.startsWith("--")) {
    throw new CliError(
      "usage: adapterctl scaffold NAME --ecosystem VALUE --package NAME [--usage-only] [--dry-run]",
      64,
    );
  }
  let ecosystem: string | null = null;
  let packageName: string | null = null;
  let coverage: ScaffoldOptions["coverage"] = "artifact-install";
  let dryRun = false;
  for (let index = 1; index < arguments_.length; index += 1) {
    const argument = arguments_[index];
    if (argument === "--usage-only") {
      coverage = "usage-only";
      continue;
    }
    if (argument === "--dry-run") {
      dryRun = true;
      continue;
    }
    const value = arguments_[index + 1];
    if (value === undefined) throw new CliError(`${argument ?? "option"} requires a value`, 64);
    if (argument === "--ecosystem") ecosystem = value;
    else if (argument === "--package") packageName = value;
    else throw new CliError(`unknown scaffold option: ${argument ?? ""}`, 64);
    index += 1;
  }
  if (ecosystem === null || packageName === null) {
    throw new CliError("scaffold requires --ecosystem and --package", 64);
  }
  return { name, ecosystem, packageName, coverage, dryRun };
}
