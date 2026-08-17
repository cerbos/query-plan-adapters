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
  executeRun,
  planRun,
  type RunOptions,
  type RunProfile,
} from "./runner.ts";
import { scaffold, type ScaffoldOptions } from "./scaffold.ts";
import { renderHumanView } from "./ui.ts";

export type CliDependencies = {
  root: string;
  stdout: Writable;
  stderr: Writable;
};

export async function runCli(args: {
  arguments_: string[];
  dependencies: CliDependencies;
}): Promise<number> {
  const { arguments_, dependencies } = args;
  try {
    if (arguments_.length === 1 && arguments_[0] === "list") {
      const adapters = await new Repository(dependencies.root).list();
      dependencies.stdout.write(renderHumanView({ kind: "adapter-list", adapters }));
      return 0;
    }
    if (arguments_[0] === "validate") {
      const options = parseValidate(arguments_.slice(1));
      const result = await new Repository(dependencies.root).validate(options);
      dependencies.stdout.write(renderHumanView({
        kind: "validation-notices",
        notices: result.notices,
      }));
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
        dependencies.stdout.write(renderHumanView({
          kind: "markdown-report",
          markdown: reportMarkdown(result.controlPlane),
        }));
      }
      return 0;
    }
    if (arguments_[0] === "explain") {
      if (arguments_.length !== 3) {
        throw new CliError({
          message: "usage: adapterctl explain adapter|action <name>",
          exitCode: 64,
        });
      }
      const kind = arguments_[1];
      const name = arguments_[2];
      if (name === undefined) {
        throw new CliError({ message: "explain requires a name", exitCode: 64 });
      }
      const result = await new Repository(dependencies.root).validate({
        discovery: true,
        adapter: null,
      });
      if (kind === "adapter") {
        const explanation = explainAdapter({ controlPlane: result.controlPlane, name });
        if (explanation !== null) {
          dependencies.stdout.write(renderHumanView({
            kind: "adapter-explanation",
            explanation,
          }));
          return 0;
        }
      } else if (kind === "action") {
        const explanation = explainAction({ controlPlane: result.controlPlane, name });
        if (explanation !== null) {
          dependencies.stdout.write(renderHumanView({
            kind: "action-explanation",
            explanation,
          }));
          return 0;
        }
      }
      throw new CliError({
        message: `unknown ${kind ?? "explain kind"}: ${name}`,
        exitCode: 64,
      });
    }
    if (arguments_[0] === "docs") {
      const mode = arguments_.length === 2 ? arguments_[1] : undefined;
      if (mode !== "--check" && mode !== "--write") {
        throw new CliError({ message: "usage: adapterctl docs --check|--write", exitCode: 64 });
      }
      const result = await new Repository(dependencies.root).validate({
        discovery: true,
        adapter: null,
      });
      if (mode === "--write") {
        const path = await writeDocs({ root: dependencies.root, controlPlane: result.controlPlane });
        dependencies.stdout.write(renderHumanView({ kind: "docs-confirmation", path }));
      } else {
        await checkDocs({ root: dependencies.root, controlPlane: result.controlPlane });
      }
      return 0;
    }
    if (arguments_[0] === "run") {
      const options = parseRun(arguments_.slice(1));
      const result = await new Repository(dependencies.root).validate({
        discovery: true,
        adapter: options.adapter,
      });
      const executions = planRun({ controlPlane: result.controlPlane, options });
      if (options.dryRun) {
        dependencies.stdout.write(renderHumanView({
          kind: "run-plan",
          executions,
        }));
      } else {
        await executeRun({
          root: dependencies.root,
          executions,
          profile: options.profile,
          stdout: dependencies.stdout,
          stderr: dependencies.stderr,
          progress: (event) => dependencies.stdout.write(renderHumanView({
            kind: "run-progress",
            event,
          })),
        });
      }
      return 0;
    }
    if (arguments_[0] === "scaffold") {
      const options = parseScaffold(arguments_.slice(1));
      dependencies.stdout.write(renderHumanView({
        kind: "scaffold-result",
        result: await scaffold({ root: dependencies.root, options }),
      }));
      return 0;
    }
    throw new CliError({ message: "usage: adapterctl <command> [options]", exitCode: 64 });
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
  throw new CliError({
    message: "usage: adapterctl report [--format json|markdown]",
    exitCode: 64,
  });
}

function parseValidate(arguments_: string[]): {
  discovery: boolean;
  adapter: string | null;
} {
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
      if (value === undefined) {
        throw new CliError({ message: "--adapter requires a name", exitCode: 64 });
      }
      adapter = value;
      index += 1;
      continue;
    }
    throw new CliError({
      message: `unknown validate option: ${argument ?? ""}`,
      exitCode: 64,
    });
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
    if (value === undefined) {
      throw new CliError({
        message: `${argument ?? "option"} requires a value`,
        exitCode: 64,
      });
    }
    switch (argument) {
      case "--profile":
        if (!isRunProfile(value)) {
          throw new CliError({ message: `unknown run profile: ${value}`, exitCode: 64 });
        }
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
        throw new CliError({
          message: `unknown run option: ${argument ?? ""}`,
          exitCode: 64,
        });
    }
    index += 1;
  }
  if (profile === null) {
    throw new CliError({ message: "run requires --profile", exitCode: 64 });
  }
  return { profile, adapter, environment, action, dryRun };
}

function isRunProfile(value: string): value is RunProfile {
  return value === "test" || value === "typecheck" || value === "conformance" ||
    value === "consumer" || value === "golden";
}

function parseScaffold(arguments_: string[]): ScaffoldOptions {
  const name = arguments_[0];
  if (name === undefined || name.startsWith("--")) {
    throw new CliError({
      message:
        "usage: adapterctl scaffold NAME --ecosystem VALUE --package NAME [--usage-only] [--dry-run]",
      exitCode: 64,
    });
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
    if (value === undefined) {
      throw new CliError({
        message: `${argument ?? "option"} requires a value`,
        exitCode: 64,
      });
    }
    if (argument === "--ecosystem") ecosystem = value;
    else if (argument === "--package") packageName = value;
    else {
      throw new CliError({
        message: `unknown scaffold option: ${argument ?? ""}`,
        exitCode: 64,
      });
    }
    index += 1;
  }
  if (ecosystem === null || packageName === null) {
    throw new CliError({
      message: "scaffold requires --ecosystem and --package",
      exitCode: 64,
    });
  }
  return { name, ecosystem, packageName, coverage, dryRun };
}
