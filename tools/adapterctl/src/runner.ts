import { spawn } from "node:child_process";
import { join } from "node:path";
import type { Writable } from "node:stream";

import { CliError } from "./errors.ts";
import type { Command, ControlPlane, Manifest } from "./model.ts";

export type RunProfile = "test" | "typecheck" | "conformance" | "consumer" | "golden";

export type RunOptions = {
  profile: RunProfile;
  adapter: string | null;
  environment: string | null;
  action: string | null;
  dryRun: boolean;
};

export type Execution = {
  adapter: string;
  environment: string | null;
  command: Extract<Command, { kind: "command" }>;
  env: Record<string, string>;
};

export type RunProgress =
  | { kind: "execution-started"; execution: Execution }
  | { kind: "golden-unchanged"; adapter: string }
  | { kind: "golden-changed"; adapter: string; changes: string };

export function planRun(args: { controlPlane: ControlPlane; options: RunOptions }): Execution[] {
  const { controlPlane, options } = args;
  if (options.action !== null && options.profile !== "conformance") {
    throw new CliError({
      message: "--action is only valid with --profile conformance",
      exitCode: 64,
    });
  }
  if (options.environment !== null && options.profile !== "conformance") {
    throw new CliError({
      message: "--environment is only valid with --profile conformance",
      exitCode: 64,
    });
  }
  if (options.action !== null && !controlPlane.catalog.actions.some(
    (action) => action.name === options.action,
  )) {
    throw new CliError({ message: `unknown action: ${options.action}`, exitCode: 64 });
  }
  const executions: Execution[] = [];
  for (const manifest of controlPlane.manifests) {
    if (options.profile === "conformance") {
      const environments = options.environment === null ? manifest.semanticEnvironments :
        manifest.semanticEnvironments.filter((environment) => environment.name === options.environment);
      if (options.adapter !== null && environments.length === 0 && options.environment === null) {
        throw new CliError({
          message: `${manifest.adapter}: conformance command is unavailable`,
          exitCode: 64,
        });
      }
      if (options.environment !== null && environments.length === 0) {
        throw new CliError({
          message: `${manifest.adapter}: unknown semantic environment ${options.environment}`,
          exitCode: 64,
        });
      }
      for (const environment of environments) {
        const env = { ...environment.env };
        if (options.action !== null) env["ADAPTERCTL_ACTION"] = options.action;
        executions.push({
          adapter: manifest.adapter,
          environment: environment.name,
          command: environment.command,
          env,
        });
      }
      continue;
    }
    const command = commandForProfile(manifest, options.profile);
    if (command.kind === "unavailable") {
      if (options.adapter !== null) {
        throw new CliError({
          message: `${manifest.adapter}: ${options.profile} command is unavailable`,
          exitCode: 64,
        });
      }
      continue;
    }
    executions.push({
      adapter: manifest.adapter,
      environment: null,
      command,
      env: {},
    });
  }
  return executions;
}

export async function executeRun(args: {
  root: string;
  executions: Execution[];
  profile: RunProfile;
  stdout: Writable;
  stderr: Writable;
  progress: (event: RunProgress) => void;
}): Promise<void> {
  const failures: string[] = [];
  for (const execution of args.executions) {
    args.progress({ kind: "execution-started", execution });
    const code = await spawnCommand({
      command: execution.command,
      cwd: join(args.root, execution.adapter),
      env: { ...process.env, ...execution.env },
      stdout: args.stdout,
      stderr: args.stderr,
    });
    if (code !== 0) {
      failures.push(`${executionLabel(execution)}: command exited with status ${code}`);
      continue;
    }
    if (args.profile === "golden") {
      const changed = await captureCommand({
        command: {
          kind: "command",
          arguments: ["git", "status", "--short", "--", `${execution.adapter}/golden`],
        },
        cwd: args.root,
      });
      if (changed.trim().length === 0) {
        args.progress({ kind: "golden-unchanged", adapter: execution.adapter });
      } else {
        args.progress({
          kind: "golden-changed",
          adapter: execution.adapter,
          changes: changed.trimEnd(),
        });
      }
    }
  }
  if (failures.length > 0) {
    throw new CliError({ message: `run failed:\n${failures.join("\n")}` });
  }
}

function executionLabel(execution: Execution): string {
  return execution.environment === null ? execution.adapter :
    `${execution.adapter}/${execution.environment}`;
}

function commandForProfile(manifest: Manifest, profile: Exclude<RunProfile, "conformance">): Command {
  switch (profile) {
    case "test":
      return manifest.commands.test;
    case "typecheck":
      return manifest.commands.typecheck;
    case "consumer":
      return manifest.commands.consumer;
    case "golden":
      return manifest.commands.golden;
    default: {
      const exhaustive: never = profile;
      return exhaustive;
    }
  }
}

async function spawnCommand(args: {
  command: Extract<Command, { kind: "command" }>;
  cwd: string;
  env: NodeJS.ProcessEnv;
  stdout: Writable;
  stderr: Writable;
}): Promise<number> {
  const [executable, ...commandArguments] = args.command.arguments;
  return await new Promise((resolveCode, reject) => {
    const child = spawn(executable, commandArguments, {
      cwd: args.cwd,
      env: args.env,
      shell: false,
      stdio: ["inherit", "pipe", "pipe"],
    });
    child.stdout.pipe(args.stdout, { end: false });
    child.stderr.pipe(args.stderr, { end: false });
    child.once("error", reject);
    child.once("close", (code) => resolveCode(code ?? 1));
  });
}

async function captureCommand(args: {
  command: Extract<Command, { kind: "command" }>;
  cwd: string;
}): Promise<string> {
  const [executable, ...commandArguments] = args.command.arguments;
  return await new Promise((resolveOutput, reject) => {
    const child = spawn(executable, commandArguments, {
      cwd: args.cwd,
      shell: false,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (code) => {
      if (code === 0) resolveOutput(stdout);
      else reject(new CliError({ message: `git status failed: ${stderr.trim()}` }));
    });
  });
}
