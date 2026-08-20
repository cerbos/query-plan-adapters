import { createElement } from "react";
import { Box, renderToString, Text } from "ink";

import type { Outcome } from "./model.ts";
import type { ActionExplanation, AdapterExplanation } from "./reporter.ts";
import type { Execution, RunProgress } from "./runner.ts";
import type { ScaffoldResult } from "./scaffold.ts";

export type HumanView =
  | { kind: "adapter-list"; adapters: string[] }
  | { kind: "markdown-report"; markdown: string }
  | { kind: "validation-notices"; notices: string[] }
  | { kind: "adapter-explanation"; explanation: AdapterExplanation }
  | { kind: "action-explanation"; explanation: ActionExplanation }
  | { kind: "run-plan"; executions: Execution[] }
  | { kind: "run-progress"; event: RunProgress }
  | { kind: "docs-confirmation"; path: string }
  | { kind: "scaffold-result"; result: ScaffoldResult };

function AdapterListView(props: { adapters: string[] }) {
  return createElement(
    Box,
    { flexDirection: "column" },
    ...props.adapters.map((adapter) => createElement(Text, { key: adapter }, adapter)),
  );
}

function MarkdownReportView(props: { markdown: string }) {
  return createElement(
    Box,
    { flexDirection: "column" },
    createElement(Text, null, props.markdown.trimEnd()),
  );
}

function ValidationNoticesView(props: { notices: string[] }) {
  return createElement(
    Box,
    { flexDirection: "column" },
    ...props.notices.map((notice, index) =>
      createElement(Text, { key: `notice-${index}` }, notice)
    ),
  );
}

function AdapterExplanationView(props: { explanation: AdapterExplanation }) {
  const { commands, semanticEnvironments, summary } = props.explanation;
  return createElement(
    Box,
    { flexDirection: "column" },
    createElement(Text, null, `Adapter: ${summary.adapter}`),
    createElement(
      Text,
      null,
      `Package: ${props.explanation.package.ecosystem}:${props.explanation.package.name}`,
    ),
    createElement(Text, null, `Workflow: ${props.explanation.workflow}`),
    createElement(Text, null, `Consumer coverage: ${summary.consumerCoverage}`),
    createElement(
      Text,
      null,
      `Outcomes: ${summary.matched} matched, ${summary.rejected} rejected, ${summary.upstreamBlocked} upstream-blocked, ${summary.unassessed} unassessed`,
    ),
    createElement(Text, null, "Commands:"),
    ...Object.entries(commands).map(([profile, command]) =>
      createElement(Text, { key: profile }, `- ${profile}: ${formatCommand(command)}`)
    ),
    createElement(Text, null, "Semantic environments:"),
    ...(semanticEnvironments.length === 0 ? [createElement(Text, { key: "none" }, "- none")] :
      semanticEnvironments.map((environment) => {
        const entries = Object.entries(environment.env)
          .sort(([left], [right]) => left.localeCompare(right));
        const env = entries.length === 0 ? "" :
          ` [${entries.map(([key, value]) => `${key}=${value}`).join(", ")}]`;
        return createElement(
          Text,
          { key: environment.name },
          `- ${environment.name}: ${formatCommand(environment.command)}${env}`,
        );
      })),
  );
}

function ActionExplanationView(props: { explanation: ActionExplanation }) {
  const { action, outcomes } = props.explanation;
  const reason = "reason" in action.oracleExpectation ?
    ` — ${action.oracleExpectation.reason}` : "";
  return createElement(
    Box,
    { flexDirection: "column" },
    createElement(Text, null, `Action: ${action.name}`),
    createElement(
      Text,
      null,
      `Oracle expectation: ${action.oracleExpectation.kind}${reason}`,
    ),
    createElement(Text, null, "Adapter outcomes:"),
    ...outcomes.map(({ adapter, outcome }) =>
      createElement(Text, { key: adapter }, `- ${adapter}: ${formatOutcome(outcome)}`)
    ),
  );
}

function RunPlanView(props: { executions: Execution[] }) {
  return createElement(
    Box,
    { flexDirection: "column" },
    ...props.executions.map((execution, index) =>
      createElement(Text, { key: `execution-${index}` }, formatExecution(execution))
    ),
  );
}

function RunProgressView(props: { event: RunProgress }) {
  const { event } = props;
  switch (event.kind) {
    case "execution-started":
      return createElement(Text, null, `running ${formatExecution(event.execution)}`);
    case "golden-unchanged":
      return createElement(Text, null, `${event.adapter}: no golden changes`);
    case "golden-changed":
      return createElement(
        Box,
        { flexDirection: "column" },
        createElement(Text, null, `${event.adapter} golden changes:`),
        createElement(Text, null, event.changes),
      );
    default: {
      const exhaustive: never = event;
      return exhaustive;
    }
  }
}

function DocsConfirmationView(props: { path: string }) {
  return createElement(
    Box,
    null,
    createElement(Text, null, `wrote ${props.path}`),
  );
}

function ScaffoldResultView(props: { result: ScaffoldResult }) {
  const verb = props.result.status === "planned" ? "would write" : "wrote";
  return createElement(
    Box,
    { flexDirection: "column" },
    createElement(Text, null, `${verb} ${props.result.manifestPath}`),
    createElement(Text, null, "Next steps:"),
    createElement(Text, null, "- set commands.test to the native translator test command"),
    createElement(Text, null, "- add each native semantic environment and its array command"),
    createElement(Text, null, "- set commands.consumer to the native consumer smoke command"),
    createElement(
      Text,
      null,
      `- create ${props.result.workflow} with adapter, conformance, and demo path triggers`,
    ),
    createElement(
      Text,
      null,
      `- run ./adapterctl validate --discovery --adapter ${props.result.adapter}`,
    ),
  );
}

function HumanViewComponent(props: { view: HumanView }) {
  switch (props.view.kind) {
    case "adapter-list":
      return createElement(AdapterListView, { adapters: props.view.adapters });
    case "markdown-report":
      return createElement(MarkdownReportView, { markdown: props.view.markdown });
    case "validation-notices":
      return createElement(ValidationNoticesView, { notices: props.view.notices });
    case "adapter-explanation":
      return createElement(AdapterExplanationView, { explanation: props.view.explanation });
    case "action-explanation":
      return createElement(ActionExplanationView, { explanation: props.view.explanation });
    case "run-plan":
      return createElement(RunPlanView, { executions: props.view.executions });
    case "run-progress":
      return createElement(RunProgressView, { event: props.view.event });
    case "docs-confirmation":
      return createElement(DocsConfirmationView, { path: props.view.path });
    case "scaffold-result":
      return createElement(ScaffoldResultView, { result: props.view.result });
    default: {
      const exhaustive: never = props.view;
      return exhaustive;
    }
  }
}

function formatExecution(execution: Execution): string {
  const label = execution.environment === null ? execution.adapter :
    `${execution.adapter}/${execution.environment}`;
  const env = Object.entries(execution.env)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => shellQuote(`${key}=${value}`))
    .join(" ");
  const prefix = env.length === 0 ? "" : `env ${env} -- `;
  return `${label}: ${prefix}${execution.command.arguments.map(shellQuote).join(" ")}`;
}

function formatCommand(command: AdapterExplanation["commands"]["test"]): string {
  return command.kind === "unavailable" ? "unavailable" :
    command.arguments.map(shellQuote).join(" ");
}

function shellQuote(value: string): string {
  if (/^[A-Za-z0-9_@%+=:,./-]+$/.test(value)) return value;
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

function formatOutcome(outcome: Outcome): string {
  switch (outcome.kind) {
    case "matched":
    case "unassessed":
      return outcome.kind;
    case "rejected":
      return `rejected — ${outcome.reason} (message: ${outcome.message})`;
    case "upstream-blocked":
      return `upstream-blocked — ${outcome.reason}`;
    default: {
      const exhaustive: never = outcome;
      return exhaustive;
    }
  }
}

export function renderHumanView(view: HumanView): string {
  const rendered = renderToString(createElement(HumanViewComponent, { view }), {
    columns: 10_000,
  });
  return rendered.length === 0 ? "" : `${rendered}\n`;
}
