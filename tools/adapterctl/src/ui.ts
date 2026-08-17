import { createElement } from "react";
import { renderToString, Text } from "ink";

export function renderLines(lines: string[]): string {
  if (lines.length === 0) return "";
  const rendered = renderToString(createElement(Text, null, lines.join("\n")), { columns: 10_000 });
  return `${rendered}\n`;
}
