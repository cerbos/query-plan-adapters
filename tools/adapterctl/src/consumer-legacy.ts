import { isDeepStrictEqual } from "node:util";

import { isRecord } from "./decode.ts";
import type { ConsumerCases } from "./model.ts";

export function compareConsumerLegacy(cases: ConsumerCases, legacy: unknown): string[] {
  if (!isRecord(legacy) || !isRecord(legacy["shapes"])) {
    return ["demo/expected.json: expected shapes object"];
  }
  const normalizedLegacy: Record<string, unknown> = {};
  for (const [operation, shape] of Object.entries(legacy["shapes"])) {
    if (!isRecord(shape) || !isRecord(shape["results"])) {
      return [`demo/expected.json.shapes.${operation}: expected results object`];
    }
    normalizedLegacy[operation] = shape["results"];
  }
  const projected: Record<string, Record<string, unknown>> = {};
  for (const consumerCase of cases.cases) {
    const results = projected[consumerCase.operation] ?? {};
    const key = `${consumerCase.principal}/${consumerCase.action}`;
    if (consumerCase.operation === "paginated") {
      if (consumerCase.pagination === null) continue;
      results[key] = {
        kind: consumerCase.expected.kind,
        pageSize: consumerCase.pagination.pageSize,
        pageSizes: consumerCase.pagination.pageSizes,
        ids: consumerCase.expected.ids,
      };
    } else {
      results[key] = {
        kind: consumerCase.expected.kind,
        ids: consumerCase.expected.ids,
      };
    }
    projected[consumerCase.operation] = results;
  }
  return isDeepStrictEqual(projected, normalizedLegacy) ? [] : [
    "demo/cases.json does not match demo/expected.json legacy results",
  ];
}
