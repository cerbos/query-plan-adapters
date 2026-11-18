import type { GenericValidator } from "convex/values";

import type { MapperConfig } from "../../src/index";

export interface DomainMapping {
  table: Record<string, GenericValidator>;
  mapper: Record<string, MapperConfig>;
  nulls: "omitted" | "explicit";
  toDocument?: (resource: { id: string; attr: Record<string, unknown> }) => Record<string, unknown>;
}

/**
 * One entry per attribute path the policies read, `true` where the path can be absent from a
 * document (so its predicates run in the post-filter), `false` where it is always stored. A string
 * names a document field that differs from the attribute path.
 */
export function mapperOf(
  paths: Record<string, boolean | [string, boolean]>,
): Record<string, MapperConfig> {
  return {
    "request.resource.id": { field: "id", nullable: false },
    ...Object.fromEntries(
      Object.entries(paths).map(([path, spec]) => {
        const [field, nullable] = Array.isArray(spec) ? spec : [path, spec];

        return [`request.resource.attr.${path}`, { field, nullable }];
      }),
    ),
  };
}
