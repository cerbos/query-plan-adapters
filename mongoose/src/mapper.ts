import type { Mapper, MapperConfig } from "./index";

/** The caller's entry for exactly `reference`, whichever of the two mapper forms it supplied. */
const lookupConfig = (
  mapper: Mapper,
  reference: string,
): MapperConfig | undefined =>
  typeof mapper === "function" ? mapper(reference) : mapper[reference];

/** `a.b` → the `b` entry in the `fields` of the relation `a` is mapped to, if any. */
const relationFieldConfig = (
  reference: string,
  mapper: Mapper,
): MapperConfig | undefined => {
  const parts = reference.split(".");
  const lastPart = parts.pop();
  if (parts.length === 0 || !lastPart) {
    return undefined;
  }
  return lookupConfig(mapper, parts.join("."))?.relation?.fields?.[lastPart];
};

/** The mapper entry for a reference: its own, or the one its parent relation declares for it. */
export const resolveMapperConfig = (
  reference: string,
  mapper: Mapper,
): MapperConfig | undefined =>
  lookupConfig(mapper, reference) || relationFieldConfig(reference, mapper);

export const isNullableReference = (
  reference: string,
  mapper: Mapper,
): boolean => resolveMapperConfig(reference, mapper)?.nullable === true;

export const applyValueParser = (
  reference: string,
  value: unknown,
  mapper: Mapper,
): unknown => {
  // Unlike resolveMapperConfig, a reference's own entry does not shadow its relation's parser.
  const parser =
    lookupConfig(mapper, reference)?.valueParser ||
    relationFieldConfig(reference, mapper)?.valueParser;
  return parser ? parser(value) : value;
};

export type ResolvedFieldReference = {
  /** The document path; a to-many relation keeps its array segment first so it can be split off. */
  path: string[];
  relation?: {
    name: string;
    type: "one" | "many";
    requiresParent?: string;
  };
};

const relationReference = (
  { name, type, requiresParent }: NonNullable<MapperConfig["relation"]>,
  field: string | undefined,
): ResolvedFieldReference => ({
  path: !field ? [name] : type === "one" ? [`${name}.${field}`] : [name, field],
  relation: { name, type, requiresParent },
});

/** Resolves a plan variable to a document path, through the relation it belongs to if any. */
export const resolveFieldReference = (
  reference: string,
  mapper: Mapper,
): ResolvedFieldReference => {
  const parts = reference.split(".");
  const lastPart = parts[parts.length - 1];
  if (!lastPart) {
    return { path: [reference] };
  }

  const config = lookupConfig(mapper, reference);
  if (config?.relation) {
    return relationReference(config.relation, config.relation.field);
  }
  if (config?.field) {
    return { path: [config.field] };
  }

  if (parts.length > 1) {
    const parentRelation = lookupConfig(
      mapper,
      parts.slice(0, -1).join("."),
    )?.relation;
    if (parentRelation) {
      return relationReference(
        parentRelation,
        parentRelation.fields?.[lastPart]?.field || lastPart,
      );
    }
  }

  return { path: [reference] };
};

/**
 * The mapper a collection macro's lambda body is translated with: the iteration variable (and
 * `variable.field`) resolve against the relation's element `fields`, relative to the element;
 * every other key falls through to the caller's mapper.
 */
export const createScopedMapper =
  (collectionPath: string, variableName: string, fullMapper: Mapper): Mapper =>
  (key: string) => {
    if (key !== variableName && !key.startsWith(`${variableName}.`)) {
      return typeof fullMapper === "function"
        ? fullMapper(key)
        : fullMapper[key] || { field: key };
    }
    const relation = lookupConfig(fullMapper, collectionPath)?.relation;
    if (key === variableName) {
      const field = relation?.field;
      if (!field) return { field: key };
      return relation.fields?.[field] ?? { field };
    }
    const elementField = key.slice(variableName.length + 1);
    return relation?.fields?.[elementField] || { field: elementField };
  };
