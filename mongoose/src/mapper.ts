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

/** `config` with `nullable: true` wherever it, or a relation field beneath it, declares nothing. */
const nullableByDefault = (config: MapperConfig): MapperConfig => {
  const fields = config.relation?.fields;
  return {
    ...config,
    nullable: config.nullable ?? true,
    ...(config.relation && fields
      ? {
          relation: {
            ...config.relation,
            fields: Object.fromEntries(
              Object.entries(fields).map(([key, field]) => [
                key,
                nullableByDefault(field),
              ]),
            ),
          },
        }
      : {}),
  };
};

/**
 * The mapper as the `"omitted"` convention reads it: every entry that does not declare `nullable`
 * is `nullable: true`, and `nullable: false` still opts an entry out.
 *
 * The call-level convention is the default for an attribute that declares nothing (ADR 0004).
 * Under `"omitted"`, a NULL field sends no attribute and CEL denies the document on a
 * missing-attribute error, while MongoDB's `$ne` and `$nor` match a document the path is absent
 * from or null in. Refusing null operands alone left `R.attr.x != "a"` returning those documents
 * on every field that did not declare `nullable` (cerbos/query-plan-adapters#493).
 */
export const withOmittedNullDefault = (mapper: Mapper): Mapper => {
  if (typeof mapper === "function") {
    return (key) => {
      const config = mapper(key);
      return config && nullableByDefault(config);
    };
  }
  return Object.fromEntries(
    Object.entries(mapper).map(([key, config]) => [
      key,
      nullableByDefault(config),
    ]),
  );
};

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

/** The document path a plan variable maps to, or undefined when the mapper has no entry for it. */
const lookupFieldReference = (
  reference: string,
  mapper: Mapper,
): ResolvedFieldReference | undefined => {
  const config = lookupConfig(mapper, reference);
  if (config?.relation) {
    return relationReference(config.relation, config.relation.field);
  }
  if (config?.field) {
    return { path: [config.field] };
  }

  const parts = reference.split(".");
  const lastPart = parts.pop();
  if (parts.length > 0 && lastPart) {
    const parentRelation = lookupConfig(mapper, parts.join("."))?.relation;
    if (parentRelation) {
      return relationReference(
        parentRelation,
        parentRelation.fields?.[lastPart]?.field || lastPart,
      );
    }
  }

  // An entry with neither `field` nor `relation` is the caller's opt-in to the plan path verbatim.
  return config ? { path: [reference] } : undefined;
};

/** Resolves a plan variable to a document path, through the relation it belongs to if any. */
export const resolveFieldReference = (
  reference: string,
  mapper: Mapper,
): ResolvedFieldReference => {
  const resolved = lookupFieldReference(reference, mapper);
  if (!resolved) {
    throw unmappedReferenceError(reference);
  }
  return resolved;
};

/**
 * The relation a plan variable is reached through, if any. Unlike `resolveFieldReference` it does
 * not refuse an unmapped name: the guards that ask it walk every variable in an operand, field
 * names included (`get-field`'s second operand), and the emission site refuses the references.
 */
export const relationOfReference = (
  reference: string,
  mapper: Mapper,
): ResolvedFieldReference["relation"] =>
  lookupFieldReference(reference, mapper)?.relation;

/**
 * An unmapped reference is refused rather than used verbatim as a document path.
 *
 * A plan reference such as `request.resource.attr.status` names no field in any real collection,
 * and MongoDB's negations match a document the path is absent from: `$ne` and `$nor` over a path
 * nothing stores select every document, so a typo or a missing mapper entry turned
 * `R.attr.status != "x"` into a filter returning the whole collection
 * (cerbos/query-plan-adapters#492). A caller whose documents really are shaped like the plan path
 * opts in per reference, with an entry — `{}` or `{ field: reference }` — or a function mapper
 * that returns one.
 */
export const unmappedReferenceError = (reference: string): Error =>
  new Error(
    `No mapper entry for ${reference}: an unmapped reference is not used verbatim as a ` +
      "document path, because MongoDB's $ne and $nor match every document a path is absent " +
      "from. Map it to a field, or declare it with an entry to use the plan path as-is.",
  );

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
