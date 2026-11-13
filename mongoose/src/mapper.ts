import type { Mapper, MapperConfig } from "./index";
import { UnsupportedQueryPlanError } from "./errors";
import { isFunction } from "./narrowing";
import type { MongoValue } from "./expression";

/**
 * The caller's mapper as one translation scope reads it. `queryPlanToMongoose` builds the root
 * scope, and each macro body gets its own.
 */
export type ScopedMapper = {
  /** The entry for exactly `reference`. */
  readonly lookup: (reference: string) => MapperConfig | undefined;
  /** Whether an entry that does not declare `nullable` is nullable (the call's convention). */
  readonly nullableDefault: boolean;
  /** The macro variables whose elements this scope reads. */
  readonly bound: readonly string[];
};

/** `a.b` → the `b` entry in the `fields` of the relation `a` is mapped to, if any. */
const relationFieldConfig = (
  reference: string,
  mapper: ScopedMapper,
): MapperConfig | undefined => {
  const parts = reference.split(".");
  const lastPart = parts.pop();

  if (parts.length === 0 || !lastPart) {
    return undefined;
  }

  return mapper.lookup(parts.join("."))?.relation?.fields?.[lastPart];
};

/** The mapper entry for a reference: its own, or the one its parent relation declares for it. */
export const resolveMapperConfig = (
  reference: string,
  mapper: ScopedMapper,
): MapperConfig | undefined =>
  mapper.lookup(reference) || relationFieldConfig(reference, mapper);

/**
 * The scalar type a reference's stored value is declared with — for a relation mapped to one
 * element field (`relation.field`), that element field's — or undefined when a constant cannot be
 * checked against it: no `valueType`, a `dateTime` (compared through its own path), or a
 * `valueParser`, which is the caller's explicit override of the constant.
 */
export const declaredScalarType = (
  reference: string,
  mapper: ScopedMapper,
): "number" | "string" | "boolean" | undefined => {
  const config = resolveMapperConfig(reference, mapper);
  const relation = config?.relation;

  const typed = relation
    ? relation.field
      ? relation.fields?.[relation.field]
      : undefined
    : config;

  if (!typed || typed.valueParser || config?.valueParser) {
    return undefined;
  }

  return typed.valueType === "dateTime" ? undefined : typed.valueType;
};

/** `config` with `nullable: true` wherever it, or a relation field beneath it, declares nothing. */
const nullableByDefault = (config: MapperConfig): MapperConfig => {
  const fields = config.relation?.fields;

  const defaulted: MapperConfig = { ...config, nullable: config.nullable ?? true };

  if (config.relation && fields) {
    defaulted.relation = {
      ...config.relation,
      fields: Object.fromEntries(
        Object.entries(fields).map(([key, field]) => [key, nullableByDefault(field)]),
      ),
    };
  }

  return defaulted;
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
export const withOmittedNullDefault =
  (lookup: ScopedMapper["lookup"]): ScopedMapper["lookup"] =>
  (reference) => {
    const config = lookup(reference);

    return config && nullableByDefault(config);
  };

/** The root scope: the caller's mapper, in either of its two forms, under the call's convention. */
export const rootMapper = (mapper: Mapper, nullableDefault: boolean): ScopedMapper => {
  if (isFunction(mapper)) {
    return {
      lookup: nullableDefault ? withOmittedNullDefault(mapper) : mapper,
      nullableDefault,
      bound: [],
    };
  }

  const entries = nullableDefault
    ? Object.fromEntries(
        Object.entries(mapper).map(([key, config]) => [key, nullableByDefault(config)]),
      )
    : mapper;

  return { lookup: (reference) => entries[reference], nullableDefault, bound: [] };
};

export const isNullableReference = (
  reference: string,
  mapper: ScopedMapper,
): boolean => resolveMapperConfig(reference, mapper)?.nullable === true;

export const applyValueParser = (
  reference: string,
  value: MongoValue,
  mapper: ScopedMapper,
): MongoValue => {
  // Unlike resolveMapperConfig, a reference's own entry does not shadow its relation's parser.
  const parser =
    mapper.lookup(reference)?.valueParser ||
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
  mapper: ScopedMapper,
): ResolvedFieldReference | undefined => {
  const config = mapper.lookup(reference);

  if (config?.relation) {
    return relationReference(config.relation, config.relation.field);
  }

  if (config?.field) {
    return { path: [config.field] };
  }

  const parts = reference.split(".");
  const lastPart = parts.pop();

  if (parts.length > 0 && lastPart) {
    const parentRelation = mapper.lookup(parts.join("."))?.relation;

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
  mapper: ScopedMapper,
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
  mapper: ScopedMapper,
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
  reference.startsWith("request.principal.")
    ? new UnsupportedQueryPlanError(
        `The plan leaves ${reference} unresolved: the planner did not fold it into a value, and a principal attribute is not a field of any document`,
      )
    : new Error(
    `No mapper entry for ${reference}: an unmapped reference is not used verbatim as a ` +
      "document path, because MongoDB's $ne and $nor match every document a path is absent " +
      "from. Map it to a field, or declare it with an entry to use the plan path as-is.",
  );

/**
 * The mapper a collection macro's lambda body is translated with: the iteration variable (and
 * `variable.field`) resolve against the relation's element `fields`, relative to the element;
 * every other key falls through to the caller's mapper.
 */
export const createScopedMapper = (
  collectionPath: string,
  variableName: string,
  fullMapper: ScopedMapper,
): ScopedMapper => ({
  ...fullMapper,
  lookup: scopedLookup(collectionPath, variableName, fullMapper),
});

const scopedLookup =
  (collectionPath: string, variableName: string, fullMapper: ScopedMapper): ScopedMapper["lookup"] =>
  (key: string) => {
    if (key !== variableName && !key.startsWith(`${variableName}.`)) {
      return fullMapper.lookup(key);
    }

    const relation = fullMapper.lookup(collectionPath)?.relation;

    if (key === variableName) {
      const field = relation?.field;

      if (!field) return { field: key };

      return relation.fields?.[field] ?? { field };
    }

    const elementField = key.slice(variableName.length + 1);

    return relation?.fields?.[elementField] || { field: elementField };
  };

/** The declared stored type of a reference: its own entry's, or its relation's field's. */
export const valueTypeOf = (
  reference: string,
  mapper: ScopedMapper,
): MapperConfig["valueType"] => resolveMapperConfig(reference, mapper)?.valueType;
