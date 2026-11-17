import type { MapperConfig } from "./index";
import type { ScopedMapper } from "./mapper";
import { isFunction } from "./narrowing";

/** The part of a Mongoose `SchemaType` the adapter reads. */
export type SchemaTypeLike = {
  readonly instance?: string;
  readonly $isSingleNested?: boolean;
  readonly $isMongooseDocumentArray?: boolean;
  readonly schema?: SchemaLike;
};

/** The part of a Mongoose `Schema` the adapter reads, so the adapter need not import `mongoose`. */
export type SchemaLike = {
  path(path: string): SchemaTypeLike | undefined;
  pathType(path: string): string;
  readonly paths: Readonly<Record<string, SchemaTypeLike>>;
};

/** A `Schema`, or a `Model`, which carries one. */
export type SchemaSource = SchemaLike | { readonly schema: SchemaLike };

const RESOURCE_ATTR = "request.resource.attr.";

const SCALAR_TYPES: Readonly<Record<string, NonNullable<MapperConfig["valueType"]>>> = {
  String: "string",
  Number: "number",
  Boolean: "boolean",
  Date: "dateTime",
};

type Shape =
  | { kind: "one" | "many" }
  | { kind: "scalar"; valueType?: MapperConfig["valueType"] }
  | { kind: "absent" };

export const schemaOf = (source: SchemaSource): SchemaLike =>
  isFunction((source as SchemaLike).path) ? (source as SchemaLike) : (source as { schema: SchemaLike }).schema;

/**
 * What a document path holds. A single-nested subdocument or a nested object is to-one, a document
 * array is to-many, and an array of scalars, `Mixed`, `ObjectId` or `Map` is a scalar with no type
 * the adapter can check a literal against.
 */
const shapeOf = (schema: SchemaLike, path: string): Shape => {
  const pathType = schema.pathType(path);

  if (pathType === "nested") return { kind: "one" };
  if (pathType !== "real") return { kind: "absent" };

  const type = schema.path(path);

  if (type?.$isSingleNested) return { kind: "one" };
  if (type?.$isMongooseDocumentArray) return { kind: "many" };

  const valueType = type?.instance ? SCALAR_TYPES[type.instance] : undefined;

  return valueType ? { kind: "scalar", valueType } : { kind: "scalar" };
};

/** The immediate child names under a to-one or to-many document path. */
const childrenOf = (schema: SchemaLike, path: string): string[] => {
  const type = schema.path(path);
  const prefix = `${path}.`;
  const paths = type?.schema
    ? Object.keys(type.schema.paths).map((child) => `${prefix}${child}`)
    : Object.keys(schema.paths).filter((child) => child.startsWith(prefix));

  return [...new Set(paths.map((child) => child.slice(prefix.length).split(".")[0]!))].filter(
    (child) => child !== "_id",
  );
};

/**
 * `config` with what the schema says about `path` filled in wherever `config` declares nothing.
 * `name` is the field the filter reads, which inside a to-many element is relative to the element.
 */
const inferAt = (schema: SchemaLike, path: string, name: string, config: MapperConfig): MapperConfig => {
  const shape = shapeOf(schema, path);
  const { field: _field, ...rest } = config;

  if (shape.kind === "one" || shape.kind === "many") {
    return { ...rest, relation: inferRelation(schema, path, { name, type: shape.kind }) };
  }

  return shape.kind === "scalar" && shape.valueType && config.valueType === undefined
    ? { ...rest, field: name, valueType: shape.valueType }
    : { ...rest, field: name };
};

/**
 * The relation with `type` and the element `fields` it omits filled in from the schema. `path` is
 * where `relation.name` sits in the schema, which inside a to-many element is relative to the
 * element and so differs from the name.
 */
const inferRelation = (
  schema: SchemaLike,
  path: string,
  relation: NonNullable<MapperConfig["relation"]>,
): NonNullable<MapperConfig["relation"]> => {
  const shape = shapeOf(schema, path);
  const type =
    relation.type ?? (shape.kind === "one" || shape.kind === "many" ? shape.kind : undefined);

  if (!type) {
    throw new Error(
      `Relation ${relation.name} declares no type, and the schema holds no subdocument or document array at ${path}`,
    );
  }

  const declared = relation.fields ?? {};
  const names = [...new Set([...childrenOf(schema, path), ...Object.keys(declared)])];

  const fields = Object.fromEntries(
    names.map((name) => {
      const field = declared[name];
      const elementPath = `${path}.${field?.relation?.name ?? field?.field ?? name}`;

      if (field?.relation) {
        return [name, { ...field, relation: inferRelation(schema, elementPath, field.relation) }];
      }

      return [name, inferAt(schema, elementPath, field?.field ?? name, field ?? {})];
    }),
  );

  return { ...relation, type, fields };
};

/** The relation a reference's longest mapped or inferable prefix resolves to, and what is left. */
const enclosingRelation = (
  reference: string,
  lookup: ScopedMapper["lookup"],
): { relation: NonNullable<MapperConfig["relation"]>; rest: string } | undefined => {
  const parts = reference.slice(RESOURCE_ATTR.length).split(".");

  for (let length = parts.length - 1; length > 0; length--) {
    const relation = lookup(`${RESOURCE_ATTR}${parts.slice(0, length).join(".")}`)?.relation;

    if (relation) return { relation, rest: parts.slice(length).join(".") };
  }

  return undefined;
};

/**
 * The caller's lookup with the schema's answers filled in.
 *
 * - A relation that omits `type` takes it from the schema path its `name` names, and its element
 *   `fields` gain every schema path the caller did not declare, typed.
 * - An entry naming a `field` that holds a subdocument or document array becomes that relation; one
 *   that holds a `String`, `Number`, `Boolean` or `Date` gains the matching `valueType`.
 * - A resource attribute with no entry resolves to the document path of the same name when the
 *   schema has one, through a to-one relation if a prefix of it is one. A name the schema does not
 *   hold stays unmapped and is refused, so a typo still cannot reach `$ne` as an absent path
 *   (cerbos/query-plan-adapters#492).
 *
 * `nullable` and `requiresParent` are never inferred: a schema default of `null` does not say
 * whether the application sends a stored null as an explicit null or no attribute at all, and a
 * to-one on the `check()` side may be stored as an array.
 */
export const withSchema = (
  lookup: ScopedMapper["lookup"],
  source: SchemaSource | undefined,
): ScopedMapper["lookup"] => {
  if (!source) return withDeclaredRelationTypes(lookup);

  const schema = schemaOf(source);
  const cache = new Map<string, MapperConfig | undefined>();

  const infer = (reference: string): MapperConfig | undefined => {
    const config = lookup(reference);

    if (config?.relation) {
      return { ...config, relation: inferRelation(schema, config.relation.name, config.relation) };
    }

    if (config?.field) return inferAt(schema, config.field, config.field, config);

    if (!reference.startsWith(RESOURCE_ATTR)) return config;

    const enclosing = enclosingRelation(reference, inferred);

    if (enclosing) {
      if (enclosing.relation.type !== "one") return config;

      const path = `${enclosing.relation.name}.${enclosing.rest}`;
      const shape = shapeOf(schema, path);

      return shape.kind === "one" || shape.kind === "many" ? inferAt(schema, path, path, config ?? {}) : config;
    }

    const path = reference.slice(RESOURCE_ATTR.length);

    return shapeOf(schema, path).kind === "absent" ? config : inferAt(schema, path, path, config ?? {});
  };

  const inferred = (reference: string): MapperConfig | undefined => {
    if (!cache.has(reference)) cache.set(reference, infer(reference));

    return cache.get(reference);
  };

  return inferred;
};

/** Without a schema, a relation must say whether it is to-one or to-many. */
const withDeclaredRelationTypes =
  (lookup: ScopedMapper["lookup"]): ScopedMapper["lookup"] =>
  (reference) => {
    const config = lookup(reference);

    assertRelationTypes(config);

    return config;
  };

const assertRelationTypes = (config: MapperConfig | undefined): void => {
  const relation = config?.relation;

  if (!relation) return;

  if (!relation.type) {
    throw new Error(
      `Relation ${relation.name} declares no type: declare "one" or "many", or pass the schema so the adapter can read it`,
    );
  }

  Object.values(relation.fields ?? {}).forEach(assertRelationTypes);
};
