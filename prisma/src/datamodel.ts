// Completing the caller's mapper from the Prisma schema: whether a relation is to-one or to-many,
// which model it reaches, and the type and nullability of the scalar columns along the way.

import type { Mapper, MapperConfig } from "./index";
import { isFunction } from "./narrowing";

export type PrismaDatamodelField = {
  readonly name: string;
  readonly kind: string;
  readonly type: string;
  readonly isList: boolean;
  readonly isRequired?: boolean;
};

/**
 * The part of Prisma's DMMF datamodel the adapter reads: `Prisma.dmmf.datamodel` from a
 * `prisma-client-js` client (Prisma 5 and 6), or the module the `@cerbos/orm-prisma/generator`
 * generator writes (any Prisma version, and the only source under the `prisma-client` generator,
 * whose runtime model carries no list or required flags).
 */
export type PrismaDatamodel = {
  readonly models: ReadonlyArray<{
    readonly name: string;
    readonly fields: ReadonlyArray<PrismaDatamodelField>;
  }>;
};

type Models = ReadonlyMap<string, ReadonlyMap<string, PrismaDatamodelField>>;

type RelationMapping = NonNullable<MapperConfig["relation"]>;

const VALUE_TYPES: Record<string, MapperConfig["valueType"]> = {
  String: "string",
  Int: "number",
  Float: "number",
  Boolean: "boolean",
  DateTime: "dateTime",
};

function indexModels(datamodel: PrismaDatamodel): Models {
  return new Map(
    datamodel.models.map((model) => [
      model.name,
      new Map(model.fields.map((field) => [field.name, field])),
    ])
  );
}

function isRelationField(models: Models, field: PrismaDatamodelField): boolean {
  return field.kind === "object" && models.has(field.type);
}

function completeRelation(
  models: Models,
  model: string,
  relation: RelationMapping,
  reference: string
): RelationMapping {
  const field = models.get(model)?.get(relation.name);

  if (!field || !isRelationField(models, field)) {
    throw new Error(
      `"${reference}" declares a relation "${relation.name}", but "${model}.${relation.name}" is not a relation field`
    );
  }

  const type = field.isList ? "many" : "one";

  if (relation.type !== undefined && relation.type !== type) {
    throw new Error(
      `"${reference}" declares relation "${relation.name}" as "${relation.type}", but the Prisma schema makes it "${type}"`
    );
  }

  if (relation.model !== undefined && relation.model !== field.type) {
    throw new Error(
      `"${reference}" declares relation "${relation.name}" on model "${relation.model}", but the Prisma schema relates it to "${field.type}"`
    );
  }

  return {
    ...relation,
    type,
    model: field.type,
    fields: completeFields(models, field.type, relation.fields ?? {}, reference),
  };
}

/**
 * The element mappings of a relation, with every field the mapping leaves out read from the
 * related model by name. A relation reached through an unmapped segment is still a relation, so
 * `parent.inner.x` joins through `inner` without `inner` being declared. Unmapped entries are
 * built on first read, since relations can cycle back to the model they started from.
 */
function completeFields(
  models: Models,
  model: string,
  declared: Record<string, MapperConfig>,
  reference: string
): Record<string, MapperConfig> {
  const completed: Record<string, MapperConfig> = {};

  for (const [key, config] of Object.entries(declared)) {
    completed[key] = completeConfig(models, model, key, config, `${reference}.${key}`);
  }

  const inferred = new Map<string, MapperConfig | undefined>();

  return new Proxy(completed, {
    get(target, key, receiver) {
      if (typeof key !== "string" || Object.hasOwn(target, key)) {
        return Reflect.get(target, key, receiver);
      }

      if (!inferred.has(key)) {
        const field = models.get(model)?.get(key);
        inferred.set(
          key,
          field && completeConfig(models, model, key, {}, `${reference}.${key}`)
        );
      }

      return inferred.get(key);
    },
  });
}

/** `config` with what the Prisma schema says about the field it names on `model` filled in. */
function completeConfig(
  models: Models,
  model: string,
  key: string | undefined,
  config: MapperConfig,
  reference: string
): MapperConfig {
  if (config.relation) {
    return { ...config, relation: completeRelation(models, model, config.relation, reference) };
  }

  const name = config.field ?? key;
  const field = name === undefined ? undefined : models.get(model)?.get(name);

  if (!field || name === undefined) return config;

  if (isRelationField(models, field)) {
    const { field: _relationName, ...rest } = config;

    return completeConfig(models, model, key, { ...rest, relation: { name } }, reference);
  }

  if (field.kind !== "scalar" || field.isList) {
    return config;
  }

  return {
    ...config,
    field: name,
    valueType: config.valueType ?? VALUE_TYPES[field.type],
    nullable: config.nullable ?? (field.isRequired === undefined ? undefined : !field.isRequired),
  };
}

function rejectUntypedRelations(config: MapperConfig, reference: string): MapperConfig {
  const relation = config.relation;

  if (!relation) return config;

  if (relation.type === undefined) {
    throw new Error(
      `"${reference}" maps relation "${relation.name}" without a \`type\`; declare "one" or "many", or pass the Prisma \`datamodel\` to infer it`
    );
  }

  for (const [key, nested] of Object.entries(relation.fields ?? {})) {
    rejectUntypedRelations(nested, `${reference}.${key}`);
  }

  return config;
}

/**
 * The mapper the translation reads. With a datamodel, each mapping is completed from the schema
 * of the model its reference starts from; without one, every relation must declare its type.
 */
export function prepareMapper(
  mapper: Mapper,
  model: string | undefined,
  datamodel: PrismaDatamodel | undefined
): Mapper {
  if (!datamodel) {
    if (isFunction(mapper)) {
      return (key) => rejectUntypedRelations(mapper(key), key);
    }

    return Object.fromEntries(
      Object.entries(mapper).map(([key, config]) => [key, rejectUntypedRelations(config, key)])
    );
  }

  if (model === undefined) {
    throw new Error("The `datamodel` option requires the `model` option to name the root model");
  }

  const models = indexModels(datamodel);

  if (!models.has(model)) {
    throw new Error(`Model "${model}" is not in the Prisma datamodel`);
  }

  const complete = (key: string, config: MapperConfig) =>
    completeConfig(models, model, undefined, config, key);

  if (isFunction(mapper)) {
    return (key) => complete(key, mapper(key));
  }

  return Object.fromEntries(
    Object.entries(mapper).map(([key, config]) => [key, complete(key, config)])
  );
}
