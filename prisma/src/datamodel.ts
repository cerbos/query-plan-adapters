// Completing the caller's mapper from the Prisma schema: whether a relation is to-one or to-many,
// which model it reaches, and the type and nullability of the scalar columns along the way.

import type { Mapper, MapperConfig } from "./index";
import type { PreparedMapper, PreparedMapperConfig, PreparedRelation } from "./mapping";
import { isFunction } from "./narrowing";

export type PrismaDatamodelField = {
  readonly name: string;
  readonly kind: string;
  readonly type: string;
  readonly isList: boolean;
  readonly isRequired?: boolean;
  readonly isId?: boolean;
};

/**
 * The part of Prisma's DMMF datamodel the adapter reads: `Prisma.dmmf.datamodel` from a
 * `prisma-client-js` client (Prisma 5 and 6), or the module the `cerbos-prisma-datamodel`
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

const VALUE_TYPES = new Map<string, MapperConfig["valueType"]>([
  ["String", "string"],
  ["Int", "number"],
  ["Float", "number"],
  ["Boolean", "boolean"],
  ["DateTime", "dateTime"],
]);

type ElementMappings = Record<string, PreparedMapperConfig>;

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
): PreparedRelation {
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
): ElementMappings {
  const completed: ElementMappings = {};

  for (const [key, config] of Object.entries(declared)) {
    completed[key] = completeConfig(models, model, key, config, `${reference}.${key}`);
  }

  return withInferred(completed, (key) =>
    models.get(model)?.has(key)
      ? completeConfig(models, model, key, {}, `${reference}.${key}`)
      : undefined
  );
}

/**
 * `declared` with every other key inferred on first read and remembered, misses included. Only
 * `get` is trapped: `key in mappings` still answers for the declared keys alone, which is the key
 * set `"k" in R.attr.parent` tests.
 */
function withInferred(
  declared: ElementMappings,
  infer: (key: string) => MapperConfig | undefined
) {
  const inferred = new Map<string, MapperConfig | undefined>();

  const lookup = (key: string): MapperConfig | undefined => {
    if (!inferred.has(key)) inferred.set(key, infer(key));

    return inferred.get(key);
  };

  return new Proxy(declared, {
    get: (target, key) => (key in target ? target[String(key)] : lookup(String(key))),
  });
}

/** `config` with what the Prisma schema says about the field it names on `model` filled in. */
function completeConfig(
  models: Models,
  model: string,
  key: string | undefined,
  config: MapperConfig,
  reference: string
): PreparedMapperConfig {
  const { relation, ...scalar } = config;

  if (relation) {
    return { ...scalar, relation: completeRelation(models, model, relation, reference) };
  }

  const name = scalar.field ?? key;
  const field = name === undefined ? undefined : models.get(model)?.get(name);

  if (!field || name === undefined) return scalar;

  if (isRelationField(models, field)) {
    const { field: _relationName, ...rest } = scalar;

    return completeConfig(models, model, key, { ...rest, relation: { name } }, reference);
  }

  if (field.kind !== "scalar" || field.isList) {
    return scalar;
  }

  return {
    ...scalar,
    field: name,
    valueType: config.valueType ?? VALUE_TYPES.get(field.type),
    nullable: config.nullable ?? (field.isRequired === undefined ? undefined : !field.isRequired),
  };
}

function requireRelationTypes(config: MapperConfig, reference: string): PreparedMapperConfig {
  const { relation, ...scalar } = config;

  if (!relation) return scalar;

  const { type, fields, ...rest } = relation;

  if (type === undefined) {
    throw new Error(
      `"${reference}" maps relation "${relation.name}" without a \`type\`; declare "one" or "many", or pass the Prisma \`datamodel\` to infer it`
    );
  }

  return {
    ...scalar,
    relation: {
      ...rest,
      type,
      ...(fields && {
        fields: Object.fromEntries(
          Object.entries(fields).map(([key, nested]) => [
            key,
            requireRelationTypes(nested, `${reference}.${key}`),
          ])
        ),
      }),
    },
  };
}

/**
 * The mapper the translation reads. With a datamodel, each mapping is completed from the schema
 * of the model its reference starts from; without one, every relation must declare its type.
 */
export function prepareMapper(
  mapper: Mapper,
  model: string | undefined,
  datamodel: PrismaDatamodel | undefined
): PreparedMapper {
  if (!datamodel) {
    if (isFunction(mapper)) {
      return (key) => {
        const config = mapper(key);

        return config && requireRelationTypes(config, key);
      };
    }

    return Object.fromEntries(
      Object.entries(mapper).map(([key, config]) => [key, requireRelationTypes(config, key)])
    );
  }

  if (model === undefined) {
    throw new Error("The `datamodel` option requires the `model` option to name the root model");
  }

  const models = indexModels(datamodel);

  if (!models.has(model)) {
    throw new Error(`Model "${model}" is not in the Prisma datamodel`);
  }

  const complete = (
    key: string,
    config: MapperConfig | undefined
  ): PreparedMapperConfig | undefined => {
    const name = rootFieldName(models, model, key);

    if (config === undefined && name === undefined) return undefined;

    return completeConfig(models, model, name, config ?? {}, key);
  };

  if (isFunction(mapper)) {
    return (key) => complete(key, mapper(key));
  }

  const completed: ElementMappings = {};

  for (const [key, config] of Object.entries(mapper)) {
    completed[key] = completeConfig(models, model, rootFieldName(models, model, key), config, key);
  }

  return withInferred(completed, (key) => complete(key, undefined));
}

/**
 * The root model's field an unmapped reference names by convention: `request.resource.id` is the
 * model's `@id` field, and `request.resource.attr.<name>` is the field called `<name>`. A deeper
 * reference resolves through its longest mapped prefix instead.
 */
function rootFieldName(models: Models, model: string, key: string): string | undefined {
  const fields = models.get(model);

  if (key === "request.resource.id") {
    return [...(fields?.values() ?? [])].find((field) => field.isId)?.name;
  }

  const prefix = "request.resource.attr.";

  if (!key.startsWith(prefix)) return undefined;
  const name = key.slice(prefix.length);

  return !name.includes(".") && fields?.has(name) ? name : undefined;
}
