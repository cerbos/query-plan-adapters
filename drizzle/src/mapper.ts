import type { PlanExpressionOperand } from "@cerbos/core";
import { sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";

import { isNameOperand } from "./operands";
import { SCOPED_RELATION } from "./types";
import type {
  BaseMapperEntry,
  Mapper,
  MapperEntry,
  MappingConfig,
  NullAttributeRepresentation,
  RelationMapping,
  RelationValue,
  ResolvedMapping,
  ScopedRelationEntry,
} from "./types";

/**
 * Resolving a CEL attribute reference through the caller's mapper: which column, transform or
 * relation it names, and which relation chain has to be joined to reach it.
 */

// -- entry shapes --------------------------------------------------------------------------------

const isScopedRelationEntry = (
  entry: MapperEntry,
): entry is ScopedRelationEntry =>
  typeof entry === "object" && entry !== null && SCOPED_RELATION in entry;

export const isMappingConfig = (entry: MapperEntry): entry is MappingConfig =>
  typeof entry === "object" &&
  entry !== null &&
  !isScopedRelationEntry(entry) &&
  ("column" in entry ||
    "transform" in entry ||
    "relation" in entry ||
    "valueType" in entry ||
    "indexable" in entry ||
    "collectionValueType" in entry);

export const isRelationValue = (
  entry: BaseMapperEntry,
): entry is RelationValue =>
  typeof entry === "object" &&
  entry !== null &&
  "kind" in entry &&
  entry.kind === "relation";

export const isColumn = (entry: BaseMapperEntry): entry is AnyColumn =>
  typeof entry === "object" &&
  entry !== null &&
  !isRelationValue(entry) &&
  !isMappingConfig(entry) &&
  typeof entry !== "function";

/** Whether an entry declares its relation a collection of scalars (`collectionValueType`). */
export const isScalarCollection = (entry: MapperEntry | undefined): boolean =>
  entry !== undefined &&
  isMappingConfig(entry) &&
  entry.collectionValueType === "scalar";

const toBaseMapperEntry = (entry: MapperEntry): BaseMapperEntry =>
  isScopedRelationEntry(entry) ? entry.resolve().mapping : entry;

const makeScopedRelationEntry = (
  resolution: ResolvedMapping,
): ScopedRelationEntry => ({
  [SCOPED_RELATION]: true,
  resolve: () => resolution,
});

/**
 * The convention declared on a mapper entry, if it declares one. An entry that declares nothing
 * inherits the call-level option.
 */
export const mappingNullRepresentation = (
  mapping: BaseMapperEntry,
): NullAttributeRepresentation | undefined =>
  isMappingConfig(mapping) ? mapping.nullAttributeRepresentation : undefined;

// -- resolution ----------------------------------------------------------------------------------

export const getMappingEntry = (
  reference: string,
  mapper: Mapper,
): MapperEntry | undefined =>
  typeof mapper === "function" ? mapper(reference) : mapper[reference];

/** Every `[prefix, suffix]` split of a dotted reference, longest prefix first. */
function* referencePrefixes(reference: string): Generator<[string, string[]]> {
  const parts = reference.split(".");
  for (let i = parts.length - 1; i > 0; i--) {
    yield [parts.slice(0, i).join("."), parts.slice(i)];
  }
}

const resolveRelationField = (
  relation: RelationMapping,
  path: string[],
  reference: string,
  accumulated: RelationMapping[],
  allowDefaultField = true,
): ResolvedMapping => {
  const relations = [...accumulated, relation];

  if (path.length === 0) {
    if (!allowDefaultField) {
      return { relations, mapping: { kind: "relation", relation } };
    }
    if (!relation.field) {
      throw new Error(
        `Relation mapping for '${reference}' does not define a default field`,
      );
    }
    return { relations, mapping: toBaseMapperEntry(relation.field) };
  }

  const [segment, ...rest] = path;
  if (segment === undefined) {
    throw new Error(
      `Invalid relation path for reference '${reference}': missing segment`,
    );
  }
  const fieldEntry = (relation.fields ?? {})[segment];

  if (fieldEntry !== undefined) {
    if (isMappingConfig(fieldEntry) && fieldEntry.relation) {
      return resolveRelationField(
        fieldEntry.relation,
        rest,
        reference,
        relations,
      );
    }
    if (rest.length > 0) {
      throw new Error(
        `Mapping for '${segment}' does not support further nesting in '${reference}'`,
      );
    }
    return { relations, mapping: toBaseMapperEntry(fieldEntry) };
  }

  // A segment the relation does not declare falls back to the table's own column of that name.
  const inferredColumn =
    segment in relation.table ? (relation.table as never)[segment] : undefined;

  if (inferredColumn !== undefined) {
    if (rest.length > 0) {
      throw new Error(
        `Unable to resolve nested path '${segment}.${rest.join(".")}' for relation '${reference}'`,
      );
    }
    return { relations, mapping: toBaseMapperEntry(inferredColumn) };
  }

  throw new Error(
    `No mapping found for relation segment '${segment}' in reference '${reference}'`,
  );
};

/**
 * Resolve a reference to its mapping. An exact entry wins; otherwise the longest mapped prefix
 * that is a relation resolves the rest of the path through that relation's fields.
 */
export const resolveFieldReference = (
  reference: string,
  mapper: Mapper,
): ResolvedMapping => {
  const direct = getMappingEntry(reference, mapper);
  if (direct !== undefined) {
    if (isScopedRelationEntry(direct)) {
      return direct.resolve();
    }
    if (isMappingConfig(direct) && direct.relation) {
      return resolveRelationField(direct.relation, [], reference, [], false);
    }
    return { relations: [], mapping: direct };
  }

  for (const [prefix, suffix] of referencePrefixes(reference)) {
    const entry = getMappingEntry(prefix, mapper);
    if (!entry || !isMappingConfig(entry) || !entry.relation) {
      continue;
    }
    return resolveRelationField(entry.relation, suffix, reference, []);
  }

  throw new Error(`No mapping found for reference: ${reference}`);
};

/** The relation chain a collection reference iterates, root first. */
const resolveRelationChain = (
  reference: string,
  mapper: Mapper,
): RelationMapping[] => {
  const direct = getMappingEntry(reference, mapper);
  if (direct !== undefined) {
    if (isScopedRelationEntry(direct)) {
      return direct.resolve().relations;
    }
    if (isMappingConfig(direct) && direct.relation) {
      return [direct.relation];
    }
  }

  for (const [prefix, suffix] of referencePrefixes(reference)) {
    const entry = getMappingEntry(prefix, mapper);
    if (!entry) {
      continue;
    }
    // A prefix inside an enclosing lambda scope carries the relations already joined to reach it.
    const { relations, mapping } = isScopedRelationEntry(entry)
      ? entry.resolve()
      : { relations: [], mapping: entry };
    if (isMappingConfig(mapping) && mapping.relation) {
      return resolveRelationField(mapping.relation, suffix, reference, relations)
        .relations;
    }
  }

  throw new Error(`No relation mapping found for reference: ${reference}`);
};

/** A reference that resolved to a whole relation stands for that relation's default `field`. */
export const resolveRelationDefaultField = (
  resolved: ResolvedMapping,
  reference: string,
): ResolvedMapping => {
  if (!isRelationValue(resolved.mapping)) {
    return resolved;
  }
  const defaultField = resolved.mapping.relation.field;
  if (!defaultField) {
    throw new Error(
      `Relation mapping for '${reference}' does not define a default field`,
    );
  }
  return {
    relations: resolved.relations,
    mapping: toBaseMapperEntry(defaultField),
  };
};

// -- lambda scopes -------------------------------------------------------------------------------

/**
 * The scope a collection macro's lambda body is translated in: a mapper that resolves the lambda
 * variable to an element of the collection, and the relation chain that collection lives behind,
 * split into the relation being iterated (`primaryRelation`) and the hops leading to it.
 */
export interface CollectionScope {
  mapper: Mapper;
  primaryRelation: RelationMapping;
  leadingRelations: RelationMapping[];
  /** Every relation of the chain: the lambda body's subqueries are already correlated to them. */
  skipRelations: Set<RelationMapping>;
}

export const createCollectionScope = (
  collectionReference: string,
  variableName: string,
  mapper: Mapper,
): CollectionScope => {
  const relationChain = resolveRelationChain(collectionReference, mapper);
  const primaryRelation = relationChain[relationChain.length - 1];
  if (!primaryRelation) {
    throw new Error(
      `No relation mapping found for reference: ${collectionReference}`,
    );
  }
  const leadingRelations = relationChain.slice(0, -1);

  const scopedMapper = (reference: string): MapperEntry | undefined => {
    if (reference === variableName) {
      const resolved = resolveRelationField(
        primaryRelation,
        [],
        collectionReference,
        leadingRelations,
      );
      // An element of a scalar collection is a VALUE in CEL, so a NULL element is an explicit
      // null rather than a missing attribute.
      if (isScalarCollection(getMappingEntry(collectionReference, mapper))) {
        if (isColumn(resolved.mapping)) {
          resolved.mapping = {
            column: resolved.mapping,
            nullAttributeRepresentation: "explicit",
          };
        } else if (isMappingConfig(resolved.mapping)) {
          resolved.mapping = {
            ...resolved.mapping,
            nullAttributeRepresentation: "explicit",
          };
        }
      }
      return makeScopedRelationEntry(resolved);
    }

    if (reference.startsWith(`${variableName}.`)) {
      const remainder = reference.slice(variableName.length + 1);
      return makeScopedRelationEntry(
        resolveRelationField(
          primaryRelation,
          remainder.split("."),
          `${collectionReference}.${remainder}`,
          leadingRelations,
        ),
      );
    }

    return getMappingEntry(reference, mapper);
  };

  return {
    mapper: scopedMapper,
    primaryRelation,
    leadingRelations,
    skipRelations: new Set([primaryRelation, ...leadingRelations]),
  };
};

// -- columns -------------------------------------------------------------------------------------

/** The drizzle column an operand names, if it names one directly. */
export const columnForOperand = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): AnyColumn | undefined => {
  if (!isNameOperand(operand)) {
    return undefined;
  }
  const { mapping } = resolveFieldReference(operand.name, mapper);
  if (isMappingConfig(mapping)) {
    return mapping.column;
  }
  return isColumn(mapping) ? mapping : undefined;
};

/** A mapping read as a scalar SQL value: only a column (bare or configured) qualifies. */
export const buildColumnExpression = (
  mapping: BaseMapperEntry,
  reference: string,
): SQL => {
  if (isRelationValue(mapping)) {
    throw new Error(
      `Cannot use relation '${reference}' as a scalar value expression`,
    );
  }
  if (typeof mapping === "function") {
    throw new Error(
      `Cannot use transform mapping for '${reference}' as a value expression`,
    );
  }
  if (isMappingConfig(mapping)) {
    if (mapping.relation) {
      throw new Error(
        `Cannot use relation mapping for '${reference}' as a scalar value expression`,
      );
    }
    if (!mapping.column) {
      throw new Error(
        `Mapping for '${reference}' requires a column to be used as a value expression`,
      );
    }
    return sql`${mapping.column}`;
  }
  if (!isColumn(mapping)) {
    throw new Error(`Expected column mapping for '${reference}'`);
  }
  return sql`${mapping}`;
};
