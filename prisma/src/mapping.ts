// Resolving plan references through the caller's mapper, and the per-translation state that
// resolution reads: the mapper in scope, the root model, the null convention and the lambda scopes.

import type { PlanExpressionOperand } from "@cerbos/core";

import type {
  Mapper,
  MapperConfig,
  NullAttributeRepresentation,
  PrismaFilter,
} from "./index";
import { isNamedOperand, isOperatorOperand, isValueOperand } from "./plan";
import { UnsupportedQueryPlanError } from "./errors";

// Each translation owns its lambda scopes, including when a function mapper re-enters the
// adapter. Scopes collect nullable element fields for three-valued-logic guards.
export type LambdaScope = {
  variableName: string;
  relationModel: string | undefined;
  nullableFields: Set<string>;
  unknownFilters: PrismaFilter[];
};

export type TranslationContext = {
  readonly mapper: Mapper;
  readonly rootModel: string | undefined;
  readonly nullRepresentation: NullAttributeRepresentation;
  readonly scopes: readonly LambdaScope[];
};

/** The innermost lambda scope, if the translation is inside one. */
export function currentScope(context: TranslationContext): LambdaScope | undefined {
  return context.scopes[context.scopes.length - 1];
}

export type RelationConfig = {
  name: string;
  type: "one" | "many";
  model?: string;
  field?: string;
  subqueryFilter?: PrismaFilter;
};

export type ResolvedFieldReference = {
  path: string[];
  nullable?: boolean;
  timestampWrapped?: boolean;
  relations?: RelationConfig[];
  valueType?: MapperConfig["valueType"];
  nullAttributeRepresentation?: NullAttributeRepresentation;
};

export type ResolvedValue = {
  value: any;
  /**
   * The value is a timestamp literal between two milliseconds, bound as the next millisecond: a
   * comparison against it must go through roundSubMillisecond.
   */
  subMillisecond?: boolean;
};

export type ResolvedOperand = ResolvedFieldReference | ResolvedValue;

export function isResolvedFieldReference(
  operand: ResolvedOperand
): operand is ResolvedFieldReference {
  return "path" in operand;
}

export function isResolvedValue(
  operand: ResolvedOperand
): operand is ResolvedValue {
  return "value" in operand;
}

/** Whether this reference is one the caller sends as an explicit `null` when the column is NULL. */
export function isExplicitNullReference(
  fieldRef: ResolvedFieldReference
): boolean {
  return fieldRef.nullAttributeRepresentation === "explicit";
}

export function getLeafField(path: string[]): string {
  const fieldName = path[path.length - 1];
  if (!fieldName) {
    throw new Error("Field path cannot be empty");
  }
  return fieldName;
}

/** The mapping the caller declared for `key`, through either mapper form. */
export function lookupMapping(
  mapper: Mapper,
  key: string
): MapperConfig | undefined {
  return typeof mapper === "function" ? mapper(key) : mapper[key];
}

/**
 * The element field mappings of the collection at `collectionPath`. A chained collection
 * (`R.attr.a.b`) has no mapping of its own, so it is found by descending from the longest mapped
 * prefix through the nested `relation.fields`, the same walk resolveFieldReference performs.
 */
function lookupElementFields(
  mapper: Mapper,
  collectionPath: string
): Record<string, MapperConfig> | undefined {
  const direct = lookupMapping(mapper, collectionPath);
  if (direct) {
    return direct.relation?.fields;
  }
  const parts = collectionPath.split(".");
  for (let i = parts.length - 1; i > 0; i--) {
    let relation = lookupMapping(mapper, parts.slice(0, i).join("."))?.relation;
    if (!relation) {
      continue;
    }
    for (const part of parts.slice(i)) {
      relation = relation?.fields?.[part]?.relation;
    }
    return relation?.fields;
  }
  return undefined;
}

function toRelationConfig(
  relation: NonNullable<MapperConfig["relation"]>
): RelationConfig {
  return {
    name: relation.name,
    type: relation.type,
    model: relation.model,
    field: relation.field,
    subqueryFilter: relation.subqueryFilter,
  };
}

/**
 * Resolves a field reference considering relations and nested fields.
 */
export function resolveFieldReference(
  reference: string,
  context: TranslationContext
): ResolvedFieldReference {
  const parts = reference.split(".");
  const config = lookupMapping(context.mapper, reference);

  // Without a direct match, the longest dotted prefix that has a mapping applies.
  let matchedPrefix = "";
  let matchedConfig: MapperConfig | undefined;
  if (!config) {
    for (let i = parts.length - 1; i >= 0; i--) {
      const prefix = parts.slice(0, i + 1).join(".");
      const prefixConfig = lookupMapping(context.mapper, prefix);
      if (prefixConfig) {
        matchedPrefix = prefix;
        matchedConfig = prefixConfig;
        break;
      }
    }
  }

  const activeConfig = config ?? matchedConfig;

  if (!activeConfig?.relation) {
    return {
      path: [activeConfig?.field || reference],
      valueType: activeConfig?.valueType,
      nullable: activeConfig?.nullable,
      nullAttributeRepresentation: activeConfig?.nullAttributeRepresentation,
    };
  }

  const { fields } = activeConfig.relation;
  const remainingParts = matchedPrefix
    ? parts.slice(matchedPrefix.split(".").length)
    : [];

  const lastPart = parts[parts.length - 1];
  // A function mapper may return a relation config for the full leaf reference.
  // Resolve that leaf here too, before any filter is constructed.
  let field =
    activeConfig.relation.field ??
    (config && fields && lastPart
      ? (fields[lastPart]?.field ?? lastPart)
      : undefined);
  const relations = [toRelationConfig(activeConfig.relation)];

  // Walk the remaining segments through nested relation mappings; the first segment that is
  // not a relation names the leaf column.
  if (fields) {
    let currentMapper: Record<string, MapperConfig> = fields;
    let currentParts = remainingParts;
    while (currentParts.length > 0) {
      const currentPart = currentParts[0];
      if (!currentPart) {
        break;
      }
      const nextConfig: MapperConfig | undefined = currentMapper[currentPart];
      if (!nextConfig?.relation) {
        const leafPart = currentParts[currentParts.length - 1];
        if (leafPart) {
          field = nextConfig?.field || leafPart;
        }
        break;
      }
      relations.push(toRelationConfig(nextConfig.relation));
      field = nextConfig.relation.field;
      currentMapper = nextConfig.relation.fields || {};
      currentParts = currentParts.slice(1);
    }
  }

  return {
    path: field ? [field] : remainingParts,
    relations,
    valueType: activeConfig.valueType,
    nullable: activeConfig.nullable,
    nullAttributeRepresentation: activeConfig.nullAttributeRepresentation,
  };
}

/**
 * Records that the lambda body being built touches a nullable element column, so the
 * enclosing collection operator can add its three-valued-logic guard.
 *
 * An element column is nullable unless its mapping says `nullable: false`. Omitting the guard is
 * what over-grants (a negated `exists`, an `all` or a `hasIntersection` over `map` would admit
 * rows holding a NULL element the PDP denies), so silence has to mean the safe reading. A
 * nested relation is not a column and never gets a NULL guard.
 */
function recordNullableElementField(
  context: TranslationContext,
  config: MapperConfig,
  defaultField: string
): void {
  if (config.nullable === false || config.relation) {
    return;
  }
  currentScope(context)?.nullableFields.add(config.field || defaultField);
}

/**
 * The context for a lambda body iterating `collectionPath`: `scope` is pushed, and references
 * to the lambda variable resolve against the collection's element mapping.
 */
export function enterLambdaScope(
  context: TranslationContext,
  collectionPath: string,
  scope: LambdaScope
): TranslationContext {
  const scopedContext: TranslationContext = {
    ...context,
    scopes: [...context.scopes, scope],
  };
  const { variableName } = scope;
  const fullMapper = context.mapper;

  const scopedMapper: Mapper = (key: string) => {
    if (key === variableName) {
      const { relations } = resolveFieldReference(collectionPath, scopedContext);
      const projection = relations?.[relations.length - 1]?.field;
      if (projection) {
        return {
          field: projection,
          // A projected list carries null values, unlike missing fields on object elements.
          nullAttributeRepresentation: "explicit",
        };
      }
    }

    // A key that starts with the variable name accesses the collection element.
    if (key.startsWith(variableName + ".")) {
      const strippedKey = key.replace(variableName + ".", "");
      const baseConfig = lookupElementFields(fullMapper, collectionPath);
      if (!baseConfig) {
        const fieldConfig = { field: strippedKey };
        if (!strippedKey.includes(".")) {
          recordNullableElementField(scopedContext, fieldConfig, strippedKey);
        }
        return fieldConfig;
      }

      // For nested paths, traverse the fields configuration.
      const parts = strippedKey.split(".");
      let currentConfig = baseConfig;
      let field = parts[0] || strippedKey;
      for (let i = 0; i < parts.length - 1; i++) {
        const part = parts[i];
        const nextPart = parts[i + 1];
        if (!part || !nextPart) {
          break;
        }
        const nextFields = currentConfig[part]?.relation?.fields;
        if (!nextFields) {
          break;
        }
        currentConfig = nextFields;
        field = nextPart;
      }

      // The field config if it exists, otherwise a default one.
      const fieldConfig = currentConfig[field] || { field };
      recordNullableElementField(scopedContext, fieldConfig, field);
      return fieldConfig;
    }

    // Keys not referencing the collection element resolve through the enclosing mapper.
    if (typeof fullMapper === "function") {
      return fullMapper(key);
    }
    return fullMapper[key] || { field: key };
  };
  return { ...scopedContext, mapper: scopedMapper };
}

/**
 * Guards every site that would emit a NULL-selecting predicate out of a `null` comparison
 * operand.
 *
 * Under the `"omitted"` representation a NULL column carries no attribute, so CEL raises a
 * missing-attribute error and `check()` denies the row — `IS NULL` would return exactly the rows
 * the PDP refuses. The rejection is deliberately wider than the over-granting shapes: `ne(x,
 * null)` on its own is aligned, but Prisma applies negation by wrapping (`{ NOT: ... }`) rather
 * than by pushing it into the leaf, so a leaf cannot tell whether an enclosing `not` will flip
 * `IS NOT NULL` back into a NULL-selecting predicate. Rejecting every null operand is correct
 * under any nesting; narrowing it requires negation-parity tracking.
 */
export function assertNullOperandTranslatable(
  translation: TranslationContext,
  context: string,
  declared?: NullAttributeRepresentation
): void {
  if ((declared ?? translation.nullRepresentation) === "omitted") {
    throw new UnsupportedQueryPlanError(
      `Cannot translate ${context} under nullAttributeRepresentation "omitted": a NULL column ` +
        "sends no attribute, so Cerbos evaluates the comparison as a missing-attribute error " +
        "(deny) while a NULL-selecting filter would return those rows. Send NULL columns as " +
        'explicit nulls and use "explicit", or keep this shape out of the policy.'
    );
  }
}

/**
 * Applies the null-operand guard to struct literals (`set-field` members) anywhere in the plan,
 * under the convention of the attribute each literal is compared with.
 */
export function assertStructuralNulls(
  operand: PlanExpressionOperand,
  context: TranslationContext,
  declared?: NullAttributeRepresentation
): void {
  if (!isOperatorOperand(operand)) return;
  const sibling = operand.operands.find(isNamedOperand);
  const mapping = sibling && lookupMapping(context.mapper, sibling.name);
  const convention = mapping?.nullAttributeRepresentation ?? declared;
  if (
    operand.operator === "set-field" &&
    operand.operands.some((part) => isValueOperand(part) && part.value === null)
  ) {
    assertNullOperandTranslatable(
      context,
      "a null member in a struct literal",
      convention
    );
  }
  operand.operands.forEach((part) =>
    assertStructuralNulls(part, context, convention)
  );
}
