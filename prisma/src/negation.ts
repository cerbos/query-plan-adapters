import { isPrismaFilter, type PrismaFilter, type PrismaValue } from "./value";

// Prisma renders `NOT x` without parentheses when `x` is a single condition, and so does the
// negated predicate of `every` and `isNot`. Under MySQL's HIGH_NOT_PRECEDENCE, `NOT a < ?` parses
// as `(NOT a) < ?` and over-grants. Prisma does parenthesise a conjunction of two conditions, so a
// lone condition is negated as `AND: [x, x]`, which is `x` in three-valued logic.

const RELATION_OPERATORS = new Set(["is", "isNot", "some", "every", "none"]);

function isRelationFilter(value: PrismaValue): value is PrismaFilter {
  return isPrismaFilter(value) && Object.keys(value).some((key) => RELATION_OPERATORS.has(key));
}

function isCombinator(key: string): boolean {
  return key === "AND" || key === "OR" || key === "NOT";
}

function rendersEmpty(value: PrismaValue): boolean {
  if (Array.isArray(value)) {
    return value.every(rendersEmpty);
  }

  return (
    isPrismaFilter(value) &&
    Object.entries(value).every(
      ([key, child]) => child === undefined || (isCombinator(key) && rendersEmpty(child))
    )
  );
}

function rendersBare(value: PrismaValue): boolean {
  if (Array.isArray(value)) {
    const conditions = value.filter((element) => !rendersEmpty(element));

    return conditions.length < 2 && conditions.every(rendersBare);
  }

  if (!isPrismaFilter(value)) {
    return true;
  }

  const entries = Object.entries(value).filter(
    ([key, child]) => child !== undefined && !(isCombinator(key) && rendersEmpty(child))
  );

  if (entries.length !== 1) {
    return entries.length === 0;
  }

  const [key, child] = entries[0]!;

  if (key === "AND" || key === "OR") {
    return rendersBare(child);
  }

  if (key === "NOT") {
    return false;
  }

  if (isRelationFilter(child)) {
    return Object.entries(child).every(
      ([operator, nested]) =>
        (operator === "is" || operator === "isNot") && (nested === null || rendersEmpty(nested))
    );
  }

  return true;
}

function parenthesised(value: PrismaValue): PrismaValue {
  if (Array.isArray(value)) {
    return value.map(parenthesised);
  }

  return rendersBare(value) ? { AND: [value, value] } : value;
}

function parenthesiseNegations(value: PrismaValue): PrismaValue {
  if (Array.isArray(value)) {
    return value.map(parenthesiseNegations);
  }

  return isPrismaFilter(value) ? parenthesiseFilter(value) : value;
}

export function parenthesiseFilter(filter: PrismaFilter): PrismaFilter {
  const relation = isRelationFilter(filter);

  return Object.fromEntries(
    Object.entries(filter).map(([key, child]) => {
      const inner = parenthesiseNegations(child);
      const negated = key === "NOT" || (relation && (key === "every" || key === "isNot"));

      return [key, negated && inner !== null ? parenthesised(inner) : inner];
    })
  );
}
