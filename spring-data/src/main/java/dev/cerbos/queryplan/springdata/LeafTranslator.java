/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Translates one column against one constant, and applies the explicit-null convention.
 *
 * <p>Every scalar (field, value) translation goes through {@link #withOverride}, so a
 * registered {@link OperatorFunction} applies on every path that produces its operator.
 */
final class LeafTranslator {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final Map<String, OperatorFunction> overrides;

    LeafTranslator(CriteriaBuilder cb, TriPredicate tri, Map<String, OperatorFunction> overrides) {
        this.cb = cb;
        this.tri = tri;
        this.overrides = overrides;
    }

    boolean overridden(String op) {
        return overrides.get(op) != null;
    }

    /** Applies {@code op} through any registered override, else {@link #defaultLeaf}. */
    Predicate applyLeaf(String op, Path<?> path, Object value) {
        return withOverride(op, path, value, () -> defaultLeaf(op, path, value));
    }

    /**
     * Uses the override registered for {@code op} if there is one, else {@code dflt}. A
     * mirrored comparison is looked up under its mirrored name (see {@link NormalizedBinary}).
     */
    Predicate withOverride(String op, Expression<?> field,
                           Object value, Supplier<Predicate> dflt) {
        OperatorFunction override = overrides.get(op);
        if (override != null) {
            return override.apply(cb, field, value);
        }
        return dflt.get();
    }

    /**
     * Whether {@code cerbosVar} is sent as an explicit null: a Field declared
     * {@link NullAttributeRepresentation#EXPLICIT}, or a bare lambda element. Read from the
     * mapping, because one column can be mapped under two names with different conventions.
     */
    boolean isExplicitNull(String cerbosVar, Scope scope) {
        if (!(scope.resolve(cerbosVar) instanceof Scope.ResolvedScalar scalar)) return false;
        return scalar.mapping() instanceof AttributeMapping.Relation
                || scalar.mapping() instanceof AttributeMapping.Field field
                && field.nullAttributeRepresentation() == NullAttributeRepresentation.EXPLICIT;
    }

    /**
     * Whether {@code cerbosVar} is a Field declared {@link NullAttributeRepresentation#OMITTED}.
     */
    boolean isDeclaredOmitted(String cerbosVar, Scope scope) {
        return scope.resolve(cerbosVar) instanceof Scope.ResolvedScalar scalar
                && scalar.mapping() instanceof AttributeMapping.Field field
                && field.nullAttributeRepresentation() == NullAttributeRepresentation.OMITTED;
    }

    /**
     * Equality for operands sent as explicit nulls. In CEL {@code null == "x"} is false,
     * {@code null != "x"} is true and two nulls are equal, while SQL answers UNKNOWN to all
     * three.
     *
     * <p>Not a null-safe equality operator, because this must be asymmetric: a NULL on a side
     * that does not declare the convention is a missing attribute, which CEL denies, so it has
     * to stay UNKNOWN.
     */
    Predicate definiteEquality(String op,
                               Expression<?> left,
                               Expression<?> right,
                               boolean leftExplicit, boolean rightExplicit) {
        if (!compatibleTypes(left.getJavaType(), right.getJavaType())) {
            Predicate equality = leftExplicit && rightExplicit
                    ? cb.and(cb.isNull(left), cb.isNull(right)) : cb.disjunction();
            List<Predicate> missing = new ArrayList<>();
            if (!leftExplicit) missing.add(cb.isNull(left));
            if (!rightExplicit) missing.add(cb.isNull(right));
            if (!missing.isEmpty()) {
                equality = tri.baseUnlessUnknown(equality,
                        () -> cb.or(missing.toArray(new Predicate[0])));
            }
            return "ne".equals(op) ? tri.not(equality) : equality;
        }
        List<Predicate> present = new ArrayList<>();
        if (leftExplicit) {
            present.add(cb.isNotNull(left));
        }
        if (rightExplicit) {
            present.add(cb.isNotNull(right));
        }
        present.add(cb.equal(left, right));
        Predicate equality = cb.and(present.toArray(new Predicate[0]));
        if (leftExplicit && rightExplicit) {
            equality = cb.or(cb.and(cb.isNull(left), cb.isNull(right)), equality);
        }
        // tri.not keeps a nested negation from collapsing in Hibernate.
        return "ne".equals(op) ? tri.not(equality) : equality;
    }

    /**
     * Translates a leaf with no override. A constant of an incompatible type, or a string
     * match on a non-string, has no CEL overload and errors, so it is decided here as FALSE or
     * UNKNOWN rather than sent to the database.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    Predicate defaultLeaf(String op, Path<?> path, Object value) {
        StringMatch match = StringMatch.of(op);
        if (match != null) {
            if (!String.class.equals(path.getJavaType()) || !(value instanceof String needle)) {
                return tri.unknown();
            }
            return cb.like(path.as(String.class), match.pattern(PlanValues.escapeLike(needle)), '\\');
        }
        if (ComparisonTranslator.COMPARISON_OPS.contains(op) && value != null
                && !compatibleTypes(path.getJavaType(), value.getClass())) {
            if ("eq".equals(op) || "ne".equals(op)) {
                return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                        () -> cb.isNull(path));
            }
            return tri.unknown();
        }
        // A fractional constant is a Double: compare as double, because Hibernate will not
        // coerce 1.5 onto an Integer path and `intColumn >= 1.5` is legal CEL.
        Expression raw = (value instanceof Double) ? path.as(Double.class) : path;
        return switch (op) {
            case "eq" -> cb.equal(raw, value);
            case "ne" -> cb.notEqual(raw, value);
            case "lt" -> cb.lessThan(raw, (Comparable) value);
            case "gt" -> cb.greaterThan(raw, (Comparable) value);
            case "le" -> cb.lessThanOrEqualTo(raw, (Comparable) value);
            case "ge" -> cb.greaterThanOrEqualTo(raw, (Comparable) value);
            // e.g. `matches`, unless an override is registered for it.
            default -> throw Refusals.unsupported("Unsupported operator: " + op);
        };
    }

    /** Whether two types compare in SQL as in CEL: the same type, or both numbers. */
    static boolean compatibleTypes(Class<?> left, Class<?> right) {
        return left.equals(right)
                || (Number.class.isAssignableFrom(left) && Number.class.isAssignableFrom(right));
    }
}
