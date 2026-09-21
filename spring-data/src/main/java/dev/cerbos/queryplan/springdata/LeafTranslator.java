package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The scalar leaf: one mapped column against one plan constant.
 *
 * <p>Owns the default lowering of each leaf operator ({@link #defaultLeaf}), the routing of
 * every scalar (field, value) translation through the caller's {@link OperatorFunction}
 * overrides ({@link #withOverride}), and the two rules of the explicit-null convention — which
 * attributes the caller sends as an explicit null ({@link #isExplicitNull}) and the
 * asymmetric definite equality that convention entitles them to ({@link #definiteEquality}).
 * It is the one place a registered override is consulted, so "an override wins on EVERY path
 * that produces this operator" is a property of this class rather than of each caller: the
 * comparison seam, the arithmetic path, the timestamp leaf, the bare-boolean variable and
 * scalar {@code in} all come through here.
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

    /** Whether the caller registered an {@link OperatorFunction} for {@code op}. */
    boolean overridden(String op) {
        return overrides.get(op) != null;
    }

    /**
     * Apply a scalar leaf operator, consulting the per-operator {@code overrides} hook first so a
     * registered {@link OperatorFunction} wins on EVERY path that produces this operator — direct
     * comparison, {@code add}-folded comparison, and bare-boolean — not just the direct one.
     */
    Predicate applyLeaf(String op, Path<?> path, Object value) {
        return withOverride(op, path, value, () -> defaultLeaf(op, path, value));
    }

    /**
     * Route a scalar (field, value) translation through the per-operator {@code overrides}
     * hook: a registered {@link OperatorFunction} owns the operator's full translation
     * (mirrored operators are consulted under the mirrored name — see
     * {@link NormalizedBinary}); otherwise the supplied default applies.
     */
    Predicate withOverride(String op, jakarta.persistence.criteria.Expression<?> field,
                           Object value, Supplier<Predicate> dflt) {
        OperatorFunction override = overrides.get(op);
        if (override != null) {
            return override.apply(cb, field, value);
        }
        return dflt.get();
    }

    /**
     * Whether {@code cerbosVar} maps to a scalar the caller sends as an explicit null.
     *
     * <p>Read from the mapping rather than from the path: the same column is legitimately
     * mapped twice under two attribute names with two conventions, so the JPA path cannot
     * discriminate them.
     */
    boolean isExplicitNull(String cerbosVar, Scope scope) {
        if (!(scope.resolve(cerbosVar) instanceof Scope.ResolvedScalar scalar)) return false;
        return scalar.mapping() instanceof AttributeMapping.Relation
                || scalar.mapping() instanceof AttributeMapping.Field field
                && field.nullAttributeRepresentation() == NullAttributeRepresentation.EXPLICIT;
    }

    /**
     * An equality that can never be SQL UNKNOWN, for operands the caller sends as explicit
     * nulls.
     *
     * <p>A null VALUE is what CEL holds under that convention, so {@code null == "x"} is a
     * definite FALSE, {@code null != "x"} a definite TRUE, and two nulls are EQUAL. SQL
     * answers UNKNOWN to all three, which excludes the row under BOTH polarities — so the
     * NOT an enclosing negation applies has nothing definite to flip.
     *
     * <p>Deliberately not a null-safe equality operator. Two reasons, and the second is the
     * load-bearing one: Hibernate would need a dialect function — and a null-safe equality is
     * SYMMETRIC while this rewrite must not be. When only ONE side declares the convention,
     * the other side's NULL is a MISSING attribute on the check side, so CEL raises an error
     * and denies; only the asymmetric expansion below keeps propagating UNKNOWN for it. A
     * null-safe operator would match the two NULLs and over-grant.
     */
    Predicate definiteEquality(String op,
                               jakarta.persistence.criteria.Expression<?> left,
                               jakarta.persistence.criteria.Expression<?> right,
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
        // The junction barrier matters: cb.not(cb.not(p)) collapses in Hibernate, and this
        // predicate is frequently built under an enclosing negation. TriPredicate.not IS the
        // barrier (cb.not(cb.and(p))), and it is the adapter's only negation site.
        return "ne".equals(op) ? tri.not(equality) : equality;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    Predicate defaultLeaf(String op, Path<?> path, Object value) {
        if (("contains".equals(op) || "startsWith".equals(op) || "endsWith".equals(op))
                && (!String.class.equals(path.getJavaType()) || !(value instanceof String))) {
            return tri.unknown();
        }
        if (ComparisonTranslator.COMPARISON_OPS.contains(op) && value != null
                && !compatibleTypes(path.getJavaType(), value.getClass())) {
            if ("eq".equals(op) || "ne".equals(op)) {
                return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                        () -> cb.isNull(path));
            }
            return tri.unknown();
        }
        // Fractional constants compare in double space: protoValueToJava yields Double only
        // for non-whole numbers, and Hibernate refuses to coerce e.g. 1.5 into an
        // Integer-typed path ("not a whole number") — but `intColumn >= 1.5` is legal CEL
        // that the planner emits verbatim.
        jakarta.persistence.criteria.Expression raw =
                (value instanceof Double) ? path.as(Double.class) : path;
        return switch (op) {
            case "eq" -> cb.equal(raw, value);
            case "ne" -> cb.notEqual(raw, value);
            case "lt" -> cb.lessThan(raw, (Comparable) value);
            case "gt" -> cb.greaterThan(raw, (Comparable) value);
            case "le" -> cb.lessThanOrEqualTo(raw, (Comparable) value);
            case "ge" -> cb.greaterThanOrEqualTo(raw, (Comparable) value);
            case "contains" -> cb.like(path.as(String.class),
                    "%" + PlanValues.escapeLike(String.valueOf(value)) + "%", '\\');
            case "startsWith" -> cb.like(path.as(String.class),
                    PlanValues.escapeLike(String.valueOf(value)) + "%", '\\');
            case "endsWith" -> cb.like(path.as(String.class),
                    "%" + PlanValues.escapeLike(String.valueOf(value)), '\\');
            // An operator no leaf case knows — `matches` is the policy-reachable one. An
            // OperatorFunction override registered under that name is consulted first.
            default -> throw Refusals.unsupported("Unsupported operator: " + op);
        };
    }
    static boolean compatibleTypes(Class<?> left, Class<?> right) {
        return left.equals(right) || (Number.class.isAssignableFrom(left) && Number.class.isAssignableFrom(right));
    }

}
