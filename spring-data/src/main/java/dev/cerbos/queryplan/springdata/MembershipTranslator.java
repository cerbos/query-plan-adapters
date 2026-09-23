/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Translates {@code in} and {@code hasIntersection} over a scalar column, a relation chain, or a
 * {@code map()} projection of one.
 *
 * <p>A null element in a value list becomes an {@code IS NULL} disjunct, because SQL
 * {@code IN (..., NULL)} never matches. A NULL member column is a null element when the
 * collection is compared directly ({@link #collectionContainsAny}), but a missing attribute,
 * and so UNKNOWN, under {@code map()} member access ({@link #handleMapIntersection}).
 */
final class MembershipTranslator {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final ChainSubqueries subqueries;

    MembershipTranslator(CriteriaBuilder cb, TriPredicate tri, LeafTranslator leaf,
                         ChainSubqueries subqueries) {
        this.cb = cb;
        this.tri = tri;
        this.leaf = leaf;
        this.subqueries = subqueries;
    }

    // The planner can emit a null scalar (`null in R.attr.items`), and List.of rejects null.
    private static List<?> asList(Object val) {
        return (val instanceof List<?> l) ? l : Collections.singletonList(val);
    }

    Predicate handleIn(List<Operand> rawOperands, Scope scope) {
        if (rawOperands.size() != 2) {
            throw Refusals.malformed("in requires exactly 2 operands");
        }
        // After normalization the mapping kind (Relation or Field), not the operand order,
        // decides between collection membership and a scalar IN.
        List<Operand> operands = NormalizedBinary.of("in", rawOperands).operands();
        Operand fieldOp = operands.get(0);
        Operand valueOp = operands.get(1);
        // `R.attr.createdBy in R.attr.ownedBy`. Normalization never swaps two variables, so the
        // member is first.
        if (fieldOp.getNodeCase() == Operand.NodeCase.VARIABLE
                && valueOp.getNodeCase() == Operand.NodeCase.VARIABLE) {
            return handleInVariableVariable(fieldOp.getVariable(), valueOp.getVariable(), scope);
        }
        if (fieldOp.getNodeCase() != Operand.NodeCase.VARIABLE
                || valueOp.getNodeCase() != Operand.NodeCase.VALUE) {
            // A computed collection or member: legal CEL with no column pair to compare.
            throw Refusals.unsupported("Unsupported in operand combination: "
                    + Refusals.describeOperand(rawOperands.get(0)) + " / "
                    + Refusals.describeOperand(rawOperands.get(1)));
        }
        String var = fieldOp.getVariable();
        Object val = PlanValues.protoValueToJava(valueOp.getValue());

        if (scope.resolve(var) instanceof Scope.ResolvedRelation relRef) {
            if (rawOperands.get(0).getNodeCase() == Operand.NodeCase.VALUE && val instanceof List<?>) {
                return subqueries.chainContains(scope, relRef, (sub, tailJoin, rebased) -> cb.disjunction());
            }
            return collectionContainsAny(scope, relRef, asList(val));
        }

        Path<?> path = scope.path(var);
        return leaf.withOverride("in", path, val, () -> {
            if (val instanceof List<?> list) {
                if (list.isEmpty()) {
                    return cb.disjunction();
                }
                Predicate membership = scalarInWithNullElements(path, list);
                // For an explicit-null attribute, CEL's `null in [...]` without a null element
                // is a definite false, but SQL leaves `NOT (col IN (...))` UNKNOWN.
                if (list.stream().noneMatch(Objects::isNull)
                        && leaf.isExplicitNull(var, scope)) {
                    return cb.and(cb.isNotNull(path), membership);
                }
                return membership;
            }
            // A scalar on a Field mapping is equality; against null that is IS NULL.
            if (val == null) {
                return cb.isNull(path);
            }
            // The eq leaf decides a type mismatch as CEL's false instead of asking the database.
            return leaf.defaultLeaf("eq", path, val);
        });
    }

    /**
     * {@code R.attr.createdBy in R.attr.ownedBy}: a scalar column tested against a Relation on
     * the same resource, as a correlated EXISTS comparing each element with the scalar.
     *
     * <p>A NULL scalar makes the result UNKNOWN (a missing attribute), unless the member is
     * declared explicit-null: then it matches a NULL element and otherwise is false. Over a
     * chain, {@link ChainSubqueries#chainContains} keeps an absent to-one parent UNKNOWN.
     * {@link OperatorFunction} overrides are not consulted, since there is no (field, value)
     * pair.
     */
    private Predicate handleInVariableVariable(String memberVar, String collectionVar,
                                               Scope scope) {
        // resolve() throws for an unknown attribute.
        if (!(scope.resolve(collectionVar) instanceof Scope.ResolvedRelation ref)) {
            throw Refusals.unmapped(
                    "in(" + memberVar + ", " + collectionVar + ") requires the second "
                            + "attribute to be mapped as a Relation (collection membership), "
                            + "but " + collectionVar + " resolves to a scalar Field mapping");
        }
        // Resolve the member now so an unknown or Relation member reports its own error. The
        // subquery body rebuilds the path against the rebased scope.
        Path<?> member = scope.path(memberVar);
        boolean explicit = leaf.isExplicitNull(memberVar, scope);
        Predicate membership = subqueries.chainContains(scope, ref, (sub, tailJoin, rebased) -> {
            Path<?> element = Scope.memberPath(tailJoin, ref.tail(), null);
            // Resolved through the rebased scope to be a legal correlation reference.
            Path<?> outer = rebased.path(memberVar);
            return explicit ? cb.or(cb.equal(element, outer),
                    cb.and(cb.isNull(element), cb.isNull(outer))) : cb.equal(element, outer);
        });
        return explicit ? membership : tri.baseUnlessUnknown(membership, () -> cb.isNull(member));
    }

    /**
     * {@code path IN (list)} with CEL null-element semantics: {@code x in [..., null]} is true
     * for a null {@code x}, so a null element becomes an {@code IS NULL} disjunct. Callers
     * pass a non-empty list.
     */
    private Predicate scalarInWithNullElements(Path<?> path, List<?> list) {
        List<?> nonNull = list.stream().filter(Objects::nonNull).toList();
        boolean hasNull = nonNull.size() < list.size();
        // Constants of another type are CEL's false (`5 in ["5", 2]`) and are dropped. H2 would
        // coerce '5' and match 5.
        List<?> comparable = comparableTo(path, nonNull);
        if (comparable.isEmpty()) {
            if (hasNull) {
                return cb.isNull(path);
            }
            // False for a present value, UNKNOWN for a NULL column, like `path IN (...)`.
            return tri.baseUnlessUnknown(cb.disjunction(), () -> cb.isNull(path));
        }
        Predicate membership = path.in(comparable);
        return hasNull ? cb.or(membership, cb.isNull(path)) : membership;
    }

    // -- hasIntersection --

    Predicate handleHasIntersection(List<Operand> rawOperands, Scope scope) {
        if (rawOperands.size() != 2) {
            throw Refusals.malformed("hasIntersection requires exactly 2 operands");
        }
        // The planner keeps source order, so a folded principal list can come first.
        List<Operand> operands = NormalizedBinary.of("hasIntersection", rawOperands).operands();
        Operand first = operands.get(0);
        Operand second = operands.get(1);

        if (first.getNodeCase() == Operand.NodeCase.VARIABLE
                && second.getNodeCase() == Operand.NodeCase.VALUE) {
            String var = first.getVariable();
            Object val = PlanValues.protoValueToJava(second.getValue());
            List<?> values = asList(val);

            if (scope.resolve(var) instanceof Scope.ResolvedRelation relRef) {
                return collectionContainsAny(scope, relRef, values);
            }
            Path<?> path = scope.path(var);
            // Avoids an empty `IN ()`, which is dialect-dependent.
            if (values.isEmpty()) {
                return cb.disjunction();
            }
            return scalarInWithNullElements(path, values);
        }

        if (first.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "map".equals(first.getExpression().getOperator())) {
            if (second.getNodeCase() != Operand.NodeCase.VALUE) {
                // A projection against another column: legal CEL, no constant list.
                throw Refusals.unsupported(
                        "hasIntersection second operand must be a value list when used with map()");
            }
            Object val = PlanValues.protoValueToJava(second.getValue());
            return handleMapIntersection(first.getExpression(), asList(val), scope);
        }

        // Two collection attributes, or a computed collection on either side.
        throw Refusals.unsupported(
                "Unsupported hasIntersection operand shape: " + Refusals.describeOperand(first) + " / "
                        + Refusals.describeOperand(second) + ". Supported shapes are "
                        + "hasIntersection(collection-attribute, [values...]) and "
                        + "hasIntersection(map(collection, lambda), [values...]).");
    }

    /** {@code hasIntersection(map(collection, lambda), values)}. */
    private Predicate handleMapIntersection(PlanResourcesFilter.Expression mapExpr,
                                            List<?> values, Scope scope) {
        if (values.isEmpty()) {
            return cb.disjunction();
        }

        List<Operand> mapOperands = mapExpr.getOperandsList();
        if (mapOperands.size() != 2) {
            throw Refusals.malformed("map requires exactly 2 operands");
        }
        Operand collectionOperand = mapOperands.get(0);
        Operand lambdaOperand = mapOperands.get(1);

        if (collectionOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            // e.g. `tags.filter(...).map(...)`: legal CEL with no join chain.
            throw Refusals.unsupported("map first operand must be a variable");
        }
        String collectionVar = collectionOperand.getVariable();

        ParsedLambda lambda = ParsedLambda.parse(lambdaOperand,
                "map second operand must be a lambda",
                "map lambda requires exactly 2 operands (body, variable)",
                "map lambda body must be a simple variable projection");
        Operand projection = lambda.body();
        if (projection.getNodeCase() != Operand.NodeCase.VARIABLE) {
            // e.g. `map(t, t.a + "x")`: legal CEL with no column to compare.
            throw Refusals.unsupported("map lambda body must be a simple variable projection");
        }
        String memberField = Scope.extractLambdaSuffix(projection.getVariable(), lambda.varName());

        // A single Relation and a dotted chain both join through every hop, so the projection
        // ranges over the flattened tail elements.
        if (!(scope.resolve(collectionVar) instanceof Scope.ResolvedRelation ref)) {
            throw Refusals.unmapped(
                    "map can only be applied to a collection mapped as Relation: " + collectionVar);
        }
        // A NULL projected column is a missing element attribute, and map() does not absorb
        // errors, so any NULL projection makes the whole result UNKNOWN, even when another
        // element intersects.
        //
        // A column cannot hold an explicitly-null member, so null list elements never match
        // and are dropped.
        List<?> nonNull = values.stream().filter(Objects::nonNull).toList();
        Predicate base = nonNull.isEmpty()
                ? cb.disjunction()
                : subqueries.existsSubquery(scope, ref, (sub, tailJoin, rebased) -> {
                    Path<?> member = Scope.memberPath(tailJoin, ref.tail(), memberField);
                    List<?> comparable = comparableTo(member, nonNull);
                    return comparable.isEmpty() ? cb.disjunction() : member.in(comparable);
                });
        return tri.baseUnlessUnknown(
                base,
                () -> subqueries.existsSubquery(scope, ref, (sub, tailJoin, rebased) ->
                        cb.isNull(Scope.memberPath(tailJoin, ref.tail(), memberField))));
    }

    private Predicate collectionContainsAny(Scope scope, Scope.ResolvedRelation ref, List<?> values) {
        // Avoids an empty `IN ()`, which is dialect-dependent.
        if (values.isEmpty()) {
            return subqueries.chainContains(scope, ref, (sub, tailJoin, rebased) -> cb.disjunction());
        }
        // Here a NULL member column is a null element, so a null constant matches it through
        // an IS NULL disjunct. Contrast handleMapIntersection, where it is a missing attribute.
        List<?> nonNull = values.stream().filter(Objects::nonNull).toList();
        boolean hasNull = nonNull.size() < values.size();
        return subqueries.chainContains(scope, ref, (sub, tailJoin, rebased) -> {
            Path<?> field = Scope.memberPath(tailJoin, ref.tail(), null);
            List<?> comparable = comparableTo(field, nonNull);
            if (comparable.isEmpty()) {
                return hasNull ? cb.isNull(field) : cb.disjunction();
            }
            Predicate match = comparable.size() == 1
                    ? cb.equal(field, comparable.get(0)) : field.in(comparable);
            return hasNull ? cb.or(match, cb.isNull(field)) : match;
        });
    }

    /**
     * Drops constants whose type cannot equal the column. CEL equality across types is a
     * definite false ({@code "2" == 2}), but H2 would coerce {@code '2'} and match 2, and
     * Hibernate refuses a Boolean-to-String comparison. Dropping is safe because membership
     * is a disjunction over the constants.
     */
    private static List<?> comparableTo(Path<?> element, List<?> nonNullValues) {
        return nonNullValues.stream()
                .filter(v -> LeafTranslator.compatibleTypes(element.getJavaType(), v.getClass()))
                .toList();
    }
}
