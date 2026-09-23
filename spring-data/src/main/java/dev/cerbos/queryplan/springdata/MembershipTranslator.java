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
 * Membership: {@code in} and {@code hasIntersection}, over a scalar column, a relation chain,
 * or a {@code map()} projection of one.
 *
 * <p>Owns the CEL null-element semantics of a value list — a null element is an
 * {@code IS NULL} disjunct, never an {@code IN (..., NULL)} that SQL silently drops — and the
 * two views of a NULL member column that decide it: under the scalar-projection view
 * ({@link #collectionContainsAny}) a NULL member IS the null element, while under member
 * ACCESS ({@link #handleMapIntersection}) it is a missing element attribute and therefore an
 * UNKNOWN row. Every existence test over a chain goes through
 * {@link ChainSubqueries#chainContains} so an absent to-one parent stays UNKNOWN under both
 * polarities.
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

    /**
     * Wrap a scalar plan constant as a single-element list; lists pass through unchanged.
     * The scalar may be the null constant ({@code null in R.attr.items} is planner-emitted),
     * so the wrapper must be null-tolerant — {@code List.of} is not.
     */
    private static List<?> asList(Object val) {
        return (val instanceof List<?> l) ? l : Collections.singletonList(val);
    }

    Predicate handleIn(List<Operand> rawOperands, Scope scope) {
        if (rawOperands.size() != 2) {
            throw Refusals.malformed("in requires exactly 2 operands");
        }
        // Both shapes — `field in [values]` and `value in collection-field` — resolve the
        // same way once normalized field-first: the mapping kind (Relation vs Field) decides
        // whether this is collection membership or a scalar IN, not the operand order.
        List<Operand> operands = NormalizedBinary.of("in", rawOperands).operands();
        Operand fieldOp = operands.get(0);
        Operand valueOp = operands.get(1);
        // in(variable, variable) — attribute-in-attribute membership
        // (`R.attr.createdBy in R.attr.ownedBy` arrives verbatim; PDP-verified). CEL `in`
        // is receiver-shaped — the member is always FIRST, the list second — and two
        // VARIABLE operands rank equally so normalization never swaps them: source order
        // is authoritative here.
        if (fieldOp.getNodeCase() == Operand.NodeCase.VARIABLE
                && valueOp.getNodeCase() == Operand.NodeCase.VARIABLE) {
            return handleInVariableVariable(fieldOp.getVariable(), valueOp.getVariable(), scope);
        }
        if (fieldOp.getNodeCase() != Operand.NodeCase.VARIABLE
                || valueOp.getNodeCase() != Operand.NodeCase.VALUE) {
            // Membership in a computed collection (`x in R.attr.tags.map(...)`) or of a
            // computed member: legal CEL, no column pair to compare.
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
                // Without a null element nothing has made the membership definite yet:
                // `NOT (col IN (…))` over a NULL column is UNKNOWN and drops the row, while
                // CEL compares a null VALUE against each element and gets a definite false.
                // With a null element `scalarInWithNullElements` already adds the IS NULL
                // disjunct, which settles it (#308).
                if (list.stream().noneMatch(Objects::isNull)
                        && leaf.isExplicitNull(var, scope)) {
                    return cb.and(cb.isNotNull(path), membership);
                }
                return membership;
            }
            // Scalar membership over a Field mapping is equality — and equality against
            // the null constant is IS NULL, mirroring the eq-null leaf translation.
            if (val == null) {
                return cb.isNull(path);
            }
            // The eq leaf, not a bare cb.equal: a constant the column's type cannot equal is
            // CEL's heterogeneous FALSE, which the leaf decides rather than the database.
            return leaf.defaultLeaf("eq", path, val);
        });
    }

    /**
     * {@code in(variable, variable)} — a scalar attribute tested for membership of a
     * collection attribute on the SAME resource ({@code R.attr.createdBy in
     * R.attr.ownedBy}; PDP-verified the shape arrives verbatim). The member variable must
     * resolve to a scalar column and the collection variable to a Relation mapping; the
     * translation is a correlated EXISTS whose body compares the collection's member
     * column against the outer scalar column:
     *
     * <pre>{@code EXISTS (SELECT 1 FROM <relation chain> e
     *          WHERE e.member = outer.scalar
     *             OR (e.member IS NULL AND outer.scalar IS NULL))}</pre>
     *
     * <p>Null semantics, verified against a live PDP {@code check()} oracle under the
     * adapter's established column conventions (a NULL scalar column is the
     * explicitly-null attribute — the {@code eq(x, null) → IS NULL} convention; a NULL
     * member column is an explicit null list element — the {@code collectionContainsAny}
     * convention; an empty join is the empty list):
     * <ul>
     *   <li>member matches an element → TRUE (row included);</li>
     *   <li>no match (including the empty collection) → the EXISTS is FALSE, so the row
     *       is excluded and {@code not(...)} includes it — matching CEL, where a
     *       non-matching {@code in} is plain FALSE, not an error;</li>
     *   <li>NULL scalar vs a null element → TRUE ({@code null in [..., null]} is TRUE in
     *       CEL — the IS NULL conjunct is what matches it, since SQL {@code = NULL} never
     *       does);</li>
     *   <li>NULL scalar vs no null element → FALSE (the equality is UNKNOWN and the
     *       IS NULL conjunct fails on the member side, so no subquery row qualifies).</li>
     * </ul>
     * A direct relation's EXISTS is two-valued, so {@code tri.not} composes exactly; over a
     * CHAIN the membership goes through {@link ChainSubqueries#chainContains}, which is
     * UNKNOWN for an absent to-one parent so that the negation cannot readmit it. Like
     * field-to-field comparisons, there is no (field, value) pair — {@link OperatorFunction}
     * overrides are not consulted.
     */
    private Predicate handleInVariableVariable(String memberVar, String collectionVar,
                                               Scope scope) {
        // resolve() is total, so an unmapped collectionVar throws "Unknown attribute" here
        // rather than needing a separate call made purely for its throw.
        if (!(scope.resolve(collectionVar) instanceof Scope.ResolvedRelation ref)) {
            throw Refusals.unmapped(
                    "in(" + memberVar + ", " + collectionVar + ") requires the second "
                            + "attribute to be mapped as a Relation (collection membership), "
                            + "but " + collectionVar + " resolves to a scalar Field mapping");
        }
        // Check the member eagerly: resolved only inside the subquery body, an unknown or
        // Relation-valued member would be masked by chainSubquery's own failure (the
        // bulk-delete guard). The path itself has to be rebuilt against the REBASED scope
        // below to be a legal correlation reference, so this call is a check, not a value.
        Path<?> member = scope.path(memberVar);
        boolean explicit = leaf.isExplicitNull(memberVar, scope);
        Predicate membership = subqueries.chainContains(scope, ref, (sub, tailJoin, rebased) -> {
            Path<?> element = Scope.memberPath(tailJoin, ref.tail(), null);
            // The outer scalar resolves through the REBASED scope so the produced path is
            // a legal correlation reference inside the subquery.
            Path<?> outer = rebased.path(memberVar);
            return explicit ? cb.or(cb.equal(element, outer),
                    cb.and(cb.isNull(element), cb.isNull(outer))) : cb.equal(element, outer);
        });
        return explicit ? membership : tri.baseUnlessUnknown(membership, () -> cb.isNull(member));
    }

    /**
     * {@code path IN (list)} with CEL null-element semantics. CEL {@code x in [..., null]}
     * is TRUE for an explicitly-null {@code x} (PDP-verified for both {@code in} and
     * {@code hasIntersection}; the planner even folds the degenerate {@code x in [null]}
     * to {@code eq(x, null)}, which this adapter translates as IS NULL) — so a null list
     * element must become an IS NULL disjunct. Passing it to {@code path.in} instead
     * renders {@code IN (..., NULL)} (verified on Hibernate 6.6/H2), whose SQL
     * three-valued semantics silently EXCLUDE null rows — and make the negation UNKNOWN
     * for every non-matching row, returning nothing. Both disjuncts here are two-valued
     * for every row (IS NULL absorbs the NULL-column case), so {@code tri.not} composes
     * cleanly over the OR. Callers guarantee a non-empty list.
     */
    private Predicate scalarInWithNullElements(Path<?> path, List<?> list) {
        List<?> nonNull = list.stream().filter(Objects::nonNull).toList();
        boolean hasNull = nonNull.size() < list.size();
        // A constant the column's type cannot equal is CEL's heterogeneous FALSE disjunct
        // (`5 in ["5", 2]` is decided by the 2 alone), so it never reaches the database —
        // H2 would coerce '5' onto the numeric column and match 5 (an over-grant).
        List<?> comparable = comparableTo(path, nonNull);
        if (comparable.isEmpty()) {
            if (hasNull) {
                return cb.isNull(path);
            }
            // Every constant dropped: FALSE for a present value, and still UNKNOWN for a
            // NULL column, exactly as `path IN (...)` would have been, so the negation
            // cannot readmit a missing attribute (handleIn adds the explicit-null guard).
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
        // Intersection is symmetric, and the planner preserves policy source order —
        // `hasIntersection(P.attr.tags, R.attr.tags)` folds the principal side to a value
        // list in the FIRST position. Normalization puts the field/map side first.
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
            // hasIntersection(field, []) is always false; avoid a dialect-dependent empty `IN ()`.
            if (values.isEmpty()) {
                return cb.disjunction();
            }
            return scalarInWithNullElements(path, values);
        }

        if (first.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "map".equals(first.getExpression().getOperator())) {
            if (second.getNodeCase() != Operand.NodeCase.VALUE) {
                // An intersection of a projection with another column: legal CEL, no
                // constant list for the projected IN.
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

    /** Translate {@code hasIntersection(map(collection, lambda), values)}. */
    private Predicate handleMapIntersection(PlanResourcesFilter.Expression mapExpr,
                                            List<?> values, Scope scope) {
        // hasIntersection(map(...), []) is always false; short-circuit before the subquery.
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
            // A chained projection (`tags.filter(...).map(...)`): legal CEL, no chain.
            throw Refusals.unsupported("map first operand must be a variable");
        }
        String collectionVar = collectionOperand.getVariable();

        ParsedLambda lambda = ParsedLambda.parse(lambdaOperand,
                "map second operand must be a lambda",
                "map lambda requires exactly 2 operands (body, variable)",
                "map lambda body must be a simple variable projection");
        // map()'s extra shape constraint: the body must project a plain member variable.
        Operand projection = lambda.body();
        if (projection.getNodeCase() != Operand.NodeCase.VARIABLE) {
            // A computed projection (`map(t, t.a + "x")`): legal CEL, no column to IN over.
            throw Refusals.unsupported("map lambda body must be a simple variable projection");
        }
        String memberField = Scope.extractLambdaSuffix(projection.getVariable(), lambda.varName());

        // Resolve the collection to its owner-anchored join chain. Single Relations and
        // dotted chains ("request.resource.attr.categories.subCategories") share one path:
        // the subquery correlates the OWNING From and joins through every hop, so the
        // projection ranges over the flattened tail elements.
        if (!(scope.resolve(collectionVar) instanceof Scope.ResolvedRelation ref)) {
            throw Refusals.unmapped(
                    "map can only be applied to a collection mapped as Relation: " + collectionVar);
        }
        // CEL map() has no error absorption: a NULL projected column is a missing element
        // attribute, so the whole hasIntersection(map(...), values) is an evaluation error
        // (deny) even when another element would intersect — the strict
        // TriPredicate.baseUnlessUnknown table, with the null-witness EXISTS as the unknown
        // detector (IS NULL itself is two-valued, so both EXISTS legs are safe to compose).
        //
        // A null element in the constant list only matches an explicitly-null projection,
        // which member ACCESS can never yield from the column model: a NULL member column
        // is the missing-attribute error above (PDP-verified: tags=[{}] denies under BOTH
        // polarities even with null in the list; tags=[{"name": null}] would allow, but a
        // column cannot distinguish that case and the error convention wins here). Null
        // elements are therefore inert — stripped so they don't render as a never-matching
        // SQL `IN (..., NULL)` literal — while NULL-projection rows stay UNKNOWN.
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
        // Intersection with an empty value set is always false — and an EXISTS wrapping an
        // empty `IN ()` is dialect-dependent — so short-circuit before building the subquery.
        if (values.isEmpty()) {
            return subqueries.chainContains(scope, ref, (sub, tailJoin, rebased) -> cb.disjunction());
        }
        // CEL membership/intersection with a null constant is satisfied by a collection
        // element that IS null (PDP-verified for both routes here: `null in R.attr.xs`
        // with xs=["a", null] allows, and hasIntersection(R.attr.xs, ["public", null])
        // with xs=[null] allows). A related row whose member column is NULL is exactly
        // such an element under the scalar-projection (defaultMemberField) view, so the
        // null constant becomes an IS NULL disjunct inside the EXISTS body —
        // `member IN (...)`/`member = NULL` never matches it in SQL. (Contrast with
        // map(t, t.name) member ACCESS, where a NULL column is a MISSING element
        // attribute → CEL error; see handleMapIntersection.)
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
     * The constants a column — a collection's element column, or a scalar attribute's — can
     * equal, in CEL's sense.
     *
     * <p>CEL's equality is heterogeneous: {@code "2" == 2} and {@code "true" == true} are a
     * definite FALSE, not an error, so {@code "2" in R.attr.aNumberList} is false for every row and
     * a {@code hasIntersection} literal of the wrong type contributes nothing. Handing such a
     * constant to the database is wrong either way it goes — H2 coerces {@code '2'} onto a
     * numeric element column and matches the rows holding 2 (an over-grant), and Hibernate
     * refuses to build a Boolean-to-String comparison at all. So a constant whose type cannot compare
     * with the element column is dropped here, which is exactly the FALSE disjunct CEL gives it.
     * Dropping is sound only because membership is a disjunction over the constants and an
     * element-to-constant equality between present values is two-valued; callers keep their own
     * handling of a null constant and of a NULL element column.
     */
    private static List<?> comparableTo(Path<?> element, List<?> nonNullValues) {
        return nonNullValues.stream()
                .filter(v -> LeafTranslator.compatibleTypes(element.getJavaType(), v.getClass()))
                .toList();
    }
}
