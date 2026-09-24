/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.AbstractQuery;
import jakarta.persistence.criteria.From;
import jakarta.persistence.criteria.JoinType;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.metamodel.Bindable;
import jakarta.persistence.metamodel.ManagedType;
import jakarta.persistence.metamodel.PluralAttribute;
import jakarta.persistence.metamodel.SingularAttribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Resolves a plan variable such as {@code request.resource.attr.foo} or a lambda-scoped
 * {@code t.name} to a scalar JPA {@link Path} or a relation, relative to the current
 * (sub)query's {@code From}.
 */
sealed interface Scope permits Scope.RootScope, Scope.LambdaScope {

    /**
     * Resolves {@code cerbosVar} against this scope. Never returns null: throws
     * {@link UnmappedAttributeException} for a name the mapping does not cover and
     * {@link MalformedPlanException} for an unbound lambda reference.
     *
     * <p>A dotted variable matches the longest registered Relation prefix, and the rest is
     * walked through nested {@code fields()} maps. A {@link LambdaScope} resolves its own
     * variable against the joined element and delegates everything else outward.
     */
    Resolution resolve(String cerbosVar);

    /**
     * The JPA path to compare {@code cerbosVar} as. Throws for a relation, or for a Field with
     * no column on this scope's {@code From}.
     */
    default Path<?> path(String cerbosVar) {
        if (resolve(cerbosVar) instanceof ResolvedScalar scalar) {
            if (scalar.path() == null) {
                throw Refusals.unknownAttribute(cerbosVar);
            }
            return scalar.path();
        }
        throw Refusals.unmapped(
                "Attribute " + cerbosVar + " is a Relation; cannot resolve as a scalar path");
    }

    From<?, ?> from();

    AbstractQuery<?> parentQuery();

    /** What a Cerbos plan variable denotes in a scope: a scalar value, or a relation. */
    sealed interface Resolution permits ResolvedScalar, ResolvedRelation {}

    /**
     * A variable denoting a single value, and the mapping it was resolved through.
     *
     * <p>For the bare lambda variable ({@code t} in {@code tags.exists(t, ...)}) the mapping
     * is the {@link AttributeMapping.Relation} it ranges over. {@code path} is null when the
     * Field is reachable only through a relation chain, so it has no column on this scope's
     * entity; {@link #path} reports that as an unknown attribute.
     */
    record ResolvedScalar(Path<?> path, AttributeMapping mapping) implements Resolution {}

    /**
     * A relation-valued variable: the Relations to join through, in hop order. A multi-hop
     * chain denotes the flattened union of the last hop's elements.
     *
     * <p>{@code owner} is the scope whose {@code from()} holds the first hop. A subquery over
     * the relation must correlate that {@code From}; any other could silently query a
     * same-named collection on the wrong entity.
     */
    record ResolvedRelation(Scope owner, List<AttributeMapping.Relation> chain)
            implements Resolution {
        public ResolvedRelation {
            chain = List.copyOf(chain);
        }

        boolean isChained() {
            return chain.size() > 1;
        }

        AttributeMapping.Relation tail() {
            return chain.get(chain.size() - 1);
        }
    }

    static Scope root(From<?, ?> root, AbstractQuery<?> query, Map<String, AttributeMapping> mapper) {
        return new RootScope(root, query, mapper);
    }

    static Scope lambda(From<?, ?> from, AbstractQuery<?> parentQuery,
                        AttributeMapping.Relation relation, String lambdaVar, Scope outer) {
        return new LambdaScope(from, parentQuery, relation, lambdaVar, outer);
    }

    /**
     * Re-roots the scope chain for use inside subquery {@code sub}, which correlated
     * {@code target}'s {@code from()}. The {@code target} level is re-rooted at
     * {@code correlated}; levels between {@code scope} and {@code target} keep their Froms but
     * build deeper subqueries against {@code sub}. {@code target} is matched by identity, since
     * it is the {@link ResolvedRelation#owner()} returned on this chain.
     */
    static Scope rebaseAt(Scope scope, Scope target, From<?, ?> correlated, AbstractQuery<?> sub) {
        if (scope == target) {
            if (scope instanceof RootScope rs) {
                return new RootScope(correlated, sub, rs.mapper());
            }
            LambdaScope ls = (LambdaScope) scope;
            return new LambdaScope(correlated, sub, ls.relation(), ls.lambdaVar(), ls.outer());
        }
        if (scope instanceof LambdaScope ls && ls.outer() != null) {
            return new LambdaScope(ls.from(), sub, ls.relation(), ls.lambdaVar(),
                    rebaseAt(ls.outer(), target, correlated, sub));
        }
        // An adapter invariant, not a plan refusal. ScopeTest pins the exception type.
        throw new IllegalArgumentException(
                "Relation owner scope is not on the current resolution chain");
    }

    record RootScope(From<?, ?> from, AbstractQuery<?> parentQuery, Map<String, AttributeMapping> mapper)
            implements Scope {
        @Override
        public Resolution resolve(String cerbosVar) {
            if (mapper.get(cerbosVar) instanceof AttributeMapping.Field f) {
                return new ResolvedScalar(traversePath(from, f.jpaPath()), f);
            }
            // Otherwise a registered Relation or a dotted path off one.
            RelationChain chain = resolveRelationChain(mapper, cerbosVar);
            if (chain == null) {
                throw Refusals.unknownAttribute(cerbosVar);
            }
            if (chain.tail() == null) {
                return new ResolvedRelation(this, chain.relations());
            }
            // A Field reached through the chain has no column here; see ResolvedScalar.
            return new ResolvedScalar(null, chain.tail());
        }
    }

    /**
     * Scope inside a collection lambda. The lambda variable resolves against the joined
     * element; anything else delegates to {@code outer}, the enclosing scope re-rooted inside
     * the subquery.
     */
    record LambdaScope(From<?, ?> from, AbstractQuery<?> parentQuery,
                       AttributeMapping.Relation relation, String lambdaVar,
                       Scope outer) implements Scope {

        private boolean isLambdaRef(String cerbosVar) {
            return cerbosVar.equals(lambdaVar) || cerbosVar.startsWith(lambdaVar + ".");
        }

        @Override
        public Resolution resolve(String cerbosVar) {
            if (!isLambdaRef(cerbosVar)) {
                // An outer reference is resolved and owned further out.
                if (outer != null) {
                    return outer.resolve(cerbosVar);
                }
                throw Refusals.notALambdaReference(cerbosVar, lambdaVar);
            }
            String suffix = extractLambdaSuffix(cerbosVar, lambdaVar);
            if (suffix.isEmpty()) {
                // The bare lambda variable is the element itself.
                return new ResolvedScalar(memberPath(from, relation, suffix), relation);
            }
            List<AttributeMapping.Relation> chain = relationChain(suffix);
            if (chain != null) {
                return new ResolvedRelation(this, chain);
            }
            // A member scalar: its nested mapping if registered, else the suffix as a JPA path.
            AttributeMapping nested = relation.fields().get(suffix);
            return new ResolvedScalar(memberPath(from, relation, suffix),
                    nested != null ? nested : AttributeMapping.field(suffix));
        }

        /**
         * The Relations {@code suffix} walks through the element's nested {@code fields()}, or
         * {@code null} if any part is not a Relation.
         */
        private List<AttributeMapping.Relation> relationChain(String suffix) {
            List<AttributeMapping.Relation> chain = new ArrayList<>();
            AttributeMapping.Relation current = relation;
            for (String part : suffix.split("\\.")) {
                if (!(current.fields().get(part) instanceof AttributeMapping.Relation next)) {
                    return null;
                }
                chain.add(next);
                current = next;
            }
            return chain;
        }
    }

    /** A chain of Relations, ending in the Field {@code tail}, or in the last Relation when null. */
    record RelationChain(List<AttributeMapping.Relation> relations, AttributeMapping.Field tail) {}

    /**
     * Resolves {@code cerbosVar} by its longest registered Relation prefix that the rest of the
     * path can be walked from, or returns {@code null}.
     */
    static RelationChain resolveRelationChain(Map<String, AttributeMapping> mapper, String cerbosVar) {
        AttributeMapping direct = mapper.get(cerbosVar);
        if (direct instanceof AttributeMapping.Relation rel) {
            return new RelationChain(List.of(rel), null);
        }
        String[] parts = cerbosVar.split("\\.");
        for (int i = parts.length - 1; i > 0; i--) {
            String prefix = String.join(".", Arrays.copyOfRange(parts, 0, i));
            if (mapper.get(prefix) instanceof AttributeMapping.Relation head) {
                RelationChain chain = walkSuffix(head, Arrays.copyOfRange(parts, i, parts.length));
                if (chain != null) {
                    return chain;
                }
            }
        }
        return null;
    }

    /**
     * Walks {@code suffix} through {@code head}'s nested {@code fields()}. Every part must be a
     * Relation except the last, which may be a Field; otherwise returns {@code null}.
     */
    private static RelationChain walkSuffix(AttributeMapping.Relation head, String[] suffix) {
        List<AttributeMapping.Relation> chain = new ArrayList<>(List.of(head));
        AttributeMapping.Relation current = head;
        for (int i = 0; i < suffix.length; i++) {
            AttributeMapping next = current.fields().get(suffix[i]);
            if (next instanceof AttributeMapping.Relation relation) {
                chain.add(relation);
                current = relation;
            } else if (next instanceof AttributeMapping.Field field && i == suffix.length - 1) {
                return new RelationChain(chain, field);
            } else {
                return null;
            }
        }
        return new RelationChain(chain, null);
    }

    /**
     * A member path off a join over {@code rel}. With no {@code memberField}: the
     * {@code defaultMemberField}, else the element itself. Otherwise the nested Field mapping,
     * else the raw name as a JPA path.
     */
    static Path<?> memberPath(From<?, ?> from, AttributeMapping.Relation rel, String memberField) {
        if (memberField == null || memberField.isEmpty()) {
            if (rel.defaultMemberField() != null && !rel.defaultMemberField().isEmpty()) {
                return from.get(rel.defaultMemberField());
            }
            return (Path<?>) from;
        }
        AttributeMapping nested = rel.fields().get(memberField);
        if (nested instanceof AttributeMapping.Field f) {
            return traversePath(from, f.jpaPath());
        }
        return traversePath(from, memberField);
    }

    /**
     * Walks a dotted JPA path, LEFT-joining each association before the last segment.
     *
     * <p>{@code Path.get()} would inner-join, dropping the row from the whole query when the
     * association is absent, even when another {@code OR} branch allows it. A LEFT join makes
     * the missing hop NULL, which only makes its own comparison UNKNOWN. Embeddables stay on
     * {@code get()}.
     */
    static Path<?> traversePath(From<?, ?> from, String dottedJpaPath) {
        String[] parts = dottedJpaPath.split("\\.");
        Path<?> p = from;
        for (int i = 0; i < parts.length; i++) {
            boolean last = i == parts.length - 1;
            if (!last && p instanceof From<?, ?> f && isAssociation(f, parts[i])) {
                p = f.join(parts[i], JoinType.LEFT);
            } else {
                p = p.get(parts[i]);
            }
        }
        return p;
    }

    /**
     * Whether {@code part} is an association on {@code from}'s type. Asks the metamodel, because
     * resolving the path would register an implicit inner join.
     */
    private static boolean isAssociation(From<?, ?> from, String part) {
        ManagedType<?> managed = managedTypeOf(from);
        if (managed == null) {
            return false;
        }
        try {
            return managed.getAttribute(part).isAssociation();
        } catch (IllegalArgumentException notAnAttribute) {
            // Let get() raise the provider's own error for an unknown name.
            return false;
        }
    }

    /**
     * The managed type of {@code from}. A {@code Join}'s model is the attribute it was created
     * from, so the target type is read from that attribute.
     */
    private static ManagedType<?> managedTypeOf(From<?, ?> from) {
        Bindable<?> model = from.getModel();
        if (model instanceof ManagedType<?> managed) {
            return managed;
        }
        if (model instanceof SingularAttribute<?, ?> singular
                && singular.getType() instanceof ManagedType<?> target) {
            return target;
        }
        if (model instanceof PluralAttribute<?, ?, ?> plural
                && plural.getElementType() instanceof ManagedType<?> target) {
            return target;
        }
        return null;
    }

    static String extractLambdaSuffix(String variable, String lambdaVar) {
        if (variable.equals(lambdaVar)) {
            return "";
        }
        String prefix = lambdaVar + ".";
        if (!variable.startsWith(prefix)) {
            throw Refusals.notALambdaReference(variable, lambdaVar);
        }
        return variable.substring(prefix.length());
    }
}
