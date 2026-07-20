package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.AbstractQuery;
import jakarta.persistence.criteria.From;
import jakarta.persistence.criteria.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Resolution context for Cerbos plan variables: maps a variable such as
 * {@code request.resource.attr.foo} (or a lambda-scoped {@code t.name}) to a JPA {@link Path}
 * or an {@link AttributeMapping}, relative to the {@code From} the current (sub)query is built
 * against.
 */
sealed interface Scope permits Scope.RootScope, Scope.LambdaScope {

    Path<?> resolvePath(String cerbosVar);

    AttributeMapping resolveMapping(String cerbosVar);

    /**
     * Resolve a relation-valued variable to the JOIN CHAIN reaching its elements together with
     * the scope that OWNS the first hop, or {@code null} when the variable is not
     * relation-valued. The owner is the resolution site — the root scope for
     * {@code request.resource.attr.*} references (even when resolved from inside a lambda,
     * whose scope merely delegates outward), or the lambda scope itself when the chain hangs
     * off the lambda element. A subquery over the relation must correlate the OWNER's
     * {@code from()}: joining the chain off any other {@code From} either fails at query-build
     * time or silently queries a same-named collection on the wrong entity.
     */
    ResolvedRelation resolveRelation(String cerbosVar);

    From<?, ?> from();

    AbstractQuery<?> parentQuery();

    /**
     * A relation-valued variable resolved to the Relations to join through — in hop order,
     * first hop owned by {@code owner.from()} — ending at the {@code tail} Relation whose
     * elements the enclosing operator ranges over. Multi-hop chains
     * ({@code categories.subCategories}) denote the FLATTENED union of tail elements across
     * the intermediate hops, which is exactly what a correlated join chain expresses.
     */
    record ResolvedRelation(Scope owner, List<AttributeMapping.Relation> chain) {
        public ResolvedRelation {
            chain = List.copyOf(chain);
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
     * Re-root {@code scope} at the correlated copy of its {@code from} inside a subquery, so
     * paths resolved through it become valid correlation references of that subquery.
     */
    static Scope rebase(Scope scope, From<?, ?> correlated, AbstractQuery<?> sub) {
        if (scope instanceof RootScope rs) {
            return new RootScope(correlated, sub, rs.mapper());
        }
        LambdaScope ls = (LambdaScope) scope;
        return new LambdaScope(correlated, sub, ls.relation(), ls.lambdaVar(), ls.outer());
    }

    /**
     * Re-root the scope CHAIN for use inside a subquery that correlated {@code target}'s
     * {@code from()}: the level identical to {@code target} is rebased at {@code correlated}
     * (see {@link #rebase}); levels between {@code scope} and the target keep their Froms —
     * paths through them stay legal as implicit correlation references, the same reliance
     * {@link #rebase} already has on untouched {@code outer} links — but adopt {@code sub} as
     * the query any deeper subqueries are built against. When {@code scope == target} this is
     * exactly {@link #rebase}. Identity comparison is deliberate: the target is always a scope
     * object returned by {@link #resolveRelation} on this same chain.
     */
    static Scope rebaseAt(Scope scope, Scope target, From<?, ?> correlated, AbstractQuery<?> sub) {
        if (scope == target) {
            return rebase(scope, correlated, sub);
        }
        if (scope instanceof LambdaScope ls && ls.outer() != null) {
            return new LambdaScope(ls.from(), sub, ls.relation(), ls.lambdaVar(),
                    rebaseAt(ls.outer(), target, correlated, sub));
        }
        throw new IllegalArgumentException(
                "Relation owner scope is not on the current resolution chain");
    }

    record RootScope(From<?, ?> from, AbstractQuery<?> parentQuery, Map<String, AttributeMapping> mapper)
            implements Scope {
        @Override
        public Path<?> resolvePath(String cerbosVar) {
            AttributeMapping m = mapper.get(cerbosVar);
            if (m == null) {
                throw new IllegalArgumentException("Unknown attribute: " + cerbosVar);
            }
            if (m instanceof AttributeMapping.Field f) {
                return traversePath(from, f.jpaPath());
            }
            throw new IllegalArgumentException(
                    "Attribute " + cerbosVar + " is a Relation; cannot resolve as a scalar path");
        }

        @Override
        public AttributeMapping resolveMapping(String cerbosVar) {
            AttributeMapping m = mapper.get(cerbosVar);
            if (m != null) {
                return m;
            }
            // Try resolving as a dotted suffix off a registered Relation prefix.
            // Example: mapper has "request.resource.attr.categories" → Relation("categories", fields={"subCategories": Relation(...)})
            // and we're asked for "request.resource.attr.categories.subCategories" — walk the chain.
            RelationChain chain = resolveRelationChain(mapper, cerbosVar);
            if (chain != null) {
                return chain.tail() != null
                        ? chain.tail()
                        : chain.relations().get(chain.relations().size() - 1);
            }
            throw new IllegalArgumentException("Unknown attribute: " + cerbosVar);
        }

        @Override
        public ResolvedRelation resolveRelation(String cerbosVar) {
            RelationChain chain = resolveRelationChain(mapper, cerbosVar);
            if (chain != null && chain.tail() == null) {
                return new ResolvedRelation(this, chain.relations());
            }
            return null;
        }
    }

    /**
     * Scope inside a collection lambda. Variables prefixed with the lambda variable resolve
     * against the joined collection element; anything else (e.g. another
     * {@code request.resource.attr.*} reference in the lambda body) delegates to {@code outer}
     * — the enclosing scope re-rooted at the subquery's correlated parent, so the produced
     * path is a legal correlation reference.
     */
    record LambdaScope(From<?, ?> from, AbstractQuery<?> parentQuery,
                       AttributeMapping.Relation relation, String lambdaVar,
                       Scope outer) implements Scope {

        private boolean isLambdaRef(String cerbosVar) {
            return cerbosVar.equals(lambdaVar) || cerbosVar.startsWith(lambdaVar + ".");
        }

        @Override
        public Path<?> resolvePath(String cerbosVar) {
            if (!isLambdaRef(cerbosVar)) {
                if (outer != null) {
                    return outer.resolvePath(cerbosVar);
                }
                throw new IllegalArgumentException(
                        "Variable '" + cerbosVar + "' does not start with lambda variable '" + lambdaVar + "'");
            }
            return memberPath(from, relation, extractLambdaSuffix(cerbosVar, lambdaVar));
        }

        @Override
        public AttributeMapping resolveMapping(String cerbosVar) {
            if (!isLambdaRef(cerbosVar)) {
                if (outer != null) {
                    return outer.resolveMapping(cerbosVar);
                }
                throw new IllegalArgumentException(
                        "Variable '" + cerbosVar + "' does not start with lambda variable '" + lambdaVar + "'");
            }
            String suffix = extractLambdaSuffix(cerbosVar, lambdaVar);
            if (suffix.isEmpty()) {
                return relation;
            }
            AttributeMapping nested = relation.fields().get(suffix);
            if (nested != null) {
                return nested;
            }
            return AttributeMapping.field(suffix);
        }

        @Override
        public ResolvedRelation resolveRelation(String cerbosVar) {
            if (!isLambdaRef(cerbosVar)) {
                // An outer reference: the OWNING scope is found further out — e.g.
                // request.resource.attr.tags inside a categories lambda is owned by the root.
                return outer != null ? outer.resolveRelation(cerbosVar) : null;
            }
            String suffix = extractLambdaSuffix(cerbosVar, lambdaVar);
            if (suffix.isEmpty()) {
                return null; // the bare lambda var is the element itself, not a relation
            }
            List<AttributeMapping.Relation> chain = new ArrayList<>();
            AttributeMapping.Relation current = relation;
            for (String part : suffix.split("\\.")) {
                if (!(current.fields().get(part) instanceof AttributeMapping.Relation next)) {
                    return null; // scalar (or unmapped) hop — not relation-valued
                }
                chain.add(next);
                current = next;
            }
            return new ResolvedRelation(this, chain);
        }
    }

    /**
     * A dotted top-level Cerbos attribute resolved to a chain of Relations, ending in either a
     * leaf {@code tail} Field or (when {@code tail} is null) the final Relation itself.
     */
    record RelationChain(List<AttributeMapping.Relation> relations, AttributeMapping.Field tail) {}

    /**
     * Resolve a Cerbos variable to a {@link RelationChain} by matching the longest registered
     * Relation prefix and walking the remaining dotted suffix through nested {@code fields()}
     * maps. Returns {@code null} if no prefix resolves all the way.
     */
    static RelationChain resolveRelationChain(Map<String, AttributeMapping> mapper, String cerbosVar) {
        AttributeMapping direct = mapper.get(cerbosVar);
        if (direct instanceof AttributeMapping.Relation rel) {
            return new RelationChain(List.of(rel), null);
        }
        String[] parts = cerbosVar.split("\\.");
        for (int i = parts.length - 1; i > 0; i--) {
            String prefix = String.join(".", Arrays.copyOfRange(parts, 0, i));
            if (!(mapper.get(prefix) instanceof AttributeMapping.Relation rel)) {
                continue;
            }
            String[] suffixParts = Arrays.copyOfRange(parts, i, parts.length);
            List<AttributeMapping.Relation> chain = new ArrayList<>();
            chain.add(rel);
            AttributeMapping current = rel;
            boolean ok = true;
            for (int s = 0; s < suffixParts.length; s++) {
                if (!(current instanceof AttributeMapping.Relation r)) {
                    ok = false;
                    break;
                }
                AttributeMapping next = r.fields().get(suffixParts[s]);
                if (next == null) {
                    ok = false;
                    break;
                }
                if (next instanceof AttributeMapping.Relation nextRel) {
                    chain.add(nextRel);
                    current = nextRel;
                } else if (next instanceof AttributeMapping.Field leafField && s == suffixParts.length - 1) {
                    return new RelationChain(chain, leafField);
                } else {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return new RelationChain(chain, null);
            }
        }
        return null;
    }

    /**
     * Resolve a member path off a join over {@code rel}: an empty/null {@code memberField}
     * yields the relation's {@code defaultMemberField} if set, else the joined element itself
     * ({@code @ElementCollection} of primitives); otherwise the member resolves through the
     * relation's {@code fields()} mapping, falling back to the raw name as a JPA path.
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

    static Path<?> traversePath(From<?, ?> from, String dottedJpaPath) {
        Path<?> p = from;
        for (String part : dottedJpaPath.split("\\.")) {
            p = p.get(part);
        }
        return p;
    }

    static String extractLambdaSuffix(String variable, String lambdaVar) {
        if (variable.equals(lambdaVar)) {
            return "";
        }
        String prefix = lambdaVar + ".";
        if (!variable.startsWith(prefix)) {
            throw new IllegalArgumentException(
                    "Variable '" + variable + "' does not start with lambda variable '" + lambdaVar + "'");
        }
        return variable.substring(prefix.length());
    }
}
