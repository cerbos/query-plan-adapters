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

    From<?, ?> from();

    AbstractQuery<?> parentQuery();

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
