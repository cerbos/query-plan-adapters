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
 * Resolution context for Cerbos plan variables: maps a variable such as
 * {@code request.resource.attr.foo} (or a lambda-scoped {@code t.name}) to what it denotes —
 * a scalar JPA {@link Path} or a relation to join through — relative to the {@code From} the
 * current (sub)query is built against.
 */
sealed interface Scope permits Scope.RootScope, Scope.LambdaScope {

    /**
     * Classify a Cerbos plan variable and resolve it against this scope.
     *
     * <p>TOTAL: every variable either lands in one of {@link Resolution}'s two arms or throws
     * {@link IllegalArgumentException} naming why — there is no null return and no second
     * classifier to consult afterwards. Callers that need a column narrow with {@link #path};
     * callers that need a collection pattern-match {@link ResolvedRelation}, and get the
     * unmapped-variable rejection from this throw rather than from a second call made purely
     * for it.
     *
     * <p>The three resolution rules live here and only here:
     * <ul>
     *   <li><b>chain walking</b> — a dotted variable is matched against the longest registered
     *       Relation prefix and its suffix walked through nested {@code fields()} maps, so
     *       {@code ...attr.categories.subCategories} resolves to the two-hop join chain;</li>
     *   <li><b>lambda delegation</b> — a {@link LambdaScope} resolves variables prefixed with
     *       its lambda variable against the joined element and delegates everything else
     *       outward;</li>
     *   <li><b>owner anchoring</b> — a {@link ResolvedRelation} carries the scope that OWNS
     *       its first hop, which is where a subquery over it must correlate.</li>
     * </ul>
     */
    Resolution resolve(String cerbosVar);

    /**
     * Narrow {@link #resolve} to the scalar arm: the JPA path to compare {@code cerbosVar} as.
     *
     * <p>Relation-valued variables and Fields with no column on this scope's {@code From} are
     * rejected here rather than resolved to a guessed path — the adapter fails closed rather
     * than comparing the wrong column.
     */
    default Path<?> path(String cerbosVar) {
        if (resolve(cerbosVar) instanceof ResolvedScalar scalar) {
            if (scalar.path() == null) {
                throw new IllegalArgumentException("Unknown attribute: " + cerbosVar);
            }
            return scalar.path();
        }
        throw new IllegalArgumentException(
                "Attribute " + cerbosVar + " is a Relation; cannot resolve as a scalar path");
    }

    From<?, ?> from();

    AbstractQuery<?> parentQuery();

    /** What a Cerbos plan variable denotes in a scope: a scalar value, or a relation. */
    sealed interface Resolution permits ResolvedScalar, ResolvedRelation {}

    /**
     * A variable denoting a single value: {@code path} is the JPA path to compare it as and
     * {@code mapping} is the {@link AttributeMapping} it was resolved through.
     *
     * <p>{@code mapping} is never null, but it is a {@link AttributeMapping.Relation} for the
     * bare lambda variable — {@code t} inside {@code tags.exists(t, ...)} denotes the ELEMENT,
     * whose {@code path} is the relation's {@code defaultMemberField} (or the joined element
     * itself), and whose only mapping is the relation it came from. Callers that need a
     * genuine scalar attribute test {@code mapping instanceof AttributeMapping.Field}.
     *
     * <p>{@code path} is null exactly when the Field is reachable only THROUGH a relation
     * chain ({@code ...attr.categories.name} against a root scope): mapped, and scalar per
     * ELEMENT, but with no column on the entity this scope is rooted at. It resolves to this
     * arm rather than throwing so the collection operators can say the variable is a scalar
     * one instead of calling it unknown; {@link #path} reports it as the "Unknown attribute"
     * it has always been.
     */
    record ResolvedScalar(Path<?> path, AttributeMapping mapping) implements Resolution {}

    /**
     * A relation-valued variable resolved to the Relations to join through — in hop order,
     * first hop owned by {@code owner.from()} — ending at the {@code tail} Relation whose
     * elements the enclosing operator ranges over. Multi-hop chains
     * ({@code categories.subCategories}) denote the FLATTENED union of tail elements across
     * the intermediate hops, which is exactly what a correlated join chain expresses.
     *
     * <p>The owner is the resolution site — the root scope for
     * {@code request.resource.attr.*} references (even when resolved from inside a lambda,
     * whose scope merely delegates outward), or the lambda scope itself when the chain hangs
     * off the lambda element. A subquery over the relation must correlate the OWNER's
     * {@code from()}: joining the chain off any other {@code From} either fails at query-build
     * time or silently queries a same-named collection on the wrong entity.
     */
    record ResolvedRelation(Scope owner, List<AttributeMapping.Relation> chain)
            implements Resolution {
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
     * Re-root the scope CHAIN for use inside a subquery that correlated {@code target}'s
     * {@code from()}: the level identical to {@code target} is re-rooted at {@code correlated},
     * so paths resolved through it become valid correlation references of that subquery; levels
     * between {@code scope} and the target keep their Froms — paths through them stay legal as
     * implicit correlation references, the same reliance the base case already places on
     * untouched {@code outer} links — but adopt {@code sub} as the query any deeper subqueries
     * are built against. Identity comparison is deliberate: the target is always the
     * {@link ResolvedRelation#owner()} {@link #resolve} returned on this same chain.
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
        throw new IllegalArgumentException(
                "Relation owner scope is not on the current resolution chain");
    }

    record RootScope(From<?, ?> from, AbstractQuery<?> parentQuery, Map<String, AttributeMapping> mapper)
            implements Scope {
        @Override
        public Resolution resolve(String cerbosVar) {
            // A directly registered Field is a column on this From — the common case, and the
            // only one where the variable names a scalar the entity actually holds.
            if (mapper.get(cerbosVar) instanceof AttributeMapping.Field f) {
                return new ResolvedScalar(traversePath(from, f.jpaPath()), f);
            }
            // Otherwise the variable is either a registered Relation, or a dotted suffix off
            // one. Example: mapper has "request.resource.attr.categories" →
            // Relation("categories", fields={"subCategories": Relation(...)}) and we are asked
            // for "request.resource.attr.categories.subCategories" — walk the chain.
            RelationChain chain = resolveRelationChain(mapper, cerbosVar);
            if (chain == null) {
                throw new IllegalArgumentException("Unknown attribute: " + cerbosVar);
            }
            if (chain.tail() == null) {
                return new ResolvedRelation(this, chain.relations());
            }
            // A Field reached THROUGH the chain: scalar per element, no column here — see
            // ResolvedScalar on why this is an arm rather than a throw.
            return new ResolvedScalar(null, chain.tail());
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
        public Resolution resolve(String cerbosVar) {
            if (!isLambdaRef(cerbosVar)) {
                // An outer reference: it resolves — and, when relation-valued, is OWNED —
                // further out. request.resource.attr.tags inside a categories lambda belongs
                // to the root, so a subquery over it correlates the root's From, not this
                // lambda's element join.
                if (outer != null) {
                    return outer.resolve(cerbosVar);
                }
                throw new IllegalArgumentException(
                        "Variable '" + cerbosVar + "' does not start with lambda variable '" + lambdaVar + "'");
            }
            String suffix = extractLambdaSuffix(cerbosVar, lambdaVar);
            if (suffix.isEmpty()) {
                // The bare lambda variable is the element itself, not a relation: its value is
                // the relation's scalar projection, its mapping the relation it came from.
                return new ResolvedScalar(memberPath(from, relation, suffix), relation);
            }
            List<AttributeMapping.Relation> chain = relationChain(suffix);
            if (chain != null) {
                return new ResolvedRelation(this, chain);
            }
            // Not a chain off the element, so a member scalar: the mapping registered under
            // the whole suffix if there is one, else the suffix read as a raw JPA path.
            AttributeMapping nested = relation.fields().get(suffix);
            return new ResolvedScalar(memberPath(from, relation, suffix),
                    nested != null ? nested : AttributeMapping.field(suffix));
        }

        /**
         * Walk {@code suffix}'s dotted parts through the element's nested {@code fields()}
         * maps, or {@code null} as soon as a hop is scalar or unmapped: {@code c.subCategories}
         * is a relation chain hanging off the element, {@code c.name} is not.
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

    /**
     * Walks a dotted JPA path, joining any to-ONE association it passes through with a LEFT join.
     *
     * <p>{@code Path.get()} on an association is an INNER join, which removes the row from the
     * WHOLE query when the association is absent. That is indistinguishable from the correct
     * answer for a predicate that stands alone — both exclude the row — but it is wrong under a
     * disjunction, where the missing hop must only make ITS OWN branch unknown: a row with no
     * parent that satisfies the other branch is one the PDP allows, and an inner join drops it
     * (cerbos/query-plan-adapters#375).
     *
     * <p>A LEFT join makes the absent hop SQL NULL instead, which is CEL's missing-path error and
     * is branch-local: NULL propagates through the comparison, {@code NOT NULL} is still NULL so
     * the row stays excluded under both polarities, and {@code NULL OR TRUE} is TRUE. The
     * associations reached this way are to-ONE, so the join cannot multiply rows.
     *
     * <p>Embeddable members are left on {@code get()}: they are not associations, they are
     * columns on the same row, and joining them is not portable.
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
     * Whether {@code part} names an association on {@code from}'s type, asked of the metamodel
     * rather than by resolving a path — resolving one would register the very implicit inner join
     * this check exists to avoid.
     */
    private static boolean isAssociation(From<?, ?> from, String part) {
        ManagedType<?> managed = managedTypeOf(from);
        if (managed == null) {
            return false;
        }
        try {
            return managed.getAttribute(part).isAssociation();
        } catch (IllegalArgumentException notAnAttribute) {
            // An unknown name here is not this method's error to raise: get() below produces the
            // provider's own diagnostic, which names the entity and the property.
            return false;
        }
    }

    /**
     * The managed type {@code from} exposes attributes of.
     *
     * <p>A {@code Root} models its own entity type, but a {@code Join} models the ATTRIBUTE it was
     * created from, so the target type has to be read back off that attribute. Missing this is why
     * only the first hop of a multi-level path would be joined, leaving the second an inner join
     * off the first — and one inner join anywhere on the path is enough to drop the row.
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
            throw new IllegalArgumentException(
                    "Variable '" + variable + "' does not start with lambda variable '" + lambdaVar + "'");
        }
        return variable.substring(prefix.length());
    }
}
