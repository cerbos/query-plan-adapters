package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.From;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;

import java.util.stream.Collectors;

/**
 * The correlated-subquery shapes every collection operator composes over.
 *
 * <p>Owns the {@link ChainSubquery} skeleton ({@link #chainSubquery}) and the shapes built on
 * it: the two-valued {@code EXISTS}, the {@code COUNT} seed, the tri-state macro score
 * ({@code exists}/{@code all}/{@code filter}), the strict match counter
 * ({@code exists_one}/{@code size(filter(...))}) with its undetermined-poison term, and the
 * leading-hop guards that keep an absent to-one parent UNKNOWN under both polarities. It is
 * also the one place the SELECT-only guard fires: a Specification evaluated outside its own
 * SELECT cannot correlate, so every relation subquery throws from here before anything is
 * built (see {@link SpringDataQueryPlanAdapter the class documentation}).
 *
 * <p>Nothing here reads a plan. A caller resolves its variable to a
 * {@link Scope.ResolvedRelation}, translates the lambda body into a
 * {@link SubqueryBodyBuilder} and hands both in; which SQL shape answers which CEL operator
 * is the caller's decision, and this class only guarantees that every shape ranges over the
 * same flattened element set.
 */
final class ChainSubqueries {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final boolean selectInvocation;

    ChainSubqueries(CriteriaBuilder cb, TriPredicate tri, boolean selectInvocation) {
        this.cb = cb;
        this.tri = tri;
        this.selectInvocation = selectInvocation;
    }

    /**
     * The single-subquery scoring translation shared by {@code exists}/{@code filter}
     * (score 2/0) and {@code all} (score 0/2). Each element of the relation
     * chain is scored with a searched CASE:
     *
     * <pre>{@code CASE WHEN body THEN trueScore WHEN NOT body THEN falseScore ELSE 1 END}</pre>
     *
     * An UNKNOWN body matches neither WHEN (SQL treats an UNKNOWN condition as not taken),
     * so undetermined elements land in the ELSE — that is what makes the polarity pair
     * sufficient to distinguish all three states with a single scan. The subquery selects
     *
     * <pre>{@code NULLIF(COALESCE(MAX(score), 0), 1)}</pre>
     *
     * i.e. the dominant score with the empty collection folded to 0 and the
     * "only undetermined elements dominate" state (max score 1) mapped to SQL NULL. A
     * two-valued equality against 2 (exists) or 0 (all) then yields TRUE / FALSE /
     * UNKNOWN exactly per the CEL macro truth tables, and {@code NOT} keeps UNKNOWN rows
     * excluded ({@code NOT(UNKNOWN) = UNKNOWN}).
     *
     * <p>The body is translated exactly twice — once per polarity, the minimum Hibernate 6's
     * stateful negation permits (see {@link TriPredicate}) — so nested macros grow at
     * {@code 2^depth}, not the {@code 3^depth} of the previous
     * EXISTS-plus-two-COUNT-probes translation.
     */
    Subquery<Integer> macroScoreSubquery(Scope scope, Scope.ResolvedRelation ref,
                                         SubqueryBodyBuilder bodyBuilder,
                                         int trueScore, int falseScore) {
        ChainSubquery<Integer> cs = chainSubquery(Integer.class, scope, ref);
        Expression<Integer> score = cb.<Integer>selectCase()
                .when(cs.body(bodyBuilder), trueScore)
                .when(tri.not(cs.body(bodyBuilder)), falseScore)
                .otherwise(1);
        cs.sub().select(cb.nullif(cb.coalesce(cb.max(score), 0), 1));
        return cs.sub();
    }

    /**
     * The strict counting subquery behind {@code exists_one} and {@code size(filter(...))}:
     * selects the number of elements whose body is determined-true, poisoned to SQL NULL
     * when ANY element body is UNKNOWN (CEL's strict macros error if any element errors —
     * no absorption). Shape:
     *
     * <pre>{@code COALESCE(SUM(CASE WHEN body THEN 1 ELSE 0 END), 0) + poisonTerm}</pre>
     *
     * where {@code poisonTerm} ({@link #undeterminedPoisonTerm}) is 0 when every element is
     * determined and NULL otherwise — NULL is absorbing under addition, so any undetermined
     * element nulls the whole count and every comparison against it goes UNKNOWN (row
     * excluded under both polarities). The empty collection yields 0 + 0 = 0, matching CEL
     * ({@code exists_one} over an empty list is false, a zero count compares normally).
     *
     * <p>Costs three body translations (one positive in the match counter, one per polarity
     * in the poison term); see {@link #macroScoreSubquery} for why two is the floor.
     */
    Subquery<Long> strictMatchCountSubquery(Scope scope, Scope.ResolvedRelation ref,
                                            SubqueryBodyBuilder bodyBuilder) {
        ChainSubquery<Long> cs = chainSubquery(Long.class, scope, ref);
        Expression<Long> match = cb.<Long>selectCase()
                .when(cs.body(bodyBuilder), 1L)
                .otherwise(0L);
        cs.sub().select(cb.sum(
                cb.coalesce(cb.sum(match), 0L),
                undeterminedPoisonTerm(cs, bodyBuilder)));
        return cs.sub();
    }

    /**
     * A subquery selecting ONLY the poison term: 0 when every element body is determined
     * (or the collection is empty), SQL NULL when any element body is UNKNOWN. Used by the
     * statically-collapsed {@code size(filter(...))} comparisons, whose count comparison is
     * pre-decided but whose error semantics still depend on the lambda body.
     */
    Subquery<Long> undeterminedPoisonSubquery(Scope scope, Scope.ResolvedRelation ref,
                                              SubqueryBodyBuilder bodyBuilder) {
        ChainSubquery<Long> cs = chainSubquery(Long.class, scope, ref);
        cs.sub().select(undeterminedPoisonTerm(cs, bodyBuilder));
        return cs.sub();
    }

    /**
     * {@code NULLIF(COALESCE(MAX(CASE WHEN body THEN 0 WHEN NOT body THEN 0 ELSE 1 END), 0), 1)}
     * — 0 when every element body is determined (either WHEN taken; also the empty
     * collection via COALESCE), SQL NULL when at least one element body is UNKNOWN (both
     * WHENs skipped → ELSE 1 dominates the MAX → NULLIF). The body is translated once per
     * polarity (stateful negation — see {@link TriPredicate#not}).
     */
    private Expression<Long> undeterminedPoisonTerm(
            ChainSubquery<?> cs, SubqueryBodyBuilder bodyBuilder) {
        Expression<Long> determined = cb.<Long>selectCase()
                .when(cs.body(bodyBuilder), 0L)
                .when(tri.not(cs.body(bodyBuilder)), 0L)
                .otherwise(1L);
        return cb.nullif(cb.coalesce(cb.max(determined), 0L), 1L);
    }

    /** Correlate {@code outerFrom} (the relation owner's {@code From}) into {@code sub}. */
    @SuppressWarnings("unchecked")
    private static From<?, ?> correlate(Subquery<?> sub, From<?, ?> outerFrom) {
        if (outerFrom instanceof Root<?> r) {
            return sub.correlate(r);
        }
        if (outerFrom instanceof Join<?, ?> j) {
            return sub.correlate((Join<Object, Object>) j);
        }
        // Every Scope is rooted at a Root or a Join; no plan input reaches this.
        throw Refusals.internal("Cannot correlate from non-Root, non-Join scope: " + outerFrom);
    }

    /**
     * Build the shared skeleton of every relation subquery. Two invariants fix the two
     * join-anchoring failure modes:
     * <ul>
     *   <li>the correlation anchor is {@code ref.owner().from()} — the {@code From} that
     *       OWNS the first relation attribute — never the evaluation scope's own
     *       {@code from()}, which inside a lambda is the lambda element join and does not
     *       hold outer relations like {@code request.resource.attr.tags};</li>
     *   <li>a multi-hop chain ({@code categories.subCategories}) joins THROUGH every hop
     *       off that anchor, so the subquery ranges over the flattened tail elements —
     *       joining only the tail attribute off the anchor would either fail at query-build
     *       time or silently query a same-named collection on the wrong entity.</li>
     * </ul>
     * EXISTS over the join chain, aggregate scoring over {@code tailJoin}
     * ({@link #macroScoreSubquery}/{@link #strictMatchCountSubquery}) and COUNT over
     * {@code tailJoin} therefore express exists/in/hasIntersection membership and
     * {@code size()} of the flattened union with the same element set, so the tri-state
     * unknown-element machinery composes with chains unchanged.
     */
    <T> ChainSubquery<T> chainSubquery(Class<T> resultType, Scope scope,
                                       Scope.ResolvedRelation ref) {
        if (!selectInvocation) {
            String chain = ref.chain().stream()
                    .map(AttributeMapping.Relation::joinAttribute)
                    .collect(Collectors.joining("."));
            throw new UnsupportedOperationException(
                    "Relation '" + chain + "' requires a correlated subquery, but this Specification "
                    + "is being evaluated outside its own SELECT query — e.g. via "
                    + "repository.delete(Specification) or a criteria bulk delete/update. "
                    + "Hibernate's multi-table bulk delete first clears @ElementCollection/join "
                    + "tables using this same predicate, which self-invalidates the correlated "
                    + "subquery: 0 entity rows are deleted while their collection rows are "
                    + "silently destroyed. The Cerbos Specification is SELECT-only; fetch the "
                    + "matching ids with findAll(spec) and delete them with deleteAllById(ids).");
        }
        Subquery<T> sub = scope.parentQuery().subquery(resultType);
        From<?, ?> correlated = correlate(sub, ref.owner().from());
        Join<?, ?> join = correlated.join(ref.chain().get(0).joinAttribute());
        for (int i = 1; i < ref.chain().size(); i++) {
            join = join.join(ref.chain().get(i).joinAttribute());
        }
        Scope rebased = Scope.rebaseAt(scope, ref.owner(), correlated, sub);
        return new ChainSubquery<>(sub, join, rebased);
    }

    /** A chain subquery seeded to {@code SELECT COUNT(tailJoin)} — the shared seed of every counting shape. */
    ChainSubquery<Long> countSubquery(Scope scope, Scope.ResolvedRelation ref) {
        ChainSubquery<Long> cs = chainSubquery(Long.class, scope, ref);
        cs.sub().select(cb.count(cs.tailJoin()));
        return cs;
    }

    /**
     * "Every intermediate hop of a dotted path exists", or {@code null} for a direct relation.
     *
     * <p>CEL cannot dot through a list, so each intermediate segment of {@code a.b.c} is a
     * to-ONE parent: absent, the caller sends no attribute at all and CEL raises a
     * missing-path error, which denies. A subquery rooted at the entity cannot see that — an
     * absent parent and a childless parent both return nothing — so {@code all} reads TRUE,
     * {@code !exists} reads TRUE and the count reads 0, each admitting rows the PDP denies
     * (cerbos/query-plan-adapters#309).
     */
    Predicate leadingHopsExist(Scope scope, Scope.ResolvedRelation ref) {
        if (!ref.isChained()) {
            return null;
        }
        Scope.ResolvedRelation hops = new Scope.ResolvedRelation(
                ref.owner(), ref.chain().subList(0, ref.chain().size() - 1));
        return existsSubquery(scope, hops, (sub, tailJoin, rebased) -> cb.conjunction());
    }

    /**
     * Make {@code value} SQL NULL unless every intermediate to-one hop exists, so an absent
     * parent leaves the enclosing comparison UNKNOWN and the row excluded under BOTH
     * polarities. A CASE with no ELSE yields NULL for the missing case.
     */
    <N> Expression<N> requireLeadingHops(
            Scope scope, Scope.ResolvedRelation ref,
            Expression<N> value, Class<N> type) {
        Predicate guard = leadingHopsExist(scope, ref);
        if (guard == null) {
            return value;
        }
        return cb.<N>selectCase().when(guard, value).otherwise(cb.nullLiteral(type));
    }

    Predicate existsSubquery(Scope scope, Scope.ResolvedRelation ref,
                             SubqueryBodyBuilder bodyBuilder) {
        ChainSubquery<Integer> cs = chainSubquery(Integer.class, scope, ref);
        cs.sub().select(cb.literal(1));
        cs.sub().where(cs.body(bodyBuilder));
        return cb.exists(cs.sub());
    }

    /**
     * "Some element of the chain satisfies the body", as a THREE-valued predicate: UNKNOWN
     * rather than FALSE when an intermediate to-one hop is absent.
     *
     * <p>Every operator whose whole answer is an existence test over a chain must build it
     * here rather than calling {@link #existsSubquery} directly. {@code EXISTS} is
     * two-valued, so {@code NOT EXISTS} over an absent to-one parent is TRUE and readmits
     * every parentless row — which is how {@code !("x" in R.attr.parent.names)} and its
     * {@code hasIntersection} sibling kept over-granting after the collection macros were
     * fixed (cerbos/query-plan-adapters#315). Counting instead of testing existence lets the
     * guard live on the count EXPRESSION, so both polarities inherit it.
     *
     * <p>A direct relation keeps the plain {@code EXISTS}: it has no hop to require, and its
     * empty-collection semantics are already correct under both polarities.
     */
    Predicate chainContains(Scope scope, Scope.ResolvedRelation ref,
                            SubqueryBodyBuilder bodyBuilder) {
        if (!ref.isChained()) {
            return existsSubquery(scope, ref, bodyBuilder);
        }
        ChainSubquery<Long> cs = countSubquery(scope, ref);
        cs.sub().where(cs.body(bodyBuilder));
        return cb.greaterThan(
                requireLeadingHops(scope, ref, cs.sub(), Long.class), 0L);
    }
}
