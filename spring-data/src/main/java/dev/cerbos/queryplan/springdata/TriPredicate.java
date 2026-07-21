package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Predicate;

import java.util.function.Supplier;

/**
 * The tri-state (three-valued) predicate algebra behind the adapter's error→deny contract.
 *
 * <p>Semantic contract: a CEL evaluation error (e.g. touching a missing attribute) makes Cerbos
 * DENY the check. In SQL, a predicate whose CEL counterpart would error must therefore evaluate
 * UNKNOWN — never FALSE — so that {@code NOT(...)} cannot flip a NULL-derived row back to
 * included ({@code NOT(UNKNOWN) = UNKNOWN}, but {@code NOT(FALSE) = TRUE} would leak rows the
 * PDP denies). Every composition in this class preserves that property.
 *
 * <p>Two structural invariants are owned here rather than by comment discipline at call sites:
 * <ul>
 *   <li><b>Junction barrier:</b> ALL logical negation goes through {@link #not}, which wraps the
 *       operand in a single-element conjunction before negating. Hibernate 6's SQM negation is
 *       stateful for comparison predicates: {@code cb.not(cb.not(p))} stays negated instead of
 *       toggling back (verified against Hibernate 6.6.18 — a double-negated {@code eq} still
 *       renders a single {@code NOT}). The barrier gives each {@code not} a fresh node to negate,
 *       so nested negations compose correctly. {@code cb.not} must not be called anywhere else
 *       in the adapter.</li>
 *   <li><b>Fresh predicate per polarity:</b> a Hibernate {@code Predicate} node must NEVER be
 *       shared between a positive and a negated occurrence (stateful negation again). Every
 *       method here that consumes an input in more than one polarity takes a
 *       {@link Supplier Supplier&lt;Predicate&gt;} — not a pre-built {@code Predicate} — and
 *       invokes it once per occurrence, so fresh-per-occurrence is enforced by the signature.
 *       Methods that consume an input exactly once accept a plain {@code Predicate}.</li>
 * </ul>
 *
 * <p>Design note — rejected alternative: a combinator/wrapper style
 * ({@code TriPredicate.of(cb, supplier).and(...).orUnknownWhen(...).toPredicate()}) was sketched
 * first. It was rejected because it needs a larger interface (a wrapper type plus generic
 * of/and/or/not/orUnknownWhen/terminal combinators) while still leaving the macro truth tables —
 * the actual invariant-bearing knowledge — assembled at the adapter call sites, which is exactly
 * the comment-discipline failure mode this module exists to remove. The chosen shape names each
 * truth table once, here, and callers cannot re-derive a wrong one.
 */
final class TriPredicate {

    private final CriteriaBuilder cb;

    TriPredicate(CriteriaBuilder cb) {
        this.cb = cb;
    }

    /**
     * A constant SQL UNKNOWN: {@code 1 = NULL}. Composes by three-valued logic exactly like CEL
     * error absorption: {@code x AND UNKNOWN} is FALSE when x is FALSE (a false witness absorbs
     * the error) and UNKNOWN when x is TRUE; {@code x OR UNKNOWN} is TRUE when x is TRUE and
     * UNKNOWN when x is FALSE. Its negation is UNKNOWN as well, so predicates carrying it stay
     * excluded under both polarities.
     */
    Predicate unknown() {
        return cb.equal(cb.literal(1), cb.nullLiteral(Integer.class));
    }

    /**
     * Junction-barriered logical negation: {@code cb.not(cb.and(p))}. The single-element
     * conjunction is the barrier that makes nested negations compose under Hibernate 6's
     * stateful SQM negation (see the class Javadoc). {@code p} is consumed in exactly one
     * (negated) polarity, so a pre-built node is safe here — callers must not reuse it
     * positively elsewhere.
     */
    Predicate not(Predicate p) {
        return cb.not(cb.and(p));
    }

    /**
     * {@code body OR NOT body} — TRUE iff {@code body} is determined (two-valued), UNKNOWN iff
     * {@code body} is UNKNOWN. This is the determinedness test the unknown-element COUNT probes
     * filter on. {@code body} is built twice (once per polarity).
     */
    Predicate determined(Supplier<Predicate> body) {
        return cb.or(body.get(), not(body.get()));
    }

    /**
     * The CEL ternary {@code if(c, a, b)} as a pure predicate:
     *
     * <pre>{@code (c AND then) OR (NOT c AND else) OR NOT(c OR NOT c)}</pre>
     *
     * The third arm is FALSE when {@code c} is known (no effect on the OR) and UNKNOWN when
     * {@code c} is UNKNOWN, driving the whole predicate to UNKNOWN so the row is excluded under
     * BOTH polarities — matching the CEL evaluation error (deny) on a null/missing condition.
     * The two branch arms alone are not enough: with both branch predicates false the predicate
     * would collapse to FALSE, which {@code NOT} flips to TRUE and leaks rows the PDP denies.
     * The condition is built fresh four times (two polarities, plus both polarities again in the
     * third arm); each branch is built exactly once.
     */
    Predicate ternary(Supplier<Predicate> condition,
                      Supplier<Predicate> thenBranch,
                      Supplier<Predicate> elseBranch) {
        return cb.or(
                cb.and(condition.get(), thenBranch.get()),
                cb.and(not(condition.get()), elseBranch.get()),
                unknownWhenUnknown(condition));
    }

    /**
     * UNKNOWN exactly when {@code condition} is UNKNOWN, FALSE when it is known:
     * {@code NOT(c OR NOT c)}. Truth table: condition TRUE → {@code NOT(TRUE OR FALSE)} =
     * FALSE; condition FALSE → {@code NOT(FALSE OR TRUE)} = FALSE; condition UNKNOWN →
     * {@code NOT(UNKNOWN OR UNKNOWN)} = UNKNOWN.
     */
    private Predicate unknownWhenUnknown(Supplier<Predicate> condition) {
        return not(determined(condition));
    }

    /**
     * OR with error absorption — the {@code exists}/{@code filter}/{@code except} table:
     * {@code trueWitness OR (unknownWitness AND UNKNOWN)}.
     * <ul>
     *   <li>witness TRUE → TRUE (a true witness absorbs any unknown sibling)</li>
     *   <li>witness FALSE, unknown witness TRUE → UNKNOWN (deny)</li>
     *   <li>witness FALSE, unknown witness FALSE → FALSE</li>
     * </ul>
     * Both inputs are consumed once, positively; {@code unknownWitness} must be a two-valued
     * detector (e.g. an EXISTS or a COUNT comparison).
     */
    Predicate anyTrueOrUnknown(Predicate trueWitness, Predicate unknownWitness) {
        return cb.or(trueWitness, cb.and(unknownWitness, unknown()));
    }

    /**
     * AND with error absorption — the {@code all} table:
     * {@code NOT falseWitness AND (NOT unknownWitness OR UNKNOWN)}.
     * <ul>
     *   <li>false witness TRUE → FALSE (a false witness absorbs any unknown sibling)</li>
     *   <li>false witness FALSE, unknown witness TRUE → UNKNOWN (deny)</li>
     *   <li>false witness FALSE, unknown witness FALSE → TRUE</li>
     * </ul>
     * Both inputs are consumed once, each in a single (negated) polarity; both must be
     * two-valued detectors.
     */
    Predicate allTrueOrUnknown(Predicate falseWitness, Predicate unknownWitness) {
        return cb.and(not(falseWitness), cb.or(not(unknownWitness), unknown()));
    }

    /**
     * Strict guard — the {@code exists_one} / {@code size(filter(...))} /
     * map-intersection table: {@code (base AND NOT unknownWitness) OR (unknownWitness AND
     * UNKNOWN)}. No absorption: ANY unknown witness poisons the result.
     * <ul>
     *   <li>unknown witness TRUE → UNKNOWN (deny), even when {@code base} is TRUE</li>
     *   <li>unknown witness FALSE → {@code base}</li>
     * </ul>
     * {@code unknownWitness} is consumed in both polarities and therefore built fresh twice; it
     * must be a two-valued detector. {@code base} is consumed once, positively.
     */
    Predicate baseUnlessUnknown(Predicate base, Supplier<Predicate> unknownWitness) {
        return cb.or(
                cb.and(base, not(unknownWitness.get())),
                cb.and(unknownWitness.get(), unknown()));
    }
}
