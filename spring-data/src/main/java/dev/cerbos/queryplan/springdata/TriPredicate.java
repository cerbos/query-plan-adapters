/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Predicate;

import java.util.function.Supplier;

/**
 * Three-valued predicate helpers. A CEL error makes Cerbos deny, so the SQL counterpart must
 * be UNKNOWN, not FALSE: {@code NOT(UNKNOWN)} stays UNKNOWN, while {@code NOT(FALSE)} would
 * return rows the PDP denies.
 *
 * <p>Two Hibernate 6 rules are enforced here:
 * <ul>
 *   <li>All negation goes through {@link #not}. Hibernate 6 negation is stateful, so
 *       {@code cb.not(cb.not(p))} still renders one {@code NOT}; wrapping the operand in a
 *       one-element conjunction gives each negation a fresh node. Do not call {@code cb.not}
 *       anywhere else.</li>
 *   <li>A predicate node must not be used in both polarities. Methods that need an input
 *       more than once take a {@link Supplier Supplier&lt;Predicate&gt;} and build it fresh
 *       each time.</li>
 * </ul>
 */
final class TriPredicate {

    private final CriteriaBuilder cb;

    TriPredicate(CriteriaBuilder cb) {
        this.cb = cb;
    }

    /**
     * A constant SQL UNKNOWN ({@code 1 = NULL}). It absorbs in AND/OR the way a CEL error does,
     * and its negation is also UNKNOWN.
     */
    Predicate unknown() {
        return cb.equal(cb.literal(1), cb.nullLiteral(Integer.class));
    }

    /**
     * {@code cb.not(cb.and(p))}; see the class Javadoc. Callers must not also use {@code p}
     * un-negated.
     */
    Predicate not(Predicate p) {
        return cb.not(cb.and(p));
    }

    /** {@code body OR NOT body}: TRUE when {@code body} is TRUE or FALSE, UNKNOWN otherwise. */
    Predicate determined(Supplier<Predicate> body) {
        return cb.or(body.get(), not(body.get()));
    }

    /**
     * The CEL ternary {@code if(c, a, b)}:
     *
     * <pre>{@code (c AND then) OR (NOT c AND else) OR NOT(c OR NOT c)}</pre>
     *
     * The third arm is UNKNOWN when {@code c} is UNKNOWN, so the row stays excluded under
     * negation, as the CEL error would deny it. Without it the predicate could be FALSE and
     * {@code NOT} would flip it to TRUE.
     */
    Predicate ternary(Supplier<Predicate> condition,
                      Supplier<Predicate> thenBranch,
                      Supplier<Predicate> elseBranch) {
        return cb.or(
                cb.and(condition.get(), thenBranch.get()),
                cb.and(not(condition.get()), elseBranch.get()),
                unknownWhenUnknown(condition));
    }

    /** {@code NOT(c OR NOT c)}: UNKNOWN when {@code condition} is UNKNOWN, FALSE otherwise. */
    private Predicate unknownWhenUnknown(Supplier<Predicate> condition) {
        return not(determined(condition));
    }

    /**
     * {@code (base AND NOT unknownWitness) OR (unknownWitness AND UNKNOWN)}: {@code base} when
     * the witness is FALSE, UNKNOWN when it is TRUE, even if {@code base} is TRUE.
     * {@code unknownWitness} must be two-valued.
     */
    Predicate baseUnlessUnknown(Predicate base, Supplier<Predicate> unknownWitness) {
        return cb.or(
                cb.and(base, not(unknownWitness.get())),
                cb.and(unknownWitness.get(), unknown()));
    }
}
