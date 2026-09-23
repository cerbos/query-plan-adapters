/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.util.List;

/**
 * {@code size(x) op N}: string length over a scalar column, element count over a relation
 * chain, and the strict {@code size(filter(...))} count.
 *
 * <p>Owns the threshold arithmetic — a fractional or out-of-int-range constant against an
 * integral COUNT/LENGTH is decided statically, never truncated — and the choice between the
 * two-valued EXISTS shortcuts and the tri-state COUNT: a direct relation may take
 * {@code EXISTS}/{@code NOT EXISTS}, a chain never may, because an absent to-one parent has to
 * leave the comparison UNKNOWN under both polarities. It is a step of
 * {@link ComparisonTranslator#translate}, not a resolved-operand case, because its SQL shapes
 * are subquery translations pinned by the differential oracle rather than a (field, value)
 * pair.
 */
final class SizeTranslator {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final PlanWalker walker;
    private final ChainSubqueries subqueries;

    SizeTranslator(CriteriaBuilder cb, TriPredicate tri, PlanWalker walker,
                   ChainSubqueries subqueries) {
        this.cb = cb;
        this.tri = tri;
        this.walker = walker;
        this.subqueries = subqueries;
    }

    /**
     * The comparison against an integral size, with its constant already resolved.
     *
     * <p>A fractional constant f can never be hit exactly by a COUNT or LENGTH. Truncating it
     * ({@code >= 1.5} becoming {@code >= 1}) over-included rows the PDP denies, so it is
     * resolved into integer-count semantics:
     * <ul>
     *   <li>{@code eq f} → always-false, {@code ne f} → always-true: {@code decided} holds the
     *       answer, and each size kind still has to keep the rows CEL denies excluded;</li>
     *   <li>{@code ge f}/{@code gt f} → {@code ge ceil(f)} (the count being integral makes
     *       {@code gt} and {@code ge} coincide);</li>
     *   <li>{@code le f}/{@code lt f} → {@code le floor(f)}.</li>
     * </ul>
     * An integral constant keeps its operator, and {@code decided} is {@code null}.
     */
    private record Threshold(String op, long value, Boolean decided) {

        static Threshold of(String op, double raw) {
            if (raw == Math.rint(raw)) {
                return new Threshold(op, (long) raw, null);
            }
            return switch (op) {
                case "eq" -> new Threshold(op, (long) Math.floor(raw), Boolean.FALSE);
                case "ne" -> new Threshold(op, (long) Math.floor(raw), Boolean.TRUE);
                case "gt", "ge" -> new Threshold("ge", (long) Math.ceil(raw), null);
                case "lt", "le" -> new Threshold("le", (long) Math.floor(raw), null);
                // size() yields an int; anything but a comparison over it is a CEL type error
                // the planner would not have shipped.
                default -> throw Refusals.malformed("Unsupported size comparison operator: " + op);
            };
        }
    }

    /**
     * What {@code size()} is taken of: a variable, and — for {@code size(filter(...))} — the
     * lambda that selects which of its elements count.
     */
    private record SizeArgument(String variable, ParsedLambda filter) {

        static SizeArgument parse(PlanResourcesFilter.Expression sizeExpr) {
            List<Operand> sizeOps = sizeExpr.getOperandsList();
            if (sizeOps.size() != 1) {
                throw Refusals.malformed(
                        "Unsupported size() expression: size() takes exactly 1 argument, got "
                                + sizeOps.size());
            }
            Operand arg = sizeOps.get(0);
            if (arg.getNodeCase() == Operand.NodeCase.VARIABLE) {
                return new SizeArgument(arg.getVariable(), null);
            }
            String argOperator = arg.getNodeCase() == Operand.NodeCase.EXPRESSION
                    ? arg.getExpression().getOperator() : null;
            if ("filter".equals(argOperator)) {
                // size(coll.filter(x, pred)) — count only the elements matching the lambda.
                List<Operand> filterOps = arg.getExpression().getOperandsList();
                if (filterOps.size() != 2) {
                    throw Refusals.malformed("Unsupported size(filter(...)) expression");
                }
                if (filterOps.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
                    // filter() over a computed collection (a map() projection, a nested
                    // filter): legal CEL with no join chain to count over.
                    throw Refusals.unsupported("Unsupported size(filter(...)) expression");
                }
                return new SizeArgument(filterOps.get(0).getVariable(),
                        ParsedLambda.parse(filterOps.get(1),
                                "Unsupported size(filter(...)) expression",
                                "lambda requires exactly 2 operands",
                                "lambda requires exactly 2 operands"));
            }
            if ("except".equals(argOperator)) {
                // size(coll.except([...])) — the PDP-verified wire shape of every real
                // except() policy. List difference has no JPA translation; the shared
                // named error points at the equivalent exists(...) rewrite.
                throw Refusals.exceptUnsupported();
            }
            // size() of a computed collection (a map() projection, a literal list).
            throw Refusals.unsupported(
                    "Unsupported size() expression: size() argument must be a collection "
                            + "attribute or filter(...), got " + Refusals.describeOperand(arg));
        }
    }

    /**
     * Translate {@code op(size(...), N)}, or return {@code null} when the comparison has no
     * {@code size()} operand or no numeric constant. Operands must already be normalized
     * field-first (see {@link NormalizedBinary}).
     */
    Predicate trySizeComparison(String op, List<Operand> operands, Scope scope) {
        // Detect the size() operand first: every ordinary leaf comparison probes through
        // here, and converting the VALUE operand up front would materialize lists/structs
        // only to discard them when no size() expression is present.
        PlanResourcesFilter.Expression sizeExpr = null;
        Double constant = null;
        for (Operand o : operands) {
            if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "size".equals(o.getExpression().getOperator())) {
                sizeExpr = o.getExpression();
            } else if (o.getNodeCase() == Operand.NodeCase.VALUE
                    && o.getValue().getKindCase() == Value.KindCase.NUMBER_VALUE) {
                constant = o.getValue().getNumberValue();
            }
        }
        if (sizeExpr == null || constant == null) {
            return null;
        }

        Threshold threshold = Threshold.of(op, constant);
        SizeArgument arg = SizeArgument.parse(sizeExpr);
        Scope.Resolution resolved = scope.resolve(arg.variable());
        if (!(resolved instanceof Scope.ResolvedRelation ref)) {
            return stringLength(arg, (Scope.ResolvedScalar) resolved, threshold, scope);
        }
        if (arg.filter() == null) {
            return elementCount(ref, threshold, scope);
        }
        return matchingElementCount(ref, arg.filter(), threshold, scope);
    }

    /** {@code size(string)} — CEL string length, {@code LENGTH(column) <op> N}. */
    private Predicate stringLength(SizeArgument arg, Scope.ResolvedScalar scalar,
                                   Threshold threshold, Scope scope) {
        // Only a genuine scalar ATTRIBUTE has a string length to take. The bare lambda element
        // lands in the scalar arm too, but its mapping is the Relation it came from — size()
        // of a relation element is not a length.
        if (!(scalar.mapping() instanceof AttributeMapping.Field)) {
            throw Refusals.unmapped(
                    "size() requires a collection (Relation) mapping for " + arg.variable());
        }
        if (arg.filter() != null) {
            throw Refusals.unmapped("size(filter(...)) requires a collection (Relation) mapping for "
                    + arg.variable());
        }
        Path<?> path = scope.path(arg.variable());
        if (!String.class.equals(path.getJavaType())) {
            return tri.unknown();
        }
        // Every "vacuously true" arm below still requires IS NOT NULL, never an unconditional
        // 1=1: a NULL column is a missing attribute → CEL error → deny.
        if (threshold.decided() != null) {
            return threshold.decided() ? cb.isNotNull(path) : cb.disjunction();
        }
        // cb.length(...) is Expression<Integer>, so the threshold must fit in an int. An
        // unguarded narrowing cast wraps thresholds outside int range (2147483648 →
        // −2147483648, 4294967296 → 0), silently flipping the filter — `size(s) > 4294967296`
        // became `LENGTH(s) > 0` (always-true over-inclusion while check() denies every row).
        // No string's length leaves int range, so these comparisons fold statically instead.
        if (threshold.value() > Integer.MAX_VALUE) {
            // LENGTH(s) < 2^31 for every present string: eq/gt/ge can never hold; lt/le/ne
            // always hold for a present string.
            return switch (threshold.op()) {
                case "eq", "gt", "ge" -> cb.disjunction();
                case "lt", "le", "ne" -> cb.isNotNull(path);
                default -> throw Refusals.malformed(
                        "Unsupported size comparison operator: " + threshold.op());
            };
        }
        if (threshold.value() < Integer.MIN_VALUE) {
            // LENGTH(s) >= 0 > any threshold below int range: gt/ge/ne always hold for a
            // present string; eq/lt/le can never hold.
            return switch (threshold.op()) {
                case "gt", "ge", "ne" -> cb.isNotNull(path);
                case "eq", "lt", "le" -> cb.disjunction();
                default -> throw Refusals.malformed(
                        "Unsupported size comparison operator: " + threshold.op());
            };
        }
        return compareCount(cb.length(path.as(String.class)), threshold.op(),
                (int) threshold.value());
    }

    /**
     * {@code size(collection)} — counts rows without evaluating a lambda, so no element can be
     * UNKNOWN and the plain EXISTS/COUNT comparisons are already exact.
     */
    private Predicate elementCount(Scope.ResolvedRelation ref, Threshold threshold, Scope scope) {
        if (threshold.decided() != null) {
            // A COUNT is never fractional, so the comparison is statically decided. It is not
            // unconditional though: an absent to-one parent is a CEL missing-path error (deny),
            // and folding to TRUE would return every parentless row (#309).
            //
            // The guard has to be TRI-STATE, like every other chained comparison: `hops AND
            // constant` is two-valued, so `NOT(hops AND constant)` is TRUE for a parentless row
            // under BOTH collapses and readmits all of them (cerbos/query-plan-adapters#333). A
            // CASE with no ELSE yields SQL NULL instead, leaving the comparison UNKNOWN under
            // both polarities.
            if (!ref.isChained()) {
                return threshold.decided() ? cb.conjunction() : cb.disjunction();
            }
            return cb.equal(
                    subqueries.requireLeadingHops(scope, ref, cb.literal(1L), Long.class),
                    threshold.decided() ? 1L : 0L);
        }
        String op = threshold.op();
        long n = threshold.value();
        // The EXISTS emptiness shortcuts below are TWO-valued, so a chain must not take them:
        // `NOT EXISTS` is TRUE for an absent to-one parent, which is why `!(size(chain) > 0)`
        // readmitted every parentless row even though `size(chain) == 0` — guarded by a
        // separate AND — did not (cerbos/query-plan-adapters#316). Guarding the COUNT
        // EXPRESSION instead of each comparison shortcut is what makes `== 0`, `> 0`, `>= N` and
        // all their negations inherit the guard: the count is SQL NULL without the hop, so
        // every comparison built on it is UNKNOWN under BOTH polarities.
        if (!ref.isChained()) {
            boolean nonEmpty = ("gt".equals(op) && n == 0L) || ("ge".equals(op) && n == 1L);
            boolean empty = ("eq".equals(op) && n == 0L)
                    || ("le".equals(op) && n == 0L)
                    || ("lt".equals(op) && n == 1L);
            if (nonEmpty) {
                return anyElement(scope, ref);
            }
            if (empty) {
                return tri.not(anyElement(scope, ref));
            }
        }
        // Arbitrary N (and every threshold over a chain) → correlated (SELECT COUNT(...)) <op>
        // N. For a multi-hop chain the COUNT joins through every hop, so it counts the
        // FLATTENED tail elements — the same element set the EXISTS shortcuts range over.
        return compareCount(
                subqueries.requireLeadingHops(scope, ref,
                        subqueries.countSubquery(scope, ref).sub(), Long.class),
                op, n);
    }

    private Predicate anyElement(Scope scope, Scope.ResolvedRelation ref) {
        return subqueries.existsSubquery(scope, ref, (sub, tailJoin, rebased) -> cb.conjunction());
    }

    /**
     * {@code size(coll.filter(x, pred))}. CEL filter has NO error absorption — any element whose
     * predicate errors (NULL-derived UNKNOWN body) errors the whole expression (deny), even when
     * the count comparison would otherwise hold. Same strict table as {@code exists_one}: the
     * strict match count is SQL NULL whenever any element body is UNKNOWN, so every comparison
     * against it goes UNKNOWN and the row stays excluded under both polarities.
     */
    private Predicate matchingElementCount(Scope.ResolvedRelation ref, ParsedLambda filter,
                                           Threshold threshold, Scope scope) {
        SubqueryBodyBuilder bodyBuilder = (sub, tailJoin, rebased) -> walker.traverse(
                filter.body(), Scope.lambda(tailJoin, sub, ref.tail(), filter.varName(), rebased));
        return walker.enterMacro("size(filter(...))", () -> {
            if (threshold.decided() != null) {
                // The count comparison itself is statically decided (a COUNT is never
                // fractional), but an erroring lambda body must still deny the row: the poison
                // term is 0 when every element body is determined and SQL NULL otherwise,
                // making the collapse UNKNOWN exactly when CEL errors. An absent to-one parent
                // denies for a different reason and needs its own guard (#309) — carried on the
                // poison EXPRESSION rather than ANDed beside it, so both polarities inherit it
                // the way every other chained comparison does (cerbos/query-plan-adapters#333).
                Expression<Long> poison = subqueries.requireLeadingHops(scope, ref,
                        subqueries.undeterminedPoisonSubquery(scope, ref, bodyBuilder),
                        Long.class);
                return threshold.decided() ? cb.equal(poison, 0L) : cb.notEqual(poison, 0L);
            }
            return compareCount(subqueries.strictMatchCountSubquery(scope, ref, bodyBuilder),
                    threshold.op(), threshold.value());
        });
    }

    /** Compare a numeric size expression (COUNT subquery or LENGTH) against a constant. */
    private <N extends Number & Comparable<N>> Predicate compareCount(
            Expression<N> count, String op, N n) {
        return switch (op) {
            case "eq" -> cb.equal(count, n);
            case "ne" -> cb.notEqual(count, n);
            case "lt" -> cb.lessThan(count, n);
            case "gt" -> cb.greaterThan(count, n);
            case "le" -> cb.lessThanOrEqualTo(count, n);
            case "ge" -> cb.greaterThanOrEqualTo(count, n);
            default -> throw Refusals.malformed(
                    "Unsupported size comparison operator: " + op);
        };
    }
}
