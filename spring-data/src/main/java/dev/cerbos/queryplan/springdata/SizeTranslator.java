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
 * Translates {@code size(x) op N}: string length of a column, element count of a relation
 * chain, and the strict {@code size(filter(...))} count.
 *
 * <p>A direct relation may use the two-valued EXISTS shortcuts. A chain always uses a COUNT
 * guarded by {@link ChainSubqueries#requireLeadingHops}, so an absent to-one parent stays
 * UNKNOWN under both polarities.
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
     * The constant, adjusted for an integral count. Truncating a fraction ({@code >= 1.5} as
     * {@code >= 1}) would return rows the PDP denies, so:
     * <ul>
     *   <li>{@code eq f} is always false and {@code ne f} always true, held in
     *       {@code decided};</li>
     *   <li>{@code gt f} and {@code ge f} become {@code ge ceil(f)};</li>
     *   <li>{@code lt f} and {@code le f} become {@code le floor(f)}.</li>
     * </ul>
     * An integral constant keeps its operator and {@code decided} is {@code null}.
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
                // The planner only emits comparisons over size().
                default -> throw Refusals.malformed("Unsupported size comparison operator: " + op);
            };
        }
    }

    /**
     * The variable {@code size()} is taken of, plus the lambda for {@code size(filter(...))};
     * for a filter over a literal list, the list in place of the variable.
     */
    private record SizeArgument(String variable, ParsedLambda filter, Value literal) {

        static SizeArgument parse(PlanResourcesFilter.Expression sizeExpr) {
            List<Operand> sizeOps = sizeExpr.getOperandsList();
            if (sizeOps.size() != 1) {
                throw Refusals.malformed(
                        "Unsupported size() expression: size() takes exactly 1 argument, got "
                                + sizeOps.size());
            }
            Operand arg = sizeOps.get(0);
            if (arg.getNodeCase() == Operand.NodeCase.VARIABLE) {
                return new SizeArgument(arg.getVariable(), null, null);
            }
            String argOperator = arg.getNodeCase() == Operand.NodeCase.EXPRESSION
                    ? arg.getExpression().getOperator() : null;
            if ("filter".equals(argOperator)) {
                List<Operand> filterOps = arg.getExpression().getOperandsList();
                if (filterOps.size() != 2) {
                    throw Refusals.malformed("Unsupported size(filter(...)) expression");
                }
                Operand collection = filterOps.get(0);
                boolean literal = collection.getNodeCase() == Operand.NodeCase.VALUE;
                if (!literal && collection.getNodeCase() != Operand.NodeCase.VARIABLE) {
                    // filter() over a computed collection: legal CEL with no join chain.
                    throw Refusals.unsupported("Unsupported size(filter(...)) expression");
                }
                return new SizeArgument(literal ? null : collection.getVariable(),
                        ParsedLambda.parse(filterOps.get(1),
                                "Unsupported size(filter(...)) expression",
                                "lambda requires exactly 2 operands",
                                "lambda requires exactly 2 operands"),
                        literal ? collection.getValue() : null);
            }
            if ("except".equals(argOperator)) {
                // size(coll.except([...])): list difference has no JPA translation.
                throw Refusals.exceptUnsupported();
            }
            // A computed collection, e.g. a map() projection or a literal list.
            throw Refusals.unsupported(
                    "Unsupported size() expression: size() argument must be a collection "
                            + "attribute or filter(...), got " + Refusals.describeOperand(arg));
        }
    }

    /**
     * Translates {@code op(size(...), N)}, or returns {@code null} when there is no
     * {@code size()} operand or numeric constant. Operands must already be field-first.
     */
    Predicate trySizeComparison(String op, List<Operand> operands, Scope scope) {
        // Every leaf comparison passes through here, so avoid converting values until a
        // size() operand is found.
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
        if (arg.literal() != null) {
            return literalMatchCount(arg, threshold, scope);
        }
        Scope.Resolution resolved = scope.resolve(arg.variable());
        if (!(resolved instanceof Scope.ResolvedRelation ref)) {
            return stringLength(arg, (Scope.ResolvedScalar) resolved, threshold, scope);
        }
        if (arg.filter() == null) {
            return elementCount(ref, threshold, scope);
        }
        return matchingElementCount(ref, arg.filter(), threshold, scope);
    }

    /** {@code LENGTH(column) op N}. */
    private Predicate stringLength(SizeArgument arg, Scope.ResolvedScalar scalar,
                                   Threshold threshold, Scope scope) {
        // A bare lambda element also resolves as a scalar, but its mapping is a Relation and
        // has no string length.
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
        // A decided arm must stay UNKNOWN for a NULL column (a missing attribute, which CEL
        // denies under both polarities): IS NOT NULL and a bare FALSE both flip under NOT.
        if (threshold.decided() != null) {
            return presentStringFold(path, threshold.decided());
        }
        // cb.length is an Integer expression, and narrowing an out-of-range threshold would
        // wrap (4294967296 becomes 0). No string length leaves int range, so those fold here.
        if (threshold.value() > Integer.MAX_VALUE) {
            return switch (threshold.op()) {
                case "eq", "gt", "ge" -> presentStringFold(path, false);
                case "lt", "le", "ne" -> presentStringFold(path, true);
                default -> throw Refusals.malformed(
                        "Unsupported size comparison operator: " + threshold.op());
            };
        }
        if (threshold.value() < Integer.MIN_VALUE) {
            return switch (threshold.op()) {
                case "gt", "ge", "ne" -> presentStringFold(path, true);
                case "eq", "lt", "le" -> presentStringFold(path, false);
                default -> throw Refusals.malformed(
                        "Unsupported size comparison operator: " + threshold.op());
            };
        }
        return compareCount(cb.length(path.as(String.class)), threshold.op(),
                (int) threshold.value());
    }

    /**
     * A {@code size(string)} comparison decided for every present string: {@code holds}, but
     * UNKNOWN when the column is NULL, so NOT of the fold still excludes the NULL rows. The
     * corpus's {@code size-huge-*-not} and {@code size-frac-*-not} actions pin both halves.
     */
    private Predicate presentStringFold(Path<?> path, boolean holds) {
        return tri.baseUnlessUnknown(holds ? cb.conjunction() : cb.disjunction(),
                () -> cb.isNull(path));
    }

    /**
     * {@code size(collection)}. No lambda is evaluated, so no element can be UNKNOWN.
     */
    private Predicate elementCount(Scope.ResolvedRelation ref, Threshold threshold, Scope scope) {
        if (threshold.decided() != null) {
            // Decided statically, but over a chain an absent to-one parent must stay UNKNOWN
            // under both polarities, so the answer is guarded by requireLeadingHops.
            if (!ref.isChained()) {
                return threshold.decided() ? cb.conjunction() : cb.disjunction();
            }
            return cb.equal(
                    subqueries.requireLeadingHops(scope, ref, cb.literal(1L), Long.class),
                    threshold.decided() ? 1L : 0L);
        }
        String op = threshold.op();
        long n = threshold.value();
        // The EXISTS shortcuts are two-valued: NOT EXISTS is TRUE for an absent parent. A chain
        // uses the guarded COUNT instead, which is NULL without the hop.
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
        // A correlated COUNT, joined through every hop, so it counts the flattened tail
        // elements.
        return compareCount(
                subqueries.requireLeadingHops(scope, ref,
                        subqueries.countSubquery(scope, ref).sub(), Long.class),
                op, n);
    }

    private Predicate anyElement(Scope scope, Scope.ResolvedRelation ref) {
        return subqueries.existsSubquery(scope, ref, (sub, tailJoin, rebased) -> cb.conjunction());
    }

    /**
     * {@code size(coll.filter(x, pred))}. CEL filter errors if any element's predicate errors,
     * so the strict count is NULL when any body is UNKNOWN and the row is excluded under both
     * polarities.
     */
    private Predicate matchingElementCount(Scope.ResolvedRelation ref, ParsedLambda filter,
                                           Threshold threshold, Scope scope) {
        SubqueryBodyBuilder bodyBuilder = (sub, tailJoin, rebased) -> walker.traverse(
                filter.body(), Scope.lambda(tailJoin, sub, ref.tail(), filter.varName(), rebased));
        return walker.enterMacro("size(filter(...))", () -> {
            if (threshold.decided() != null) {
                // The count is decided, but an UNKNOWN body must still deny: the poison term is
                // 0 when every body is determined and NULL otherwise. It is also guarded for an
                // absent to-one parent.
                Expression<Long> poison = subqueries.requireLeadingHops(scope, ref,
                        subqueries.undeterminedPoisonSubquery(scope, ref, bodyBuilder),
                        Long.class);
                return threshold.decided() ? cb.equal(poison, 0L) : cb.notEqual(poison, 0L);
            }
            return compareCount(subqueries.strictMatchCountSubquery(scope, ref, bodyBuilder),
                    threshold.op(), threshold.value());
        });
    }

    /**
     * {@code size(filter(list, pred))} over a literal list, the planner's form for a list too
     * long to unroll: the strict count of {@link CollectionTranslator#literalStrictCount}, NULL
     * when any body is UNKNOWN. A decided threshold multiplies the count by zero, so it is
     * still UNKNOWN when poisoned.
     */
    private Predicate literalMatchCount(SizeArgument arg, Threshold threshold, Scope scope) {
        return walker.enterMacro("size(filter(...))", () -> {
            Expression<Long> count = CollectionTranslator.literalStrictCount(
                    cb, walker, arg.literal(), arg.filter(), scope);
            if (threshold.decided() != null) {
                Expression<Long> poison = cb.prod(count, 0L);
                return threshold.decided() ? cb.equal(poison, 0L) : cb.notEqual(poison, 0L);
            }
            return compareCount(count, threshold.op(), threshold.value());
        });
    }

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
