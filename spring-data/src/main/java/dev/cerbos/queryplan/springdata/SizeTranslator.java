package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
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

    /** Operands must already be normalized field-first (see {@link NormalizedBinary}). */
    Predicate trySizeComparison(String op, List<Operand> operands, Scope scope) {
        // Detect the size() operand first: every ordinary leaf comparison probes through
        // here, and converting the VALUE operand up front would materialize lists/structs
        // only to discard them when no size() expression is present.
        PlanResourcesFilter.Expression sizeExpr = null;
        for (Operand o : operands) {
            if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "size".equals(o.getExpression().getOperator())) {
                sizeExpr = o.getExpression();
            }
        }
        if (sizeExpr == null) {
            return null;
        }
        Double numRaw = null;
        for (Operand o : operands) {
            if (o.getNodeCase() == Operand.NodeCase.VALUE
                    && o.getValue().getKindCase() == Value.KindCase.NUMBER_VALUE) {
                numRaw = o.getValue().getNumberValue();
            }
        }
        if (numRaw == null) {
            return null;
        }

        // Fractional thresholds: COUNT/LENGTH are integral, so a fractional constant f can
        // never be hit exactly. Truncating (`>= 1.5` becoming `>= 1`) over-included rows
        // the PDP denies. Correct integer-count semantics:
        //   eq f      → always-false
        //   ne f      → always-true (Field-mapping NULL caveat handled below: a NULL
        //               string column is a missing attribute → CEL error → deny)
        //   ge f/gt f → ge ceil(f)   (the count being integral makes gt and ge coincide)
        //   le f/lt f → le floor(f)
        // Integral thresholds keep the operator untouched. The always-true/false collapses
        // flow through the same constant predicates the other static folds use
        // (cb.conjunction()/cb.disjunction()), so the size(filter(...)) unknown-element
        // machinery below still wraps them.
        String cmpOp = op;
        long numValue;
        Boolean fractionalCollapse = null; // TRUE → always-true, FALSE → always-false
        if (numRaw != Math.rint(numRaw)) {
            switch (op) {
                case "eq" -> fractionalCollapse = Boolean.FALSE;
                case "ne" -> fractionalCollapse = Boolean.TRUE;
                case "gt", "ge" -> cmpOp = "ge";
                case "lt", "le" -> cmpOp = "le";
                // size() yields an int; anything but a comparison over it is a CEL
                // type error the planner would not have shipped.
                default -> throw Refusals.malformed(
                        "Unsupported size comparison operator: " + op);
            }
            numValue = "ge".equals(cmpOp)
                    ? (long) Math.ceil(numRaw)
                    : (long) Math.floor(numRaw);
        } else {
            numValue = numRaw.longValue();
        }
        List<Operand> sizeOps = sizeExpr.getOperandsList();
        if (sizeOps.size() != 1) {
            throw Refusals.malformed(
                    "Unsupported size() expression: size() takes exactly 1 argument, got "
                            + sizeOps.size());
        }
        Operand sizeArg = sizeOps.get(0);
        String var;
        Operand lambdaBody = null;
        String lambdaVarName = null;
        if (sizeArg.getNodeCase() == Operand.NodeCase.VARIABLE) {
            var = sizeArg.getVariable();
        } else if (sizeArg.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "filter".equals(sizeArg.getExpression().getOperator())) {
            // size(coll.filter(x, pred)) — count only the elements matching the lambda.
            List<Operand> filterOps = sizeArg.getExpression().getOperandsList();
            if (filterOps.size() != 2) {
                throw Refusals.malformed("Unsupported size(filter(...)) expression");
            }
            if (filterOps.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
                // filter() over a computed collection (a map() projection, a nested
                // filter): legal CEL with no join chain to count over.
                throw Refusals.unsupported("Unsupported size(filter(...)) expression");
            }
            var = filterOps.get(0).getVariable();
            ParsedLambda lambda = ParsedLambda.parse(filterOps.get(1),
                    "Unsupported size(filter(...)) expression",
                    "lambda requires exactly 2 operands",
                    "lambda requires exactly 2 operands");
            lambdaBody = lambda.body();
            lambdaVarName = lambda.varName();
        } else if (sizeArg.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "except".equals(sizeArg.getExpression().getOperator())) {
            // size(coll.except([...])) — the PDP-verified wire shape of every real
            // except() policy. List difference has no JPA translation; the shared
            // named error points at the equivalent exists(...) rewrite.
            throw Refusals.exceptUnsupported();
        } else {
            // size() of a computed collection (a map() projection, a literal list).
            throw Refusals.unsupported(
                    "Unsupported size() expression: size() argument must be a collection "
                            + "attribute or filter(...), got " + Refusals.describeOperand(sizeArg));
        }
        Scope.Resolution resolved = scope.resolve(var);
        if (!(resolved instanceof Scope.ResolvedRelation ref)) {
            // Only a genuine scalar ATTRIBUTE has a string length to take. The bare
            // lambda element lands in the scalar arm too, but its mapping is the
            // Relation it came from — size() of a relation element is not a length.
            Scope.ResolvedScalar scalar = (Scope.ResolvedScalar) resolved;
            if (!(scalar.mapping() instanceof AttributeMapping.Field)) {
                throw Refusals.unmapped(
                        "size() requires a collection (Relation) mapping for " + var);
            }
            // size(string) — CEL string length → LENGTH(column) <op> N.
            if (lambdaBody != null) {
                throw Refusals.unmapped(
                        "size(filter(...)) requires a collection (Relation) mapping for " + var);
            }
            Path<?> path = scope.path(var);
            if (fractionalCollapse != null) {
                // ne f is vacuously true only for a PRESENT string: a NULL column is a
                // missing attribute → CEL error → deny, so it must stay excluded —
                // IS NOT NULL, never an unconditional 1=1. eq f excludes everything.
                return fractionalCollapse ? cb.isNotNull(path) : cb.disjunction();
            }
            // cb.length(...) is Expression<Integer>, so the threshold must fit in an
            // int. An unguarded narrowing cast wraps thresholds outside int range
            // (2147483648 → −2147483648, 4294967296 → 0), silently flipping the
            // filter — `size(s) > 4294967296` became `LENGTH(s) > 0` (always-true
            // over-inclusion while check() denies every row). No string's length
            // leaves int range, so these comparisons fold statically instead —
            // CEL-faithfully: the "vacuously true" arms still require IS NOT NULL
            // because a NULL column is a missing attribute → CEL error → deny.
            if (numValue > Integer.MAX_VALUE) {
                // LENGTH(s) < 2^31 for every present string: eq/gt/ge can never
                // hold; lt/le/ne always hold for a present string.
                return switch (cmpOp) {
                    case "eq", "gt", "ge" -> cb.disjunction();
                    case "lt", "le", "ne" -> cb.isNotNull(path);
                    default -> throw Refusals.malformed(
                            "Unsupported size comparison operator: " + cmpOp);
                };
            }
            if (numValue < Integer.MIN_VALUE) {
                // LENGTH(s) >= 0 > any threshold below int range: gt/ge/ne always
                // hold for a present string; eq/lt/le can never hold.
                return switch (cmpOp) {
                    case "gt", "ge", "ne" -> cb.isNotNull(path);
                    case "eq", "lt", "le" -> cb.disjunction();
                    default -> throw Refusals.malformed(
                            "Unsupported size comparison operator: " + cmpOp);
                };
            }
            return compareCount(cb.length(path.as(String.class)), cmpOp, (int) numValue);
        }
        final Operand fBody = lambdaBody;
        final String fVar = lambdaVarName;
        if (fBody == null) {
            // size(collection) counts rows without evaluating a lambda — no element can
            // be UNKNOWN, so the plain EXISTS/COUNT comparisons are already exact.
            if (fractionalCollapse != null) {
                // A COUNT is never fractional, so the comparison is statically decided.
                // It is not unconditional though: an absent to-one parent is a CEL
                // missing-path error (deny), and folding to TRUE would return every
                // parentless row (#309).
                //
                // The guard has to be TRI-STATE, like every other chained comparison:
                // `hops AND constant` is two-valued, so `NOT(hops AND constant)` is TRUE
                // for a parentless row under BOTH collapses and readmits all of them
                // (cerbos/query-plan-adapters#333). A CASE with no ELSE yields SQL NULL
                // instead, leaving the comparison UNKNOWN under both polarities.
                if (subqueries.leadingHopsExist(scope, ref) == null) {
                    return fractionalCollapse ? cb.conjunction() : cb.disjunction();
                }
                return cb.equal(
                        subqueries.requireLeadingHops(scope, ref, cb.literal(1L), Long.class),
                        fractionalCollapse ? 1L : 0L);
            }
            boolean nonEmpty = ("gt".equals(cmpOp) && numValue == 0L)
                    || ("ge".equals(cmpOp) && numValue == 1L);
            boolean empty = ("eq".equals(cmpOp) && numValue == 0L)
                    || ("le".equals(cmpOp) && numValue == 0L)
                    || ("lt".equals(cmpOp) && numValue == 1L);
            // The EXISTS emptiness shortcuts below are TWO-valued, so a chain must not
            // take them: `NOT EXISTS` is TRUE for an absent to-one parent, which is why
            // `!(size(chain) > 0)` readmitted every parentless row even though
            // `size(chain) == 0` — guarded by a separate AND — did not
            // (cerbos/query-plan-adapters#316). Guarding the COUNT EXPRESSION instead of
            // each comparison shortcut is what makes `== 0`, `> 0`, `>= N` and all their
            // negations inherit the guard: the count is SQL NULL without the hop, so
            // every comparison built on it is UNKNOWN under BOTH polarities.
            boolean chained = subqueries.leadingHopsExist(scope, ref) != null;
            if (nonEmpty && !chained) {
                return subqueries.existsSubquery(scope, ref, (sub, tailJoin, rebased) -> cb.conjunction());
            }
            if (empty && !chained) {
                return tri.not(subqueries.existsSubquery(scope, ref,
                        (sub, tailJoin, rebased) -> cb.conjunction()));
            }
            // Arbitrary N (and every threshold over a chain) → correlated
            // (SELECT COUNT(...)) <op> N. For a multi-hop chain the COUNT joins through
            // every hop, so it counts the FLATTENED tail elements — the same element set
            // the EXISTS shortcuts range over.
            return compareCount(
                    subqueries.requireLeadingHops(scope, ref,
                            subqueries.countSubquery(scope, ref).sub(), Long.class),
                    cmpOp, numValue);
        }
        // size(coll.filter(x, pred)): CEL filter has NO error absorption — any element
        // whose predicate errors (NULL-derived UNKNOWN body) errors the whole expression
        // (deny), even when the count comparison would otherwise hold. Same strict table
        // as exists_one: strictMatchCount yields SQL NULL whenever any element body is
        // UNKNOWN, so every comparison against it goes UNKNOWN and the row stays excluded
        // under both polarities.
        SubqueryBodyBuilder bodyBuilder = (sub, tailJoin, rebased) ->
                walker.traverse(fBody, Scope.lambda(tailJoin, sub, ref.tail(), fVar, rebased));
        final String finalCmpOp = cmpOp;
        final long finalNumValue = numValue;
        final Boolean finalCollapse = fractionalCollapse;
        return walker.enterMacro("size(filter(...))", () -> {
            if (finalCollapse != null) {
                // The count comparison itself is statically decided (a COUNT is never
                // fractional), but an erroring lambda body must still deny the row: the
                // poison term is 0 when every element body is determined and SQL NULL
                // otherwise, making the collapse UNKNOWN exactly when CEL errors.
                // An absent to-one parent denies for a different reason and needs its
                // own guard (#309) — carried on the poison EXPRESSION rather than ANDed
                // beside it, so both polarities inherit it the way every other chained
                // comparison does (cerbos/query-plan-adapters#333).
                jakarta.persistence.criteria.Expression<Long> poison =
                        subqueries.requireLeadingHops(scope, ref,
                                subqueries.undeterminedPoisonSubquery(scope, ref, bodyBuilder),
                                Long.class);
                return finalCollapse
                        ? cb.equal(poison, 0L)
                        : cb.notEqual(poison, 0L);
            }
            return compareCount(subqueries.strictMatchCountSubquery(scope, ref, bodyBuilder),
                    finalCmpOp, finalNumValue);
        });
    }

    /** Compare a numeric size expression (COUNT subquery or LENGTH) against a constant. */
    private <N extends Number & Comparable<N>> Predicate compareCount(
            jakarta.persistence.criteria.Expression<N> count, String op, N n) {
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
