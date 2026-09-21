package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.util.List;
import java.util.function.Supplier;

/**
 * The one walk over a plan's expression tree.
 *
 * <p>It lowers the boolean connectives ({@code and}/{@code or}/{@code not}) and the bare
 * boolean variable itself, and dispatches every other operator by name to the collaborator
 * that owns it: the collection macros, membership, the ternary, the hierarchy operators, and
 * — for everything not handled by name — the comparison seam. Where a variable resolves is
 * the {@link Scope} threaded through every call; which truth value is being proved is NOT a
 * walk parameter here, unlike the Elasticsearch adapter's walk: negation is
 * junction-barriered in {@link TriPredicate#not} and applied around a built predicate, and
 * the collaborators that need a sub-tree in both polarities translate it twice through the
 * suppliers {@link TriPredicate} demands. Pushing polarity down the walk would change the
 * emitted SQL.
 *
 * <p>One instance per Specification evaluation, built from the caller's
 * {@link SpringDataQueryPlanAdapter.Options} and the {@code CriteriaBuilder} the repository
 * hands over. Its only mutable state is the collection-macro nesting depth, which is why the
 * public facade builds a fresh one on every invocation.
 */
final class PlanWalker {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final HierarchyTranslator hierarchy;
    private final TernaryTranslator ternary;
    private final ComparisonTranslator comparisons;
    private final MembershipTranslator membership;
    private final CollectionTranslator collections;
    private final int maxMacroDepth;
    /** Current collection-macro nesting depth; maintained by {@link #enterMacro}. */
    private int macroDepth;

    PlanWalker(CriteriaBuilder cb, SpringDataQueryPlanAdapter.Options options,
               boolean selectInvocation) {
        this.cb = cb;
        this.tri = new TriPredicate(cb);
        this.maxMacroDepth = options.effectiveMaxMacroDepth();
        this.leaf = new LeafTranslator(cb, tri, options.operatorOverrides());
        this.hierarchy = new HierarchyTranslator(cb);
        ChainSubqueries subqueries = new ChainSubqueries(cb, tri, selectInvocation);
        this.ternary = new TernaryTranslator(cb, tri, this);
        this.comparisons = new ComparisonTranslator(cb, tri, leaf, ternary,
                new SizeTranslator(cb, tri, this, subqueries));
        this.membership = new MembershipTranslator(cb, tri, leaf, subqueries);
        this.collections = new CollectionTranslator(cb, this, subqueries);
    }

    /**
     * Track one collection-macro nesting level around {@code body}, failing closed when the
     * plan nests deeper than {@link SpringDataQueryPlanAdapter.Options#effectiveMaxMacroDepth()}
     * allows, including literal-collection folds. Relation macros translate each body polarity;
     * literal folds translate the body per element. These multiply nested work, so a runaway-deep
     * policy must throw a clear error at translation time instead of silently emitting a filter
     * that times out on production-sized tables.
     */
    Predicate enterMacro(String op, Supplier<Predicate> body) {
        macroDepth++;
        try {
            if (macroDepth > maxMacroDepth) {
                throw Refusals.unsupported(
                        "Collection-macro nesting depth " + macroDepth + " exceeds the maximum of "
                        + maxMacroDepth + " (reached via operator '" + op + "'). "
                        + "Nested relation macros and literal folds "
                        + "multiply translation work and correlated subqueries, so deeply "
                        + "nested macros degrade query latency "
                        + "sharply. If the policy shape is intentional, raise the limit via "
                        + "the '" + SpringDataQueryPlanAdapter.MAX_MACRO_DEPTH_PROPERTY
                        + "' system property.");
            }
            return body.get();
        } finally {
            macroDepth--;
        }
    }

    Predicate traverse(Operand operand, Scope scope) {
        return switch (operand.getNodeCase()) {
            case EXPRESSION -> traverseExpression(operand.getExpression(), scope);
            case VARIABLE -> handleBareVariable(operand.getVariable(), scope);
            default -> throw Refusals.malformed("Unexpected operand type: " + operand.getNodeCase());
        };
    }

    private Predicate handleBareVariable(String variable, Scope scope) {
        Path<?> path = scope.path(variable);
        return leaf.applyLeaf("eq", path, true);
    }

    Predicate traverseExpression(PlanResourcesFilter.Expression expression, Scope scope) {
        String op = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();

        return switch (op) {
            case "and" -> cb.and(operands.stream()
                    .map(o -> traverse(o, scope)).toArray(Predicate[]::new));
            case "or" -> cb.or(operands.stream()
                    .map(o -> traverse(o, scope)).toArray(Predicate[]::new));
            case "not" -> {
                if (operands.size() != 1) {
                    throw Refusals.malformed("not requires exactly 1 operand");
                }
                yield tri.not(traverse(operands.get(0), scope));
            }
            case "exists", "exists_one", "all" ->
                    collections.handleCollectionOperator(op, operands, scope);
            // filter() yields a list, not a boolean. Reaching it here means the plan used it
            // as a predicate, and there is no meaning to pick — `filter(...)` is not
            // `size(filter(...)) > 0` (cerbos/query-plan-adapters#313). The legitimate
            // size(filter(...)) form is intercepted by the size handler before this.
            case "filter" -> throw Refusals.unsupported(
                    "filter() returns a list, not a boolean, so it cannot be a condition on "
                            + "its own; only size(filter(...)) has a boolean meaning");
            // Cerbos except() is a two-list function — PDP-verified wire shape:
            // size(R.attr.tags.except(["archived"])) > 0 arrives as
            // gt(size(except(variable, value-list)), 0). No lambda form exists on the wire
            // (a previous lambda-except translation here was unreachable from any real
            // plan), and list difference has no JPA Criteria translation — fail closed
            // with a named error instead.
            case "except" -> throw Refusals.exceptUnsupported();
            // has_intersection is the deprecated pre-camelCase alias still accepted by the PDP.
            case "hasIntersection", "has_intersection" ->
                    membership.handleHasIntersection(operands, scope);
            case "in" -> membership.handleIn(operands, scope);
            case "if" -> ternary.handleBareTernary(operands, scope);
            case "overlaps" -> hierarchy.handleOverlaps(operands, scope);
            case "ancestorOf" -> hierarchy.handleAncestorDescendant(operands, scope, true);
            case "descendentOf" -> hierarchy.handleAncestorDescendant(operands, scope, false);
            default -> comparisons.translate(op, operands, scope);
        };
    }
}
