/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.util.List;
import java.util.function.Supplier;

/**
 * Walks a plan's expression tree. It lowers {@code and}/{@code or}/{@code not} and bare
 * boolean variables itself and dispatches every other operator to its translator.
 *
 * <p>Polarity is not passed down the walk: {@code not} wraps the built predicate (see
 * {@link TriPredicate#not}). Build a new instance per Specification evaluation, because it
 * tracks macro nesting depth.
 */
final class PlanWalker {

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final HierarchyTranslator hierarchy;
    private final RegexTranslator regex;
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
        this.hierarchy = new HierarchyTranslator(cb, tri);
        this.regex = new RegexTranslator(cb, tri);
        ChainSubqueries subqueries = new ChainSubqueries(cb, tri, selectInvocation);
        this.ternary = new TernaryTranslator(cb, tri, this);
        this.comparisons = new ComparisonTranslator(cb, tri, leaf, ternary,
                new SizeTranslator(cb, tri, this, subqueries));
        this.membership = new MembershipTranslator(cb, tri, leaf, subqueries);
        this.collections = new CollectionTranslator(cb, this, subqueries);
    }

    /**
     * Runs {@code body} one macro level deeper, throwing when the depth exceeds the configured
     * maximum. Each level multiplies translation work and subqueries.
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
            // filter() is a list, not a boolean; size(filter(...)) is handled before this.
            case "filter" -> throw Refusals.unsupported(
                    "filter() returns a list, not a boolean, so it cannot be a condition on "
                            + "its own; only size(filter(...)) has a boolean meaning");
            // List difference has no JPA Criteria translation.
            case "except" -> throw Refusals.exceptUnsupported();
            // has_intersection is a deprecated alias the PDP still accepts.
            case "hasIntersection", "has_intersection" ->
                    membership.handleHasIntersection(operands, scope);
            case "in" -> {
                Predicate viaTernary = ternary.tryTernaryComparison(op, operands, scope);
                if (viaTernary != null) {
                    yield viaTernary;
                }
                Operand rewritten = membershipInProjection(operands);
                if (rewritten == null) {
                    rewritten = membershipInConcatenation(operands, scope);
                }
                if (rewritten != null) {
                    yield traverse(rewritten, scope);
                }
                if (operands.size() == 2 && isComputedList(operands.get(1))) {
                    List<Operand> elements = operands.get(1).getExpression().getOperandsList();
                    yield overComputedList(elements,
                            elements.stream().map(e -> expression("eq", operands.get(0), e))
                                    .toList(),
                            true, scope);
                }
                yield membership.handleIn(operands, scope);
            }
            case "isSubset" -> {
                Operand rewritten = subsetAsMembership(operands, scope);
                yield rewritten != null
                        ? traverse(rewritten, scope) : leafComparison(op, operands, scope);
            }
            case "if" -> ternary.handleBareTernary(operands, scope);
            case "overlaps" -> hierarchy.handleOverlaps(operands, scope);
            case "ancestorOf" -> hierarchy.handleAncestorDescendant(operands, scope, true);
            case "descendentOf" -> hierarchy.handleAncestorDescendant(operands, scope, false);
            case "matches" -> {
                if (leaf.overridden("matches") || operands.size() != 2
                        || operands.get(0).getNodeCase() != Operand.NodeCase.VARIABLE
                        || operands.get(1).getNodeCase() != Operand.NodeCase.VALUE
                        || operands.get(1).getValue().getKindCase()
                                != Value.KindCase.STRING_VALUE) {
                    yield leafComparison(op, operands, scope);
                }
                yield regex.matches(scope.path(operands.get(0).getVariable()),
                        operands.get(1).getValue().getStringValue());
            }
            case "eq", "ne" -> {
                Operand predicate = booleanComparison(op, operands);
                if (predicate == null) {
                    predicate = intersectionEmptiness(op, operands);
                }
                if (predicate != null) {
                    yield traverse(predicate, scope);
                }
                Predicate lookup = mapLiteralLookup(op, operands, scope);
                if (lookup != null) {
                    yield lookup;
                }
                Operand rewritten = wholeListEquality(op, operands, scope);
                yield rewritten != null
                        ? traverse(rewritten, scope) : leafComparison(op, operands, scope);
            }
            default -> leafComparison(op, operands, scope);
        };
    }

    /**
     * {@code matches(...) == true} and its spellings, as the predicate or its negation:
     * {@code == true} and {@code != false} keep it, {@code == false} and {@code != true} negate
     * it. A {@code matches()} error stays UNKNOWN under both. {@code null} for any other shape.
     */
    private static Operand booleanComparison(String op, List<Operand> operands) {
        if (operands.size() != 2) {
            return null;
        }
        Operand predicate = operands.get(0);
        Operand literal = operands.get(1);
        if (predicate.getNodeCase() == Operand.NodeCase.VALUE) {
            predicate = operands.get(1);
            literal = operands.get(0);
        }
        if (predicate.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"matches".equals(predicate.getExpression().getOperator())
                || literal.getNodeCase() != Operand.NodeCase.VALUE
                || literal.getValue().getKindCase() != Value.KindCase.BOOL_VALUE) {
            return null;
        }
        boolean keep = literal.getValue().getBoolValue() == "eq".equals(op);
        return keep ? predicate : expression("not", predicate);
    }

    /** A leaf comparison: a positional list read if an operand is one, else the comparison. */
    private Predicate leafComparison(String op, List<Operand> operands, Scope scope) {
        Predicate positional = collections.tryPositionalRead(op, operands, scope);
        return positional != null ? positional : comparisons.translate(op, operands, scope);
    }

    /** A lambda variable no plan names, for the element of a rewritten list equality. */
    private static final String LIST_ELEMENT = "__cerbos_list_element";

    /**
     * {@code coll == [v1, ..., vn]} with {@code coll} a relation, a {@code map()} projection of
     * one ({@code t} or {@code t.f}), or an {@code except()} of one, rewritten into operators
     * that translate, and {@code !=} as its negation. CEL list equality is ordered:
     * <ul>
     *   <li>at most one element has no order to compare, so it is
     *       {@code size(coll) == n && coll.exists(e, e == v)};</li>
     *   <li>more need the relation's declared position field, and are
     *       {@code size(coll) == n && coll[0] == v1 && ...}, each read in range once the size
     *       matches;</li>
     *   <li>an {@code except()} difference is compared only with at most one element, as
     *       {@code size(diff) == n} and, for one, a kept element equal to it.</li>
     * </ul>
     * Returns {@code null} for any other shape, which the comparison then refuses.
     */
    private static Operand wholeListEquality(String op, List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            return null;
        }
        Operand collection = operands.get(0);
        Operand literal = operands.get(1);
        if (collection.getNodeCase() == Operand.NodeCase.VALUE) {
            collection = operands.get(1);
            literal = operands.get(0);
        }
        if (literal.getNodeCase() != Operand.NodeCase.VALUE
                || literal.getValue().getKindCase() != Value.KindCase.LIST_VALUE) {
            return null;
        }
        List<Value> elements = literal.getValue().getListValue().getValuesList();
        Operand equality = switch (collection.getNodeCase()) {
            case VARIABLE -> relationEquality(collection.getVariable(), null, elements, scope);
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = collection.getExpression();
                if ("map".equals(e.getOperator()) && e.getOperandsCount() == 2
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE) {
                    ParsedLambda projection = ParsedLambda.parse(e.getOperands(1),
                            "map second operand must be a lambda",
                            "lambda requires exactly 2 operands",
                            "lambda variable must be a variable operand");
                    yield relationEquality(e.getOperands(0).getVariable(), projection,
                            elements, scope);
                }
                if ("except".equals(e.getOperator()) && e.getOperandsCount() == 2
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE
                        && elements.size() <= 1
                        && isRelation(e.getOperands(0).getVariable(), scope)) {
                    yield differenceEquality(collection, e, elements);
                }
                yield null;
            }
            default -> null;
        };
        if (equality == null) {
            return null;
        }
        return "ne".equals(op) ? expression("not", equality) : equality;
    }

    /**
     * The equality of {@code variable}, or of its {@code projection}, with {@code elements};
     * {@code null} when {@code variable} is no relation, the projection is not {@code t} or
     * {@code t.f}, or the list is longer than one and the relation declares no position.
     */
    private static Operand relationEquality(String variable, ParsedLambda projection,
                                            List<Value> elements, Scope scope) {
        if (!isRelation(variable, scope)) {
            return null;
        }
        String field = null;
        if (projection != null) {
            Operand body = projection.body();
            String name = projection.varName();
            if (body.getNodeCase() != Operand.NodeCase.VARIABLE) {
                return null;
            }
            String projected = body.getVariable();
            if (projected.startsWith(name + ".") && projected.indexOf('.', name.length() + 1) < 0) {
                field = projected.substring(name.length() + 1);
            } else if (!projected.equals(name)) {
                return null;
            }
        }
        Operand listVar = Operand.newBuilder().setVariable(variable).build();
        Operand equality = expression("eq", expression("size", listVar), number(elements.size()));
        if (elements.size() == 1) {
            Operand element = Operand.newBuilder().setVariable(
                    field == null ? LIST_ELEMENT : LIST_ELEMENT + "." + field).build();
            Operand body = expression("eq", element, constant(elements.get(0)));
            return expression("and", equality, expression("exists", listVar, expression("lambda",
                    body, Operand.newBuilder().setVariable(LIST_ELEMENT).build())));
        }
        if (elements.size() > 1) {
            Scope.Resolution resolved = scope.resolve(variable);
            if (!(resolved instanceof Scope.ResolvedRelation ref) || ref.isChained()
                    || ref.tail().positionField() == null) {
                return null;
            }
            PlanResourcesFilter.Expression.Builder all = PlanResourcesFilter.Expression
                    .newBuilder().setOperator("and").addOperands(equality);
            for (int k = 0; k < elements.size(); k++) {
                Operand read = expression("index", listVar, number(k));
                if (field != null) {
                    read = expression("get-field", read,
                            Operand.newBuilder().setVariable(field).build());
                }
                all.addOperands(expression("eq", read, constant(elements.get(k))));
            }
            return Operand.newBuilder().setExpression(all).build();
        }
        return equality;
    }

    /**
     * {@code rel.except(b) == []} or {@code == [v]}: the difference is empty, or holds one
     * element and it is {@code v}.
     */
    private static Operand differenceEquality(Operand difference,
                                              PlanResourcesFilter.Expression except,
                                              List<Value> elements) {
        Operand size = expression("eq", expression("size", difference), number(elements.size()));
        if (elements.isEmpty()) {
            return size;
        }
        ParsedLambda keep = SizeTranslator.exceptKeeps(except);
        Operand element = Operand.newBuilder().setVariable(keep.varName()).build();
        Operand kept = expression("and", keep.body(),
                expression("eq", element, constant(elements.get(0))));
        Operand matches = expression("filter", except.getOperands(0),
                expression("lambda", kept, element));
        return expression("and", size, expression("gt", expression("size", matches), number(0)));
    }

    private static boolean isRelation(String variable, Scope scope) {
        try {
            return scope.resolve(variable) instanceof Scope.ResolvedRelation;
        } catch (IllegalArgumentException unmapped) {
            // Left to the comparison, which reports the list constant.
            return false;
        }
    }

    private static Operand constant(Value value) {
        return Operand.newBuilder().setValue(value).build();
    }

    /**
     * {@code x in coll.map(t, body)}, over a literal list or a relation, rewritten as
     * {@code size(coll.filter(t, x == body)) > 0}, whose strict count is UNKNOWN when any
     * element's comparison is. That is exact because CEL's {@code map} errors when any element's
     * body does, and {@code x == body} is UNKNOWN only where the body errors or {@code x} is a
     * missing attribute.
     *
     * <p>{@code x in coll.filter(t, p)} likewise becomes
     * {@code size(coll.filter(t, p ? x == t : false)) > 0}: CEL's {@code filter} errors when
     * any element's {@code p} does, and the ternary keeps an UNKNOWN {@code p} UNKNOWN where
     * {@code p && x == t} would let a FALSE comparison absorb it.
     *
     * <p>Returns {@code null} for any other shape, and when {@code x} reads the lambda
     * variable's name, which the rewrite would capture.
     */
    private static Operand membershipInProjection(List<Operand> operands) {
        if (operands.size() != 2
                || operands.get(1).getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return null;
        }
        PlanResourcesFilter.Expression macro = operands.get(1).getExpression();
        boolean map = "map".equals(macro.getOperator());
        if ((!map && !"filter".equals(macro.getOperator())) || macro.getOperandsCount() != 2
                || (macro.getOperands(0).getNodeCase() != Operand.NodeCase.VALUE
                        && macro.getOperands(0).getNodeCase() != Operand.NodeCase.VARIABLE)) {
            return null;
        }
        ParsedLambda lambda = ParsedLambda.parse(macro.getOperands(1),
                macro.getOperator() + " second operand must be a lambda",
                "lambda requires exactly 2 operands",
                "lambda variable must be a variable operand");
        Operand needle = operands.get(0);
        if (readsName(needle, lambda.varName())) {
            return null;
        }
        Operand variable = Operand.newBuilder().setVariable(lambda.varName()).build();
        Operand body = map
                ? expression("eq", needle, lambda.body())
                : expression("if", lambda.body(), expression("eq", needle, variable),
                        Operand.newBuilder().setValue(Value.newBuilder().setBoolValue(false))
                                .build());
        Operand filter = expression("filter", macro.getOperands(0),
                expression("lambda", body, variable));
        return expression("gt", expression("size", filter), number(0));
    }

    /**
     * {@code x in a + b}, rewritten as {@code x in a || x in b}, when every part of the
     * concatenation is a literal list or a direct relation. Neither can error, so the
     * disjunction errors exactly where {@code x in (a + b)} does: when {@code x} is a missing
     * attribute. A part that can error (a relation reached through a to-one parent, a computed
     * list) is left to the refusal, since a TRUE disjunct would hide its error.
     */
    private static Operand membershipInConcatenation(List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            return null;
        }
        List<Operand> parts = new java.util.ArrayList<>();
        if (!concatenationParts(operands.get(1), scope, parts) || parts.size() < 2) {
            return null;
        }
        PlanResourcesFilter.Expression.Builder any = PlanResourcesFilter.Expression.newBuilder()
                .setOperator("or");
        parts.forEach(part -> any.addOperands(expression("in", operands.get(0), part)));
        return Operand.newBuilder().setExpression(any).build();
    }

    private static boolean concatenationParts(Operand o, Scope scope, List<Operand> parts) {
        switch (o.getNodeCase()) {
            case VALUE -> {
                parts.add(o);
                return o.getValue().getKindCase() == Value.KindCase.LIST_VALUE;
            }
            case VARIABLE -> {
                parts.add(o);
                return isDirectRelation(o.getVariable(), scope);
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = o.getExpression();
                return "add".equals(e.getOperator()) && e.getOperandsCount() == 2
                        && concatenationParts(e.getOperands(0), scope, parts)
                        && concatenationParts(e.getOperands(1), scope, parts);
            }
            default -> {
                return false;
            }
        }
    }

    private static boolean isDirectRelation(String variable, Scope scope) {
        try {
            return scope.resolve(variable) instanceof Scope.ResolvedRelation ref
                    && !ref.isChained();
        } catch (IllegalArgumentException unmapped) {
            return false;
        }
    }

    /** A {@code list(...)} the literal fold left as an expression: an element is computed. */
    static boolean isComputedList(Operand o) {
        return o.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "list".equals(o.getExpression().getOperator());
    }

    /**
     * A macro or membership over a list built from expressions ({@code [R.attr.a, "x"]}),
     * folded per element: {@code any} joins {@code perElement} with {@code or}
     * ({@code exists}, {@code in}), otherwise with {@code and} ({@code all}). CEL builds the
     * list first, so one erroring element errors the whole result, even where another element
     * decides it; a plain disjunction would let a TRUE element hide that error. Each computed
     * element's {@code e == e || !(e == e)} is TRUE where the element evaluates and UNKNOWN
     * where it errors, and the fold is taken under that guard as a ternary, which is UNKNOWN
     * under both polarities whenever the guard is.
     */
    Predicate overComputedList(List<Operand> elements, List<Operand> perElement, boolean any,
                               Scope scope) {
        if (perElement.isEmpty()) {
            return any ? cb.disjunction() : cb.conjunction();
        }
        PlanResourcesFilter.Expression.Builder fold = PlanResourcesFilter.Expression.newBuilder()
                .setOperator(any ? "or" : "and").addAllOperands(perElement);
        PlanResourcesFilter.Expression.Builder evaluated =
                PlanResourcesFilter.Expression.newBuilder().setOperator("and");
        for (Operand element : elements) {
            if (element.getNodeCase() != Operand.NodeCase.VALUE) {
                Operand same = expression("eq", element, element);
                evaluated.addOperands(expression("or", same, expression("not", same)));
            }
        }
        Operand body = Operand.newBuilder().setExpression(fold).build();
        if (evaluated.getOperandsCount() == 0) {
            return traverse(body, scope);
        }
        Operand guard = Operand.newBuilder().setExpression(evaluated).build();
        return tri.ternary(() -> traverse(guard, scope), () -> traverse(body, scope),
                cb::disjunction);
    }

    /**
     * {@code rel.isSubset([...])}, Cerbos's "every element of {@code rel} is in the list", as
     * {@code rel.all(e, e in [...])}. {@code null} for any other shape.
     */
    private static Operand subsetAsMembership(List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            return null;
        }
        Operand receiver = operands.get(0);
        Operand other = operands.get(1);
        if (receiver.getNodeCase() == Operand.NodeCase.VARIABLE
                && isRelation(receiver.getVariable(), scope)
                && other.getNodeCase() == Operand.NodeCase.VALUE
                && other.getValue().getKindCase() == Value.KindCase.LIST_VALUE) {
            Operand element = Operand.newBuilder().setVariable(LIST_ELEMENT).build();
            return expression("all", receiver,
                    expression("lambda", expression("in", element, other), element));
        }
        return null;
    }

    /**
     * {@code intersect(a, b) == []} as {@code !hasIntersection(a, b)}, and {@code != []} as
     * {@code hasIntersection(a, b)}: Cerbos's {@code intersect} keeps the elements the two
     * lists share, so it is empty exactly when they share none. Its size is not translated, as
     * it counts duplicates from whichever list is shorter. {@code null} for any other shape.
     */
    private static Operand intersectionEmptiness(String op, List<Operand> operands) {
        if (operands.size() != 2) {
            return null;
        }
        for (int i = 0; i < 2; i++) {
            Operand intersect = operands.get(i);
            Operand empty = operands.get(1 - i);
            if (intersect.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "intersect".equals(intersect.getExpression().getOperator())
                    && intersect.getExpression().getOperandsCount() == 2
                    && empty.getNodeCase() == Operand.NodeCase.VALUE
                    && empty.getValue().getKindCase() == Value.KindCase.LIST_VALUE
                    && empty.getValue().getListValue().getValuesCount() == 0) {
                PlanResourcesFilter.Expression.Builder shared = intersect.getExpression()
                        .toBuilder().setOperator("hasIntersection");
                Operand any = Operand.newBuilder().setExpression(shared).build();
                return "eq".equals(op) ? expression("not", any) : any;
            }
        }
        return null;
    }

    /**
     * {@code {"k1": v1, ...}[key] == c} (or {@code !=}), a lookup in a map literal by a
     * computed key: the keys whose value equals {@code c} (or differs from it) when
     * {@code key} is one of the map's keys, and UNKNOWN under both polarities when it is not,
     * as CEL errors on a missing key. Values are compared with CEL equality (numbers by
     * value, other types by kind and value). {@code null} for any other shape.
     */
    private Predicate mapLiteralLookup(String op, List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            return null;
        }
        for (int i = 0; i < 2; i++) {
            Operand index = operands.get(i);
            Operand compared = operands.get(1 - i);
            if (index.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"index".equals(index.getExpression().getOperator())
                    || index.getExpression().getOperandsCount() != 2
                    || compared.getNodeCase() != Operand.NodeCase.VALUE) {
                continue;
            }
            Operand map = index.getExpression().getOperands(0);
            Operand key = index.getExpression().getOperands(1);
            if (map.getNodeCase() != Operand.NodeCase.VALUE
                    || map.getValue().getKindCase() != Value.KindCase.STRUCT_VALUE
                    || key.getNodeCase() == Operand.NodeCase.VALUE) {
                continue;
            }
            Object target = PlanValues.protoValueToJava(compared.getValue());
            com.google.protobuf.ListValue.Builder keys = com.google.protobuf.ListValue.newBuilder();
            com.google.protobuf.ListValue.Builder selected =
                    com.google.protobuf.ListValue.newBuilder();
            map.getValue().getStructValue().getFieldsMap().forEach((k, v) -> {
                Value keyValue = Value.newBuilder().setStringValue(k).build();
                keys.addValues(keyValue);
                if (celEquals(PlanValues.protoValueToJava(v), target) == "eq".equals(op)) {
                    selected.addValues(keyValue);
                }
            });
            Operand present = expression("in", key,
                    constant(Value.newBuilder().setListValue(keys).build()));
            Operand matching = expression("in", key,
                    constant(Value.newBuilder().setListValue(selected).build()));
            return tri.ternary(() -> traverse(present, scope), () -> traverse(matching, scope),
                    tri::unknown);
        }
        return null;
    }

    private static boolean celEquals(Object left, Object right) {
        return left instanceof Number l && right instanceof Number r
                ? l.doubleValue() == r.doubleValue()
                : java.util.Objects.equals(left, right);
    }

    private static boolean readsName(Operand operand, String name) {
        return switch (operand.getNodeCase()) {
            case VARIABLE -> operand.getVariable().equals(name)
                    || operand.getVariable().startsWith(name + ".");
            case EXPRESSION -> operand.getExpression().getOperandsList().stream()
                    .anyMatch(child -> readsName(child, name));
            default -> false;
        };
    }

    private static Operand expression(String operator, Operand... operands) {
        PlanResourcesFilter.Expression.Builder e =
                PlanResourcesFilter.Expression.newBuilder().setOperator(operator);
        for (Operand operand : operands) {
            e.addOperands(operand);
        }
        return Operand.newBuilder().setExpression(e).build();
    }

    private static Operand number(double value) {
        return Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(value)).build();
    }
}
