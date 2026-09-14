/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The hierarchy relations ({@code ancestorOf} / {@code descendentOf} / {@code overlaps}).
 *
 * <p>A Cerbos hierarchy is a delimited path, and the three relations are statements about
 * SEGMENT prefixes: {@code ancestorOf(A, B)} holds when A's segments are a strict prefix of B's,
 * {@code descendentOf} is the same relation with the operands swapped, and {@code overlaps} is
 * the inclusive union of both directions with equality in the middle.
 *
 * <p>Only one side is ever a document field in a plan the planner can produce, so each relation
 * becomes a term-level query over that field's whole stored path:
 *
 * <pre>
 *   field is a strict DESCENDANT of a constant -> prefix on &lt;constant&gt;&lt;delimiter&gt;
 *   field is a strict ANCESTOR  of a constant -> terms over the constant's proper prefixes
 *   overlaps                                  -> bool.should of both, plus a term on the
 *                                                whole constant path
 * </pre>
 *
 * <p>The operand model, the strict-prefix enumeration and the two edge cases below mirror the
 * Spring Data adapter's HierarchyTranslator, which is this repository's reference lowering: same
 * wire contract, same raw-string comparison of the field value, same refusal to guess. Only the
 * emitted form differs, because a prefix query is term-level where a SQL LIKE is not — which is
 * also why none of the corpus's metacharacter traps need escaping here.
 */
final class HierarchyTranslator {

    private final Scope root;

    HierarchyTranslator(Scope root) {
        this.root = root;
    }

    /** A resolved {@code hierarchy(...)} operand. */
    private sealed interface Hierarchy
            permits Hierarchy.Constant, Hierarchy.FieldRef, Hierarchy.Segmented {

        /** A literal delimited path, split into segments. */
        record Constant(List<String> segments, String delimiter) implements Hierarchy {}

        /** A document field holding the whole delimited path. */
        record FieldRef(String field, String delimiter) implements Hierarchy {}

        /** A {@code list(...)} of segments, each a literal or a document field. */
        record Segmented(List<HierarchySegment> segments) implements Hierarchy {}
    }

    /** One segment of a {@link Hierarchy.Segmented}. */
    private sealed interface HierarchySegment
            permits HierarchySegment.Literal, HierarchySegment.Field {
        record Literal(String value) implements HierarchySegment {}

        record Field(String field) implements HierarchySegment {}
    }

    /**
     * Lower one hierarchy relation. The false direction throws before any operand is examined —
     * see {@link #negatedHierarchy}.
     *
     * <p>The emitted clauses are built from the DEFAULT term-level forms rather than through
     * {@code operatorOverrides}: an override replaces one named plan OPERATOR, and a hierarchy
     * relation is not {@code startsWith} or {@code in} even though it borrows their shapes.
     * Hierarchy therefore introduces no new mapping assumption: it needs the same
     * exactly-compared ({@code keyword}) field mapping every term-level query this adapter emits
     * already needs.
     */
    Map<String, Object> translate(String operator, List<Operand> operands, Polarity polarity) {
        if (!polarity.holds()) {
            throw negatedHierarchy(operator);
        }
        if (operands.size() != 2) {
            throw malformed(operator + " requires exactly 2 operands, got " + operands.size());
        }
        Hierarchy left = normalizeHierarchy(resolveHierarchy(operator, operands.get(0)));
        Hierarchy right = normalizeHierarchy(resolveHierarchy(operator, operands.get(1)));
        return "overlaps".equals(operator)
                ? hierarchyOverlaps(operator, left, right)
                : hierarchyStrict(operator, left, right);
    }

    /** {@code ancestorOf(A, B)} and its mirror {@code descendentOf(A, B)}. */
    private static Map<String, Object> hierarchyStrict(
            String operator, Hierarchy left, Hierarchy right) {
        boolean isAncestor = "ancestorOf".equals(operator);
        Hierarchy ancestor = isAncestor ? left : right;
        Hierarchy descendant = isAncestor ? right : left;

        if (ancestor instanceof Hierarchy.Constant constant
                && descendant instanceof Hierarchy.FieldRef field) {
            String prefix = String.join(field.delimiter(), constant.segments())
                    + field.delimiter();
            return prefixQuery(field.field(), prefix);
        }
        if (ancestor instanceof Hierarchy.FieldRef field
                && descendant instanceof Hierarchy.Constant constant) {
            List<String> prefixes = strictPrefixes(constant.segments(), field.delimiter());
            // A one-segment path has no proper prefix, so NOTHING is a strict ancestor of it. An
            // empty `terms` list is a query Elasticsearch accepts and matches nothing with, but it
            // reads as an oversight; `match_none` says the emptiness was the answer.
            return prefixes.isEmpty() ? Queries.matchNone() : defaultMembership(field.field(), prefixes);
        }
        if (ancestor instanceof Hierarchy.Constant a && descendant instanceof Hierarchy.Constant d) {
            // Both sides constant: the relation is decidable here, and only one answer is
            // reachable — the planner folds a false constant comparison away rather than shipping
            // it, so a plan that arrives holding one is an upstream bug and not a filter to guess.
            if (d.segments().size() > a.segments().size() && isPrefixOf(a.segments(), d.segments())) {
                return Queries.matchAll();
            }
            throw unfoldedConstantHierarchy(operator);
        }
        throw unsupportedHierarchyOperands(operator, left, right);
    }

    /** {@code overlaps(A, B)} — the inclusive union, symmetric in its operands. */
    private static Map<String, Object> hierarchyOverlaps(
            String operator, Hierarchy left, Hierarchy right) {
        if (left instanceof Hierarchy.Constant a && right instanceof Hierarchy.Constant b) {
            // Inclusive and symmetric: whichever path is shorter must be a prefix of the other.
            boolean overlap = a.segments().size() <= b.segments().size()
                    ? isPrefixOf(a.segments(), b.segments())
                    : isPrefixOf(b.segments(), a.segments());
            if (overlap) {
                return Queries.matchAll();
            }
            throw unfoldedConstantHierarchy(operator);
        }

        Hierarchy.FieldRef field;
        Hierarchy other;
        if (left instanceof Hierarchy.FieldRef f && !(right instanceof Hierarchy.FieldRef)) {
            field = f;
            other = right;
        } else if (right instanceof Hierarchy.FieldRef f && !(left instanceof Hierarchy.FieldRef)) {
            field = f;
            other = left;
        } else {
            throw unsupportedHierarchyOperands(operator, left, right);
        }
        if (!(other instanceof Hierarchy.Constant constant)) {
            throw unsupportedHierarchyOperands(operator, left, right);
        }

        String delimiter = field.delimiter();
        String whole = String.join(delimiter, constant.segments());
        List<Map<String, Object>> clauses = new ArrayList<>();
        // ...the field is a strict ancestor of the constant...
        List<String> prefixes = strictPrefixes(constant.segments(), delimiter);
        if (!prefixes.isEmpty()) {
            clauses.add(defaultMembership(field.field(), prefixes));
        }
        // ...or equal to it...
        clauses.add(Queries.DEFAULT_OPERATORS.get("eq").apply(field.field(), whole));
        // ...or a strict descendant of it.
        clauses.add(prefixQuery(field.field(), whole + delimiter));
        return Queries.boolShould(List.copyOf(clauses));
    }

    private static MalformedPlanException unfoldedConstantHierarchy(String operator) {
        return malformed(operator
                + ": constant hierarchy operands do not satisfy the relation, so the planner"
                + " should have folded this comparison rather than emitting it");
    }

    /**
     * Resolve one {@code hierarchy(...)} wrapper into the three forms the planner emits:
     * {@code hierarchy(<value>)} with the default {@code .} delimiter,
     * {@code hierarchy(<value|field>, <delimiter>)}, and {@code hierarchy(list(<segments>))}.
     */
    private Hierarchy resolveHierarchy(String operator, Operand operand) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"hierarchy".equals(operand.getExpression().getOperator())) {
            throw malformed(operator + " requires hierarchy(...) operands");
        }
        List<Operand> operands = operand.getExpression().getOperandsList();
        if (operands.size() == 2) {
            Operand path = operands.get(0);
            Operand delimiterOperand = operands.get(1);
            if (delimiterOperand.getNodeCase() != Operand.NodeCase.VALUE) {
                throw malformed("hierarchy delimiter must be a value");
            }
            String delimiter = String.valueOf(PlanValues.protoValueToJava(delimiterOperand.getValue()));
            return switch (path.getNodeCase()) {
                case VALUE -> new Hierarchy.Constant(
                        splitLiteral(String.valueOf(PlanValues.protoValueToJava(path.getValue())), delimiter),
                        delimiter);
                case VARIABLE -> new Hierarchy.FieldRef(
                        root.field(path.getVariable()), delimiter);
                default -> throw malformed(
                        "hierarchy(path, delimiter) requires a value or field path");
            };
        }
        if (operands.size() == 1) {
            Operand inner = operands.get(0);
            return switch (inner.getNodeCase()) {
                case VALUE -> new Hierarchy.Constant(
                        splitLiteral(String.valueOf(PlanValues.protoValueToJava(inner.getValue())), "."), ".");
                case VARIABLE -> new Hierarchy.FieldRef(
                        root.field(inner.getVariable()), ".");
                case EXPRESSION -> {
                    if (!"list".equals(inner.getExpression().getOperator())) {
                        throw malformed("hierarchy requires a value, field or list operand, got "
                                + inner.getExpression().getOperator());
                    }
                    List<HierarchySegment> segments = new ArrayList<>();
                    for (Operand segment : inner.getExpression().getOperandsList()) {
                        switch (segment.getNodeCase()) {
                            case VALUE -> segments.add(new HierarchySegment.Literal(
                                    String.valueOf(PlanValues.protoValueToJava(segment.getValue()))));
                            case VARIABLE -> segments.add(new HierarchySegment.Field(
                                    root.field(segment.getVariable())));
                            default -> throw malformed(
                                    "hierarchy list segment must be a value or a field, got "
                                            + segment.getNodeCase());
                        }
                    }
                    yield new Hierarchy.Segmented(List.copyOf(segments));
                }
                default -> throw malformed(
                        "hierarchy requires a value, field or list operand, got "
                                + inner.getNodeCase());
            };
        }
        throw malformed("hierarchy requires 1 or 2 operands, got " + operands.size());
    }

    /** Collapse an all-literal {@code list(...)} to a plain constant path. */
    private static Hierarchy normalizeHierarchy(Hierarchy hierarchy) {
        if (!(hierarchy instanceof Hierarchy.Segmented segmented)) {
            return hierarchy;
        }
        List<String> literals = new ArrayList<>();
        for (HierarchySegment segment : segmented.segments()) {
            if (!(segment instanceof HierarchySegment.Literal literal)) {
                return hierarchy;
            }
            literals.add(literal.value());
        }
        return new Hierarchy.Constant(List.copyOf(literals), ".");
    }

    /**
     * The operand combinations a term-level query cannot answer, each named for its own mechanism.
     *
     * <p>A {@link Hierarchy.Segmented} that survived {@link #normalizeHierarchy} carries a
     * document field as a path SEGMENT, so comparing it would mean concatenating a stored value
     * into a path before matching — the same missing evaluation step that refuses arithmetic here.
     * Two {@link Hierarchy.FieldRef}s are the ordinary field-to-field comparison.
     */
    private static UnsupportedPlanShapeException unsupportedHierarchyOperands(
            String operator, Hierarchy left, Hierarchy right) {
        if (left instanceof Hierarchy.Segmented || right instanceof Hierarchy.Segmented) {
            return unsupported("A hierarchy path constructed by list() from a "
                    + "document field cannot be compared: Elasticsearch Query DSL has no way to "
                    + "concatenate a field into a path without scripts");
        }
        return unsupported(
                "Elasticsearch Query DSL cannot compare two document fields without scripts");
    }

    /**
     * Why the false direction of a hierarchy relation is refused rather than negated.
     *
     * <p>A SQL adapter gets the exclusion free from three-valued logic: {@code NULL LIKE 'x%'} is
     * UNKNOWN, so a row with no scope drops out of a negated hierarchy test on its own. Here the
     * relation lowers to {@code prefix} / {@code terms} / {@code term}, and a
     * {@code bool.must_not} around any of them MATCHES a document that has no value for the field
     * — which is the CEL missing-attribute error, an error the PDP denies on. An
     * {@code exists}-guarded negation would express it, exactly as {@code eq}, {@code in},
     * {@code contains} and {@code startsWith} already are; no corpus action negates a hierarchy
     * shape, so that guard would ship unproven and this fails closed instead.
     */
    private static UnsupportedPlanShapeException negatedHierarchy(String operator) {
        return unsupported("Negated " + operator + " cannot be expressed safely: "
                + "a bool.must_not over the prefix/terms/term queries a hierarchy relation lowers "
                + "to matches every document that has no value for the field");
    }

    private static Map<String, Object> prefixQuery(String field, String prefix) {
        return Queries.DEFAULT_OPERATORS.get("startsWith").apply(field, prefix);
    }

    /** The default {@code terms} form, for the hierarchy lowering that borrows it by shape only. */
    private static Map<String, Object> defaultMembership(String field, Object value) {
        return Queries.DEFAULT_OPERATORS.get("in").apply(field, value);
    }

    private static boolean isPrefixOf(List<String> shorter, List<String> longer) {
        for (int index = 0; index < shorter.size(); index++) {
            if (!shorter.get(index).equals(longer.get(index))) {
                return false;
            }
        }
        return true;
    }

    /** Every proper (strict) ancestor of a segment list, joined back with {@code delimiter}. */
    private static List<String> strictPrefixes(List<String> segments, String delimiter) {
        if (segments.size() <= 1) {
            return List.of();
        }
        List<String> prefixes = new ArrayList<>();
        StringBuilder current = new StringBuilder(segments.get(0));
        prefixes.add(current.toString());
        for (int index = 1; index < segments.size() - 1; index++) {
            current.append(delimiter).append(segments.get(index));
            prefixes.add(current.toString());
        }
        return List.copyOf(prefixes);
    }

    /**
     * Split on a LITERAL delimiter, keeping trailing empty segments — {@code split(..., -1)}
     * semantics without handing the delimiter to the regex engine, where {@code .} (the default)
     * would match every character.
     *
     * <p>An empty delimiter is refused by name. Splitting on it would produce one segment per
     * character plus a trailing empty one, and a relation over THAT segmentation is not the
     * relation the policy stated — there is no delimiter for the field's stored path to be
     * compared on.
     */
    private static List<String> splitLiteral(String raw, String delimiter) {
        if (delimiter.isEmpty()) {
            throw unsupported("hierarchy delimiter is empty: a path cannot be split into "
                    + "segments on an empty delimiter, so the relation has no segment prefix to "
                    + "compare the field's stored path against");
        }
        List<String> parts = new ArrayList<>();
        int start = 0;
        int index;
        while ((index = raw.indexOf(delimiter, start)) >= 0) {
            parts.add(raw.substring(start, index));
            start = index + delimiter.length();
        }
        parts.add(raw.substring(start));
        return List.copyOf(parts);
    }
}
