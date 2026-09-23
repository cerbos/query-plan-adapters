/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.malformed;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Translates {@code ancestorOf}, {@code descendentOf} and {@code overlaps}. A hierarchy is a
 * delimited path, and the relations compare segment prefixes. With one side a field and the other
 * a constant:
 *
 * <pre>
 *   field is a strict descendant -&gt; prefix on &lt;constant&gt;&lt;delimiter&gt;
 *   field is a strict ancestor   -&gt; terms over the constant's proper prefixes
 *   overlaps                     -&gt; bool.should of both, plus a term on the whole constant
 * </pre>
 */
final class HierarchyTranslator {

    private final Scope root;
    private final Map<String, ScalarType> scalarTypes;

    HierarchyTranslator(Scope root, Map<String, ScalarType> scalarTypes) {
        this.root = root;
        this.scalarTypes = scalarTypes;
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
     * Translates one relation. Negated {@code overlaps} requires the field to exist; negated
     * {@code ancestorOf}/{@code descendentOf} is refused. Operator overrides do not apply here.
     */
    Map<String, Object> translate(String operator, List<Operand> operands, Polarity polarity) {
        if (comparesANonStringField(operands)) {
            // A hierarchy over a non-string field is a CEL type error, so nothing matches.
            return Queries.matchNone();
        }
        if (!polarity.holds() && !"overlaps".equals(operator)) {
            throw negatedHierarchy(operator);
        }
        if (operands.size() != 2) {
            throw malformed(operator + " requires exactly 2 operands, got " + operands.size());
        }
        Hierarchy left = normalizeHierarchy(resolveHierarchy(operator, operands.get(0)));
        Hierarchy right = normalizeHierarchy(resolveHierarchy(operator, operands.get(1)));
        Map<String, Object> positive = "overlaps".equals(operator)
                ? hierarchyOverlaps(operator, left, right)
                : hierarchyStrict(operator, left, right);
        if (polarity.holds()) return positive;
        if (left instanceof Hierarchy.FieldRef field) {
            return Queries.definedAndNot(field.field(), positive);
        }
        if (right instanceof Hierarchy.FieldRef field) {
            return Queries.definedAndNot(field.field(), positive);
        }
        throw negatedHierarchy(operator);
    }

    private boolean comparesANonStringField(List<Operand> operands) {
        for (Operand operand : operands) {
            if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"hierarchy".equals(operand.getExpression().getOperator())
                    || operand.getExpression().getOperandsCount() == 0) {
                continue;
            }
            Operand path = operand.getExpression().getOperands(0);
            if (path.getNodeCase() == Operand.NodeCase.VARIABLE) {
                ScalarType type = scalarTypes.get(root.field(path.getVariable()));
                if (type != null && type != ScalarType.STRING) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Map<String, Object> hierarchyStrict(
            String operator, Hierarchy left, Hierarchy right) {
        boolean isAncestor = "ancestorOf".equals(operator);
        Hierarchy ancestor = isAncestor ? left : right;
        Hierarchy descendant = isAncestor ? right : left;

        if (ancestor instanceof Hierarchy.Constant constant
                && descendant instanceof Hierarchy.FieldRef field) {
            String prefix = String.join(field.delimiter(), constant.segments())
                    + field.delimiter();
            return Queries.prefix(field.field(), prefix);
        }
        if (ancestor instanceof Hierarchy.FieldRef field
                && descendant instanceof Hierarchy.Constant constant) {
            List<String> prefixes = strictPrefixes(constant.segments(), field.delimiter());
            // A one-segment path has no strict ancestor.
            return prefixes.isEmpty() ? Queries.matchNone() : Queries.terms(field.field(), prefixes);
        }
        if (ancestor instanceof Hierarchy.Constant a && descendant instanceof Hierarchy.Constant d) {
            // The planner folds a false constant comparison away, so only a true one is expected.
            if (d.segments().size() > a.segments().size() && isPrefixOf(a.segments(), d.segments())) {
                return Queries.matchAll();
            }
            throw unfoldedConstantHierarchy(operator);
        }
        throw unsupportedHierarchyOperands(operator, left, right);
    }

    private static Map<String, Object> hierarchyOverlaps(
            String operator, Hierarchy left, Hierarchy right) {
        if (left instanceof Hierarchy.Constant a && right instanceof Hierarchy.Constant b) {
            // The shorter path must be a prefix of the longer.
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
        // The field is a strict ancestor of the constant,
        List<String> prefixes = strictPrefixes(constant.segments(), delimiter);
        if (!prefixes.isEmpty()) {
            clauses.add(Queries.terms(field.field(), prefixes));
        }
        // or equal to it,
        clauses.add(Queries.term(field.field(), whole));
        // or a strict descendant of it.
        clauses.add(Queries.prefix(field.field(), whole + delimiter));
        return Queries.boolShould(List.copyOf(clauses));
    }

    private static MalformedPlanException unfoldedConstantHierarchy(String operator) {
        return malformed(operator
                + ": constant hierarchy operands do not satisfy the relation, so the planner"
                + " should have folded this comparison rather than emitting it");
    }

    /**
     * Resolves {@code hierarchy(<value|field>)} (delimiter {@code .}),
     * {@code hierarchy(<value|field>, <delimiter>)} or {@code hierarchy(list(<segments>))}.
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
            String delimiter = literalString(delimiterOperand);
            if (!isValueOrField(path)) {
                throw malformed("hierarchy(path, delimiter) requires a value or field path");
            }
            return pathHierarchy(path, delimiter);
        }
        if (operands.size() == 1) {
            Operand inner = operands.get(0);
            if (isValueOrField(inner)) {
                return pathHierarchy(inner, ".");
            }
            if (inner.getNodeCase() != Operand.NodeCase.EXPRESSION) {
                throw malformed("hierarchy requires a value, field or list operand, got "
                        + inner.getNodeCase());
            }
            if (!"list".equals(inner.getExpression().getOperator())) {
                throw malformed("hierarchy requires a value, field or list operand, got "
                        + inner.getExpression().getOperator());
            }
            List<HierarchySegment> segments = new ArrayList<>();
            for (Operand segment : inner.getExpression().getOperandsList()) {
                segments.add(switch (segment.getNodeCase()) {
                    case VALUE -> new HierarchySegment.Literal(literalString(segment));
                    case VARIABLE -> new HierarchySegment.Field(root.field(segment.getVariable()));
                    default -> throw malformed(
                            "hierarchy list segment must be a value or a field, got "
                                    + segment.getNodeCase());
                });
            }
            return new Hierarchy.Segmented(List.copyOf(segments));
        }
        throw malformed("hierarchy requires 1 or 2 operands, got " + operands.size());
    }

    private static boolean isValueOrField(Operand operand) {
        return operand.getNodeCase() == Operand.NodeCase.VALUE
                || operand.getNodeCase() == Operand.NodeCase.VARIABLE;
    }

    private Hierarchy pathHierarchy(Operand path, String delimiter) {
        if (path.getNodeCase() == Operand.NodeCase.VALUE) {
            return new Hierarchy.Constant(splitLiteral(literalString(path), delimiter), delimiter);
        }
        return new Hierarchy.FieldRef(root.field(path.getVariable()), delimiter);
    }

    private static String literalString(Operand value) {
        return String.valueOf(PlanValues.protoValueToJava(value.getValue()));
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
     * A {@code list()} path with a field segment would need the field concatenated into the path;
     * two field operands are a field-to-field comparison. Neither is expressible.
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
     * A bare {@code bool.must_not} would match documents missing the field, where CEL errors. An
     * {@code exists} guard would fix that, but the corpus has no negated strict-ancestry action to
     * prove it yet, so it stays refused.
     */
    private static UnsupportedPlanShapeException negatedHierarchy(String operator) {
        return unsupported("Negated " + operator + " cannot be expressed safely: "
                + "a bool.must_not over the prefix/terms/term queries a hierarchy relation lowers "
                + "to matches every document that has no value for the field");
    }

    private static boolean isPrefixOf(List<String> shorter, List<String> longer) {
        for (int index = 0; index < shorter.size(); index++) {
            if (!shorter.get(index).equals(longer.get(index))) {
                return false;
            }
        }
        return true;
    }

    /** Every strict ancestor of a segment list, joined with {@code delimiter}. */
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
     * Splits on a literal delimiter, keeping trailing empty segments. {@link String#split} would
     * treat the default {@code .} as a regex. An empty delimiter is refused.
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
