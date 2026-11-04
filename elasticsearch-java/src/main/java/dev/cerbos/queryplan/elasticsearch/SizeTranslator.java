/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import static dev.cerbos.queryplan.elasticsearch.Refusals.exceptUnsupported;
import static dev.cerbos.queryplan.elasticsearch.Refusals.unsupported;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;

import java.util.List;
import java.util.Map;

/**
 * Translates {@code size()} comparisons. The Query DSL counts nothing, so a comparison translates
 * only when the sizes it accepts need no count: none of them, every one, zero alone, or every one
 * but zero. Over a collection the last is a presence query; over a string field, zero is a term
 * on the empty string, which Elasticsearch indexes.
 */
final class SizeTranslator {

    /**
     * Lucene rejects a term longer than this many bytes, so no indexed keyword holds a string of
     * more code points. A comparison every length up to here satisfies holds for every indexed
     * value.
     */
    static final int MAX_TERM_LENGTH = 32766;

    private final Options options;
    private final Scope root;
    private final CollectionTranslator collections;

    SizeTranslator(Options options, Scope root, CollectionTranslator collections) {
        this.options = options;
        this.root = root;
        this.collections = collections;
    }

    /** The sizes a comparison accepts, among the lengths an indexed value can have. */
    private enum Accepted { NONE, EVERY, ZERO, NON_ZERO, SOME }

    private record SizeComparison(Expression size, String operator, double value) {}

    /** The query for a {@code size()} comparison, or {@code null} if neither operand is one. */
    Map<String, Object> tryTranslate(String operator, List<Operand> operands, Polarity polarity) {
        SizeComparison comparison = resolveSizeComparison(operator, operands);
        if (comparison == null) return null;
        Accepted accepted = accepted(comparison.operator(), comparison.value(), polarity);
        if (accepted == Accepted.NONE) {
            return Queries.matchNone();
        }

        Operand sized = comparison.size().getOperands(0);
        if (sized.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "filter".equals(sized.getExpression().getOperator())) {
            return filteredLiteralListSize(sized.getExpression(), accepted, comparison);
        }
        if (sized.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw unsupported("Unsupported size() expression");
        }
        String variable = sized.getVariable();
        String field = root.field(variable);
        if (options.nestedPaths().contains(field) || options.collectionFields().contains(field)) {
            return collectionSize(variable, field, accepted, comparison);
        }
        ScalarType type = options.scalarTypes().get(field);
        if (type == null) {
            throw unsupported("size() over a field not declared as a collection: Elasticsearch cannot "
                    + "tell a string's length or a number from an array count, so size("
                    + variable + ") is refused unless '" + field
                    + "' is declared in nestedPaths or collectionFields, or its scalar type in scalarTypes");
        }
        if (type != ScalarType.STRING) {
            // CEL has no size() over a number, boolean or timestamp: an error, which denies.
            return Queries.matchNone();
        }
        return switch (accepted) {
            case EVERY -> Queries.exists(field);
            case ZERO -> Queries.term(field, "");
            case NON_ZERO -> Queries.definedAndNot(field, Queries.term(field, ""));
            default -> throw unsupported("size(" + variable + ") " + comparison.operator() + " "
                    + comparison.value() + " needs the string's length, which Elasticsearch Query DSL"
                    + " cannot compare without scripts; over a string field only emptiness, and"
                    + " thresholds every length or no length satisfies, translate");
        };
    }

    /**
     * Over a collection, only non-emptiness is a query. Elasticsearch does not index an empty
     * array, so a document whose collection is empty (size 0) is the document whose collection is
     * missing (a CEL error), and the other three answers cannot tell them apart.
     */
    private Map<String, Object> collectionSize(String variable, String field, Accepted accepted,
                                               SizeComparison comparison) {
        return switch (accepted) {
            case NON_ZERO -> options.nestedPaths().contains(field)
                    ? Queries.nestedQuery(field, Queries.matchAll())
                    : Queries.exists(field);
            case ZERO, EVERY -> throw unsupported("size(" + variable + ") emptiness cannot distinguish"
                    + " a missing collection from an empty collection in Elasticsearch");
            default -> throw unsupported("Unsupported size comparison: size(" + variable + ") "
                    + comparison.operator() + " " + comparison.value()
                    + ". Only emptiness checks (size > 0, size == 0) are supported.");
        };
    }

    /**
     * {@code size(L.filter(t, p)) > 0} over a literal list is {@code L.exists(t, p)}, and
     * {@code == 0} its negation: a literal list is never missing, so the fold is exact under
     * negation. The body is kept to a comparison of the element with one operand, whose error
     * (a missing attribute) is the same for every element, as it is for the filter.
     */
    private Map<String, Object> filteredLiteralListSize(Expression filter, Accepted accepted,
                                                        SizeComparison comparison) {
        if (filter.getOperandsCount() != 2 || filter.getOperands(0).getNodeCase() != Operand.NodeCase.VALUE
                || !isElementComparison(filter.getOperands(1))) {
            throw unsupported("size() of a filter() is refused: the Query DSL counts no elements,"
                    + " and only a filter over a literal list by a comparison of each element with"
                    + " one operand folds to the exists() it is non-empty by");
        }
        Polarity polarity = switch (accepted) {
            case NON_ZERO -> Polarity.TRUE;
            case ZERO -> Polarity.FALSE;
            case EVERY -> {
                yield null;
            }
            default -> throw unsupported("size() of a filter() compared with a count other than"
                    + " zero cannot be expressed: " + comparison.operator() + " " + comparison.value()
                    + " needs the Query DSL to count the elements the predicate keeps");
        };
        if (polarity == null) {
            return Queries.matchAll();
        }
        return collections.translateMacro("exists", filter.getOperandsList(), root, polarity);
    }

    private static boolean isElementComparison(Operand lambda) {
        if (lambda.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambda.getExpression().getOperator())
                || lambda.getExpression().getOperandsCount() != 2
                || lambda.getExpression().getOperands(1).getNodeCase() != Operand.NodeCase.VARIABLE
                || lambda.getExpression().getOperands(0).getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return false;
        }
        String variable = lambda.getExpression().getOperands(1).getVariable();
        Expression body = lambda.getExpression().getOperands(0).getExpression();
        if (!List.of("eq", "ne", "in").contains(body.getOperator()) || body.getOperandsCount() != 2) {
            return false;
        }
        for (int side = 0; side < 2; side++) {
            Operand element = body.getOperands(side);
            Operand other = body.getOperands(1 - side);
            if (element.getNodeCase() == Operand.NodeCase.VARIABLE && element.getVariable().equals(variable)
                    && other.getNodeCase() != Operand.NodeCase.EXPRESSION) {
                return true;
            }
        }
        return false;
    }

    /**
     * Which lengths in {@code [0, MAX_TERM_LENGTH]} satisfy {@code size op value} under the
     * polarity. An ordering accepts a prefix or a suffix of the range, so its ends and the length
     * one decide it; CEL's size is a non-negative integer, so a fractional threshold is never
     * equal.
     */
    private static Accepted accepted(String operator, double value, Polarity polarity) {
        boolean zero = holds(operator, 0, value) == polarity.holds();
        boolean one = holds(operator, 1, value) == polarity.holds();
        boolean last = holds(operator, MAX_TERM_LENGTH, value) == polarity.holds();
        boolean equality = "eq".equals(operator) || "ne".equals(operator);
        boolean integral = value == Math.rint(value) && value >= 0 && value <= MAX_TERM_LENGTH;
        // For an ordering the lengths above zero are all accepted or all rejected when its two
        // ends agree; for an equality, when the threshold is no length at all.
        boolean rest = equality ? (!integral || value == 0) && one : one && last;
        boolean none = equality ? (!integral || value == 0) && !one : !one && !last;
        if (!zero && none) return Accepted.NONE;
        if (zero && rest) return Accepted.EVERY;
        if (zero && none) return Accepted.ZERO;
        if (!zero && rest) return Accepted.NON_ZERO;
        return Accepted.SOME;
    }

    private static boolean holds(String operator, long length, double value) {
        return switch (operator) {
            case "eq" -> length == value;
            case "ne" -> length != value;
            case "lt" -> length < value;
            case "le" -> length <= value;
            case "gt" -> length > value;
            case "ge" -> length >= value;
            default -> throw unsupported("Unsupported size comparison operator: " + operator);
        };
    }

    private SizeComparison resolveSizeComparison(String operator, List<Operand> operands) {
        Expression sizeExpression = null;
        Double value = null;
        // `0 < size(c)` arrives value-first, so the operator is mirrored when size() is second.
        boolean sizeFirst = true;
        for (int i = 0; i < operands.size(); i++) {
            Operand operand = operands.get(i);
            switch (operand.getNodeCase()) {
                case EXPRESSION -> {
                    if ("size".equals(operand.getExpression().getOperator())) {
                        sizeExpression = operand.getExpression();
                        sizeFirst = i == 0;
                    }
                }
                case VALUE -> {
                    Object resolved = PlanValues.protoValueToJava(operand.getValue());
                    if (resolved instanceof Number number) value = number.doubleValue();
                }
                default -> {}
            }
        }
        if (sizeExpression == null) return null;
        String normalizedOperator = LeafTranslator.normalizeLeafOperator(operator, sizeFirst);

        List<Operand> sizeOperands = sizeExpression.getOperandsList();
        if (sizeOperands.size() == 1
                && sizeOperands.get(0).getNodeCase() == Operand.NodeCase.EXPRESSION
                && "except".equals(sizeOperands.get(0).getExpression().getOperator())) {
            throw exceptUnsupported();
        }
        if (sizeOperands.size() != 1) {
            throw unsupported("Unsupported size() expression");
        }
        if (value == null || !Double.isFinite(value)) {
            throw unsupported("size comparison requires a finite numeric value");
        }
        return new SizeComparison(sizeExpression, normalizedOperator, value);
    }
}
