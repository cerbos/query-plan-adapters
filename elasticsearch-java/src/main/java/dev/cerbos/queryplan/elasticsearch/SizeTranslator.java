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

import java.util.List;
import java.util.Map;

/**
 * Translates {@code size()} comparisons. Only emptiness checks are supported, as a presence query
 * over a field declared as a collection.
 */
final class SizeTranslator {

    private final Options options;
    private final Scope root;

    SizeTranslator(Options options, Scope root) {
        this.options = options;
        this.root = root;
    }

    private enum SizeThreshold { NON_EMPTY, EMPTY, UNSUPPORTED }

    private record SizeComparison(
            String variable,
            String field,
            String operator,
            double value,
            SizeThreshold threshold) {}

    /** The query for a {@code size()} comparison, or {@code null} if neither operand is one. */
    Map<String, Object> tryTranslate(String operator, List<Operand> operands, Polarity polarity) {
        SizeComparison comparison = resolveSizeComparison(operator, operands);
        if (comparison == null) return null;

        Map<String, Object> present = collectionPresentQuery(comparison);
        return switch (comparison.threshold()) {
            case NON_EMPTY -> {
                if (!polarity.holds()) throw unsafeEmptyCollectionSize(comparison.variable());
                yield present;
            }
            case EMPTY -> {
                if (polarity.holds()) throw unsafeEmptyCollectionSize(comparison.variable());
                yield present;
            }
            case UNSUPPORTED -> throw unsupportedSizeComparison(comparison);
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
        if (sizeOperands.size() != 1
                || sizeOperands.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw unsupported("Unsupported size() expression");
        }
        if (value == null || !Double.isFinite(value)) {
            throw unsupported("size comparison requires a finite numeric value");
        }

        String variable = sizeOperands.get(0).getVariable();
        boolean nonEmpty = (normalizedOperator.equals("gt") && value == 0.0)
                || (normalizedOperator.equals("ge") && value == 1.0)
                || (normalizedOperator.equals("ne") && value == 0.0);
        boolean empty = (normalizedOperator.equals("eq") && value == 0.0)
                || (normalizedOperator.equals("le") && value == 0.0)
                || (normalizedOperator.equals("lt") && value == 1.0);
        return new SizeComparison(
                variable,
                root.field(variable),
                normalizedOperator,
                value,
                nonEmpty ? SizeThreshold.NON_EMPTY : empty ? SizeThreshold.EMPTY : SizeThreshold.UNSUPPORTED);
    }

    /**
     * The "collection is non-empty" query, or a refusal if the field is not declared as a
     * collection. The plan cannot tell a string length from an array count, and {@code exists}
     * would match an empty string or a number, which CEL would not.
     */
    private Map<String, Object> collectionPresentQuery(SizeComparison comparison) {
        String field = comparison.field();
        if (options.nestedPaths().contains(field)) {
            return Queries.nestedQuery(field, Queries.matchAll());
        }
        if (options.collectionFields().contains(field)) {
            return Queries.exists(field);
        }
        throw unsupported("size() over a field not declared as a collection: Elasticsearch cannot "
                + "tell a string's length or a number from an array count, so size("
                + comparison.variable() + ") is refused unless '" + field
                + "' is declared in nestedPaths or collectionFields");
    }

    private static UnsupportedPlanShapeException unsupportedSizeComparison(SizeComparison comparison) {
        return unsupported(
                "Unsupported size comparison: size(" + comparison.variable() + ") "
                        + comparison.operator() + " " + comparison.value()
                        + ". Only emptiness checks (size > 0, size == 0) are supported.");
    }

    private static UnsupportedPlanShapeException unsafeEmptyCollectionSize(String variable) {
        return unsupported(
                "size(" + variable + ") emptiness cannot distinguish a missing collection "
                        + "from an empty collection in Elasticsearch");
    }
}
