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
 * The {@code size()} comparisons: recognising one in a comparison's operands, the rule that the
 * field must be DECLARED a collection, and the two emptiness polarities that are the only
 * thresholds a presence query can answer.
 *
 * <p>A seam of its own because it is the one leaf shape whose operand is a computed value the
 * adapter does translate, and the reason it can is narrow — a count against zero is a presence
 * test — so the recognition and the refusals are easier to audit beside each other than inside
 * the general leaf path, which they run ahead of.
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

    /**
     * The presence query a {@code size()} comparison lowers to under {@code polarity}, or
     * {@code null} when neither operand is a {@code size()} and the leaf path should have it.
     */
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
        // The planner preserves policy source order, so `0 < size(coll)` arrives with the VALUE
        // first. Scanning the operands for whichever one is the size() discards that order; the
        // operator then has to be mirrored, exactly as the leaf path already does, or `0 < size`
        // is read as `size < 0` and a supported emptiness check is refused as an unsupported
        // threshold (cerbos/query-plan-adapters#387).
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
        // `size(c) != 0` is the third spelling of non-emptiness, beside `size(c) > 0` and
        // `size(c) >= 1`; CEL's size() is a count, so the three are the same predicate.
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
     * The presence query a {@code size()} emptiness check lowers to, or a refusal when the caller
     * has not said the field is a collection at all.
     *
     * <p>The adapter is handed a plan, never a mapping, so it cannot tell {@code size(aString)}
     * — a string length — from {@code size(tagNames)} — an array count: the plan looks identical
     * either way. {@code exists} over a string field matches the indexed empty string, whose CEL
     * size is 0, and over a numeric field matches every document where CEL would raise a
     * no-overload error. So a field declared neither in {@code nestedPaths} nor in
     * {@code collectionFields} is refused, before the threshold is examined.
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
