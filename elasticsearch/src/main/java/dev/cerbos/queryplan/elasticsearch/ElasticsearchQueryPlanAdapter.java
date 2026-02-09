package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ElasticsearchQueryPlanAdapter {

    public sealed interface Result permits Result.AlwaysAllowed, Result.AlwaysDenied, Result.Conditional {
        record AlwaysAllowed() implements Result {}
        record AlwaysDenied() implements Result {}
        record Conditional(Map<String, Object> query) implements Result {}
    }

    private static final Map<String, OperatorFunction> DEFAULT_OPERATORS = Map.ofEntries(
            Map.entry("eq", (field, value) ->
                    Map.of("term", Map.of(field, Map.of("value", value)))),
            Map.entry("ne", (field, value) ->
                    Map.of("bool", Map.of("must_not", List.of(
                            Map.of("term", Map.of(field, Map.of("value", value))))))),
            Map.entry("lt", (field, value) ->
                    Map.of("range", Map.of(field, Map.of("lt", value)))),
            Map.entry("gt", (field, value) ->
                    Map.of("range", Map.of(field, Map.of("gt", value)))),
            Map.entry("le", (field, value) ->
                    Map.of("range", Map.of(field, Map.of("lte", value)))),
            Map.entry("ge", (field, value) ->
                    Map.of("range", Map.of(field, Map.of("gte", value)))),
            Map.entry("in", (field, value) ->
                    Map.of("terms", Map.of(field, value instanceof List<?> l ? l : List.of(value)))),
            Map.entry("contains", (field, value) ->
                    Map.of("wildcard", Map.of(field, Map.of("value", "*" + value + "*")))),
            Map.entry("startsWith", (field, value) ->
                    Map.of("prefix", Map.of(field, Map.of("value", value)))),
            Map.entry("endsWith", (field, value) ->
                    Map.of("wildcard", Map.of(field, Map.of("value", "*" + value))))
    );

    private ElasticsearchQueryPlanAdapter() {}

    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap) {
        return toElasticsearchQuery(planResult, fieldMap, Map.of());
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides) {
        if (planResult.isAlwaysAllowed()) {
            return new Result.AlwaysAllowed();
        }
        if (planResult.isAlwaysDenied()) {
            return new Result.AlwaysDenied();
        }

        Operand condition = planResult.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Conditional plan has no condition"));

        return new Result.Conditional(traverseOperand(condition, fieldMap, operatorOverrides));
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap) {
        return toElasticsearchQuery(response, fieldMap, Map.of());
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides) {
        PlanResourcesFilter filter = response.getFilter();
        return switch (filter.getKind()) {
            case KIND_ALWAYS_ALLOWED -> new Result.AlwaysAllowed();
            case KIND_ALWAYS_DENIED -> new Result.AlwaysDenied();
            case KIND_CONDITIONAL -> {
                Operand condition = filter.getCondition();
                if (condition.getNodeCase() == Operand.NodeCase.NODE_NOT_SET) {
                    throw new IllegalArgumentException("Conditional plan has no condition");
                }
                yield new Result.Conditional(traverseOperand(condition, fieldMap, operatorOverrides));
            }
            default -> throw new IllegalArgumentException("Unknown filter kind: " + filter.getKind());
        };
    }

    private static Map<String, Object> traverseOperand(
            Operand operand,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides) {
        return switch (operand.getNodeCase()) {
            case EXPRESSION -> traverseExpression(operand.getExpression(), fieldMap, overrides);
            case VARIABLE -> {
                String field = fieldMap.get(operand.getVariable());
                if (field == null) {
                    throw new IllegalArgumentException("Unknown attribute: " + operand.getVariable());
                }
                OperatorFunction fn = overrides.getOrDefault("eq", DEFAULT_OPERATORS.get("eq"));
                yield fn.apply(field, true);
            }
            default -> throw new IllegalArgumentException(
                    "Unexpected operand type: " + operand.getNodeCase());
        };
    }

    private static Map<String, Object> traverseExpression(
            Expression expression,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides) {
        String operator = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();

        return switch (operator) {
            case "and" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperand(o, fieldMap, overrides))
                        .toList();
                yield Map.of("bool", Map.of("must", clauses));
            }
            case "or" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperand(o, fieldMap, overrides))
                        .toList();
                yield Map.of("bool", Map.of("should", clauses, "minimum_should_match", 1));
            }
            case "not" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperand(o, fieldMap, overrides))
                        .toList();
                yield Map.of("bool", Map.of("must_not", clauses));
            }
            default -> applyLeafOperator(operator, operands, fieldMap, overrides);
        };
    }

    private static Map<String, Object> applyLeafOperator(
            String operator,
            List<Operand> operands,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides) {
        String variable = null;
        Object value = null;

        for (Operand op : operands) {
            switch (op.getNodeCase()) {
                case VARIABLE -> variable = op.getVariable();
                case VALUE -> value = protoValueToJava(op.getValue());
                default -> throw new IllegalArgumentException(
                        "Unexpected operand type in leaf expression: " + op.getNodeCase());
            }
        }

        if (variable == null) {
            throw new IllegalArgumentException("Missing variable in expression");
        }

        String field = fieldMap.get(variable);
        if (field == null) {
            throw new IllegalArgumentException("Unknown attribute: " + variable);
        }

        OperatorFunction fn = overrides.get(operator);
        if (fn == null) {
            fn = DEFAULT_OPERATORS.get(operator);
        }
        if (fn == null) {
            throw new IllegalArgumentException("Unknown operator: " + operator);
        }

        return fn.apply(field, value);
    }

    static Object protoValueToJava(Value value) {
        return switch (value.getKindCase()) {
            case STRING_VALUE -> value.getStringValue();
            case NUMBER_VALUE -> {
                double d = value.getNumberValue();
                if (d == Math.floor(d) && !Double.isInfinite(d)) {
                    yield (long) d;
                }
                yield d;
            }
            case BOOL_VALUE -> value.getBoolValue();
            case NULL_VALUE -> null;
            case LIST_VALUE -> value.getListValue().getValuesList().stream()
                    .map(ElasticsearchQueryPlanAdapter::protoValueToJava)
                    .toList();
            case STRUCT_VALUE -> value.getStructValue().getFieldsMap().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> protoValueToJava(e.getValue())));
            default -> throw new IllegalArgumentException(
                    "Unsupported protobuf value type: " + value.getKindCase());
        };
    }
}
