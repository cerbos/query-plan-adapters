package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ElasticsearchQueryPlanAdapter {

    public sealed interface Result permits Result.AlwaysAllowed, Result.AlwaysDenied, Result.Conditional {
        record AlwaysAllowed() implements Result {}
        record AlwaysDenied() implements Result {}
        record Conditional(Map<String, Object> query) implements Result {}
    }

    private record LambdaScope(String nestedPath, String lambdaVariable) {}

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
                    Map.of("wildcard", Map.of(field, Map.of("value", "*" + escapeWildcard(value) + "*")))),
            Map.entry("startsWith", (field, value) ->
                    Map.of("prefix", Map.of(field, Map.of("value", value)))),
            Map.entry("endsWith", (field, value) ->
                    Map.of("wildcard", Map.of(field, Map.of("value", "*" + escapeWildcard(value))))),
            Map.entry("hasIntersection", (field, value) ->
                    Map.of("terms", Map.of(field, value instanceof List<?> l ? l : List.of(value)))),
            Map.entry("isSet", (field, value) ->
                    Boolean.TRUE.equals(value)
                            ? Map.of("exists", Map.of("field", field))
                            : Map.of("bool", Map.of("must_not", List.of(
                                    Map.of("exists", Map.of("field", field))))))
    );

    private ElasticsearchQueryPlanAdapter() {}

    // --- PlanResourcesResult overloads ---

    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap) {
        return toElasticsearchQuery(planResult, fieldMap, Map.of(), Set.of());
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides) {
        return toElasticsearchQuery(planResult, fieldMap, operatorOverrides, Set.of());
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap,
            Set<String> nestedPaths) {
        return toElasticsearchQuery(planResult, fieldMap, Map.of(), nestedPaths);
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResult planResult,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides,
            Set<String> nestedPaths) {
        if (planResult.isAlwaysAllowed()) {
            return new Result.AlwaysAllowed();
        }
        if (planResult.isAlwaysDenied()) {
            return new Result.AlwaysDenied();
        }

        Operand condition = planResult.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Conditional plan has no condition"));

        return new Result.Conditional(traverseOperand(condition, fieldMap, operatorOverrides, nestedPaths));
    }

    // --- PlanResourcesResponse overloads ---

    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap) {
        return toElasticsearchQuery(response, fieldMap, Map.of(), Set.of());
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides) {
        return toElasticsearchQuery(response, fieldMap, operatorOverrides, Set.of());
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap,
            Set<String> nestedPaths) {
        return toElasticsearchQuery(response, fieldMap, Map.of(), nestedPaths);
    }

    public static Result toElasticsearchQuery(
            PlanResourcesResponse response,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> operatorOverrides,
            Set<String> nestedPaths) {
        PlanResourcesFilter filter = response.getFilter();
        return switch (filter.getKind()) {
            case KIND_ALWAYS_ALLOWED -> new Result.AlwaysAllowed();
            case KIND_ALWAYS_DENIED -> new Result.AlwaysDenied();
            case KIND_CONDITIONAL -> {
                Operand condition = filter.getCondition();
                if (condition.getNodeCase() == Operand.NodeCase.NODE_NOT_SET) {
                    throw new IllegalArgumentException("Conditional plan has no condition");
                }
                yield new Result.Conditional(traverseOperand(condition, fieldMap, operatorOverrides, nestedPaths));
            }
            default -> throw new IllegalArgumentException("Unknown filter kind: " + filter.getKind());
        };
    }

    // --- Traversal (unscoped) ---

    private static Map<String, Object> traverseOperand(
            Operand operand,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        return switch (operand.getNodeCase()) {
            case EXPRESSION -> traverseExpression(operand.getExpression(), fieldMap, overrides, nestedPaths);
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
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        String operator = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();

        return switch (operator) {
            case "and" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperand(o, fieldMap, overrides, nestedPaths))
                        .toList();
                yield Map.of("bool", Map.of("must", clauses));
            }
            case "or" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperand(o, fieldMap, overrides, nestedPaths))
                        .toList();
                yield Map.of("bool", Map.of("should", clauses, "minimum_should_match", 1));
            }
            case "not" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperand(o, fieldMap, overrides, nestedPaths))
                        .toList();
                yield Map.of("bool", Map.of("must_not", clauses));
            }
            case "exists", "all", "except" ->
                    handleCollectionOperator(operator, operands, fieldMap, overrides, nestedPaths);
            case "hasIntersection" ->
                    handleHasIntersection(operands, fieldMap, overrides, nestedPaths);
            default -> {
                Map<String, Object> sizeResult = trySizeComparison(operator, operands, fieldMap);
                if (sizeResult != null) {
                    yield sizeResult;
                }
                yield applyLeafOperator(operator, operands, fieldMap, overrides);
            }
        };
    }

    // --- Collection operators (exists, all, except) ---

    private static Map<String, Object> handleCollectionOperator(
            String operator,
            List<Operand> operands,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException(
                    operator + " requires exactly 2 operands, got " + operands.size());
        }

        Operand listOperand = operands.get(0);
        Operand lambdaOperand = operands.get(1);

        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw new IllegalArgumentException(
                    operator + " first operand must be a variable, got " + listOperand.getNodeCase());
        }

        String cerbosAttr = listOperand.getVariable();
        String esField = fieldMap.get(cerbosAttr);
        if (esField == null) {
            throw new IllegalArgumentException("Unknown attribute: " + cerbosAttr);
        }

        if (!nestedPaths.contains(esField)) {
            throw new IllegalArgumentException(
                    "Field '" + esField + "' is not declared in nestedPaths. "
                            + "Collection operators require nested mappings.");
        }

        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            throw new IllegalArgumentException(
                    operator + " second operand must be a lambda expression");
        }

        Expression lambdaExpr = lambdaOperand.getExpression();
        if (!"lambda".equals(lambdaExpr.getOperator())) {
            throw new IllegalArgumentException(
                    operator + " second operand must be a lambda, got " + lambdaExpr.getOperator());
        }

        List<Operand> lambdaOperands = lambdaExpr.getOperandsList();
        if (lambdaOperands.size() != 2) {
            throw new IllegalArgumentException("lambda requires exactly 2 operands");
        }

        Operand bodyOperand = lambdaOperands.get(0);

        Operand lambdaVarOperand = lambdaOperands.get(1);
        if (lambdaVarOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw new IllegalArgumentException("lambda second operand must be a variable");
        }
        String lambdaVar = lambdaVarOperand.getVariable();
        LambdaScope scope = new LambdaScope(esField, lambdaVar);
        Map<String, Object> innerQuery = traverseOperandScoped(bodyOperand, scope, overrides, nestedPaths);

        return switch (operator) {
            case "exists" -> Map.of("nested", Map.of("path", esField, "query", innerQuery));
            case "all" -> Map.of("bool", Map.of("must_not", List.of(
                    Map.of("nested", Map.of("path", esField, "query",
                            Map.of("bool", Map.of("must_not", List.of(innerQuery))))))));
            case "except" -> Map.of("nested", Map.of("path", esField, "query",
                    Map.of("bool", Map.of("must_not", List.of(innerQuery)))));
            default -> throw new IllegalArgumentException("Unknown collection operator: " + operator);
        };
    }

    // --- hasIntersection (flat + nested/map) ---

    private static Map<String, Object> handleHasIntersection(
            List<Operand> operands,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("hasIntersection requires exactly 2 operands");
        }

        Operand first = operands.get(0);
        Operand second = operands.get(1);

        if (first.getNodeCase() == Operand.NodeCase.EXPRESSION
                && "map".equals(first.getExpression().getOperator())) {
            return handleMapHasIntersection(first.getExpression(), second, fieldMap, nestedPaths);
        }

        return applyLeafOperator("hasIntersection", operands, fieldMap, overrides);
    }

    private static Map<String, Object> handleMapHasIntersection(
            Expression mapExpr,
            Operand valuesOperand,
            Map<String, String> fieldMap,
            Set<String> nestedPaths) {
        List<Operand> mapOperands = mapExpr.getOperandsList();
        if (mapOperands.size() != 2) {
            throw new IllegalArgumentException("map requires exactly 2 operands");
        }

        Operand listOperand = mapOperands.get(0);
        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw new IllegalArgumentException("map first operand must be a variable");
        }

        String cerbosAttr = listOperand.getVariable();
        String esField = fieldMap.get(cerbosAttr);
        if (esField == null) {
            throw new IllegalArgumentException("Unknown attribute: " + cerbosAttr);
        }

        if (!nestedPaths.contains(esField)) {
            throw new IllegalArgumentException(
                    "Field '" + esField + "' is not declared in nestedPaths. "
                            + "map+hasIntersection requires nested mappings.");
        }

        Operand lambdaOperand = mapOperands.get(1);
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw new IllegalArgumentException("map second operand must be a lambda");
        }

        Expression lambdaExpr = lambdaOperand.getExpression();
        List<Operand> lambdaOperands = lambdaExpr.getOperandsList();
        if (lambdaOperands.size() != 2) {
            throw new IllegalArgumentException("lambda requires exactly 2 operands");
        }

        Operand projectionOperand = lambdaOperands.get(0);
        String lambdaVar = lambdaOperands.get(1).getVariable();

        if (projectionOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw new IllegalArgumentException(
                    "map lambda body must be a simple variable projection");
        }

        String projectionVar = projectionOperand.getVariable();
        String suffix = extractLambdaSuffix(projectionVar, lambdaVar);
        String nestedField = esField + "." + suffix;

        if (valuesOperand.getNodeCase() != Operand.NodeCase.VALUE) {
            throw new IllegalArgumentException("hasIntersection second operand must be a value list");
        }

        Object values = protoValueToJava(valuesOperand.getValue());
        List<?> valueList = values instanceof List<?> l ? l : List.of(values);

        return Map.of("nested", Map.of("path", esField, "query",
                Map.of("terms", Map.of(nestedField, valueList))));
    }

    // --- Scoped traversal (inside lambda) ---

    private static Map<String, Object> traverseOperandScoped(
            Operand operand,
            LambdaScope scope,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        return switch (operand.getNodeCase()) {
            case EXPRESSION -> traverseExpressionScoped(operand.getExpression(), scope, overrides, nestedPaths);
            case VARIABLE -> {
                String field = resolveScopedVariable(operand.getVariable(), scope);
                OperatorFunction fn = overrides.getOrDefault("eq", DEFAULT_OPERATORS.get("eq"));
                yield fn.apply(field, true);
            }
            default -> throw new IllegalArgumentException(
                    "Unexpected operand type: " + operand.getNodeCase());
        };
    }

    private static Map<String, Object> traverseExpressionScoped(
            Expression expression,
            LambdaScope scope,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        String operator = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();

        return switch (operator) {
            case "and" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperandScoped(o, scope, overrides, nestedPaths))
                        .toList();
                yield Map.of("bool", Map.of("must", clauses));
            }
            case "or" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperandScoped(o, scope, overrides, nestedPaths))
                        .toList();
                yield Map.of("bool", Map.of("should", clauses, "minimum_should_match", 1));
            }
            case "not" -> {
                List<Map<String, Object>> clauses = operands.stream()
                        .map(o -> traverseOperandScoped(o, scope, overrides, nestedPaths))
                        .toList();
                yield Map.of("bool", Map.of("must_not", clauses));
            }
            default -> applyScopedLeafOperator(operator, operands, scope, overrides);
        };
    }

    private static Map<String, Object> applyScopedLeafOperator(
            String operator,
            List<Operand> operands,
            LambdaScope scope,
            Map<String, OperatorFunction> overrides) {
        String variable = null;
        Object value = null;

        for (Operand op : operands) {
            switch (op.getNodeCase()) {
                case VARIABLE -> variable = op.getVariable();
                case VALUE -> value = protoValueToJava(op.getValue());
                default -> throw new IllegalArgumentException(
                        "Unexpected operand type in scoped leaf expression: " + op.getNodeCase());
            }
        }

        if (variable == null) {
            throw new IllegalArgumentException("Missing variable in expression");
        }

        String field = resolveScopedVariable(variable, scope);

        if (value == null) {
            return switch (operator) {
                case "eq" -> Map.of("bool", Map.of("must_not", List.of(
                        Map.of("exists", Map.of("field", field)))));
                case "ne" -> Map.of("exists", Map.of("field", field));
                default -> throw new IllegalArgumentException(
                        "Null values are only supported with eq and ne operators");
            };
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

    private static String resolveScopedVariable(String variable, LambdaScope scope) {
        String suffix = extractLambdaSuffix(variable, scope.lambdaVariable());
        return scope.nestedPath() + "." + suffix;
    }

    private static String extractLambdaSuffix(String variable, String lambdaVar) {
        String prefix = lambdaVar + ".";
        if (!variable.startsWith(prefix)) {
            throw new IllegalArgumentException(
                    "Variable '" + variable + "' does not start with lambda variable '" + lambdaVar + "'");
        }
        return variable.substring(prefix.length());
    }

    // --- Size comparisons ---

    private static Map<String, Object> trySizeComparison(
            String operator,
            List<Operand> operands,
            Map<String, String> fieldMap) {
        Expression sizeExpr = null;
        long numValue = -1;

        for (Operand op : operands) {
            switch (op.getNodeCase()) {
                case EXPRESSION -> {
                    if ("size".equals(op.getExpression().getOperator())) {
                        sizeExpr = op.getExpression();
                    }
                }
                case VALUE -> {
                    Object v = protoValueToJava(op.getValue());
                    if (v instanceof Number n) {
                        numValue = n.longValue();
                    }
                }
                default -> {}
            }
        }

        if (sizeExpr == null) {
            return null;
        }

        List<Operand> sizeOperands = sizeExpr.getOperandsList();
        if (sizeOperands.size() != 1 || sizeOperands.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw new IllegalArgumentException("Unsupported size() expression");
        }

        String variable = sizeOperands.get(0).getVariable();
        String field = fieldMap.get(variable);
        if (field == null) {
            throw new IllegalArgumentException("Unknown attribute: " + variable);
        }

        boolean nonEmpty = (operator.equals("gt") && numValue == 0)
                || (operator.equals("ge") && numValue == 1);
        boolean empty = (operator.equals("eq") && numValue == 0)
                || (operator.equals("le") && numValue == 0)
                || (operator.equals("lt") && numValue == 1);

        if (nonEmpty) {
            return Map.of("exists", Map.of("field", field));
        }
        if (empty) {
            return Map.of("bool", Map.of("must_not", List.of(
                    Map.of("exists", Map.of("field", field)))));
        }

        throw new IllegalArgumentException(
                "Unsupported size comparison: size(" + variable + ") " + operator + " " + numValue
                        + ". Only emptiness checks (size > 0, size == 0) are supported.");
    }

    // --- Leaf operators ---

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

        if (value == null) {
            return switch (operator) {
                case "eq" -> Map.of("bool", Map.of("must_not", List.of(
                        Map.of("exists", Map.of("field", field)))));
                case "ne" -> Map.of("exists", Map.of("field", field));
                default -> throw new IllegalArgumentException(
                        "Null values are only supported with eq and ne operators");
            };
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

    private static String escapeWildcard(Object value) {
        return value.toString()
                .replace("\\", "\\\\")
                .replace("*", "\\*")
                .replace("?", "\\?");
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
