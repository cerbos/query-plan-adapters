package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ElasticsearchQueryPlanAdapter {

    private static final Pattern RFC3339_TIMESTAMP = Pattern.compile(
            "^(?!0000-)[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
                    + "(?:\\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$");
    private static final Instant CEL_TIMESTAMP_MIN = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant CEL_TIMESTAMP_MAX =
            Instant.parse("9999-12-31T23:59:59.999999999Z");

    public sealed interface Result permits Result.AlwaysAllowed, Result.AlwaysDenied, Result.Conditional {
        record AlwaysAllowed() implements Result {}
        record AlwaysDenied() implements Result {}
        record Conditional(Map<String, Object> query) implements Result {}
    }

    private record LambdaScope(String nestedPath, String lambdaVariable) {}

    private record SizeComparison(
            String variable, String field, double value, boolean nonEmpty, boolean empty) {}

    private record ResolvedOperand(String variable, Object value, boolean isVariable) {
        static ResolvedOperand variable(String variable) {
            return new ResolvedOperand(variable, null, true);
        }

        static ResolvedOperand value(Object value) {
            return new ResolvedOperand(null, value, false);
        }
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
                    Map.of("wildcard", Map.of(field, Map.of("value", "*" + escapeWildcard(value) + "*")))),
            Map.entry("startsWith", (field, value) ->
                    Map.of("prefix", Map.of(field, Map.of("value", value)))),
            Map.entry("endsWith", (field, value) ->
                    Map.of("wildcard", Map.of(field, Map.of("value", "*" + escapeWildcard(value))))),
            Map.entry("matches", ElasticsearchQueryPlanAdapter::matchesQuery),
            Map.entry("hasIntersection", (field, value) ->
                    Map.of("terms", Map.of(field, value instanceof List<?> l ? l : List.of(value))))
    );

    /** Operators whose second operand is a lambda that binds an iteration variable. */
    private static final Set<String> LAMBDA_BINDING_OPERATORS =
            Set.of("exists", "exists_one", "all", "filter", "map", "except");

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

    private static Map<String, Object> traverseOperandFalse(
            Operand operand,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        return switch (operand.getNodeCase()) {
            case EXPRESSION -> traverseExpressionFalse(
                    operand.getExpression(), fieldMap, overrides, nestedPaths);
            case VARIABLE -> {
                String field = mappedField(operand.getVariable(), fieldMap);
                OperatorFunction fn = overrides.getOrDefault("eq", DEFAULT_OPERATORS.get("eq"));
                yield fn.apply(field, false);
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
                requireUnary("not", operands);
                yield traverseOperandFalse(operands.get(0), fieldMap, overrides, nestedPaths);
            }
            case "exists", "all", "except" ->
                    handleCollectionOperator(
                            operator, operands, fieldMap, overrides, nestedPaths, true);
            case "exists_one" -> {
                rejectUnfoldableValueListMacro(operator, operands);
                throw new IllegalArgumentException(
                        "exists_one cannot be expressed by Elasticsearch nested queries without scripts");
            }
            case "hasIntersection" ->
                    handleHasIntersection(operands, fieldMap, overrides, nestedPaths);
            default -> {
                rejectUnfoldableValueListMacro(operator, operands);
                Map<String, Object> sizeResult =
                        trySizeComparison(operator, operands, fieldMap, nestedPaths);
                if (sizeResult != null) {
                    yield sizeResult;
                }
                yield applyLeafOperator(operator, operands, fieldMap, overrides);
            }
        };
    }

    private static Map<String, Object> traverseExpressionFalse(
            Expression expression,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        String operator = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();

        return switch (operator) {
            case "and" -> boolShould(operands.stream()
                    .map(o -> traverseOperandFalse(o, fieldMap, overrides, nestedPaths)).toList());
            case "or" -> boolMust(operands.stream()
                    .map(o -> traverseOperandFalse(o, fieldMap, overrides, nestedPaths)).toList());
            case "not" -> {
                requireUnary("not", operands);
                yield traverseOperand(operands.get(0), fieldMap, overrides, nestedPaths);
            }
            case "exists", "all", "except" ->
                    handleCollectionOperator(
                            operator, operands, fieldMap, overrides, nestedPaths, false);
            case "exists_one" -> {
                rejectUnfoldableValueListMacro(operator, operands);
                throw new IllegalArgumentException(
                        "exists_one cannot be expressed by Elasticsearch nested queries without scripts");
            }
            case "hasIntersection" -> throw new IllegalArgumentException(
                    "Negated hasIntersection cannot distinguish a missing collection from an empty collection in Elasticsearch");
            default -> {
                rejectUnfoldableValueListMacro(operator, operands);
                Map<String, Object> sizeResult =
                        trySizeComparisonFalse(operator, operands, fieldMap, nestedPaths);
                if (sizeResult != null) {
                    yield sizeResult;
                }
                yield applyLeafOperatorFalse(operator, operands, fieldMap, overrides);
            }
        };
    }

    // --- Collection operators (exists, all, except) ---

    private static Map<String, Object> handleCollectionOperator(
            String operator,
            List<Operand> operands,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths,
            boolean whenTrue) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException(
                    operator + " requires exactly 2 operands, got " + operands.size());
        }

        Operand listOperand = operands.get(0);
        Operand lambdaOperand = operands.get(1);

        // A literal value-list collection arrives when the planner could not unroll a macro
        // over a known collection: at <= 10 elements it folds exists/all into an or/and chain
        // itself (cerbos/cerbos#2570, #2817; maxItems = 10 in the planner's struct matcher),
        // above that the lambda ships with the folded value list as its collection operand.
        // Apply the same fold here instead of demanding a nested mapping that cannot exist
        // for a literal.
        if (listOperand.getNodeCase() == Operand.NodeCase.VALUE) {
            return handleKnownValueCollection(
                    operator, listOperand.getValue(), lambdaOperand,
                    fieldMap, overrides, nestedPaths, whenTrue);
        }

        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw new IllegalArgumentException(
                    operator + " first operand must be a variable, got " + listOperand.getNodeCase());
        }

        String cerbosAttr = listOperand.getVariable();
        String esField = mappedField(cerbosAttr, fieldMap);

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

        if (whenTrue && "all".equals(operator)) {
            throw new IllegalArgumentException(
                    "all cannot distinguish a missing collection from an empty collection in Elasticsearch");
        }
        if (!whenTrue && "exists".equals(operator)) {
            throw new IllegalArgumentException(
                    "Negated exists cannot distinguish a missing collection from an empty collection in Elasticsearch");
        }

        Map<String, Object> innerTrue =
                traverseOperandScoped(bodyOperand, scope, overrides, nestedPaths);

        if (whenTrue) {
            return switch (operator) {
                case "exists" -> nestedQuery(esField, innerTrue);
                case "all" -> notQuery(nestedQuery(esField, notQuery(innerTrue)));
                case "except" -> nestedQuery(esField, notQuery(innerTrue));
                default -> throw new IllegalArgumentException(
                        "Unknown collection operator: " + operator);
            };
        }

        Map<String, Object> innerFalse =
                traverseOperandScopedFalse(bodyOperand, scope, overrides, nestedPaths);
        return switch (operator) {
            // exists is false only when every element is definitely false. An element for which
            // the lambda is undefined prevents both true and false, preserving CEL errors.
            case "exists" -> notQuery(nestedQuery(esField, notQuery(innerFalse)));
            case "all" -> nestedQuery(esField, innerFalse);
            case "except" -> throw new IllegalArgumentException(
                    "Negated except cannot be expressed safely without element error tracking");
            default -> throw new IllegalArgumentException("Unknown collection operator: " + operator);
        };
    }

    /**
     * Fold a collection macro whose collection operand is a literal value list: substitute each
     * element into the lambda body and combine the per-element expressions with {@code or}
     * ({@code exists}) or {@code and} ({@code all}), then translate the combined expression
     * through the normal traversal — the same fold the planner itself applies to known
     * collections of 10 or fewer elements, so the emitted query does not depend on which side
     * of that threshold the collection lands.
     *
     * <p>Unlike a nested-field collection, a literal list is fully known at plan time: there is
     * no missing-versus-empty ambiguity, so the fold is exact under negation too and none of
     * the nested-query restrictions on {@code all} or negated {@code exists} apply. The empty
     * collection keeps CEL identity semantics: {@code exists} over {@code []} is false,
     * {@code all} over {@code []} is true.
     */
    private static Map<String, Object> handleKnownValueCollection(
            String operator,
            Value collectionValue,
            Operand lambdaOperand,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths,
            boolean whenTrue) {
        if (!"exists".equals(operator) && !"all".equals(operator)) {
            throw new IllegalArgumentException(operator
                    + " over a literal collection value is not supported. "
                    + "Only exists() and all() can be folded into a flat query.");
        }
        if (collectionValue.getKindCase() != Value.KindCase.LIST_VALUE) {
            throw new IllegalArgumentException(operator
                    + " over a literal collection requires a list value");
        }

        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw new IllegalArgumentException(
                    operator + " second operand must be a lambda expression");
        }
        List<Operand> lambdaOperands = lambdaOperand.getExpression().getOperandsList();
        if (lambdaOperands.size() != 2) {
            throw new IllegalArgumentException(operator
                    + " over a literal collection supports single-variable lambdas only");
        }
        Operand bodyOperand = lambdaOperands.get(0);
        Operand lambdaVarOperand = lambdaOperands.get(1);
        if (lambdaVarOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw new IllegalArgumentException("lambda second operand must be a variable");
        }
        String lambdaVar = lambdaVarOperand.getVariable();

        List<Value> elements = collectionValue.getListValue().getValuesList();
        if (elements.isEmpty()) {
            boolean holds = "all".equals(operator);
            return holds == whenTrue ? matchAll() : matchNone();
        }

        Expression.Builder combined = Expression.newBuilder()
                .setOperator("exists".equals(operator) ? "or" : "and");
        for (Value element : elements) {
            combined.addOperands(substituteLambdaVariable(bodyOperand, lambdaVar, element));
        }
        Expression folded = combined.build();
        return whenTrue
                ? traverseExpression(folded, fieldMap, overrides, nestedPaths)
                : traverseExpressionFalse(folded, fieldMap, overrides, nestedPaths);
    }

    /**
     * Fail closed, by name, for a collection macro over a literal value list that has no flat
     * translation. {@code filter} and {@code map} reach the leaf traversal rather than
     * {@link #handleCollectionOperator}, so without this they would surface an unrelated
     * operand-shape error instead of naming the real limitation.
     */
    private static void rejectUnfoldableValueListMacro(String operator, List<Operand> operands) {
        if (LAMBDA_BINDING_OPERATORS.contains(operator)
                && operands.size() == 2
                && operands.get(0).getNodeCase() == Operand.NodeCase.VALUE) {
            throw new IllegalArgumentException(operator
                    + " over a literal collection value is not supported. "
                    + "Only exists() and all() can be folded into a flat query.");
        }
    }

    /**
     * Substitute a lambda iteration variable with a concrete collection element inside a lambda
     * body. A bare reference to the variable becomes the element itself; a
     * {@code variable.path.to.field} reference drills into the element (failing closed when the
     * path is missing — the CEL evaluation of that element would error). A nested macro whose
     * lambda rebinds the same variable name shadows the outer variable, so substitution only
     * descends into its collection operand.
     */
    private static Operand substituteLambdaVariable(
            Operand operand, String varName, Value element) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                String name = operand.getVariable();
                if (name.equals(varName)) {
                    return Operand.newBuilder().setValue(element).build();
                }
                if (name.startsWith(varName + ".")) {
                    return Operand.newBuilder()
                            .setValue(resolveElementPath(
                                    name, name.substring(varName.length() + 1), element))
                            .build();
                }
                return operand;
            }
            case EXPRESSION -> {
                Expression expr = operand.getExpression();
                List<Operand> ops = expr.getOperandsList();
                Expression.Builder rebuilt = expr.toBuilder();
                if (LAMBDA_BINDING_OPERATORS.contains(expr.getOperator()) && ops.size() == 2
                        && shadowsVariable(ops.get(1), varName)) {
                    // The nested lambda rebinds our variable: substitute only in the
                    // collection operand.
                    rebuilt.setOperands(0, substituteLambdaVariable(ops.get(0), varName, element));
                    return Operand.newBuilder().setExpression(rebuilt).build();
                }
                for (int i = 0; i < ops.size(); i++) {
                    rebuilt.setOperands(i, substituteLambdaVariable(ops.get(i), varName, element));
                }
                return Operand.newBuilder().setExpression(rebuilt).build();
            }
            default -> {
                return operand;
            }
        }
    }

    /** True when {@code lambdaOperand} is a lambda whose iteration variable is {@code varName}. */
    private static boolean shadowsVariable(Operand lambdaOperand, String varName) {
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            return false;
        }
        List<Operand> ops = lambdaOperand.getExpression().getOperandsList();
        return ops.size() == 2
                && ops.get(1).getNodeCase() == Operand.NodeCase.VARIABLE
                && varName.equals(ops.get(1).getVariable());
    }

    /** Drill a dotted path into a struct element, failing closed on a missing field. */
    private static Value resolveElementPath(String fullRef, String path, Value element) {
        Value current = element;
        for (String segment : path.split("\\.")) {
            if (current.getKindCase() != Value.KindCase.STRUCT_VALUE
                    || !current.getStructValue().containsFields(segment)) {
                throw new IllegalArgumentException("Cannot resolve \"" + fullRef
                        + "\": collection element has no field \"" + segment + "\"");
            }
            current = current.getStructValue().getFieldsOrThrow(segment);
        }
        return current;
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

        if (second.getNodeCase() == Operand.NodeCase.VALUE) {
            Object values = protoValueToJava(second.getValue());
            if (values instanceof List<?> list && list.stream().anyMatch(java.util.Objects::isNull)) {
                throw new IllegalArgumentException(
                        "hasIntersection with null requires an explicit null-value mapping");
            }
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
        String esField = mappedField(cerbosAttr, fieldMap);

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

        Map<String, Object> matchingValue = nestedQuery(
                esField, Map.of("terms", Map.of(nestedField, valueList)));
        Map<String, Object> missingProjection = nestedQuery(esField, notExists(nestedField));
        return boolMust(List.of(matchingValue, notQuery(missingProjection)));
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

    private static Map<String, Object> traverseOperandScopedFalse(
            Operand operand,
            LambdaScope scope,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        return switch (operand.getNodeCase()) {
            case EXPRESSION -> traverseExpressionScopedFalse(
                    operand.getExpression(), scope, overrides, nestedPaths);
            case VARIABLE -> {
                String field = resolveScopedVariable(operand.getVariable(), scope);
                OperatorFunction fn = overrides.getOrDefault("eq", DEFAULT_OPERATORS.get("eq"));
                yield fn.apply(field, false);
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
                requireUnary("not", operands);
                yield traverseOperandScopedFalse(operands.get(0), scope, overrides, nestedPaths);
            }
            default -> applyScopedLeafOperator(operator, operands, scope, overrides);
        };
    }

    private static Map<String, Object> traverseExpressionScopedFalse(
            Expression expression,
            LambdaScope scope,
            Map<String, OperatorFunction> overrides,
            Set<String> nestedPaths) {
        String operator = expression.getOperator();
        List<Operand> operands = expression.getOperandsList();
        return switch (operator) {
            case "and" -> boolShould(operands.stream()
                    .map(o -> traverseOperandScopedFalse(o, scope, overrides, nestedPaths)).toList());
            case "or" -> boolMust(operands.stream()
                    .map(o -> traverseOperandScopedFalse(o, scope, overrides, nestedPaths)).toList());
            case "not" -> {
                requireUnary("not", operands);
                yield traverseOperandScoped(operands.get(0), scope, overrides, nestedPaths);
            }
            default -> applyScopedLeafOperatorFalse(operator, operands, scope, overrides);
        };
    }

    private static Map<String, Object> applyScopedLeafOperator(
            String operator,
            List<Operand> operands,
            LambdaScope scope,
            Map<String, OperatorFunction> overrides) {
        return applyResolvedLeaf(
                operator, operands, variable -> resolveScopedVariable(variable, scope), overrides, true);
    }

    private static Map<String, Object> applyScopedLeafOperatorFalse(
            String operator,
            List<Operand> operands,
            LambdaScope scope,
            Map<String, OperatorFunction> overrides) {
        return applyResolvedLeaf(
                operator, operands, variable -> resolveScopedVariable(variable, scope), overrides, false);
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
            Map<String, String> fieldMap,
            Set<String> nestedPaths) {
        SizeComparison comparison = resolveSizeComparison(operator, operands, fieldMap);
        if (comparison == null) return null;

        Map<String, Object> present = collectionPresentQuery(comparison.field(), nestedPaths);
        if (comparison.nonEmpty()) return present;
        if (comparison.empty()) throw unsafeEmptyCollectionSize(comparison.variable());
        throw unsupportedSizeComparison(operator, comparison);
    }

    private static Map<String, Object> trySizeComparisonFalse(
            String operator,
            List<Operand> operands,
            Map<String, String> fieldMap,
            Set<String> nestedPaths) {
        SizeComparison comparison = resolveSizeComparison(operator, operands, fieldMap);
        if (comparison == null) return null;

        Map<String, Object> present = collectionPresentQuery(comparison.field(), nestedPaths);
        if (comparison.empty()) return present;
        if (comparison.nonEmpty()) throw unsafeEmptyCollectionSize(comparison.variable());
        throw unsupportedSizeComparison(operator, comparison);
    }

    private static SizeComparison resolveSizeComparison(
            String operator,
            List<Operand> operands,
            Map<String, String> fieldMap) {
        Expression sizeExpression = null;
        Double value = null;
        for (Operand operand : operands) {
            switch (operand.getNodeCase()) {
                case EXPRESSION -> {
                    if ("size".equals(operand.getExpression().getOperator())) {
                        sizeExpression = operand.getExpression();
                    }
                }
                case VALUE -> {
                    Object resolved = protoValueToJava(operand.getValue());
                    if (resolved instanceof Number number) value = number.doubleValue();
                }
                default -> {}
            }
        }
        if (sizeExpression == null) return null;

        List<Operand> sizeOperands = sizeExpression.getOperandsList();
        if (sizeOperands.size() != 1
                || sizeOperands.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
            throw new IllegalArgumentException("Unsupported size() expression");
        }
        if (value == null || !Double.isFinite(value)) {
            throw new IllegalArgumentException("size comparison requires a finite numeric value");
        }

        String variable = sizeOperands.get(0).getVariable();
        boolean nonEmpty = (operator.equals("gt") && value == 0.0)
                || (operator.equals("ge") && value == 1.0);
        boolean empty = (operator.equals("eq") && value == 0.0)
                || (operator.equals("le") && value == 0.0)
                || (operator.equals("lt") && value == 1.0);
        return new SizeComparison(variable, mappedField(variable, fieldMap), value, nonEmpty, empty);
    }

    private static Map<String, Object> collectionPresentQuery(
            String field, Set<String> nestedPaths) {
        return nestedPaths.contains(field)
                ? nestedQuery(field, Map.of("match_all", Map.of()))
                : exists(field);
    }

    private static IllegalArgumentException unsupportedSizeComparison(
            String operator, SizeComparison comparison) {
        return new IllegalArgumentException(
                "Unsupported size comparison: size(" + comparison.variable() + ") " + operator + " "
                        + comparison.value()
                        + ". Only emptiness checks (size > 0, size == 0) are supported.");
    }

    private static IllegalArgumentException unsafeEmptyCollectionSize(String variable) {
        return new IllegalArgumentException(
                "size(" + variable + ") emptiness cannot distinguish a missing collection "
                        + "from an empty collection in Elasticsearch");
    }

    // --- Leaf operators ---

    private static Map<String, Object> applyLeafOperator(
            String operator,
            List<Operand> operands,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides) {
        return applyResolvedLeaf(operator, operands, variable -> mappedField(variable, fieldMap),
                overrides, true);
    }

    private static Map<String, Object> applyLeafOperatorFalse(
            String operator,
            List<Operand> operands,
            Map<String, String> fieldMap,
            Map<String, OperatorFunction> overrides) {
        return applyResolvedLeaf(operator, operands, variable -> mappedField(variable, fieldMap),
                overrides, false);
    }

    private static Map<String, Object> applyResolvedLeaf(
            String operator,
            List<Operand> operands,
            Function<String, String> fieldResolver,
            Map<String, OperatorFunction> overrides,
            boolean whenTrue) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException(
                    operator + " requires exactly 2 operands, got " + operands.size());
        }

        ResolvedOperand left = resolveLeafOperand(operands.get(0));
        ResolvedOperand right = resolveLeafOperand(operands.get(1));
        if (left.isVariable() == right.isVariable()) {
            throw new IllegalArgumentException(left.isVariable()
                    ? "Elasticsearch Query DSL cannot compare two document fields without scripts"
                    : "Leaf expression must contain exactly one document field");
        }

        boolean variableFirst = left.isVariable();
        String variable = variableFirst ? left.variable() : right.variable();
        Object value = variableFirst ? right.value() : left.value();
        String field = fieldResolver.apply(variable);

        String normalizedOperator = normalizeLeafOperator(operator, variableFirst);
        if (!whenTrue && "in".equals(normalizedOperator) && !variableFirst) {
            throw new IllegalArgumentException(
                    "Negated membership in a document collection cannot distinguish a missing "
                            + "collection from an empty collection in Elasticsearch");
        }
        if (value == null) {
            return nullLeafQuery(normalizedOperator, field, variableFirst, whenTrue);
        }
        if ("in".equals(normalizedOperator) && value instanceof List<?> values
                && values.stream().anyMatch(java.util.Objects::isNull)) {
            return nullAwareMembershipQuery(field, values, whenTrue);
        }

        Map<String, Object> positive;
        if ("in".equals(normalizedOperator)) {
            positive = membershipQuery(field, value);
        } else if ("ne".equals(normalizedOperator) && !overrides.containsKey("ne")) {
            positive = definedAndNot(field, DEFAULT_OPERATORS.get("eq").apply(field, value));
        } else {
            OperatorFunction function = overrides.getOrDefault(
                    normalizedOperator, DEFAULT_OPERATORS.get(normalizedOperator));
            if (function == null) {
                throw new IllegalArgumentException("Unknown operator: " + normalizedOperator);
            }
            positive = function.apply(field, value);
        }
        if (whenTrue) {
            return positive;
        }

        return switch (normalizedOperator) {
            case "eq" -> definedAndNot(field, positive);
            case "ne" -> overrides.getOrDefault("eq", DEFAULT_OPERATORS.get("eq"))
                    .apply(field, value);
            case "lt" -> range(field, "gte", value);
            case "le" -> range(field, "gt", value);
            case "gt" -> range(field, "lte", value);
            case "ge" -> range(field, "lt", value);
            case "in", "contains", "startsWith", "endsWith", "matches" ->
                    definedAndNot(field, positive);
            default -> throw new IllegalArgumentException(
                    "Cannot safely negate operator without scripts: " + normalizedOperator);
        };
    }

    private static ResolvedOperand resolveLeafOperand(Operand operand) {
        return switch (operand.getNodeCase()) {
            case VARIABLE -> ResolvedOperand.variable(operand.getVariable());
            case VALUE -> ResolvedOperand.value(protoValueToJava(operand.getValue()));
            case EXPRESSION -> {
                Expression expression = operand.getExpression();
                if (!"timestamp".equals(expression.getOperator())
                        || expression.getOperandsCount() != 1) {
                    throw new IllegalArgumentException(
                            "Unexpected " + expression.getOperator() + " expression in leaf operand");
                }
                ResolvedOperand resolved = resolveLeafOperand(expression.getOperands(0));
                if (!resolved.isVariable()) {
                    validateTimestampLiteral(resolved.value());
                }
                yield resolved;
            }
            default -> throw new IllegalArgumentException(
                    "Unexpected operand type in leaf expression: " + operand.getNodeCase());
        };
    }

    private static void validateTimestampLiteral(Object value) {
        if (!(value instanceof String literal)) {
            throw new IllegalArgumentException("timestamp() requires an RFC 3339 string literal");
        }
        if (!RFC3339_TIMESTAMP.matcher(literal).matches()) {
            throw invalidTimestampLiteral(literal, null);
        }
        try {
            Instant instant = OffsetDateTime.parse(literal, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    .toInstant();
            if (instant.isBefore(CEL_TIMESTAMP_MIN) || instant.isAfter(CEL_TIMESTAMP_MAX)) {
                throw invalidTimestampLiteral(literal, null);
            }
            int nanos = instant.getNano();
            if (nanos % 1_000_000 != 0) {
                throw new IllegalArgumentException(
                        "Sub-millisecond timestamp literals require an explicit date_nanos "
                                + "mapping mode, which this adapter does not configure");
            }
        } catch (DateTimeParseException error) {
            throw invalidTimestampLiteral(literal, error);
        }
    }

    private static IllegalArgumentException invalidTimestampLiteral(
            String literal, DateTimeParseException cause) {
        String message = "timestamp() requires a valid RFC 3339 string literal: " + literal;
        return cause == null
                ? new IllegalArgumentException(message)
                : new IllegalArgumentException(message, cause);
    }

    private static String normalizeLeafOperator(String operator, boolean variableFirst) {
        if (variableFirst) {
            return operator;
        }
        return switch (operator) {
            case "eq", "ne", "in" -> operator;
            case "lt" -> "gt";
            case "le" -> "ge";
            case "gt" -> "lt";
            case "ge" -> "le";
            case "contains", "startsWith", "endsWith", "matches" ->
                    throw new IllegalArgumentException(
                            operator + " with a document field as the receiver argument "
                                    + "cannot be expressed without scripts");
            default -> operator;
        };
    }

    private static Map<String, Object> nullLeafQuery(
            String operator,
            String field,
            boolean variableFirst,
            boolean whenTrue) {
        return switch (operator) {
            case "eq" -> {
                if (whenTrue) {
                    throw unsafeExplicitNullComparison();
                }
                yield exists(field);
            }
            case "ne" -> {
                if (!whenTrue) {
                    throw unsafeExplicitNullComparison();
                }
                yield exists(field);
            }
            case "in" -> {
                if (!variableFirst) {
                    throw new IllegalArgumentException(
                            "null membership in a document array requires an explicit null-value mapping");
                }
                throw unsafeExplicitNullComparison();
            }
            default -> throw new IllegalArgumentException(
                    "Null values are only supported with eq, ne, and scalar in operators");
        };
    }

    private static Map<String, Object> membershipQuery(String field, Object value) {
        return DEFAULT_OPERATORS.get("in").apply(field, value);
    }

    private static Map<String, Object> nullAwareMembershipQuery(
            String field, List<?> values, boolean whenTrue) {
        if (whenTrue) {
            throw unsafeExplicitNullComparison();
        }
        List<?> nonNull = values.stream().filter(java.util.Objects::nonNull).toList();
        if (nonNull.isEmpty()) {
            return exists(field);
        }
        return definedAndNot(field, DEFAULT_OPERATORS.get("in").apply(field, nonNull));
    }

    private static IllegalArgumentException unsafeExplicitNullComparison() {
        return new IllegalArgumentException(
                "Elasticsearch cannot distinguish an explicit null value from a missing field "
                        + "without an indexed null-value sentinel");
    }

    private static String mappedField(String variable, Map<String, String> fieldMap) {
        String field = fieldMap.get(variable);
        if (field == null) {
            throw new IllegalArgumentException("Unknown attribute: " + variable);
        }
        return field;
    }

    private static void requireUnary(String operator, List<Operand> operands) {
        if (operands.size() != 1) {
            throw new IllegalArgumentException(
                    operator + " requires exactly 1 operand, got " + operands.size());
        }
    }

    private static Map<String, Object> boolMust(List<Map<String, Object>> clauses) {
        return Map.of("bool", Map.of("must", clauses));
    }

    private static Map<String, Object> boolShould(List<Map<String, Object>> clauses) {
        return Map.of("bool", Map.of("should", clauses, "minimum_should_match", 1));
    }

    private static Map<String, Object> notQuery(Map<String, Object> query) {
        return Map.of("bool", Map.of("must_not", List.of(query)));
    }

    private static Map<String, Object> matchAll() {
        return Map.of("match_all", Map.of());
    }

    private static Map<String, Object> matchNone() {
        return Map.of("match_none", Map.of());
    }

    private static Map<String, Object> exists(String field) {
        return Map.of("exists", Map.of("field", field));
    }

    private static Map<String, Object> notExists(String field) {
        return notQuery(exists(field));
    }

    private static Map<String, Object> definedAndNot(String field, Map<String, Object> query) {
        return boolMust(List.of(exists(field), notQuery(query)));
    }

    private static Map<String, Object> range(String field, String operator, Object value) {
        return Map.of("range", Map.of(field, Map.of(operator, value)));
    }

    private static Map<String, Object> nestedQuery(String path, Map<String, Object> query) {
        return Map.of("nested", Map.of("path", path, "query", query));
    }

    private static String escapeWildcard(Object value) {
        return value.toString()
                .replace("\\", "\\\\")
                .replace("*", "\\*")
                .replace("?", "\\?");
    }

    private static Map<String, Object> matchesQuery(String field, Object value) {
        String pattern = value.toString();
        boolean anchoredStart = pattern.startsWith("^");
        String body = anchoredStart ? pattern.substring(1) : pattern;
        boolean anchoredEnd = body.endsWith("$") && !isEscaped(body, body.length() - 1);
        if (anchoredStart && !anchoredEnd && !body.isEmpty() && isPlainRegexLiteral(body)) {
            return Map.of("prefix", Map.of(field, Map.of("value", body)));
        }
        return Map.of("regexp", Map.of(field, Map.of(
                "value", toLuceneRegex(pattern),
                "flags", "NONE")));
    }

    private static boolean isPlainRegexLiteral(String pattern) {
        for (int index = 0; index < pattern.length(); index++) {
            if ("\\.[](){}?*+|^$".indexOf(pattern.charAt(index)) >= 0) {
                return false;
            }
        }
        return true;
    }

    // CEL `matches()` uses RE2 partial-match semantics. Elasticsearch's `regexp`
    // query uses Lucene regex with whole-field semantics, and Lucene `.` includes
    // newlines while RE2 `.` does not. Only explicitly whole-field patterns in the
    // common syntax subset reach Lucene; simple `^literal` prefixes use `prefix`.
    // Optional Lucene operators are disabled at the query site with flags=NONE.
    static String toLuceneRegex(String celPattern) {
        boolean anchoredStart = celPattern.startsWith("^");
        String body = anchoredStart ? celPattern.substring(1) : celPattern;
        boolean anchoredEnd = body.endsWith("$") && !isEscaped(body, body.length() - 1);
        if (anchoredEnd) {
            body = body.substring(0, body.length() - 1);
        }
        body = validateAndEscapeLuceneRegexBody(body);
        if (!anchoredStart || !anchoredEnd) {
            throw new IllegalArgumentException(
                    "matches regex patterns must be fully anchored unless they are a simple "
                            + "literal prefix");
        }
        if (body.isEmpty()) {
            throw new IllegalArgumentException(
                    "matches regex for only the empty string is not supported by Elasticsearch");
        }
        return body;
    }

    private static String validateAndEscapeLuceneRegexBody(String pattern) {
        StringBuilder translated = new StringBuilder(pattern.length());
        boolean escaped = false;
        boolean inCharacterClass = false;
        for (int index = 0; index < pattern.length(); index++) {
            char current = pattern.charAt(index);
            if (escaped) {
                if (Character.isLetterOrDigit(current)) {
                    throw unsupportedRegexSyntax(pattern, index - 1);
                }
                translated.append('\\').append(current);
                escaped = false;
                continue;
            }
            if (current == '\\') {
                escaped = true;
                continue;
            }
            if (current == '[') {
                if (inCharacterClass) {
                    throw unsupportedRegexSyntax(pattern, index);
                }
                inCharacterClass = true;
                translated.append(current);
                continue;
            }
            if (current == ']') {
                if (!inCharacterClass) {
                    throw unsupportedRegexSyntax(pattern, index);
                }
                inCharacterClass = false;
                translated.append(current);
                continue;
            }
            if (!inCharacterClass && (current == '^' || current == '$')) {
                throw unsupportedRegexSyntax(pattern, index);
            }
            if (!inCharacterClass && current == '.') {
                throw unsupportedRegexSyntax(pattern, index);
            }
            if (!inCharacterClass && current == '('
                    && index + 1 < pattern.length() && pattern.charAt(index + 1) == '?') {
                throw unsupportedRegexSyntax(pattern, index);
            }
            if (current == '"') {
                translated.append("\\\"");
            } else {
                translated.append(current);
            }
        }
        if (escaped || inCharacterClass) {
            throw unsupportedRegexSyntax(pattern, pattern.length());
        }
        return translated.toString();
    }

    private static boolean isEscaped(String value, int index) {
        int backslashes = 0;
        for (int cursor = index - 1; cursor >= 0 && value.charAt(cursor) == '\\'; cursor--) {
            backslashes++;
        }
        return backslashes % 2 != 0;
    }

    private static IllegalArgumentException unsupportedRegexSyntax(String pattern, int index) {
        return new IllegalArgumentException(
                "matches regex uses syntax outside the supported RE2/Lucene subset at index "
                        + index + ": " + pattern);
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
