package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ElasticsearchQueryPlanAdapterTest {

    private static final Map<String, String> FIELD_MAP = Map.of(
            "request.resource.attr.department", "department",
            "request.resource.attr.status", "status",
            "request.resource.attr.priority", "priority",
            "request.resource.attr.aBool", "aBool",
            "request.resource.attr.aString", "aString",
            "request.resource.attr.aNumber", "aNumber",
            "request.resource.attr.title", "title"
    );

    private static PlanResourcesResponse buildResponse(PlanResourcesFilter.Kind kind) {
        return buildResponse(kind, null);
    }

    private static PlanResourcesResponse buildResponse(PlanResourcesFilter.Kind kind, Operand condition) {
        PlanResourcesFilter.Builder filterBuilder = PlanResourcesFilter.newBuilder().setKind(kind);
        if (condition != null) {
            filterBuilder.setCondition(condition);
        }
        return PlanResourcesResponse.newBuilder()
                .setFilter(filterBuilder)
                .build();
    }

    private static Operand expressionOperand(String operator, Operand... operands) {
        Expression.Builder expr = Expression.newBuilder().setOperator(operator);
        for (Operand op : operands) {
            expr.addOperands(op);
        }
        return Operand.newBuilder().setExpression(expr).build();
    }

    private static Operand variableOperand(String name) {
        return Operand.newBuilder().setVariable(name).build();
    }

    private static Operand stringValueOperand(String val) {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setStringValue(val))
                .build();
    }

    private static Operand numberValueOperand(double val) {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setNumberValue(val))
                .build();
    }

    private static Operand boolValueOperand(boolean val) {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setBoolValue(val))
                .build();
    }

    private static Operand nullValueOperand() {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
                .build();
    }

    private static Operand listValueOperand(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String v : values) {
            list.addValues(Value.newBuilder().setStringValue(v));
        }
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(list))
                .build();
    }

    @Test
    void alwaysAllowed() {
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_ALLOWED);
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);
        assertInstanceOf(Result.AlwaysAllowed.class, result);
    }

    @Test
    void alwaysDenied() {
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_ALWAYS_DENIED);
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);
        assertInstanceOf(Result.AlwaysDenied.class, result);
    }

    @Test
    void eqStringProducesTermQuery() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.department"),
                stringValueOperand("engineering"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertInstanceOf(Result.Conditional.class, result);
        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("term", Map.of("department", Map.of("value", "engineering"))), query);
    }

    @Test
    void eqBoolProducesTermQuery() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.aBool"),
                boolValueOperand(true));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertInstanceOf(Result.Conditional.class, result);
        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("term", Map.of("aBool", Map.of("value", true))), query);
    }

    @Test
    void neProducesBoolMustNotTerm() {
        Operand condition = expressionOperand("ne",
                variableOperand("request.resource.attr.status"),
                stringValueOperand("archived"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("term", Map.of("status", Map.of("value", "archived")))))),
                query);
    }

    @Test
    void ltProducesRangeQuery() {
        Operand condition = expressionOperand("lt",
                variableOperand("request.resource.attr.aNumber"),
                numberValueOperand(100));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("range", Map.of("aNumber", Map.of("lt", 100L))), query);
    }

    @Test
    void gtProducesRangeQuery() {
        Operand condition = expressionOperand("gt",
                variableOperand("request.resource.attr.aNumber"),
                numberValueOperand(50));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("range", Map.of("aNumber", Map.of("gt", 50L))), query);
    }

    @Test
    void leProducesRangeQuery() {
        Operand condition = expressionOperand("le",
                variableOperand("request.resource.attr.aNumber"),
                numberValueOperand(200));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("range", Map.of("aNumber", Map.of("lte", 200L))), query);
    }

    @Test
    void geProducesRangeQuery() {
        Operand condition = expressionOperand("ge",
                variableOperand("request.resource.attr.aNumber"),
                numberValueOperand(10));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("range", Map.of("aNumber", Map.of("gte", 10L))), query);
    }

    @Test
    void inProducesTermsQuery() {
        Operand condition = expressionOperand("in",
                variableOperand("request.resource.attr.status"),
                listValueOperand("active", "pending"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("terms", Map.of("status", List.of("active", "pending"))), query);
    }

    @Test
    void andProducesBoolMust() {
        Operand condition = expressionOperand("and",
                expressionOperand("eq",
                        variableOperand("request.resource.attr.department"),
                        stringValueOperand("engineering")),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.status"),
                        stringValueOperand("active")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must", List.of(
                        Map.of("term", Map.of("department", Map.of("value", "engineering"))),
                        Map.of("term", Map.of("status", Map.of("value", "active")))))),
                query);
    }

    @Test
    void orProducesBoolShould() {
        Operand condition = expressionOperand("or",
                expressionOperand("eq",
                        variableOperand("request.resource.attr.department"),
                        stringValueOperand("engineering")),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.department"),
                        stringValueOperand("marketing")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of(
                        "should", List.of(
                                Map.of("term", Map.of("department", Map.of("value", "engineering"))),
                                Map.of("term", Map.of("department", Map.of("value", "marketing")))),
                        "minimum_should_match", 1)),
                query);
    }

    @Test
    void notProducesBoolMustNot() {
        Operand condition = expressionOperand("not",
                expressionOperand("eq",
                        variableOperand("request.resource.attr.status"),
                        stringValueOperand("archived")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("term", Map.of("status", Map.of("value", "archived")))))),
                query);
    }

    @Test
    void nandProducesNotAnd() {
        Operand condition = expressionOperand("not",
                expressionOperand("and",
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aBool"),
                                boolValueOperand(true)),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                stringValueOperand("foo"))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("bool", Map.of("must", List.of(
                                Map.of("term", Map.of("aBool", Map.of("value", true))),
                                Map.of("term", Map.of("aString", Map.of("value", "foo"))))))))),
                query);
    }

    @Test
    void containsProducesWildcardQuery() {
        Operand condition = expressionOperand("contains",
                variableOperand("request.resource.attr.title"),
                stringValueOperand("search"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("wildcard", Map.of("title", Map.of("value", "*search*"))), query);
    }

    @Test
    void startsWithProducesPrefixQuery() {
        Operand condition = expressionOperand("startsWith",
                variableOperand("request.resource.attr.title"),
                stringValueOperand("draft"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("prefix", Map.of("title", Map.of("value", "draft"))), query);
    }

    @Test
    void endsWithProducesWildcardQuery() {
        Operand condition = expressionOperand("endsWith",
                variableOperand("request.resource.attr.title"),
                stringValueOperand(".pdf"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("wildcard", Map.of("title", Map.of("value", "*.pdf"))), query);
    }

    @Test
    void containsEscapesWildcardMetacharacters() {
        Operand condition = expressionOperand("contains",
                variableOperand("request.resource.attr.title"),
                stringValueOperand("foo*bar?baz\\qux"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("wildcard", Map.of("title", Map.of("value", "*foo\\*bar\\?baz\\\\qux*"))), query);
    }

    @Test
    void endsWithEscapesWildcardMetacharacters() {
        Operand condition = expressionOperand("endsWith",
                variableOperand("request.resource.attr.title"),
                stringValueOperand("a*b"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("wildcard", Map.of("title", Map.of("value", "*a\\*b"))), query);
    }

    @Test
    void unknownAttributeThrows() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.nonexistent"),
                stringValueOperand("value"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(ex.getMessage().contains("Unknown attribute"));
    }

    @Test
    void unknownOperatorThrows() {
        Operand condition = expressionOperand("unsupported_op",
                variableOperand("request.resource.attr.department"),
                stringValueOperand("value"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(ex.getMessage().contains("Unknown operator"));
    }

    @Test
    void operatorOverrideIsUsed() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.department"),
                stringValueOperand("engineering"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Map<String, OperatorFunction> overrides = Map.of(
                "eq", (field, value) -> Map.of("match", Map.of(field, value))
        );

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, overrides);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("match", Map.of("department", "engineering")), query);
    }

    @Test
    void nestedAndOrExpression() {
        Operand condition = expressionOperand("and",
                expressionOperand("eq",
                        variableOperand("request.resource.attr.department"),
                        stringValueOperand("engineering")),
                expressionOperand("or",
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.status"),
                                stringValueOperand("active")),
                        expressionOperand("gt",
                                variableOperand("request.resource.attr.priority"),
                                numberValueOperand(5))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must", List.of(
                        Map.of("term", Map.of("department", Map.of("value", "engineering"))),
                        Map.of("bool", Map.of(
                                "should", List.of(
                                        Map.of("term", Map.of("status", Map.of("value", "active"))),
                                        Map.of("range", Map.of("priority", Map.of("gt", 5L)))),
                                "minimum_should_match", 1))))),
                query);
    }

    @Test
    void bareBoolVariableProducesTermQuery() {
        Operand condition = variableOperand("request.resource.attr.aBool");
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertInstanceOf(Result.Conditional.class, result);
        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("term", Map.of("aBool", Map.of("value", true))), query);
    }

    @Test
    void queryWrappedInBoolFilter() {
        Operand condition = expressionOperand("and",
                expressionOperand("eq",
                        variableOperand("request.resource.attr.department"),
                        stringValueOperand("engineering")),
                expressionOperand("gt",
                        variableOperand("request.resource.attr.aNumber"),
                        numberValueOperand(5)));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);
        assertInstanceOf(Result.Conditional.class, result);

        Map<String, Object> filterClause = ((Result.Conditional) result).query();
        Map<String, Object> searchBody = Map.of("query", Map.of(
                "bool", Map.of("filter", List.of(filterClause))));

        assertEquals(Map.of("query", Map.of(
                "bool", Map.of("filter", List.of(
                        Map.of("bool", Map.of("must", List.of(
                                Map.of("term", Map.of("department", Map.of("value", "engineering"))),
                                Map.of("range", Map.of("aNumber", Map.of("gt", 5L)))))))))),
                searchBody);
    }

    @Test
    void queryWrappedInBoolFilterWithUserQuery() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.status"),
                stringValueOperand("active"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);
        assertInstanceOf(Result.Conditional.class, result);

        Map<String, Object> filterClause = ((Result.Conditional) result).query();
        Map<String, Object> userQuery = Map.of("match", Map.of("title", "search term"));
        Map<String, Object> searchBody = Map.of("query", Map.of(
                "bool", Map.of(
                        "must", List.of(userQuery),
                        "filter", List.of(filterClause))));

        Map<String, Object> expectedFilter = Map.of("term", Map.of("status", Map.of("value", "active")));
        @SuppressWarnings("unchecked")
        Map<String, Object> boolClause = (Map<String, Object>) ((Map<String, Object>) searchBody.get("query")).get("bool");
        assertEquals(List.of(expectedFilter), boolClause.get("filter"));
        assertEquals(List.of(userQuery), boolClause.get("must"));
    }

    @Test
    void eqNullProducesNotExists() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.department"),
                nullValueOperand());
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("exists", Map.of("field", "department"))))),
                query);
    }

    @Test
    void neNullProducesExists() {
        Operand condition = expressionOperand("ne",
                variableOperand("request.resource.attr.department"),
                nullValueOperand());
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("exists", Map.of("field", "department")), query);
    }

    @Test
    void notBareBoolProducesMustNot() {
        Operand condition = expressionOperand("not",
                variableOperand("request.resource.attr.aBool"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertInstanceOf(Result.Conditional.class, result);
        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("term", Map.of("aBool", Map.of("value", true)))))),
                query);
    }
}
