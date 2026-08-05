package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ElasticsearchQueryPlanAdapterTest {

    private static final Map<String, String> FIELD_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.department", "department"),
            Map.entry("request.resource.attr.status", "status"),
            Map.entry("request.resource.attr.priority", "priority"),
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.title", "title"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.ownedBy", "ownedBy"),
            Map.entry("request.resource.attr.tagObjects", "tagObjects")
    );

    private static final Set<String> NESTED_PATHS = Set.of("tagObjects");

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

    private static Operand timestampOperand(Operand operand) {
        return expressionOperand("timestamp", operand);
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

    private static Operand listValueOperandWithNull(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String value : values) {
            list.addValues(Value.newBuilder().setStringValue(value));
        }
        list.addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE));
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
    void neRequiresFieldToExistAndNotMatch() {
        Operand condition = expressionOperand("ne",
                variableOperand("request.resource.attr.status"),
                stringValueOperand("archived"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must", List.of(
                        Map.of("exists", Map.of("field", "status")),
                        Map.of("bool", Map.of("must_not", List.of(
                                Map.of("term", Map.of("status", Map.of("value", "archived"))))))))),
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
    void valueFirstLeMirrorsToFieldGe() {
        Operand condition = expressionOperand("le",
                numberValueOperand(3),
                variableOperand("request.resource.attr.aNumber"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertEquals(Map.of("range", Map.of("aNumber", Map.of("gte", 3L))),
                ((Result.Conditional) result).query());
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
    void inListContainingNullFailsClosed() {
        Operand values = Operand.newBuilder().setValue(Value.newBuilder().setListValue(
                ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("active"))
                        .addValues(Value.newBuilder().setNullValue(
                                com.google.protobuf.NullValue.NULL_VALUE))))
                .build();
        Operand condition = expressionOperand("in",
                variableOperand("request.resource.attr.status"), values);
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("explicit null value from a missing field"));
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
    void notEqualsRequiresFieldToExist() {
        Operand condition = expressionOperand("not",
                expressionOperand("eq",
                        variableOperand("request.resource.attr.status"),
                        stringValueOperand("archived")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of("must", List.of(
                        Map.of("exists", Map.of("field", "status")),
                        Map.of("bool", Map.of("must_not", List.of(
                                Map.of("term", Map.of("status", Map.of("value", "archived"))))))))),
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
        Map<String, Object> falseBool = Map.of("bool", Map.of("must", List.of(
                Map.of("exists", Map.of("field", "aBool")),
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("term", Map.of("aBool", Map.of("value", true)))))))));
        Map<String, Object> falseString = Map.of("bool", Map.of("must", List.of(
                Map.of("exists", Map.of("field", "aString")),
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("term", Map.of("aString", Map.of("value", "foo")))))))));
        assertEquals(Map.of("bool", Map.of(
                "should", List.of(falseBool, falseString), "minimum_should_match", 1)), query);
    }

    @Test
    void notAndProducesMustNotWrappingAnd() {
        // !(aBool==true && aString!="string")
        Operand condition = expressionOperand("not",
                expressionOperand("and",
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aBool"),
                                boolValueOperand(true)),
                        expressionOperand("ne",
                                variableOperand("request.resource.attr.aString"),
                                stringValueOperand("string"))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        Map<String, Object> falseBool = Map.of("bool", Map.of("must", List.of(
                Map.of("exists", Map.of("field", "aBool")),
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("term", Map.of("aBool", Map.of("value", true)))))))));
        assertEquals(Map.of("bool", Map.of(
                "should", List.of(falseBool,
                        Map.of("term", Map.of("aString", Map.of("value", "string")))),
                "minimum_should_match", 1)), query);
    }

    @Test
    void notOrProducesMustNotWrappingOr() {
        // !(aBool==true || aString!="string")
        Operand condition = expressionOperand("not",
                expressionOperand("or",
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aBool"),
                                boolValueOperand(true)),
                        expressionOperand("ne",
                                variableOperand("request.resource.attr.aString"),
                                stringValueOperand("string"))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        Map<String, Object> falseBool = Map.of("bool", Map.of("must", List.of(
                Map.of("exists", Map.of("field", "aBool")),
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("term", Map.of("aBool", Map.of("value", true)))))))));
        assertEquals(Map.of("bool", Map.of("must", List.of(
                falseBool, Map.of("term", Map.of("aString", Map.of("value", "string")))))), query);
    }

    @Test
    void notGtProducesMustNotWrappingRange() {
        // !(aNumber > 1)
        Operand condition = expressionOperand("not",
                expressionOperand("gt",
                        variableOperand("request.resource.attr.aNumber"),
                        numberValueOperand(1)));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("range", Map.of("aNumber", Map.of("lte", 1L))), query);
    }

    @Test
    void notLtProducesMustNotWrappingRange() {
        // !(aNumber < 2)
        Operand condition = expressionOperand("not",
                expressionOperand("lt",
                        variableOperand("request.resource.attr.aNumber"),
                        numberValueOperand(2)));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("range", Map.of("aNumber", Map.of("gte", 2L))), query);
    }

    @Test
    void notContainsProducesMustNotWrappingWildcard() {
        // !aString.contains("str")
        Operand condition = expressionOperand("not",
                expressionOperand("contains",
                        variableOperand("request.resource.attr.aString"),
                        stringValueOperand("str")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("bool", Map.of("must", List.of(
                Map.of("exists", Map.of("field", "aString")),
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("wildcard", Map.of("aString", Map.of("value", "*str*"))))))))), query);
    }

    @Test
    void notStartsWithProducesMustNotWrappingPrefix() {
        // !aString.startsWith("str")
        Operand condition = expressionOperand("not",
                expressionOperand("startsWith",
                        variableOperand("request.resource.attr.aString"),
                        stringValueOperand("str")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("bool", Map.of("must", List.of(
                Map.of("exists", Map.of("field", "aString")),
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("prefix", Map.of("aString", Map.of("value", "str"))))))))), query);
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
    void constantReceiverStringOperatorThrows() {
        Operand condition = expressionOperand("startsWith",
                stringValueOperand("constant"),
                variableOperand("request.resource.attr.title"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
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
    void eqNullFailsClosed() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.department"),
                nullValueOperand());
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("explicit null value from a missing field"));
    }

    @Test
    void negatedEqNullProducesExists() {
        Operand condition = expressionOperand("not",
                expressionOperand("eq",
                        variableOperand("request.resource.attr.department"),
                        nullValueOperand()));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertEquals(Map.of("exists", Map.of("field", "department")),
                ((Result.Conditional) result).query());
    }

    /**
     * cerbos/query-plan-adapters#302 pins the other adapters' NULL-column representation to a
     * caller-declared option, because {@code eq(attr, null)} means "the explicitly-null rows"
     * under one convention and "no rows at all" under the other, and the wire node is identical.
     *
     * <p>This adapter needs no such option: Elasticsearch cannot index an explicit null
     * distinguishably from a missing field, so every shape that would SELECT null documents
     * already fails closed, and only the two {@code exists}-shaped directions translate. Those
     * two are aligned under both conventions — a document whose field is absent is denied either
     * way. This test is the guard on that claim: if a future change starts emitting a
     * null-selecting query here, the adapter acquires a representation dependency and must gain
     * the option.
     */
    @Test
    void nullComparisonsAreRepresentationIndependent() {
        Operand eqNull = expressionOperand("eq",
                variableOperand("request.resource.attr.department"), nullValueOperand());
        Operand neNull = expressionOperand("ne",
                variableOperand("request.resource.attr.department"), nullValueOperand());

        // Null-SELECTING directions: rejected, so neither convention can be mistranslated.
        for (Operand rejected : List.of(eqNull, expressionOperand("not", neNull))) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, rejected);
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
            assertTrue(error.getMessage().contains("explicit null value from a missing field"));
        }

        // Presence-SELECTING directions: both translate to `exists`, which denies a document
        // with no value for the field under either convention.
        for (Operand accepted : List.of(neNull, expressionOperand("not", eqNull))) {
            PlanResourcesResponse resp =
                    buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, accepted);
            Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);
            assertEquals(Map.of("exists", Map.of("field", "department")),
                    ((Result.Conditional) result).query());
        }
    }

    @Test
    void hasIntersectionProducesTermsQuery() {
        Operand condition = expressionOperand("hasIntersection",
                variableOperand("request.resource.attr.tags"),
                listValueOperand("public", "draft"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("terms", Map.of("tags", List.of("public", "draft"))), query);
    }

    @Test
    void sizeGtZeroProducesExists() {
        Operand condition = expressionOperand("gt",
                expressionOperand("size",
                        variableOperand("request.resource.attr.ownedBy")),
                numberValueOperand(0));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("exists", Map.of("field", "ownedBy")), query);
    }

    @Test
    void sizeGeOneProducesExists() {
        Operand condition = expressionOperand("ge",
                expressionOperand("size",
                        variableOperand("request.resource.attr.ownedBy")),
                numberValueOperand(1));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("exists", Map.of("field", "ownedBy")), query);
    }

    @Test
    void sizeEqZeroFailsClosed() {
        Operand condition = expressionOperand("eq",
                expressionOperand("size",
                        variableOperand("request.resource.attr.ownedBy")),
                numberValueOperand(0));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("missing collection from an empty collection"));
    }

    @Test
    void unsupportedSizeComparisonThrows() {
        Operand condition = expressionOperand("gt",
                expressionOperand("size",
                        variableOperand("request.resource.attr.ownedBy")),
                numberValueOperand(5));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(ex.getMessage().contains("Unsupported size comparison"));
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
    void negatedNeNullFailsClosed() {
        Operand condition = expressionOperand("not",
                expressionOperand("ne",
                        variableOperand("request.resource.attr.department"),
                        nullValueOperand()));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("explicit null value from a missing field"));
    }

    @Test
    void negatedMembershipContainingNullRequiresFieldAndExcludesNonNullTerms() {
        Operand condition = expressionOperand("not",
                expressionOperand("in",
                        variableOperand("request.resource.attr.department"),
                        listValueOperandWithNull("engineering")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertEquals(Map.of("bool", Map.of("must", List.of(
                        Map.of("exists", Map.of("field", "department")),
                        Map.of("bool", Map.of("must_not", List.of(
                                Map.of("terms", Map.of("department", List.of("engineering"))))))))),
                ((Result.Conditional) result).query());
    }

    @Test
    void negatedMembershipContainingOnlyNullProducesExists() {
        Operand condition = expressionOperand("not",
                expressionOperand("in",
                        variableOperand("request.resource.attr.department"),
                        listValueOperandWithNull()));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertEquals(Map.of("exists", Map.of("field", "department")),
                ((Result.Conditional) result).query());
    }

    /**
     * The planner has no existence operator. {@code R.attr.x != null} arrives as {@code ne}
     * against a null value, and {@code isSet} is not a registered CEL function, so a policy
     * naming it does not compile and the operator can never reach the wire — see
     * cerbos/query-plan-adapters#261. These pin IS NOT NULL / IS NULL through that path.
     */
    @Test
    void neAgainstNullProducesExists() {
        Operand condition = expressionOperand("ne",
                variableOperand("request.resource.attr.department"),
                nullValueOperand());
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("exists", Map.of("field", "department")), query);
    }

    /** Value-first: the planner preserves source order, so {@code null != R.attr.x} inverts operands. */
    @Test
    void valueFirstNeAgainstNullProducesExists() {
        Operand condition = expressionOperand("ne",
                nullValueOperand(),
                variableOperand("request.resource.attr.department"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("exists", Map.of("field", "department")), query);
    }

    @Test
    void isSetIsRejectedRatherThanTranslated() {
        Operand condition = expressionOperand("isSet",
                variableOperand("request.resource.attr.department"),
                boolValueOperand(true));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(e.getMessage().contains("isSet"),
                "unknown operator must be named in the error, got: " + e.getMessage());
    }

    @Test
    void notBareBoolProducesFalseTerm() {
        Operand condition = expressionOperand("not",
                variableOperand("request.resource.attr.aBool"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertInstanceOf(Result.Conditional.class, result);
        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("term", Map.of("aBool", Map.of("value", false))), query);
    }

    // --- Collection operator helpers ---

    private static Operand lambdaOperand(String lambdaVar, Operand body) {
        return expressionOperand("lambda", body, variableOperand(lambdaVar));
    }

    // --- exists ---

    @Test
    void existsSingleConditionProducesNestedTerm() {
        // exists(tagObjects, t, t.name == "public")
        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t",
                        expressionOperand("eq",
                                variableOperand("t.name"),
                                stringValueOperand("public"))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("nested", Map.of(
                        "path", "tagObjects",
                        "query", Map.of("term", Map.of("tagObjects.name", Map.of("value", "public"))))),
                query);
    }

    @Test
    void existsMultiConditionProducesNestedBoolMust() {
        // exists(tagObjects, t, t.id == "tag1" AND t.name == "public")
        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t",
                        expressionOperand("and",
                                expressionOperand("eq",
                                        variableOperand("t.id"),
                                        stringValueOperand("tag1")),
                                expressionOperand("eq",
                                        variableOperand("t.name"),
                                        stringValueOperand("public")))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("nested", Map.of(
                        "path", "tagObjects",
                        "query", Map.of("bool", Map.of("must", List.of(
                                Map.of("term", Map.of("tagObjects.id", Map.of("value", "tag1"))),
                                Map.of("term", Map.of("tagObjects.name", Map.of("value", "public")))))))),
                query);
    }

    // --- all ---

    @Test
    void allFailsClosedBecauseMissingAndEmptyCollectionsAreIndistinguishable() {
        // all(tagObjects, t, t.name == "public")
        Operand condition = expressionOperand("all",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t",
                        expressionOperand("eq",
                                variableOperand("t.name"),
                                stringValueOperand("public"))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        resp, FIELD_MAP, NESTED_PATHS));
        assertTrue(error.getMessage().contains("missing collection from an empty collection"));
    }

    @Test
    void negatedAllStillProducesARequiredFalseElementQuery() {
        Operand condition = expressionOperand("not",
                expressionOperand("all",
                        variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t",
                                expressionOperand("eq",
                                        variableOperand("t.name"),
                                        stringValueOperand("public")))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                resp, FIELD_MAP, NESTED_PATHS);

        Map<String, Object> falseElement = Map.of("bool", Map.of("must", List.of(
                Map.of("exists", Map.of("field", "tagObjects.name")),
                Map.of("bool", Map.of("must_not", List.of(
                        Map.of("term", Map.of(
                                "tagObjects.name", Map.of("value", "public")))))))));
        assertEquals(Map.of("nested", Map.of(
                        "path", "tagObjects",
                        "query", falseElement)),
                ((Result.Conditional) result).query());
    }

    @Test
    void negatedExistsFailsClosedBecauseMissingAndEmptyCollectionsAreIndistinguishable() {
        Operand condition = expressionOperand("not",
                expressionOperand("exists",
                        variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t",
                                expressionOperand("eq",
                                        variableOperand("t.name"),
                                        stringValueOperand("public")))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        resp, FIELD_MAP, NESTED_PATHS));
        assertTrue(error.getMessage().contains("missing collection from an empty collection"));
    }

    // --- except ---

    @Test
    void exceptProducesNestedMustNot() {
        // except(tagObjects, t, t.name == "public")
        Operand condition = expressionOperand("except",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t",
                        expressionOperand("eq",
                                variableOperand("t.name"),
                                stringValueOperand("public"))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("nested", Map.of(
                        "path", "tagObjects",
                        "query", Map.of("bool", Map.of("must_not", List.of(
                                Map.of("term", Map.of("tagObjects.name", Map.of("value", "public")))))))),
                query);
    }

    // --- hasIntersection + map ---

    @Test
    void hasIntersectionWithMapRejectsMissingProjectionMembers() {
        // hasIntersection(map(tagObjects, t, t.name), ["public", "private"])
        Operand mapExpr = expressionOperand("map",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", variableOperand("t.name")));
        Operand condition = expressionOperand("hasIntersection",
                mapExpr,
                listValueOperand("public", "private"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS);

        Map<String, Object> query = ((Result.Conditional) result).query();
        Map<String, Object> matchingValue = Map.of("nested", Map.of(
                "path", "tagObjects",
                "query", Map.of("terms", Map.of("tagObjects.name", List.of("public", "private")))));
        Map<String, Object> missingProjection = Map.of("nested", Map.of(
                "path", "tagObjects",
                "query", Map.of("bool", Map.of("must_not", List.of(
                        Map.of("exists", Map.of("field", "tagObjects.name")))))));
        assertEquals(Map.of("bool", Map.of("must", List.of(
                matchingValue,
                Map.of("bool", Map.of("must_not", List.of(missingProjection)))))), query);
    }

    // --- flat hasIntersection unchanged ---

    @Test
    void flatHasIntersectionUnchangedWithNestedPaths() {
        Operand condition = expressionOperand("hasIntersection",
                variableOperand("request.resource.attr.tags"),
                listValueOperand("public", "draft"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("terms", Map.of("tags", List.of("public", "draft"))), query);
    }

    // --- error cases ---

    @Test
    void collectionOperatorWithoutNestedPathsThrows() {
        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t",
                        expressionOperand("eq",
                                variableOperand("t.name"),
                                stringValueOperand("public"))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(ex.getMessage().contains("not declared in nestedPaths"));
    }

    // --- matches (regex) ---

    @Test
    void matchesRejectsRe2DotBecauseLuceneDotIncludesNewlines() {
        Operand condition = expressionOperand("matches",
                variableOperand("request.resource.attr.aString"),
                stringValueOperand("^str.*"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("supported RE2/Lucene subset"));
    }

    @Test
    void matchesLiteralPrefixProducesPrefixQuery() {
        Operand condition = expressionOperand("matches",
                variableOperand("request.resource.attr.aString"),
                stringValueOperand("^h"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertEquals(Map.of("prefix", Map.of("aString", Map.of("value", "h"))),
                ((Result.Conditional) result).query());
    }

    @Test
    void matchesTreatsLuceneOptionalOperatorsAsLiterals() {
        Operand condition = expressionOperand("matches",
                variableOperand("request.resource.attr.aString"),
                stringValueOperand("^@$"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertEquals(Map.of("regexp", Map.of("aString", Map.of(
                "value", "@",
                "flags", "NONE"))), ((Result.Conditional) result).query());
    }

    @Test
    void matchesRejectsRegexSyntaxOutsideTheCommonSubset() {
        for (String pattern : List.of(
                "^\\d+$", "^(?i)admin$", "^a^b$", "^[[:alpha:]]$", "^a.b$", "^a.*b$")) {
            Operand condition = expressionOperand("matches",
                    variableOperand("request.resource.attr.aString"),
                    stringValueOperand(pattern));
            PlanResourcesResponse resp = buildResponse(
                    PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
            assertTrue(error.getMessage().contains("supported RE2/Lucene subset"), pattern);
        }
    }

    @Test
    void timestampEqualityAcceptsMillisecondExactLiteral() {
        Operand condition = expressionOperand("eq",
                timestampOperand(variableOperand("request.resource.attr.aString")),
                timestampOperand(stringValueOperand("2024-06-01T00:00:00.123Z")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertEquals(Map.of("term", Map.of("aString", Map.of(
                "value", "2024-06-01T00:00:00.123Z"))),
                ((Result.Conditional) result).query());
    }

    @Test
    void timestampEqualityRejectsSubMillisecondLiteral() {
        Operand condition = expressionOperand("eq",
                timestampOperand(variableOperand("request.resource.attr.aString")),
                timestampOperand(stringValueOperand("2024-06-01T00:00:00.123456Z")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("Sub-millisecond timestamp literals"));
    }

    @Test
    void timestampRejectsStringsOutsideStrictRfc3339() {
        for (String literal : List.of(
                "2024-W01-1T00:00:00Z",
                "2024-06-01 00:00:00Z",
                "0000-01-01T00:00:00Z",
                "2024-02-30T00:00:00Z",
                "0001-01-01T00:00:00+02:00",
                "9999-12-31T23:00:00-02:00")) {
            Operand condition = expressionOperand("eq",
                    timestampOperand(variableOperand("request.resource.attr.aString")),
                    timestampOperand(stringValueOperand(literal)));
            PlanResourcesResponse resp = buildResponse(
                    PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
            assertTrue(error.getMessage().contains("valid RFC 3339"), literal);
        }
    }

    // --- empty-collection (size(tags) == 0) ---

    @Test
    void sizeEqZeroOnTagsFailsClosed() {
        Operand condition = expressionOperand("eq",
                expressionOperand("size",
                        variableOperand("request.resource.attr.tags")),
                numberValueOperand(0));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("missing collection from an empty collection"));
    }

    @Test
    void negatedSizeEqZeroProducesExists() {
        Operand condition = expressionOperand("not",
                expressionOperand("eq",
                        expressionOperand("size",
                                variableOperand("request.resource.attr.tags")),
                        numberValueOperand(0)));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        assertEquals(Map.of("exists", Map.of("field", "tags")),
                ((Result.Conditional) result).query());
    }

    @Test
    void negatedSizeGtZeroFailsClosed() {
        Operand condition = expressionOperand("not",
                expressionOperand("gt",
                        expressionOperand("size",
                                variableOperand("request.resource.attr.tags")),
                        numberValueOperand(0)));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("missing collection from an empty collection"));
    }

    @Test
    void negatedHasIntersectionFailsClosed() {
        Operand condition = expressionOperand("not",
                expressionOperand("hasIntersection",
                        variableOperand("request.resource.attr.tags"),
                        listValueOperand("public")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("missing collection from an empty collection"));
    }

    @Test
    void negatedMembershipInDocumentCollectionFailsClosed() {
        Operand condition = expressionOperand("not",
                expressionOperand("in",
                        stringValueOperand("public"),
                        variableOperand("request.resource.attr.tags")));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("missing collection from an empty collection"));
    }

    // --- Unsupported: arithmetic / cast / ternary / index throw on expression operand ---

    @Test
    void arithAddOnFieldThrows() {
        // gt(add(aNumber, 1), 2)
        Operand condition = expressionOperand("gt",
                expressionOperand("add",
                        variableOperand("request.resource.attr.aNumber"),
                        numberValueOperand(1)),
                numberValueOperand(2));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
    }

    @Test
    void convertStringCastThrows() {
        // eq(string(aNumber), "1")
        Operand condition = expressionOperand("eq",
                expressionOperand("string",
                        variableOperand("request.resource.attr.aNumber")),
                stringValueOperand("1"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
    }

    @Test
    void indexListThrows() {
        // eq(index(ownedBy, 0), "user1")
        Operand condition = expressionOperand("eq",
                expressionOperand("index",
                        variableOperand("request.resource.attr.ownedBy"),
                        numberValueOperand(0)),
                stringValueOperand("user1"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
    }

    @Test
    void ternaryThrows() {
        // gt(if(aBool, aNumber, 0), 0)
        Operand condition = expressionOperand("gt",
                expressionOperand("if",
                        variableOperand("request.resource.attr.aBool"),
                        variableOperand("request.resource.attr.aNumber"),
                        numberValueOperand(0)),
                numberValueOperand(0));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
    }

    @Test
    void lambdaVariableMismatchThrows() {
        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t",
                        expressionOperand("eq",
                                variableOperand("x.name"),
                                stringValueOperand("public"))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS));
        assertTrue(ex.getMessage().contains("does not start with lambda variable"));
    }

    // --- Issue #229: locked-in operator/comparison shapes ---

    @Test
    void isNotSetFailsClosedWithoutNullValueSentinel() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.aString"),
                nullValueOperand());
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("explicit null value from a missing field"));
    }

    @Test
    void equalFieldToFieldIsUnsupported() {
        // aString == id → both operands are VARIABLE.
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.aString"),
                variableOperand("request.resource.attr.department"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP));
        assertTrue(error.getMessage().contains("cannot compare two document fields"));
    }

    @Test
    void equalBoolFalseProducesTermQuery() {
        // aBool == false → term aBool=false.
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.aBool"),
                boolValueOperand(false));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("term", Map.of("aBool", Map.of("value", false))), query);
    }

    @Test
    void inNumberProducesTermsQuery() {
        // aNumber in [1, 2, 3] → terms.
        Operand listValues = Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(ListValue.newBuilder()
                        .addValues(Value.newBuilder().setNumberValue(1))
                        .addValues(Value.newBuilder().setNumberValue(2))
                        .addValues(Value.newBuilder().setNumberValue(3))))
                .build();
        Operand condition = expressionOperand("in",
                variableOperand("request.resource.attr.aNumber"),
                listValues);
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(Map.of("terms", Map.of("aNumber", List.of(1L, 2L, 3L))), query);
    }

    // --- Issue #232: collection macro composition ---

    @Test
    void allWithMultiClauseLambdaBodyFailsClosed() {
        // all(tagObjects, t, t.name == "public" && t.id != "tag1")
        Operand condition = expressionOperand("all",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t",
                        expressionOperand("and",
                                expressionOperand("eq",
                                        variableOperand("t.name"),
                                        stringValueOperand("public")),
                                expressionOperand("ne",
                                        variableOperand("t.id"),
                                        stringValueOperand("tag1")))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        resp, FIELD_MAP, NESTED_PATHS));
        assertTrue(error.getMessage().contains("missing collection from an empty collection"));
    }

    @Test
    void mapComparedToLiteralListThrows() {
        // TODO(#232): map(...) compared directly to a list literal is unsupported.
        // Only hasIntersection(map(...), [...]) is recognised today.
        Operand condition = expressionOperand("eq",
                expressionOperand("map",
                        variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t", variableOperand("t.id"))),
                listValueOperand("tag1", "tag2"));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS));
    }

    @Test
    void sizeOfFilterThrows() {
        // TODO(#232): size(filter(...)) > 0 is unsupported. The size handler
        // requires its single operand to be a variable (a direct collection
        // reference), not a filter() expression.
        Operand condition = expressionOperand("gt",
                expressionOperand("size",
                        expressionOperand("filter",
                                variableOperand("request.resource.attr.tagObjects"),
                                lambdaOperand("t",
                                        expressionOperand("eq",
                                                variableOperand("t.name"),
                                                stringValueOperand("public"))))),
                numberValueOperand(0));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS));
    }

    @Test
    void orLeafExistsProducesBoolShouldWithNested() {
        // aBool == true OR exists(tagObjects, t, t.name == "public")
        // Heterogeneous operands: a leaf term + a nested-exists subquery.
        Operand condition = expressionOperand("or",
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aBool"),
                        boolValueOperand(true)),
                expressionOperand("exists",
                        variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t",
                                expressionOperand("eq",
                                        variableOperand("t.name"),
                                        stringValueOperand("public")))));
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS);

        Map<String, Object> query = ((Result.Conditional) result).query();
        assertEquals(
                Map.of("bool", Map.of(
                        "should", List.of(
                                Map.of("term", Map.of("aBool", Map.of("value", true))),
                                Map.of("nested", Map.of(
                                        "path", "tagObjects",
                                        "query", Map.of("term", Map.of("tagObjects.name", Map.of("value", "public")))))),
                        "minimum_should_match", 1)),
                query);
    }

    // --- Known-value collections (planner unroll cliff) ---
    //
    // The planner unrolls exists/all over a known collection into an or/and chain at <= 10
    // elements (cerbos/cerbos#2570, #2817) and ships the lambda with a literal value-list
    // collection above that. A literal list is fully known at plan time, so — unlike a nested
    // field — there is no missing-versus-empty ambiguity and the fold is exact under negation.

    private static Operand emptyListValueOperand() {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(ListValue.newBuilder()))
                .build();
    }

    private static Operand structListValueOperand(String field, String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String value : values) {
            list.addValues(Value.newBuilder().setStructValue(
                    Struct.newBuilder().putFields(
                            field, Value.newBuilder().setStringValue(value).build())));
        }
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(list))
                .build();
    }

    private static Operand valueListMacro(String operator, Operand collection, Operand body) {
        return expressionOperand(operator, collection, lambdaOperand("t", body));
    }

    private static Map<String, Object> translate(Operand condition) {
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL, condition);
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, NESTED_PATHS);
        return ((Result.Conditional) result).query();
    }

    @Test
    void existsOverValueListFoldsToBoolShould() {
        Map<String, Object> query = translate(valueListMacro("exists",
                listValueOperand("string", "anotherString"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"),
                        variableOperand("t"))));

        assertEquals(
                Map.of("bool", Map.of(
                        "should", List.of(
                                Map.of("term", Map.of("aString", Map.of("value", "string"))),
                                Map.of("term", Map.of("aString", Map.of("value", "anotherString")))),
                        "minimum_should_match", 1)),
                query);
    }

    @Test
    void allOverValueListFoldsToBoolMust() {
        // `all` over a *nested field* fails closed because a missing collection and an empty
        // one are indistinguishable; a literal list has neither problem.
        Map<String, Object> query = translate(valueListMacro("all",
                listValueOperand("string", "anotherString"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"),
                        variableOperand("t"))));

        assertEquals(
                Map.of("bool", Map.of("must", List.of(
                        Map.of("term", Map.of("aString", Map.of("value", "string"))),
                        Map.of("term", Map.of("aString", Map.of("value", "anotherString")))))),
                query);
    }

    @Test
    void negatedExistsOverValueListFoldsToBoolMustOfNegations() {
        // Negated exists over a nested field fails closed; over a literal list it is exactly
        // "every element's comparison is false".
        Map<String, Object> query = translate(expressionOperand("not",
                valueListMacro("exists",
                        listValueOperand("string", "anotherString"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t")))));

        // The invariant that matters: the fold is indistinguishable from the or-chain the
        // planner emits below the threshold, under negation as well as positively.
        assertEquals(translate(expressionOperand("not",
                expressionOperand("or",
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                stringValueOperand("string")),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                stringValueOperand("anotherString"))))),
                query);
    }

    @Test
    void variablePathDrillsIntoElementFields() {
        Map<String, Object> query = translate(valueListMacro("exists",
                structListValueOperand("name", "string", "anotherString"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"),
                        variableOperand("t.name"))));

        assertEquals(
                Map.of("bool", Map.of(
                        "should", List.of(
                                Map.of("term", Map.of("aString", Map.of("value", "string"))),
                                Map.of("term", Map.of("aString", Map.of("value", "anotherString")))),
                        "minimum_should_match", 1)),
                query);
    }

    @Test
    void emptyValueListKeepsCelIdentitySemantics() {
        Operand body = expressionOperand("eq",
                variableOperand("request.resource.attr.aString"),
                variableOperand("t"));

        // exists over [] is false; all over [] is true.
        assertEquals(Map.of("match_none", Map.of()),
                translate(valueListMacro("exists", emptyListValueOperand(), body)));
        assertEquals(Map.of("match_all", Map.of()),
                translate(valueListMacro("all", emptyListValueOperand(), body)));

        // ...and each flips under negation.
        assertEquals(Map.of("match_all", Map.of()),
                translate(expressionOperand("not",
                        valueListMacro("exists", emptyListValueOperand(), body))));
        assertEquals(Map.of("match_none", Map.of()),
                translate(expressionOperand("not",
                        valueListMacro("all", emptyListValueOperand(), body))));
    }

    @Test
    void nestedLambdaRebindingTheVariableShadowsSubstitution() {
        // The outer t is substituted; the inner exists rebinds t over a nested path, so its
        // body must keep referencing the inner binding untouched.
        Operand condition = valueListMacro("exists",
                listValueOperand("ignored-a", "ignored-b"),
                expressionOperand("exists",
                        variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t",
                                expressionOperand("eq",
                                        variableOperand("t.name"),
                                        stringValueOperand("public")))));

        Map<String, Object> nested = Map.of("nested", Map.of(
                "path", "tagObjects",
                "query", Map.of("term", Map.of("tagObjects.name", Map.of("value", "public")))));
        assertEquals(
                Map.of("bool", Map.of(
                        "should", List.of(nested, nested),
                        "minimum_should_match", 1)),
                translate(condition));
    }

    @Test
    void missingElementFieldFailsClosed() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> translate(valueListMacro("exists",
                        structListValueOperand("name", "string"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t.missing")))));
        assertTrue(error.getMessage().contains("Cannot resolve \"t.missing\""));
    }

    @ParameterizedTest
    @ValueSource(strings = {"exists_one", "filter", "map", "except"})
    void unfoldableMacroOverValueListFailsClosed(String operator) {
        // `filter` and `map` reach the leaf traversal rather than the collection handler, so
        // this also pins that they name the real limitation instead of an operand-shape error.
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> translate(valueListMacro(operator,
                        listValueOperand("string"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t")))));
        assertTrue(error.getMessage()
                        .contains(operator + " over a literal collection value is not supported"),
                "unexpected message: " + error.getMessage());
    }

    @Test
    void nonListCollectionValueFailsClosed() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> translate(valueListMacro("exists",
                        stringValueOperand("not a list"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t")))));
        assertTrue(error.getMessage()
                .contains("exists over a literal collection requires a list value"));
    }

    @Test
    void valueListMacroNeedsNoNestedPathDeclaration() {
        // The nested-path requirement exists to reach relation documents; a literal list has
        // no document to reach, so translation must succeed with an empty nestedPaths set.
        PlanResourcesResponse resp = buildResponse(PlanResourcesFilter.Kind.KIND_CONDITIONAL,
                valueListMacro("exists",
                        listValueOperand("string"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t"))));

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(resp, FIELD_MAP, Set.of());
        assertEquals(
                Map.of("bool", Map.of(
                        "should", List.of(Map.of("term", Map.of("aString", Map.of("value", "string")))),
                        "minimum_should_match", 1)),
                ((Result.Conditional) result).query());
    }
}
