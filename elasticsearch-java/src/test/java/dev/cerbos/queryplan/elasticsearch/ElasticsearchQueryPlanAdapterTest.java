/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests on hand-built plans, grouped under the three kinds of material CLAUDE.md allows only in a
 * unit test ("What a translator unit test may pin"). Corpus actions are covered by
 * {@link ElasticsearchTranslatorTest}, which reads real planner output. Needs no Docker.
 */
class ElasticsearchQueryPlanAdapterTest {

    private static final Map<String, String> FIELD_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.department", "department"),
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.title", "title"),
            Map.entry("request.resource.attr.scope", "scope"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.ownedBy", "ownedBy"),
            Map.entry("request.resource.attr.tagObjects", "tagObjects")
    );

    private static final Set<String> NESTED_PATHS = Set.of("tagObjects");

    /** {@code tags} and {@code ownedBy} are flat keyword arrays; {@code aString} is a string. */
    private static final Set<String> COLLECTION_FIELDS = Set.of("tags", "ownedBy");

    /** A flat array declares its element type; a nested sub-field is declared by its full path. */
    private static final Map<String, ScalarType> SCALAR_TYPES = Map.ofEntries(
            Map.entry("department", ScalarType.STRING),
            Map.entry("aBool", ScalarType.BOOLEAN),
            Map.entry("aString", ScalarType.STRING),
            Map.entry("aNumber", ScalarType.NUMBER),
            Map.entry("title", ScalarType.STRING),
            Map.entry("scope", ScalarType.STRING),
            Map.entry("tags", ScalarType.STRING),
            Map.entry("ownedBy", ScalarType.STRING),
            Map.entry("tagObjects.name", ScalarType.STRING));

    private static final Options OPTIONS = Options.of(FIELD_MAP)
            .withNestedPaths(NESTED_PATHS)
            .withCollectionFields(COLLECTION_FIELDS)
            .withScalarTypes(SCALAR_TYPES);

    private static PlanResourcesResponse conditionalPlan(Operand condition) {
        return PlanResourcesResponse.newBuilder()
                .setFilter(PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(condition))
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

    private static Operand lambdaOperand(String lambdaVar, Operand body) {
        return expressionOperand("lambda", body, variableOperand(lambdaVar));
    }

    private static Operand variableOperand(String name) {
        return Operand.newBuilder().setVariable(name).build();
    }

    private static Operand valueOperand(Value value) {
        return Operand.newBuilder().setValue(value).build();
    }

    private static Value string(String value) {
        return Value.newBuilder().setStringValue(value).build();
    }

    private static Value number(double value) {
        return Value.newBuilder().setNumberValue(value).build();
    }

    private static final Value NULL = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();

    private static Value list(Value... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (Value value : values) {
            list.addValues(value);
        }
        return Value.newBuilder().setListValue(list).build();
    }

    private static Value struct(String field, Value value) {
        return Value.newBuilder()
                .setStructValue(Struct.newBuilder().putFields(field, value))
                .build();
    }

    private static Operand stringValueOperand(String val) {
        return valueOperand(string(val));
    }

    private static Operand numberValueOperand(double val) {
        return valueOperand(number(val));
    }

    private static Operand listValueOperand(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String v : values) {
            list.addValues(string(v));
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }

    private static Operand emptyListValueOperand() {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(ListValue.newBuilder()))
                .build();
    }

    private static Operand structListValueOperand(String field, String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String value : values) {
            list.addValues(struct(field, string(value)));
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }

    private static Operand valueListMacro(String operator, Operand collection, Operand body) {
        return expressionOperand(operator, collection, lambdaOperand("t", body));
    }

    private static Operand hierarchy(Operand path, String delimiter) {
        return expressionOperand("hierarchy", path, stringValueOperand(delimiter));
    }

    private static Operand sizeOf(String variable) {
        return expressionOperand("size", variableOperand(variable));
    }

    private static Map<String, Object> translate(Operand condition) {
        return translate(condition, OPTIONS);
    }

    private static Map<String, Object> translate(Operand condition, Options options) {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition), options);
        return ((Result.Conditional) result).query();
    }

    private static IllegalArgumentException refusal(Operand condition) {
        return refusal(condition, OPTIONS);
    }

    private static IllegalArgumentException refusal(Operand condition, Options options) {
        return assertThrows(IllegalArgumentException.class, () -> translate(condition, options));
    }

    private static Map<String, Object> term(String field, Object value) {
        return Map.of("term", Map.of(field, Map.of("value", value)));
    }

    private static Map<String, Object> exists(String field) {
        return Map.of("exists", Map.of("field", field));
    }

    private static Map<String, Object> mustNot(Map<String, Object> query) {
        return Map.of("bool", Map.of("must_not", List.of(query)));
    }

    private static Map<String, Object> nested(String path, Map<String, Object> query) {
        return Map.of("nested", Map.of("path", path, "query", query));
    }

    private static Map<String, Object> definedAndNot(String field, Map<String, Object> query) {
        return Map.of("bool", Map.of("must", List.of(exists(field), mustNot(query))));
    }

    // ============================================================================================
    // KIND 1 — a branch CEL itself cannot reach
    // ============================================================================================

    /**
     * CEL has neither function, so the checker fails with {@code undeclared reference} and no
     * plan can carry them. Existence is spelled {@code R.attr.x != null} instead (corpus action
     * {@code null-ne}).
     */
    @ParameterizedTest
    @ValueSource(strings = {"unsupported_op", "isSet"})
    void anOperatorTheAdapterDoesNotKnowIsRefusedByName(String operator) {
        IllegalArgumentException ex = refusal(expressionOperand(operator,
                variableOperand("request.resource.attr.department"), stringValueOperand("value")));
        assertTrue(ex.getMessage().contains("Unknown operator: " + operator), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }

    /**
     * CEL's checker rejects an unbound lambda variable ({@code undeclared reference to 'x'}), so no
     * plan carries one.
     */
    @Test
    void aLambdaBodyReferencingAnUnboundVariableIsRefused() {
        IllegalArgumentException ex = refusal(expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("eq",
                        variableOperand("x.name"), stringValueOperand("public")))));
        assertTrue(ex.getMessage().contains("does not start with lambda variable"), ex.getMessage());
        assertInstanceOf(MalformedPlanException.class, ex);
    }

    /**
     * CEL rejects a scalar as a comprehension range ({@code expression of type 'string' cannot be
     * range of a comprehension}), so the planner never emits one.
     */
    @Test
    void aValueListMacroOverANonListLiteralIsRefused() {
        IllegalArgumentException ex = refusal(valueListMacro("exists",
                stringValueOperand("not a list"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"), variableOperand("t"))));
        assertTrue(ex.getMessage()
                .contains("exists over a literal collection requires a list value"), ex.getMessage());
        assertInstanceOf(MalformedPlanException.class, ex);
    }

    /**
     * CEL's {@code timestamp()} rejects each of these literals (for example {@code day out of
     * range}), so the planner cannot emit them. The adapter still validates them and reports a
     * malformed plan.
     */
    @Test
    void aTimestampLiteralOutsideStrictRfc3339IsRefused() {
        for (String literal : List.of(
                "2024-W01-1T00:00:00Z",
                "2024-06-01 00:00:00Z",
                "0000-01-01T00:00:00Z",
                "2024-02-30T00:00:00Z",
                "0001-01-01T00:00:00+02:00",
                "9999-12-31T23:00:00-02:00")) {
            IllegalArgumentException ex = refusal(expressionOperand("eq",
                    timestampOperand(variableOperand("request.resource.attr.aString")),
                    timestampOperand(stringValueOperand(literal))));
            assertTrue(ex.getMessage().contains("valid RFC 3339"), literal);
            assertInstanceOf(MalformedPlanException.class, ex, literal);
        }
    }

    // ============================================================================================
    // KIND 2 — a caller-supplied argument the corpus structurally cannot vary
    // ============================================================================================

    /** The field map is a caller argument; the corpus maps every path so it never lands here. */
    @Test
    void anUnmappedReferenceIsRefusedRatherThanUsedVerbatim() {
        IllegalArgumentException ex = refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.nonexistent"),
                stringValueOperand("value")));
        assertTrue(ex.getMessage().contains("Unknown attribute"), ex.getMessage());
        assertInstanceOf(UnmappedAttributeException.class, ex);
    }

    /** {@link OperatorFunction} overrides are a caller argument; no plan selects one. */
    @Test
    void aCallerSuppliedOperatorOverrideReplacesTheDefaultTranslation() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.department"),
                stringValueOperand("engineering"));
        Map<String, OperatorFunction> overrides = Map.of(
                "eq", (field, value) -> Map.of("match", Map.of(field, value)));

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition), FIELD_MAP, overrides);

        assertEquals(Map.of("match", Map.of("department", "engineering")),
                ((Result.Conditional) result).query());
    }

    /**
     * {@code in} reaches its override in every polarity. A negated ordering operator uses its
     * mirror's override ({@code not lt} uses {@code ge}), and a positive {@code ne} with no
     * {@code ne} override is {@code exists AND NOT eq}, using the {@code eq} override.
     */
    @Test
    void anOverrideReachesEveryPolarityOfItsOperator() {
        Map<String, OperatorFunction> overrides = Map.of(
                "in", (field, value) -> Map.of("custom_in", Map.of(field, value)),
                "ge", (field, value) -> Map.of("custom_ge", Map.of(field, value)),
                "eq", (field, value) -> Map.of("custom_eq", Map.of(field, value)));
        Options options = OPTIONS.withOperatorOverrides(overrides);
        Operand membership = expressionOperand("in",
                variableOperand("request.resource.attr.aString"), listValueOperand("a", "b"));
        Operand nullAwareMembership = expressionOperand("in",
                variableOperand("request.resource.attr.aString"),
                valueOperand(list(string("a"), NULL)));

        assertEquals(Map.of("custom_in", Map.of("aString", List.of("a", "b"))),
                translate(membership, options));
        assertEquals(definedAndNot("aString", Map.of("custom_in", Map.of("aString", List.of("a", "b")))),
                translate(expressionOperand("not", membership), options));
        assertEquals(definedAndNot("aString", Map.of("custom_in", Map.of("aString", List.of("a")))),
                translate(expressionOperand("not", nullAwareMembership), options));
        assertEquals(Map.of("custom_ge", Map.of("aNumber", 5L)),
                translate(expressionOperand("not", expressionOperand("lt",
                        variableOperand("request.resource.attr.aNumber"), numberValueOperand(5))),
                        options));
        assertEquals(definedAndNot("aString", Map.of("custom_eq", Map.of("aString", "x"))),
                translate(expressionOperand("ne",
                        variableOperand("request.resource.attr.aString"), stringValueOperand("x")),
                        options));
    }

    /**
     * Hierarchy relations and a {@code ^literal} {@code matches} emit {@code prefix} or
     * {@code terms}, but do not use the {@code startsWith} or {@code in} overrides.
     */
    @Test
    void hierarchyAndTheLiteralPrefixOfMatchesDoNotBorrowAnotherOperatorsOverride() {
        Options options = OPTIONS.withOperatorOverrides(Map.of(
                "startsWith", (field, value) -> Map.of("custom_prefix", Map.of(field, value)),
                "in", (field, value) -> Map.of("custom_in", Map.of(field, value))));

        assertEquals(Map.of("prefix", Map.of("scope", Map.of("value", "a.b."))),
                translate(expressionOperand("descendentOf",
                        hierarchy(variableOperand("request.resource.attr.scope"), "."),
                        hierarchy(stringValueOperand("a.b"), ".")), options));
        assertEquals(Map.of("terms", Map.of("scope", List.of("a", "a.b"))),
                translate(expressionOperand("ancestorOf",
                        hierarchy(variableOperand("request.resource.attr.scope"), "."),
                        hierarchy(stringValueOperand("a.b.c"), ".")), options));
        assertEquals(Map.of("prefix", Map.of("aString", Map.of("value", "adm"))),
                translate(expressionOperand("matches",
                        variableOperand("request.resource.attr.aString"),
                        stringValueOperand("^adm")), options));
    }

    /**
     * The adapter cannot read the index mapping, so a macro over a path not declared in
     * {@code nestedPaths} fails closed instead of emitting a {@code nested} query that matches
     * nothing.
     */
    @Test
    void aCollectionMacroOverAnUndeclaredNestedPathIsRefused() {
        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("eq",
                        variableOperand("t.name"), stringValueOperand("public"))));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition), FIELD_MAP));
        assertTrue(ex.getMessage().contains("not declared in nestedPaths"), ex.getMessage());
        assertInstanceOf(UnmappedAttributeException.class, ex);
    }

    /** A macro over a literal value list has no document to reach, so needs no nested path. */
    @Test
    void aValueListMacroNeedsNoNestedPathDeclaration() {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(valueListMacro("exists", listValueOperand("string"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t")))),
                Options.of(FIELD_MAP).withScalarTypes(SCALAR_TYPES));

        assertEquals(Map.of("bool", Map.of(
                        "should", List.of(term("aString", "string")),
                        "minimum_should_match", 1)),
                ((Result.Conditional) result).query());
    }

    /**
     * The adapter cannot tell {@code size(tags)} from {@code size(aString)} without a mapping, so
     * a {@code size()} check over a flat array needs a {@code collectionFields} declaration. A
     * nested path needs no second declaration.
     */
    @Test
    void sizeOverADeclaredFlatCollectionIsAnExistsCheckAndUndeclaredItIsRefused() {
        Operand nonEmpty = expressionOperand("gt", sizeOf("request.resource.attr.tags"),
                numberValueOperand(0));

        assertEquals(exists("tags"), translate(nonEmpty));
        assertEquals(nested("tagObjects", Map.of("match_all", Map.of())),
                translate(expressionOperand("gt", sizeOf("request.resource.attr.tagObjects"),
                        numberValueOperand(0))));

        IllegalArgumentException ex = refusal(nonEmpty, OPTIONS.withCollectionFields(Set.of()));
        assertTrue(ex.getMessage().contains("size() over a field not declared as a collection"),
                ex.getMessage());
        assertTrue(ex.getMessage().contains("'tags' is declared in nestedPaths or collectionFields"),
                ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }

    /**
     * {@link Options} copies every collection on the way in, each {@code with…} returns a new
     * instance, and the positional overloads equal the {@link Options} form with the rest empty.
     */
    @Test
    void optionsAreImmutableAndTheConvenienceOverloadsDelegateToThem() {
        Map<String, String> mutable = new HashMap<>(FIELD_MAP);
        Options options = Options.of(mutable);
        mutable.clear();
        assertEquals(FIELD_MAP, options.fieldMap());

        Options widened = options.withNestedPaths(NESTED_PATHS);
        assertEquals(Set.of(), options.nestedPaths());
        assertEquals(NESTED_PATHS, widened.nestedPaths());
        assertEquals(Set.of(), widened.collectionFields());
        assertEquals(Set.of(), widened.explicitNullAttributes());
        assertEquals(Map.of(), widened.operatorOverrides());
        Map<String, ElasticsearchQueryPlanAdapter.ScalarType> types = new HashMap<>();
        types.put("aNumber", ElasticsearchQueryPlanAdapter.ScalarType.NUMBER);
        Options typed = options.withScalarTypes(types);
        types.clear();
        assertEquals(ElasticsearchQueryPlanAdapter.ScalarType.NUMBER, typed.scalarTypes().get("aNumber"));
        assertEquals(typed.scalarTypes(), typed.withNestedPaths(NESTED_PATHS).scalarTypes());
        assertThrows(UnsupportedOperationException.class, () -> typed.scalarTypes().clear());
        assertThrows(NullPointerException.class, () -> options.withScalarTypes(null));
        assertThrows(NullPointerException.class, () -> Options.of(null));
        assertThrows(NullPointerException.class, () -> options.withCollectionFields(null));

        // The positional overloads carry no scalar types, so compare through an `eq` override,
        // which needs no declaration.
        Map<String, OperatorFunction> overrides = Map.of(
                "eq", (field, value) -> Map.of("custom_eq", Map.of(field, value)));
        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("eq",
                        variableOperand("t.name"), stringValueOperand("public"))));
        assertEquals(
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition), FIELD_MAP, overrides, NESTED_PATHS, Set.of()),
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition), widened.withOperatorOverrides(overrides)));
    }

    /**
     * A comparison against a field with no declared scalar type is refused: Elasticsearch coerces
     * a query term to the mapped type ({@code "5"} matches {@code 5}), but CEL's cross-type
     * equality is false. An override owns its operator, so it needs no declaration.
     */
    @Test
    void aComparisonAgainstAnUndeclaredFieldIsRefusedRatherThanCoerced() {
        Options untyped = OPTIONS.withScalarTypes(Map.of());
        List<Operand> comparisons = List.of(
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aNumber"), stringValueOperand("5")),
                expressionOperand("not", expressionOperand("ne",
                        variableOperand("request.resource.attr.aNumber"), stringValueOperand("5"))),
                expressionOperand("lt",
                        numberValueOperand(5), variableOperand("request.resource.attr.aString")),
                expressionOperand("startsWith",
                        variableOperand("request.resource.attr.aNumber"), stringValueOperand("5")),
                expressionOperand("in",
                        variableOperand("request.resource.attr.aBool"), listValueOperand("true")),
                expressionOperand("exists",
                        variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t", expressionOperand("eq",
                                variableOperand("t.name"), numberValueOperand(5)))));
        for (Operand comparison : comparisons) {
            IllegalArgumentException ex = refusal(comparison, untyped);
            assertInstanceOf(UnmappedAttributeException.class, ex);
            assertTrue(ex.getMessage().contains("has no declared scalar type"), ex.getMessage());
            assertTrue(ex.getMessage().contains("Options.withScalarTypes"), ex.getMessage());
        }
        assertTrue(refusal(comparisons.get(5), untyped).getMessage()
                .startsWith("Field 'tagObjects.name' has no declared scalar type"));

        Options overridden = untyped.withOperatorOverrides(Map.of(
                "eq", (field, value) -> Map.of("custom_eq", Map.of(field, value))));
        assertEquals(Map.of("custom_eq", Map.of("aNumber", "5")),
                translate(comparisons.get(0), overridden));
    }

    /**
     * With a declared type, a cross-type literal follows CEL: {@code ==} matches nothing,
     * {@code !=} holds wherever the field exists, and wrong-type elements are dropped from
     * {@code terms}.
     */
    @Test
    void aDeclaredTypeAnswersACrossTypeComparisonAsCelDoes() {
        Map<String, Object> matchNone = Map.of("match_none", Map.of());
        Operand eqString = expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), stringValueOperand("5"));
        assertEquals(matchNone, translate(eqString));
        assertEquals(exists("aNumber"), translate(expressionOperand("not", eqString)));
        assertEquals(exists("aNumber"), translate(expressionOperand("ne",
                variableOperand("request.resource.attr.aNumber"), stringValueOperand("5"))));

        Operand mixedMembership = expressionOperand("in",
                variableOperand("request.resource.attr.aNumber"),
                valueOperand(list(string("5"), number(6))));
        assertEquals(Map.of("terms", Map.of("aNumber", List.of(6L))), translate(mixedMembership));
        assertEquals(definedAndNot("aNumber", Map.of("terms", Map.of("aNumber", List.of(6L)))),
                translate(expressionOperand("not", mixedMembership)));

        Operand wrongTypeMembership = expressionOperand("in",
                variableOperand("request.resource.attr.aBool"), listValueOperand("true"));
        assertEquals(matchNone, translate(wrongTypeMembership));
        assertEquals(exists("aBool"), translate(expressionOperand("not", wrongTypeMembership)));
    }

    /**
     * A {@code map()}-projected {@code hasIntersection} lowers to {@code terms} on the nested
     * sub-field, so that sub-field's declared type decides which literals can match, and an
     * undeclared one is refused. The corpus has no nested sub-field a cross-type literal could be
     * coerced onto.
     */
    @Test
    void aMapProjectedIntersectionNeedsTheSubFieldsTypeAndDropsTheWrongOnes() {
        Operand projection = expressionOperand("map",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", variableOperand("t.name")));
        Operand mixed = expressionOperand("hasIntersection", projection,
                valueOperand(list(string("public"), number(5))));

        Map<String, Object> missingProjection = nested("tagObjects",
                mustNot(exists("tagObjects.name")));
        assertEquals(Map.of("bool", Map.of("must", List.of(
                        nested("tagObjects", Map.of("terms",
                                Map.of("tagObjects.name", List.of("public")))),
                        mustNot(missingProjection)))),
                translate(mixed));
        assertEquals(Map.of("match_none", Map.of()), translate(expressionOperand("hasIntersection",
                projection, valueOperand(list(number(5))))));

        IllegalArgumentException ex = refusal(mixed, OPTIONS.withScalarTypes(Map.of()));
        assertInstanceOf(UnmappedAttributeException.class, ex);
        assertTrue(ex.getMessage().startsWith("Field 'tagObjects.name' has no declared scalar type: "
                + "hasIntersection"), ex.getMessage());
    }

    /**
     * Every refusal is one of three {@link IllegalArgumentException} subtypes, so a caller can
     * route on type instead of message.
     */
    @Test
    void refusalsAreTypedByTheirMechanism() {
        assertInstanceOf(UnsupportedPlanShapeException.class, refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.aString"),
                variableOperand("request.resource.attr.title"))));
        assertInstanceOf(UnmappedAttributeException.class, refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.missing"), stringValueOperand("x"))));
        assertInstanceOf(MalformedPlanException.class, refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.aString"))));
        assertInstanceOf(MalformedPlanException.class, refusal(expressionOperand("not")));
        assertInstanceOf(MalformedPlanException.class, refusal(expressionOperand("eq",
                stringValueOperand("a"), stringValueOperand("b"))));
    }

    /**
     * A double outside {@code [-2^63, 2^63)} stays a double, because casting it to {@code long}
     * would saturate and change the value. The corpus covers negative out-of-range literals
     * ({@code double-huge-lt}, {@code double-huge-gt}) but not these boundaries.
     */
    @Test
    void anIntegralLiteralOutsideTheLongRangeStaysADouble() {
        assertEquals(term("aNumber", 1.0e19), translate(expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), numberValueOperand(1.0e19))));
        assertEquals(term("aNumber", 0x1p63), translate(expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), numberValueOperand(0x1p63))));
        assertEquals(term("aNumber", Long.MIN_VALUE), translate(expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), numberValueOperand(-0x1p63))));
        assertEquals(term("aNumber", 4294967296L), translate(expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), numberValueOperand(4294967296.0))));
    }

    /**
     * A protobuf value can hold a non-finite number, but JSON cannot, so it is refused. The PDP
     * cannot serialize such a literal and the corpus's {@code nan-ord-*} actions keep the division
     * unfolded, so no corpus action reaches this.
     */
    @Test
    void aNonFiniteNumericLiteralIsRefusedAtTheLeaf() {
        for (double value : new double[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN}) {
            IllegalArgumentException ex = refusal(expressionOperand("gt",
                    variableOperand("request.resource.attr.aNumber"), numberValueOperand(value)));
            assertTrue(ex.getMessage().contains("non-finite numeric literal"), ex.getMessage());
            assertInstanceOf(UnsupportedPlanShapeException.class, ex);
        }
        IllegalArgumentException inList = refusal(expressionOperand("in",
                variableOperand("request.resource.attr.aNumber"),
                valueOperand(list(number(1), number(Double.NaN)))));
        assertTrue(inList.getMessage().contains("non-finite numeric literal"), inList.getMessage());
    }

    // ============================================================================================
    // KIND 3 — a policy can reach these, and the corpus does not carry them yet
    //
    // Each is tracked by cerbos/query-plan-adapters#509 and deleted when its corpus action lands.
    // ============================================================================================

    /**
     * <strong>Corpus gap.</strong> Tracked by #509. Direct {@code filter}/{@code map} in boolean
     * position over a principal value list.
     */
    @ParameterizedTest
    @ValueSource(strings = {"filter", "map"})
    void anUnfoldableMacroOverAValueListIsRefusedByName(String operator) {
        IllegalArgumentException ex = refusal(valueListMacro(operator,
                listValueOperand("string"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"), variableOperand("t"))));
        assertTrue(ex.getMessage()
                        .contains(operator + " over a literal collection value is not supported"),
                "unexpected message: " + ex.getMessage());
    }

    /**
     * <strong>Corpus gap.</strong> Tracked by #509. {@code except} under negation and inside a
     * nested lambda; the corpus carries the root, size and comparison forms.
     */
    @Test
    void exceptIsRefusedByNameWhereverItAppears() {
        Operand except = expressionOperand("except",
                variableOperand("request.resource.attr.tags"), listValueOperand("archived"));
        List<Operand> positions = List.of(
                expressionOperand("not", except),
                expressionOperand("exists", variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t", expressionOperand("except",
                                variableOperand("t.labels"), listValueOperand("archived")))));
        for (Operand condition : positions) {
            IllegalArgumentException ex = refusal(condition);
            assertTrue(ex.getMessage().contains(
                    "except is not supported: Cerbos except(list, list) computes a list difference"),
                    ex.getMessage());
            assertTrue(ex.getMessage().contains("R.attr.tags.exists(t, !(t in [\"x\"]))"),
                    ex.getMessage());
            assertInstanceOf(UnsupportedPlanShapeException.class, ex);
        }
    }

    /**
     * <strong>Corpus gap.</strong> Tracked by #509. {@code size(c) != 0} and its negation over a
     * flat array; the negation is refused because an empty array and a missing field look the
     * same.
     */
    @Test
    void everySpellingOfNonEmptinessIsTheSameCheck() {
        Operand notEqualZero = expressionOperand("ne",
                sizeOf("request.resource.attr.ownedBy"), numberValueOperand(0));
        assertEquals(exists("ownedBy"), translate(notEqualZero));
        IllegalArgumentException ex = refusal(expressionOperand("not", notEqualZero));
        assertTrue(ex.getMessage().contains("emptiness cannot distinguish a missing collection"),
                ex.getMessage());
    }

    /**
     * <strong>Corpus gap.</strong> Tracked by #509. A null-bearing {@code hasIntersection} inside a
     * nested lambda.
     */
    @Test
    void hasIntersectionWithANullElementIsRefusedWhicheverPositionCarriesIt() {
        Operand withNull = valueOperand(list(string("public"), NULL));
        List<Operand> positions = List.of(
                expressionOperand("exists", variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t", expressionOperand("hasIntersection",
                                variableOperand("t.labels"), withNull))));
        for (Operand condition : positions) {
            IllegalArgumentException ex = refusal(condition);
            assertTrue(ex.getMessage().contains(
                    "hasIntersection with null requires an explicit null-value mapping"),
                    ex.getMessage());
        }
    }

    /**
     * <strong>Corpus gap.</strong> Tracked by #509. Ordering and string operators against a list,
     * plus decoder refusals for raw protobuf map and list values. Real plans carry
     * {@code struct()}/{@code list()} expressions instead.
     */
    @Test
    void aNonScalarLiteralWhereAScalarIsExpectedIsRefused() {
        Operand aString = variableOperand("request.resource.attr.aString");
        Operand aList = listValueOperand("a");
        Operand aMap = valueOperand(struct("a", number(1)));

        for (String operator : List.of("lt", "contains", "startsWith", "matches")) {
            IllegalArgumentException ex = refusal(expressionOperand(operator, aString, aList));
            assertTrue(ex.getMessage().contains(operator + " against a list literal"),
                    operator + ": " + ex.getMessage());
            assertInstanceOf(UnsupportedPlanShapeException.class, ex);
        }
        assertTrue(refusal(expressionOperand("eq", aString, aMap)).getMessage()
                .contains("eq against a map literal"));
        assertTrue(refusal(expressionOperand("eq", aString, valueOperand(struct("a", NULL))))
                .getMessage().contains("eq against a map literal"));

        assertTrue(refusal(expressionOperand("in", aString, aMap)).getMessage()
                .contains("CEL map membership tests the map's keys"));
        assertTrue(refusal(expressionOperand("in", aString, valueOperand(list(list(string("a"))))))
                .getMessage().contains("in over a list holding a list element"));
        assertTrue(refusal(expressionOperand("in", aList, variableOperand("request.resource.attr.tags")))
                .getMessage().contains("in with a list element"));
        assertTrue(refusal(expressionOperand("hasIntersection",
                variableOperand("request.resource.attr.tags"), valueOperand(list(struct("a", number(1))))))
                .getMessage().contains("hasIntersection over a list holding a map element"));
    }
}
