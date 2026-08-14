package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.ListValue;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The plans this suite hands the adapter are HAND-BUILT, and that is now the exception rather than
 * the rule. {@link ElasticsearchTranslatorTest} reads its plans from
 * {@code conformance/wire-fixtures/} and accounts for every corpus action exactly once; a
 * hand-built plan is a BELIEF about what the planner emits, and this repository keeps golden
 * fixtures because that belief has been wrong before
 * ({@code docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md}).
 *
 * <p>What remains is of two kinds, and every test below says which:
 *
 * <ol>
 *   <li><strong>No policy can reach it.</strong> A malformed operand, an operator name CEL does not
 *       have, a caller-supplied argument (the field map, {@code nestedPaths}, an
 *       {@link OperatorFunction} override), a literal {@code timestamp()} itself would reject.
 *       There is no fixture to read because there is no policy to plan. These stay permanently.
 *   <li><strong>A policy can reach it and the corpus does not carry it yet.</strong> Each of these
 *       is a corpus gap wearing a unit test — the shape belongs in
 *       {@code conformance/policies/adversarial.yaml}, where every adapter would be asked about it.
 *       They are kept because deleting one loses the coverage outright, and named so nobody
 *       mistakes them for corpus coverage. <strong>They are NOT covered by #387 or #388</strong>,
 *       whose actions are enumerated and landed; the shapes below are a separate list, filed as
 *       cerbos/query-plan-adapters#414.
 * </ol>
 *
 * <p>Everything else this file used to hold — the operator table, the negation forms, the null
 * polarities, the collection macros, the value-list fold, the fail-closed families — is a corpus
 * action now, asserted against a real planner's output and against {@code check()}.
 */
class ElasticsearchQueryPlanAdapterTest {

    private static final Map<String, String> FIELD_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.department", "department"),
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.title", "title"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.ownedBy", "ownedBy"),
            Map.entry("request.resource.attr.tagObjects", "tagObjects")
    );

    private static final Set<String> NESTED_PATHS = Set.of("tagObjects");

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

    private static Operand stringValueOperand(String val) {
        return Operand.newBuilder().setValue(Value.newBuilder().setStringValue(val)).build();
    }

    private static Operand numberValueOperand(double val) {
        return Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(val)).build();
    }

    private static Operand boolValueOperand(boolean val) {
        return Operand.newBuilder().setValue(Value.newBuilder().setBoolValue(val)).build();
    }

    private static Operand listValueOperand(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String v : values) {
            list.addValues(Value.newBuilder().setStringValue(v));
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
            list.addValues(Value.newBuilder().setStructValue(
                    Struct.newBuilder().putFields(
                            field, Value.newBuilder().setStringValue(value).build())));
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }

    private static Operand valueListMacro(String operator, Operand collection, Operand body) {
        return expressionOperand(operator, collection, lambdaOperand("t", body));
    }

    private static Map<String, Object> translate(Operand condition) {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition), FIELD_MAP, NESTED_PATHS);
        return ((Result.Conditional) result).query();
    }

    private static IllegalArgumentException refusal(Operand condition) {
        return assertThrows(IllegalArgumentException.class, () -> translate(condition));
    }

    // ============================================================================================
    // KIND 1 — no policy can reach these
    // ============================================================================================

    /**
     * The field map is a CALLER argument, so a reference it does not name is not a policy shape.
     * The corpus maps every level of every path precisely so that no corpus action lands here
     * (cerbos/query-plan-adapters#326) — this is the other side of that rule.
     */
    @Test
    void anUnmappedReferenceIsRefusedRatherThanUsedVerbatim() {
        IllegalArgumentException ex = refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.nonexistent"),
                stringValueOperand("value")));
        assertTrue(ex.getMessage().contains("Unknown attribute"), ex.getMessage());
    }

    /** CEL has no such function, so no policy compiles to this operator. */
    @Test
    void anOperatorTheAdapterDoesNotKnowIsRefusedByName() {
        IllegalArgumentException ex = refusal(expressionOperand("unsupported_op",
                variableOperand("request.resource.attr.department"), stringValueOperand("value")));
        assertTrue(ex.getMessage().contains("Unknown operator"), ex.getMessage());
    }

    /**
     * {@code isSet} is not a registered CEL function, so a policy naming it does not compile and
     * the operator can never reach the wire (cerbos/query-plan-adapters#261). It appears in zero of
     * the corpus's wire fixtures; existence is spelled {@code R.attr.x != null}, which the corpus
     * carries as {@code null-ne} and this adapter lowers to {@code exists}.
     */
    @Test
    void isSetIsRejectedRatherThanTranslated() {
        IllegalArgumentException ex = refusal(expressionOperand("isSet",
                variableOperand("request.resource.attr.department"), boolValueOperand(true)));
        assertTrue(ex.getMessage().contains("isSet"),
                "the unknown operator must be named in the error, got: " + ex.getMessage());
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
     * {@code nestedPaths} is a caller argument too, and the adapter cannot read an index mapping to
     * check it. A collection macro over a field the caller did not declare has no {@code nested}
     * query to emit, so it fails closed rather than emitting one that would match nothing.
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
    }

    /**
     * The complement of the rule above, and the reason it is scoped to DOCUMENT collections: a
     * macro over a literal value list has no document to reach, so it must translate with an empty
     * {@code nestedPaths}.
     */
    @Test
    void aValueListMacroNeedsNoNestedPathDeclaration() {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(valueListMacro("exists", listValueOperand("string"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t")))),
                FIELD_MAP, Set.of());

        assertEquals(Map.of("bool", Map.of(
                        "should", List.of(Map.of("term", Map.of("aString", Map.of("value", "string")))),
                        "minimum_should_match", 1)),
                ((Result.Conditional) result).query());
    }

    /** A lambda body referencing a variable the lambda does not bind is a malformed plan. */
    @Test
    void aLambdaBodyReferencingAnUnboundVariableIsRefused() {
        IllegalArgumentException ex = refusal(expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("eq",
                        variableOperand("x.name"), stringValueOperand("public")))));
        assertTrue(ex.getMessage().contains("does not start with lambda variable"), ex.getMessage());
    }

    /** A macro whose collection operand is a scalar literal rather than a list. */
    @Test
    void aValueListMacroOverANonListLiteralIsRefused() {
        IllegalArgumentException ex = refusal(valueListMacro("exists",
                stringValueOperand("not a list"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"), variableOperand("t"))));
        assertTrue(ex.getMessage()
                .contains("exists over a literal collection requires a list value"), ex.getMessage());
    }

    /**
     * Substituting a lambda variable into a struct element drills a dotted path into it. A path the
     * element does not carry fails closed — that element's CEL evaluation would error — rather than
     * silently dropping the comparison.
     */
    @Test
    void aValueListElementMissingTheProjectedFieldIsRefused() {
        IllegalArgumentException ex = refusal(valueListMacro("exists",
                structListValueOperand("name", "string"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"),
                        variableOperand("t.missing"))));
        assertTrue(ex.getMessage().contains("Cannot resolve \"t.missing\""), ex.getMessage());
    }

    /**
     * CEL's own {@code timestamp()} rejects each of these, so the planner cannot emit one — a
     * calendar-invalid date, a non-RFC-3339 spelling, and the two ends of CEL's representable
     * range. The adapter validates the literal anyway, because it is what decides whether a
     * {@code term} or {@code range} query is even well-formed.
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
        }
    }

    // ============================================================================================
    // KIND 2 — a policy can reach these, and the corpus does not carry them yet
    // ============================================================================================

    /**
     * <strong>Corpus gap.</strong> A {@code wildcard} query reads {@code *} and {@code ?} as
     * operators, so an unescaped one in a needle turns a substring test into a pattern match — this
     * repository's founding bug class (#258/#259) in its Elasticsearch spelling. The corpus seeds
     * {@code %}, {@code _}, {@code \} and {@code [}, which are the SQL and Lucene-regex
     * metacharacters; no seed value carries a {@code *} or a {@code ?}, so no corpus action
     * exercises the escaping this adapter actually needs.
     */
    @Test
    void containsEscapesTheWildcardMetacharactersInItsNeedle() {
        assertEquals(Map.of("wildcard", Map.of("title",
                        Map.of("value", "*foo\\*bar\\?baz\\\\qux*"))),
                translate(expressionOperand("contains",
                        variableOperand("request.resource.attr.title"),
                        stringValueOperand("foo*bar?baz\\qux"))));
    }

    /** <strong>Corpus gap.</strong> The same escaping through the other anchored operator. */
    @Test
    void endsWithEscapesTheWildcardMetacharactersInItsNeedle() {
        assertEquals(Map.of("wildcard", Map.of("title", Map.of("value", "*a\\*b"))),
                translate(expressionOperand("endsWith",
                        variableOperand("request.resource.attr.title"),
                        stringValueOperand("a*b"))));
    }

    /**
     * <strong>Corpus gap.</strong> The corpus carries exactly one {@code matches()} action —
     * {@code p-matches}, the {@code ^h} literal prefix, which lowers to a {@code prefix} query and
     * never reaches Lucene's regex engine. Everything about the RE2-to-Lucene subset is therefore
     * untested by it.
     *
     * <p>Each pattern below is one RE2 construct Lucene either lacks or reads differently: a
     * character class shorthand, an inline flag, an unanchored form, a POSIX class, and the two
     * spellings of {@code .} — which in Lucene matches a newline and in RE2 does not.
     * {@link ElasticsearchSurfaceTest} executes that last difference against a real server.
     */
    @Test
    void aRegexOutsideTheSharedRe2LuceneSubsetIsRefused() {
        for (String pattern : List.of(
                "^\\d+$", "^(?i)admin$", "^a^b$", "^[[:alpha:]]$", "^a.b$", "^a.*b$")) {
            IllegalArgumentException ex = refusal(expressionOperand("matches",
                    variableOperand("request.resource.attr.aString"),
                    stringValueOperand(pattern)));
            assertTrue(ex.getMessage().contains("supported RE2/Lucene subset"), pattern);
        }
    }

    /**
     * <strong>Corpus gap.</strong> {@code except} appears in zero of the corpus's wire fixtures —
     * no adversarial policy uses the macro — so this is the only place the nested lowering is
     * pinned. It is the exact inverse of {@code all}: a document qualifies when it holds an element
     * the predicate is false for.
     */
    @Test
    void exceptOverANestedCollectionEmitsANestedMustNot() {
        assertEquals(Map.of("nested", Map.of(
                        "path", "tagObjects",
                        "query", Map.of("bool", Map.of("must_not", List.of(
                                Map.of("term", Map.of("tagObjects.name", Map.of("value", "public")))))))),
                translate(expressionOperand("except",
                        variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t", expressionOperand("eq",
                                variableOperand("t.name"), stringValueOperand("public"))))));
    }

    /**
     * <strong>Corpus gap.</strong> The corpus drives {@code exists} and {@code all} over a
     * principal value list ({@code pv-exists}, {@code pv-all}) but not the other four macros. Two
     * of them — {@code filter} and {@code map} — reach the LEAF traversal rather than the collection
     * handler, so without an explicit guard they would surface an unrelated operand-shape error
     * instead of naming the real limitation.
     */
    @ParameterizedTest
    @ValueSource(strings = {"exists_one", "filter", "map", "except"})
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
     * <strong>Corpus gap.</strong> The corpus principal's collections are never empty, so no action
     * folds a macro over {@code []}. CEL's identity semantics say {@code exists} over an empty
     * collection is false and {@code all} over one is true, and each flips under negation — which a
     * fold that simply dropped the empty chain would get wrong in one direction.
     */
    @Test
    void anEmptyValueListKeepsCelIdentitySemantics() {
        Operand body = expressionOperand("eq",
                variableOperand("request.resource.attr.aString"), variableOperand("t"));

        assertEquals(Map.of("match_none", Map.of()),
                translate(valueListMacro("exists", emptyListValueOperand(), body)));
        assertEquals(Map.of("match_all", Map.of()),
                translate(valueListMacro("all", emptyListValueOperand(), body)));
        assertEquals(Map.of("match_all", Map.of()),
                translate(expressionOperand("not",
                        valueListMacro("exists", emptyListValueOperand(), body))));
        assertEquals(Map.of("match_none", Map.of()),
                translate(expressionOperand("not",
                        valueListMacro("all", emptyListValueOperand(), body))));
    }

    /**
     * <strong>Corpus gap.</strong> A negated macro over a principal value list. The corpus's one
     * negated {@code pv-*} action ({@code null-value-pv-not-exists}) is refused for an unrelated
     * reason — it probes the explicit-null attribute — so nothing there reaches this fold.
     *
     * <p>The invariant that matters is not the emitted shape but that the fold is
     * INDISTINGUISHABLE from the or-chain the planner emits below its 10-element unroll cliff
     * (cerbos/cerbos#2570, #2817), under negation as well as positively.
     */
    @Test
    void aNegatedValueListMacroFoldsExactlyAsTheUnrolledChainDoes() {
        assertEquals(translate(expressionOperand("not",
                        expressionOperand("or",
                                expressionOperand("eq",
                                        variableOperand("request.resource.attr.aString"),
                                        stringValueOperand("string")),
                                expressionOperand("eq",
                                        variableOperand("request.resource.attr.aString"),
                                        stringValueOperand("anotherString"))))),
                translate(expressionOperand("not",
                        valueListMacro("exists",
                                listValueOperand("string", "anotherString"),
                                expressionOperand("eq",
                                        variableOperand("request.resource.attr.aString"),
                                        variableOperand("t"))))));
    }

    /**
     * <strong>Corpus gap.</strong> The corpus's principal collections are lists of scalars, so no
     * action folds a macro whose elements are STRUCTS and whose body reads a member of one.
     */
    @Test
    void aValueListElementFieldIsDrilledIntoDuringTheFold() {
        assertEquals(Map.of("bool", Map.of(
                        "should", List.of(
                                Map.of("term", Map.of("aString", Map.of("value", "string"))),
                                Map.of("term", Map.of("aString", Map.of("value", "anotherString")))),
                        "minimum_should_match", 1)),
                translate(valueListMacro("exists",
                        structListValueOperand("name", "string", "anotherString"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t.name")))));
    }

    /**
     * <strong>Corpus gap.</strong> A nested macro that rebinds the outer macro's iteration variable
     * name. The corpus's nested macros all bind distinct names, so nothing there exercises the
     * shadowing rule — and getting it wrong would substitute a literal into the INNER lambda's
     * body, silently changing which documents match.
     */
    @Test
    void aNestedLambdaRebindingTheVariableShadowsTheSubstitution() {
        Map<String, Object> nested = Map.of("nested", Map.of(
                "path", "tagObjects",
                "query", Map.of("term", Map.of("tagObjects.name", Map.of("value", "public")))));

        assertEquals(Map.of("bool", Map.of(
                        "should", List.of(nested, nested), "minimum_should_match", 1)),
                translate(valueListMacro("exists",
                        listValueOperand("ignored-a", "ignored-b"),
                        expressionOperand("exists",
                                variableOperand("request.resource.attr.tagObjects"),
                                lambdaOperand("t", expressionOperand("eq",
                                        variableOperand("t.name"),
                                        stringValueOperand("public")))))));
    }

    /**
     * <strong>Corpus gap.</strong> The corpus's emptiness checks are {@code 0 < size(c)}
     * ({@code vf-size}) and {@code size(c) != 0} ({@code not-empty}). {@code size(c) >= 1} is the
     * third spelling of the same predicate and it takes a different branch of the threshold
     * recogniser — the one that has to know a count is an integer.
     */
    @Test
    void sizeAtLeastOneIsTheSameEmptinessCheckAsSizeAboveZero() {
        assertEquals(Map.of("exists", Map.of("field", "ownedBy")),
                translate(expressionOperand("ge",
                        expressionOperand("size", variableOperand("request.resource.attr.ownedBy")),
                        numberValueOperand(1))));
    }

    /**
     * <strong>Corpus gap, and a suspected over-grant — read this one before deleting it.</strong>
     *
     * <p>The adapter is handed a plan, never a mapping, so it cannot tell a string field from an
     * array one: {@code size(x) > 0} becomes {@code exists x} either way. For a COLLECTION that is
     * right. For a STRING it is not, and the corpus seeds the witness: {@code a8} holds
     * {@code aString: ""}. CEL evaluates {@code size("") > 0} to false and {@code check()} denies
     * that seed, while an empty string is an indexed term on a {@code keyword} field, so
     * {@code exists} matches it and the document comes back — a row the PDP denies.
     *
     * <p>The corpus cannot catch this today. Its three string-length actions are
     * {@code string-size} ({@code > 4}), {@code size-huge-gt} and {@code size-huge-lt}, and all
     * three are thresholds this adapter refuses outright; {@code size(R.attr.aString) > 0} — the
     * one string-length shape it TRANSLATES — appears in no policy. The retired integration test
     * covered the shape (`stringSizeGtZeroMatchesAllWithField`) against documents that all held a
     * non-empty string, so it never met the empty one.
     *
     * <p>This test therefore pins what the adapter does TODAY rather than what it should do. The
     * fix belongs in the corpus first, per {@code CLAUDE.md} "Changing how a condition is
     * translated" — an adapter-local fix would leave the same question unasked of every other
     * adapter. Tracked in cerbos/query-plan-adapters#414.
     */
    @Test
    void sizeAboveZeroOverAStringIsAnExistsCheck() {
        assertEquals(Map.of("exists", Map.of("field", "aString")),
                translate(expressionOperand("gt",
                        expressionOperand("size", variableOperand("request.resource.attr.aString")),
                        numberValueOperand(0))));
    }

    /**
     * <strong>Corpus gap.</strong> Every {@code terms} query the corpus emits binds strings. A
     * numeric membership test goes through the same lowering but a different value conversion —
     * the planner sends every number as a double, and the adapter narrows an integral one to a
     * {@code long} so the query does not read {@code 1.0} against an {@code integer} field.
     */
    @Test
    void membershipOverNumbersBindsIntegralValuesAsIntegers() {
        Operand values = Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(ListValue.newBuilder()
                        .addValues(Value.newBuilder().setNumberValue(1))
                        .addValues(Value.newBuilder().setNumberValue(2))
                        .addValues(Value.newBuilder().setNumberValue(3))))
                .build();

        assertEquals(Map.of("terms", Map.of("aNumber", List.of(1L, 2L, 3L))),
                translate(expressionOperand("in",
                        variableOperand("request.resource.attr.aNumber"), values)));
    }
}
