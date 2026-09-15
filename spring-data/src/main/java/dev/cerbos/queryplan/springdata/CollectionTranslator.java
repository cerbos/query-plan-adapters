package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.util.List;
import java.util.Set;

/**
 * The collection macros — {@code exists}, {@code exists_one}, {@code all} — over a relation
 * chain, and the fold of the same macros over a literal value list.
 *
 * <p>Owns the choice of tri-state subquery shape per macro (which score pair, which counter)
 * and the literal-collection fold that substitutes each element into the lambda body and
 * hands the combined {@code or}/{@code and} back to the walk. The subquery shapes
 * themselves are {@link ChainSubqueries}; the lambda body is always translated through the
 * walk, once per polarity, which is why every body is handed over as a
 * {@link SubqueryBodyBuilder} rather than a Predicate. {@code filter} in boolean position
 * is refused by the walk before it reaches here, and {@code size(filter(...))} belongs to
 * {@link SizeTranslator}.
 */
final class CollectionTranslator {

    /** Operators whose second operand is a lambda that binds an iteration variable. */
    private static final Set<String> LAMBDA_BINDING_OPERATORS =
            Set.of("exists", "exists_one", "all", "filter", "map", "except");

    private final CriteriaBuilder cb;
    private final PlanWalker walker;
    private final ChainSubqueries subqueries;

    CollectionTranslator(CriteriaBuilder cb, PlanWalker walker, ChainSubqueries subqueries) {
        this.cb = cb;
        this.walker = walker;
        this.subqueries = subqueries;
    }

    /**
     * Collection macros translate TRI-STATE to mirror CEL error semantics (per the cel-spec
     * macro definitions; a NULL element column is a missing element attribute, so a lambda
     * body touching it is a CEL evaluation error → deny):
     * <ul>
     *   <li>{@code exists} — OR with error absorption: true if ANY element matches; error if
     *       none matches and at least one errors; false otherwise.</li>
     *   <li>{@code all} — AND with error absorption: false if ANY element fails; error if
     *       none fails and at least one errors; true otherwise.</li>
     *   <li>{@code exists_one} — errors if ANY element errors; else true iff exactly one
     *       matches.</li>
     * </ul>
     * ERROR maps to SQL UNKNOWN so the row stays excluded under BOTH polarities
     * ({@code NOT(UNKNOWN) = UNKNOWN}). A plain EXISTS is not enough: an element whose body
     * is UNKNOWN silently fails to match, collapsing the error case to FALSE — which
     * {@code not(...)} flips to TRUE, an authorization leak. Each macro is a SINGLE
     * correlated aggregate subquery that scores every element into determined-true /
     * determined-false / undetermined and folds the scores into one value whose comparison
     * is TRUE, FALSE, or SQL UNKNOWN exactly per the CEL truth table — see
     * {@link ChainSubqueries#macroScoreSubquery} (exists/all/filter) and
     * {@link ChainSubqueries#strictMatchCountSubquery} (exists_one).
     *
     * <p>{@code filter} in boolean position is kept consistent with the
     * {@code exists} family. Cost note: the unknown machinery is always emitted — the
     * attribute mapping carries no column-nullability metadata, so a NULL-free lambda body
     * cannot be detected statically. The lambda body is translated once per polarity
     * (positive and negated — Hibernate 6 negation is stateful, see {@link TriPredicate},
     * so a Predicate tree cannot be shared between polarities): twice for the
     * {@code exists} family, three times for {@code exists_one} (which also needs the
     * positive body inside its match counter). Nested macros therefore multiply — a
     * depth-d exists chain emits {@code 2^d - 1} correlated subqueries (an exists_one
     * chain up to {@code (3^d - 1) / 2}) — which is why {@link PlanWalker#enterMacro}
     * bounds the nesting depth ({@link SpringDataQueryPlanAdapter#MAX_MACRO_DEPTH_PROPERTY},
     * default {@value SpringDataQueryPlanAdapter#DEFAULT_MAX_MACRO_DEPTH}).
     */
    Predicate handleCollectionOperator(String op, List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            throw Refusals.malformed(op + " requires exactly 2 operands");
        }
        Operand listOperand = operands.get(0);
        Operand lambdaOperand = operands.get(1);

        // A literal value-list collection arrives when the planner could not unroll a
        // macro over a known collection: at <= 10 elements it folds exists/all into an
        // or/and chain itself (cerbos/cerbos#2570, #2817; maxItems = 10 in the planner's
        // struct matcher), above that the lambda ships with the folded value list as its
        // collection operand. Apply the same fold here instead of demanding a Relation
        // mapping that cannot exist for a literal.
        if (listOperand.getNodeCase() == Operand.NodeCase.VALUE) {
            return handleKnownValueCollection(op, listOperand.getValue(), lambdaOperand, scope);
        }

        if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
            // A macro over a computed collection (`tags.map(...).exists(...)`): legal
            // CEL, no join chain to range over.
            throw Refusals.unsupported(op + " first operand must be a variable");
        }
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw Refusals.malformed(op + " second operand must be a lambda");
        }

        String collectionVar = listOperand.getVariable();
        // Owner-anchored chain resolution: multi-hop chains join through every hop, and a
        // relation referenced from inside a lambda anchors to the scope that owns it.
        if (!(scope.resolve(collectionVar) instanceof Scope.ResolvedRelation ref)) {
            throw Refusals.unmapped(
                    op + " requires a Relation mapping for " + collectionVar);
        }

        ParsedLambda lambda = ParsedLambda.parse(lambdaOperand,
                op + " second operand must be a lambda",
                "lambda requires exactly 2 operands",
                "lambda variable must be a variable operand");
        Operand body = lambda.body();
        String lambdaVarName = lambda.varName();

        // Every invocation re-traverses the body, so each occurrence gets a fresh Predicate
        // tree (Hibernate 6 negation is stateful — see TriPredicate.not()).
        SubqueryBodyBuilder bodyBuilder = (sub, tailJoin, rebased) -> walker.traverse(body,
                Scope.lambda(tailJoin, sub, ref.tail(), lambdaVarName, rebased));

        return walker.enterMacro(op, () -> switch (op) {
            // exists (and filter): OR with error absorption — TRUE iff any element is
            // determined-true (max score 2); UNKNOWN iff none is true but at least one is
            // undetermined (max score 1 → NULLIF yields SQL NULL); FALSE otherwise.
            case "exists" ->
                    cb.equal(subqueries.requireLeadingHops(scope, ref,
                            subqueries.macroScoreSubquery(scope, ref, bodyBuilder, 2, 0),
                            Integer.class), 2);
            // all: AND with error absorption — FALSE iff any element is determined-false
            // (max score 2 absorbs undetermined siblings); UNKNOWN iff none is false but at
            // least one is undetermined; TRUE otherwise (including the empty collection).
            case "all" ->
                    cb.equal(subqueries.requireLeadingHops(scope, ref,
                            subqueries.macroScoreSubquery(scope, ref, bodyBuilder, 0, 2),
                            Integer.class), 0);
            // exists_one: strict — any UNKNOWN element denies, else COUNT(body) = 1. The
            // strict counter goes SQL NULL when any element is undetermined, so the equality
            // is UNKNOWN and the row stays excluded under both polarities.
            case "exists_one" ->
                    cb.equal(subqueries.requireLeadingHops(scope, ref,
                            subqueries.strictMatchCountSubquery(scope, ref, bodyBuilder),
                            Long.class), 1L);
            default -> throw Refusals.internal("Unsupported collection operator: " + op);
        });
    }

    /**
     * Fold a collection macro whose collection operand is a literal value list: substitute
     * each element into the lambda body and combine the per-element expressions with
     * {@code or} ({@code exists}) or {@code and} ({@code all}), then translate the combined
     * expression through the normal {@link PlanWalker#traverse} path — the same fold the
     * planner itself applies to known collections of 10 or fewer elements, so the
     * translated filter does not depend on which side of that threshold the collection
     * lands.
     *
     * <p>The empty collection keeps CEL identity semantics: {@code exists} over {@code []}
     * is false, {@code all} over {@code []} is true. Element comparisons produced by the
     * fold flow through the standard comparison translation, so NULL columns keep their
     * SQL-UNKNOWN (row excluded under both polarities) behavior — matching how a
     * planner-unrolled chain of the same comparisons translates.
     */
    private Predicate handleKnownValueCollection(String op, Value collectionValue,
                                                 Operand lambdaOperand, Scope scope) {
        if (!"exists".equals(op) && !"all".equals(op)) {
            throw Refusals.unsupported(op
                    + " over a literal collection value is not supported. "
                    + "Only exists() and all() can be folded into a flat filter.");
        }
        if (collectionValue.getKindCase() != Value.KindCase.LIST_VALUE) {
            // CEL refuses a scalar as a comprehension range, so the planner never folds
            // a macro over one.
            throw Refusals.malformed(op
                    + " over a literal collection requires a list value");
        }
        ParsedLambda lambda = ParsedLambda.parse(lambdaOperand,
                op + " second operand must be a lambda",
                op + " over a literal collection supports single-variable lambdas only",
                "lambda variable must be a variable operand");

        List<Value> elements = collectionValue.getListValue().getValuesList();
        if (elements.isEmpty()) {
            return "exists".equals(op) ? cb.disjunction() : cb.conjunction();
        }

        PlanResourcesFilter.Expression.Builder combined = PlanResourcesFilter.Expression
                .newBuilder()
                .setOperator("exists".equals(op) ? "or" : "and");
        for (Value element : elements) {
            combined.addOperands(
                    substituteLambdaVariable(lambda.body(), lambda.varName(), element));
        }
        return walker.traverse(Operand.newBuilder().setExpression(combined).build(), scope);
    }

    /**
     * Substitute a lambda iteration variable with a concrete collection element inside a
     * lambda body. A bare reference to the variable becomes the element itself; a
     * {@code variable.path.to.field} reference drills into the element (failing closed
     * when the path is missing — the CEL evaluation of that element would error). A nested
     * macro whose lambda rebinds the same variable name shadows the outer variable, so
     * substitution only descends into its collection operand.
     */
    private static Operand substituteLambdaVariable(Operand operand, String varName,
                                                    Value element) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                String name = operand.getVariable();
                if (name.equals(varName)) {
                    return Operand.newBuilder().setValue(element).build();
                }
                if (name.startsWith(varName + ".")) {
                    return Operand.newBuilder()
                            .setValue(resolveElementPath(name,
                                    name.substring(varName.length() + 1), element))
                            .build();
                }
                return operand;
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression expr = operand.getExpression();
                List<Operand> ops = expr.getOperandsList();
                PlanResourcesFilter.Expression.Builder rebuilt = expr.toBuilder();
                if (LAMBDA_BINDING_OPERATORS.contains(expr.getOperator()) && ops.size() == 2
                        && shadowsVariable(ops.get(1), varName)) {
                    // The nested lambda rebinds our variable: substitute only in the
                    // collection operand.
                    rebuilt.setOperands(0,
                            substituteLambdaVariable(ops.get(0), varName, element));
                    return Operand.newBuilder().setExpression(rebuilt).build();
                }
                for (int i = 0; i < ops.size(); i++) {
                    rebuilt.setOperands(i,
                            substituteLambdaVariable(ops.get(i), varName, element));
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
                // That element's CEL evaluation would error and deny; a fold has no
                // per-element UNKNOWN to carry, so the whole shape is refused.
                throw Refusals.unsupported("Cannot resolve \"" + fullRef
                        + "\": collection element has no field \"" + segment + "\"");
            }
            current = current.getStructValue().getFieldsOrThrow(segment);
        }
        return current;
    }
}
