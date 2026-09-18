package dev.cerbos.queryplan.exposed

import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.ScoreCase
import dev.cerbos.queryplan.exposed.sql.nullIfUndetermined
import org.jetbrains.exposed.v1.core.Alias
import org.jetbrains.exposed.v1.core.Coalesce
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.Max
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.PlusOp
import org.jetbrains.exposed.v1.core.Sum
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.intLiteral

/**
 * The collection macros `exists`, `all` and `exists_one`, lowered to ONE scoring subquery each,
 * never a plain `EXISTS`: an element whose body is UNKNOWN silently fails to match under `EXISTS`,
 * which collapses CEL's error case to FALSE, and an enclosing NOT then readmits the row.
 *
 * The truth tables the scores encode are CEL's, not SQL's:
 *  - `exists` is OR with error absorption — TRUE if any element matches, ERROR if none matches and
 *    at least one errors, FALSE otherwise (including the empty collection);
 *  - `all` is AND with error absorption — FALSE if any element fails, ERROR if none fails and at
 *    least one errors, TRUE otherwise (including the empty collection);
 *  - `exists_one` is STRICT — any erroring element errors the whole macro, otherwise exactly one
 *    match.
 *
 * ERROR becomes SQL UNKNOWN, so the row stays excluded under both polarities.
 */
internal class CollectionTranslator(private val translation: Translation) {

    fun translate(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> {
        if (operands.size != 2) {
            throw Refusals.malformed("$operator requires exactly 2 operands, got ${operands.size}")
        }
        val collectionOperand = operands[0]
        val lambdaOperand = operands[1]

        // A literal value-list collection arrives when the planner could not unroll the macro
        // itself: at 10 elements or fewer it folds exists/all into an or/and chain, above that the
        // lambda ships with the folded list as its collection operand. Applying the same fold here
        // is what keeps the translation independent of which side of that threshold it landed on.
        //
        // It costs a macro level like any other. The fold emits no subquery of its own, but it
        // DUPLICATES everything under it once per element, so two three-element folds over one
        // relation macro emit nine correlated subqueries and an eleven-element pair emits 121.
        // Reaching it before [PlanWalker.enterMacro] left the emitted expression unbounded whatever
        // [Options.maxMacroDepth] was set to.
        if (collectionOperand.nodeCase == Operand.NodeCase.VALUE) {
            return translation.walker.enterMacro(operator) {
                foldKnownValues(operator, collectionOperand.value, lambdaOperand, scope)
            }
        }
        if (collectionOperand.nodeCase != Operand.NodeCase.VARIABLE) {
            throw RelationRefusals.computedCollection(operator)
        }
        val collection = scope.collection(collectionOperand.variable)
        val lambda = ParsedLambda.parse(
            lambdaOperand,
            "$operator second operand must be a lambda",
            "$operator supports single-variable lambdas only",
            "$operator lambda variable must be a variable operand",
        )

        return translation.walker.enterMacro(operator) {
            when (operator) {
                // The dominant score with the empty collection folded to 0 and "only undetermined
                // elements" (max score 1) mapped to SQL NULL. A two-valued equality against the
                // witness score then yields TRUE / FALSE / UNKNOWN exactly per the CEL table.
                "exists" -> scored(collection, lambda, scope, trueScore = 2, falseScore = 0, witness = 2)
                "all" -> scored(collection, lambda, scope, trueScore = 0, falseScore = 2, witness = 0)
                "exists_one" -> strictlyOne(collection, lambda, scope)
                else -> throw Refusals.internal("Unsupported collection operator: $operator")
            }
        }
    }

    /**
     * `NULLIF(COALESCE(MAX(CASE WHEN body THEN t WHEN NOT body THEN f ELSE 1 END), 0), 1) = witness`.
     *
     * One scan, one pair of scores. The scores are chosen so the macro's own witness state
     * dominates the MAX: 2 for the state that decides the macro outright, 0 for its opposite, and
     * 1 for undetermined — which therefore only wins when no element decided anything, and NULLIF
     * turns that win into the UNKNOWN the row has to inherit.
     */
    private fun scored(
        collection: Resolution.Collection,
        lambda: ParsedLambda,
        scope: Scope,
        trueScore: Int,
        falseScore: Int,
        witness: Int,
    ): Op<Boolean> {
        val fold = translation.subqueries.chainAggregate(collection, IntegerColumnType()) { alias ->
            val body = translateBody(collection, lambda, scope, alias)
            nullIfUndetermined(
                Coalesce<Int, Int?>(
                    Max<Int, Int>(ScoreCase(body, trueScore, falseScore, 1), IntegerColumnType()),
                    intLiteral(0),
                ),
            )
        }
        return EqOp(fold, intLiteral(witness))
    }

    /**
     * `COALESCE(SUM(CASE WHEN body THEN 1 ELSE 0 END), 0) + <poison> = 1`.
     *
     * `exists_one` has no error absorption: one undetermined element errors the whole macro even
     * when the count would otherwise hold. The poison term is 0 when every element is determined
     * and SQL NULL otherwise, and NULL is absorbing under addition, so the comparison goes UNKNOWN
     * exactly when CEL errors. The empty collection yields 0 + 0 = 0, which compares FALSE — which
     * is what CEL's `exists_one` over an empty list is.
     */
    private fun strictlyOne(
        collection: Resolution.Collection,
        lambda: ParsedLambda,
        scope: Scope,
    ): Op<Boolean> {
        val total = translation.subqueries.chainAggregate(collection, IntegerColumnType()) { alias ->
            val body = translateBody(collection, lambda, scope, alias)
            val matches = Coalesce<Int, Int?>(
                Sum<Int>(ScoreCase(body, 1, 0, 0), IntegerColumnType()),
                intLiteral(0),
            )
            val poison = nullIfUndetermined(
                Coalesce<Int, Int?>(
                    Max<Int, Int>(ScoreCase(body, 0, 0, 1), IntegerColumnType()),
                    intLiteral(0),
                ),
            )
            PlusOp<Int, Int>(matches, poison, IntegerColumnType())
        }
        return EqOp(total, intLiteral(1))
    }

    /**
     * The lambda body, translated against the element the subquery ranges over.
     *
     * The element scope delegates every non-lambda variable outward, so an outer reference inside
     * the body resolves against the scope the macro appeared in and reaches the subquery as an
     * ordinary correlation. The body is built ONCE and shared between the polarities a score needs:
     * an Exposed expression tree is immutable, so a node is safe to appear twice.
     */
    private fun translateBody(
        collection: Resolution.Collection,
        lambda: ParsedLambda,
        scope: Scope,
        alias: Alias<Table>,
    ): Op<Boolean> = translation.walker.traverse(
        lambda.body,
        LambdaScope(translation, alias, collection.tail, lambda.variable, scope),
    )

    /**
     * Fold a macro whose collection operand is a literal value list: substitute each element into
     * the lambda body and walk the combined `or`/`and` expression, which is the same fold the
     * planner applies below its own unroll threshold.
     *
     * Element comparisons produced by the fold flow through the ordinary comparison translation, so
     * a NULL column keeps its SQL-UNKNOWN reading exactly as it does in a chain the planner
     * unrolled itself.
     */
    private fun foldKnownValues(
        operator: String,
        collection: Value,
        lambdaOperand: Operand,
        scope: Scope,
    ): Op<Boolean> {
        if (operator != "exists" && operator != "all") {
            throw RelationRefusals.unfoldableValueCollection(operator)
        }
        if (collection.kindCase != Value.KindCase.LIST_VALUE) {
            // CEL refuses a scalar as a comprehension range, so the planner never folds one.
            throw Refusals.malformed("$operator over a literal collection requires a list value")
        }
        val lambda = ParsedLambda.parse(
            lambdaOperand,
            "$operator second operand must be a lambda",
            "$operator over a literal collection supports single-variable lambdas only",
            "$operator lambda variable must be a variable operand",
        )
        val elements = collection.listValue.valuesList
        // CEL identity over the empty collection: exists() matches nothing, all() matches
        // everything. Both are DEFINITE, so a constant is the whole answer.
        if (elements.isEmpty()) {
            return if (operator == "exists") Op.FALSE else Op.TRUE
        }
        val combined = PlanResourcesFilter.Expression.newBuilder()
            .setOperator(if (operator == "exists") "or" else "and")
        elements.forEach { element ->
            combined.addOperands(substitute(lambda.body, lambda.variable, element))
        }
        return translation.walker.traverse(Operand.newBuilder().setExpression(combined).build(), scope)
    }

    /**
     * Substitute a lambda's iteration variable with one concrete element.
     *
     * A bare reference becomes the element; a dotted one drills into it. A nested macro that
     * REBINDS the same variable name shadows this one, so substitution only descends into its
     * collection operand — otherwise the inner macro's own elements would be replaced by the outer
     * fold's.
     */
    private fun substitute(operand: Operand, variable: String, element: Value): Operand =
        when (operand.nodeCase) {
            Operand.NodeCase.VARIABLE -> {
                val name = operand.variable
                when {
                    name == variable -> Operand.newBuilder().setValue(element).build()
                    name.startsWith("$variable.") ->
                        Operand.newBuilder()
                            .setValue(drill(name, name.substring(variable.length + 1), element))
                            .build()
                    else -> operand
                }
            }
            Operand.NodeCase.EXPRESSION -> {
                val expression = operand.expression
                val rebuilt = expression.toBuilder()
                val operands = expression.operandsList
                if (expression.operator in LAMBDA_BINDING_OPERATORS &&
                    operands.size == 2 &&
                    shadows(operands[1], variable)
                ) {
                    rebuilt.setOperands(0, substitute(operands[0], variable, element))
                } else {
                    operands.forEachIndexed { index, nested ->
                        rebuilt.setOperands(index, substitute(nested, variable, element))
                    }
                }
                Operand.newBuilder().setExpression(rebuilt).build()
            }
            else -> operand
        }

    private fun shadows(lambdaOperand: Operand, variable: String): Boolean {
        if (lambdaOperand.nodeCase != Operand.NodeCase.EXPRESSION ||
            lambdaOperand.expression.operator != "lambda"
        ) {
            return false
        }
        val operands = lambdaOperand.expression.operandsList
        return operands.size == 2 &&
            operands[1].nodeCase == Operand.NodeCase.VARIABLE &&
            operands[1].variable == variable
    }

    private fun drill(reference: String, path: String, element: Value): Value {
        var current = element
        path.split('.').forEach { segment ->
            if (current.kindCase != Value.KindCase.STRUCT_VALUE ||
                !current.structValue.containsFields(segment)
            ) {
                throw RelationRefusals.unresolvableFoldedElement(reference, segment)
            }
            current = current.structValue.getFieldsOrThrow(segment)
        }
        return current
    }

    private companion object {
        /** Operators whose second operand is a lambda that binds an iteration variable. */
        val LAMBDA_BINDING_OPERATORS = setOf("exists", "exists_one", "all", "filter", "map", "except")
    }
}
