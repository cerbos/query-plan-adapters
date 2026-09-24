/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Translates every leaf comparison: field against value, field against field, constant against
 * constant, constant-receiver string matches, arithmetic, ternary and {@code size()}
 * comparisons.
 *
 * <p>{@link #resolve} classifies each operand into a {@link Resolved} shape without converting
 * it, and {@link #dispatch} translates the pair. Conversion is lazy because the error a
 * malformed operand raises depends on the whole comparison, and those messages are pinned.
 *
 * <p>To add an operand type, add a {@code Resolved} case, classify it in {@link #resolve} and
 * handle its pairings in {@link #dispatch}, as the {@code timestamp()} cases do. Unhandled
 * pairings fall through to {@link #leafOperandError}.
 */
final class ComparisonTranslator {

    static final Set<String> COMPARISON_OPS =
            Set.of("eq", "ne", "lt", "gt", "le", "ge");

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final TernaryTranslator ternary;
    private final SizeTranslator sizes;
    private final ArithmeticTranslator arithmetic;

    ComparisonTranslator(CriteriaBuilder cb, TriPredicate tri, LeafTranslator leaf,
                         TernaryTranslator ternary, SizeTranslator sizes) {
        this.cb = cb;
        this.tri = tri;
        this.leaf = leaf;
        this.ternary = ternary;
        this.sizes = sizes;
        this.arithmetic = new ArithmeticTranslator(cb, tri, leaf, this);
    }

    /**
     * Translates an operator that {@link PlanWalker#traverseExpression} does not handle by
     * name. The ternary rewrite runs first, on the raw operands, because it needs source order.
     * Then the operands are normalized field-first ({@link NormalizedBinary}), {@code size()}
     * comparisons are tried, and everything else is resolved and dispatched.
     */
    Predicate translate(String op, List<Operand> operands, Scope scope) {
        Predicate ternaryPred = ternary.tryTernaryComparison(op, operands, scope);
        if (ternaryPred != null) {
            return ternaryPred;
        }
        NormalizedBinary nb = NormalizedBinary.of(op, operands);
        // Check arity before the size() probe: its operand scan keeps the last match and would
        // silently drop an extra operand.
        if (nb.operands().size() != 2) {
            throw Refusals.malformed(
                    nb.op() + " requires exactly 2 operands, got " + nb.operands().size());
        }
        Predicate sizePred = sizes.trySizeComparison(nb.op(), nb.operands(), scope);
        if (sizePred != null) {
            return sizePred;
        }
        return dispatch(nb.op(),
                resolve(nb.operands().get(0)),
                resolve(nb.operands().get(1)),
                nb.operands(), scope);
    }

    // -- operand resolution --

    /** A comparison operand classified by shape. Values convert on demand in dispatch. */
    private sealed interface Resolved {
        /** A plan constant. */
        record Constant(Operand operand) implements Resolved {
            Object value() {
                return PlanValues.protoValueToJava(operand.getValue());
            }
        }

        /** A mapped attribute. */
        record Field(String variable) implements Resolved {}

        /** {@code add(value, value)}, folded by {@link PlanValues#foldAdd}. */
        record ConstantAdd(Operand left, Operand right) implements Resolved {
            Object fold() {
                return PlanValues.foldAdd(
                        PlanValues.protoValueToJava(left.getValue()),
                        PlanValues.protoValueToJava(right.getValue()));
            }
        }

        /**
         * {@code add(field, value)} or {@code add(value, field)}. Under eq/ne against a
         * constant it is solved for the field when the solve is exact; otherwise it lowers to
         * SQL arithmetic.
         */
        record FieldPlusConstant(String fieldVariable, Operand constant, boolean fieldIsLeft)
                implements Resolved {}

        /** Any other arithmetic expression, lowered to SQL by {@link ArithmeticTranslator}. */
        record Arithmetic(String operator) implements Resolved {}

        /** {@code timestamp(variable)}. */
        record TimestampField(String variable) implements Resolved {}

        /**
         * {@code timestamp(value)}. The planner folds {@code now() - duration(...)} into this
         * shape too. {@link #instant()} accepts any RFC-3339 offset and normalizes to the
         * absolute instant, as CEL timestamp equality does.
         */
        record TimestampConstant(Operand operand) implements Resolved {
            Instant instant() {
                Object raw = PlanValues.protoValueToJava(operand.getValue());
                // CEL's timestamp() rejects a non-string or unparseable literal, so the planner
                // cannot emit one.
                if (!(raw instanceof String s)) {
                    throw Refusals.malformed(
                            "timestamp() constant must be an RFC-3339 string, got "
                                    + (raw == null ? "null" : raw.getClass().getSimpleName()));
                }
                try {
                    return Instant.parse(s);
                } catch (DateTimeParseException e) {
                    try {
                        return OffsetDateTime.parse(s).toInstant();
                    } catch (DateTimeParseException e2) {
                        throw Refusals.malformed(
                                "timestamp() constant could not be parsed as an RFC-3339 instant", e2);
                    }
                }
            }
        }

        /**
         * {@code string(variable)}. Only a {@link Boolean} column is translated
         * ({@link #booleanStringComparison}).
         */
        record StringOfField(String variable) implements Resolved {}

        /** An operand no leaf case handles, reported by {@link #leafOperandError}. */
        record Opaque() implements Resolved {}
    }

    private Resolved resolve(Operand o) {
        return switch (o.getNodeCase()) {
            case VALUE -> new Resolved.Constant(o);
            case VARIABLE -> new Resolved.Field(o.getVariable());
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = o.getExpression();
                String exprOp = e.getOperator();
                // The planner emits temporal comparisons as timestamp(variable) or
                // timestamp(value). A nested expression has no translation.
                if ("timestamp".equals(exprOp) && e.getOperandsCount() == 1) {
                    Operand arg = e.getOperands(0);
                    if (arg.getNodeCase() == Operand.NodeCase.VARIABLE) {
                        yield new Resolved.TimestampField(arg.getVariable());
                    }
                    if (arg.getNodeCase() == Operand.NodeCase.VALUE) {
                        yield new Resolved.TimestampConstant(arg);
                    }
                    yield new Resolved.Opaque();
                }
                if ("string".equals(exprOp) && e.getOperandsCount() == 1
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE) {
                    yield new Resolved.StringOfField(e.getOperands(0).getVariable());
                }
                if (!ArithmeticTranslator.ARITHMETIC_OPS.contains(exprOp)) {
                    yield new Resolved.Opaque();
                }
                if ("add".equals(exprOp) && e.getOperandsCount() == 2) {
                    Operand l = e.getOperands(0);
                    Operand r = e.getOperands(1);
                    boolean lValue = l.getNodeCase() == Operand.NodeCase.VALUE;
                    boolean rValue = r.getNodeCase() == Operand.NodeCase.VALUE;
                    if (lValue && rValue) {
                        yield new Resolved.ConstantAdd(l, r);
                    }
                    if (l.getNodeCase() == Operand.NodeCase.VARIABLE && rValue) {
                        yield new Resolved.FieldPlusConstant(l.getVariable(), r, true);
                    }
                    if (lValue && r.getNodeCase() == Operand.NodeCase.VARIABLE) {
                        yield new Resolved.FieldPlusConstant(r.getVariable(), l, false);
                    }
                }
                yield new Resolved.Arithmetic(exprOp);
            }
            default -> new Resolved.Opaque();
        };
    }

    private static boolean isAddRooted(Resolved r) {
        return r instanceof Resolved.ConstantAdd
                || r instanceof Resolved.FieldPlusConstant
                || (r instanceof Resolved.Arithmetic a && "add".equals(a.operator()));
    }

    private static boolean isArithmeticRooted(Resolved r) {
        return r instanceof Resolved.ConstantAdd
                || r instanceof Resolved.FieldPlusConstant
                || r instanceof Resolved.Arithmetic;
    }

    // -- dispatch --

    /**
     * Translates one comparison from its resolved pair. {@code operands} are the normalized
     * raw operands, needed by arithmetic lowering and by error reporting.
     */
    private Predicate dispatch(String op, Resolved left, Resolved right,
                               List<Operand> operands, Scope scope) {
        // The planner never emits two constants, but ternary substitution does: the else
        // branch of `(aBool ? aNumber : 0) > 0` becomes gt(value(0), value(0)).
        if (COMPARISON_OPS.contains(op)
                && left instanceof Resolved.Constant lc
                && right instanceof Resolved.Constant rc) {
            return constantComparison(op, lc.value(), rc.value());
        }

        // `"a,b".contains(R.attr.x)`: the constant is the haystack and the column the needle.
        // A concat receiver is folded here too; the add solve would swap haystack and needle.
        StringMatch match = StringMatch.of(op);
        if (match != null && right instanceof Resolved.Field needleField) {
            Object receiver = left instanceof Resolved.Constant c ? c.value()
                    : left instanceof Resolved.ConstantAdd ca ? ca.fold()
                    : null;
            if (receiver != null) {
                if (!(receiver instanceof String haystack)) {
                    // CEL has no contains() on a number, so the planner cannot emit this.
                    throw Refusals.malformed(
                            op + " requires a string receiver, got "
                                    + PlanValues.typeName(receiver));
                }
                Path<?> needle = scope.path(needleField.variable());
                return fieldToFieldLike(cb.literal(haystack), needle, match);
            }
            // A null receiver falls through to the null-value error below.
        }

        if (COMPARISON_OPS.contains(op)) {
            // Both operands are expressions, so NormalizedBinary cannot reorder them; the
            // value-first form is mirrored here.
            if (left instanceof Resolved.TimestampField tsField
                    && right instanceof Resolved.TimestampConstant tsConst) {
                return timestampLeaf(op, tsField, tsConst, scope);
            }
            if (left instanceof Resolved.TimestampConstant tsConst
                    && right instanceof Resolved.TimestampField tsField) {
                return timestampLeaf(NormalizedBinary.mirror(op), tsField, tsConst, scope);
            }
            // Reachable through ternary substitution.
            if (left instanceof Resolved.TimestampConstant lts
                    && right instanceof Resolved.TimestampConstant rts) {
                return timestampConstantComparison(op, lts.instant(), rts.instant());
            }
            // NormalizedBinary puts string() first, so both spellings arrive here. Other
            // operators and constants reach leafOperandError.
            if (("eq".equals(op) || "ne".equals(op))
                    && left instanceof Resolved.StringOfField sf
                    && right instanceof Resolved.Constant c
                    && c.value() instanceof String text) {
                return booleanStringComparison(op, sf, text, operands, scope);
            }
            // `field op add(value, value)`: strings concatenate, as in CEL.
            if (left instanceof Resolved.Field f && right instanceof Resolved.ConstantAdd ca) {
                return leaf.applyLeaf(op, scope.path(f.variable()), ca.fold());
            }
            // `add(field, c) eq/ne value` is solved in Java only when the solve is exact
            // (strings, in-range integers). Floating-point subtraction does not invert
            // addition, so other numbers are compared in SQL.
            if (("eq".equals(op) || "ne".equals(op))
                    && left instanceof Resolved.FieldPlusConstant fpc
                    && right instanceof Resolved.Constant other
                    && !PlanValues.requiresSqlLowering(
                            other.value(),
                            PlanValues.protoValueToJava(fpc.constant().getValue()))) {
                return solveAddComparison(op, fpc, other, scope);
            }
            if (isArithmeticRooted(left) || isArithmeticRooted(right)) {
                return arithmetic.numericComparison(op, operands, scope);
            }
        } else if (isAddRooted(left) || isAddRooted(right)) {
            return addFoldOrError(op, operands, scope);
        }

        if (left instanceof Resolved.Field a && right instanceof Resolved.Field b) {
            return fieldToFieldComparison(op, a.variable(), b.variable(), scope);
        }

        // Either order: string-match operators are not normalized, so a null receiver arrives
        // value-first and must still reach the null-value error.
        Resolved.Field field = left instanceof Resolved.Field lf ? lf
                : right instanceof Resolved.Field rf ? rf : null;
        Resolved.Constant constant = left instanceof Resolved.Constant lc2 ? lc2
                : right instanceof Resolved.Constant rc2 ? rc2 : null;
        if (field != null && constant != null) {
            return leafFieldValue(op, field, constant, scope);
        }

        throw leafOperandError(op, operands);
    }

    private Predicate leafFieldValue(String op, Resolved.Field field,
                                     Resolved.Constant constant, Scope scope) {
        Object value = constant.value();

        // A list or map constant has no scalar-column comparison, and Hibernate would fail
        // with a raw coercion error. Checked before path resolution so a Relation-mapped
        // attribute gets this message too. The message never includes element values.
        if (value instanceof List<?> || value instanceof Map<?, ?>) {
            throw Refusals.unsupported(
                    op + " comparison against a " + constantShape(value)
                            + " constant is not supported for attribute "
                            + field.variable() + ". Whole-" + kindWord(value)
                            + " equality is not translatable to a scalar column"
                            + " comparison; map the attribute as a Relation and use"
                            + " in/hasIntersection, or compare elements individually.");
        }

        Path<?> path = scope.path(field.variable());

        if (value == null) {
            // A NULL column sends no attribute, so `x == null` is never true: a present value is
            // unequal and a missing one is a CEL error, which denies. The upfront OMITTED scan
            // lets this shape through only without an override for op.
            if (("eq".equals(op) || "ne".equals(op)) && !leaf.overridden(op)
                    && leaf.isDeclaredOmitted(field.variable(), scope)) {
                return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                        () -> cb.isNull(path));
            }
            return leaf.withOverride(op, path, null, () -> switch (op) {
                case "eq" -> cb.isNull(path);
                case "ne" -> cb.isNotNull(path);
                // `x < null` is a CEL error, which denies. No ordering predicate matches that.
                default -> throw Refusals.unsupported(
                        "Null values are only supported with eq and ne operators (got " + op + ")");
            });
        }

        // An explicit-null attribute holds a null value in CEL, so eq/ne is definite. An
        // overridden operator is left to the override.
        if (("eq".equals(op) || "ne".equals(op))
                && !leaf.overridden(op)
                && leaf.isExplicitNull(field.variable(), scope)) {
            return leaf.definiteEquality(op, path, cb.literal(value), true, false);
        }

        return leaf.applyLeaf(op, path, value);
    }

    /**
     * {@code timestamp(field) op timestamp(constant)}, with {@code op} already field-first.
     *
     * <p>Only {@link Instant} and {@link OffsetDateTime} columns are translated, because both
     * denote an absolute instant. {@code LocalDateTime}, {@code java.util.Date} and
     * {@code String} do not, and guessing a zone could return rows the PDP denies, so they
     * throw. A registered {@link OperatorFunction} override is tried first, with the parsed
     * {@link Instant}. A NULL column makes the comparison UNKNOWN, matching CEL's deny.
     */
    private Predicate timestampLeaf(String op, Resolved.TimestampField field,
                                    Resolved.TimestampConstant constant, Scope scope) {
        Instant instant = constant.instant();
        Path<?> path = scope.path(field.variable());
        return leaf.withOverride(op, path, instant, () -> {
            Class<?> javaType = path.getJavaType();
            Object bound;
            if (Instant.class.equals(javaType)) {
                bound = instant;
            } else if (OffsetDateTime.class.equals(javaType)) {
                bound = instant.atOffset(ZoneOffset.UTC);
            } else {
                // Unmapped, not unsupported: the caller fixes it by remapping the column or
                // registering an override.
                throw Refusals.unmapped(
                        "timestamp() comparison requires a column mapped to java.time.Instant "
                                + "or java.time.OffsetDateTime, but '" + field.variable()
                                + "' maps to " + javaType.getSimpleName()
                                + ". Other temporal representations (LocalDateTime, "
                                + "java.util.Date, String) are ambiguous about the absolute "
                                + "instant they store; remap the column or register an "
                                + "OperatorFunction override for '" + op + "'.");
            }
            return leaf.defaultLeaf(op, path, bound);
        });
    }

    private Predicate timestampConstantComparison(String op, Instant left, Instant right) {
        return constant(holds(op, left.compareTo(right)));
    }

    /**
     * {@code string(boolColumn) eq/ne "text"}. CEL renders a bool as exactly {@code "true"} or
     * {@code "false"}, so the constant is matched here and only the boolean column reaches SQL.
     * SQL has no portable spelling of the conversion: {@code CAST} gives {@code '1'} on MySQL,
     * and a text-producing {@code CASE} compares in the connection collation, which is
     * case-insensitive under MySQL Connector/J.
     *
     * <p>Any other constant matches nothing. A NULL column stays UNKNOWN, because CEL's
     * {@code string()} errors on it and the PDP denies. Only {@link Boolean} columns qualify:
     * a primitive {@code boolean} path fails the leaf's type check and folds to a constant.
     */
    private Predicate booleanStringComparison(String op, Resolved.StringOfField field,
                                              String value, List<Operand> operands, Scope scope) {
        Path<?> path = scope.path(field.variable());
        if (!Boolean.class.equals(path.getJavaType())) {
            throw leafOperandError(op, operands);
        }
        if ("true".equals(value) || "false".equals(value)) {
            return leaf.applyLeaf(op, path, Boolean.valueOf(value));
        }
        return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                () -> cb.isNull(path));
    }

    // Size and kind only: element values never go into an error message.
    private static String constantShape(Object value) {
        if (value instanceof List<?> l) {
            return "list of " + l.size() + " element" + (l.size() == 1 ? "" : "s");
        }
        Map<?, ?> m = (Map<?, ?>) value;
        return "map of " + m.size() + " entr" + (m.size() == 1 ? "y" : "ies");
    }

    private static String kindWord(Object value) {
        return value instanceof List<?> ? "list" : "map";
    }

    /**
     * Solves {@code add(field, c) eq/ne value} for the field. With no solution (e.g.
     * {@code "projects:123" == "users:" + R.id}) eq is always false and ne always true, except
     * that a NULL field stays UNKNOWN: {@code "users:" + null} is a CEL error, which denies.
     */
    private Predicate solveAddComparison(String op, Resolved.FieldPlusConstant fpc,
                                         Resolved.Constant other, Scope scope) {
        Object otherValue = other.value();
        Object addConst = PlanValues.protoValueToJava(fpc.constant().getValue());
        Object solved = PlanValues.solveAdd(otherValue, addConst, fpc.fieldIsLeft());
        if (solved == null) {
            return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                    () -> cb.isNull(scope.path(fpc.fieldVariable())));
        }
        return leaf.applyLeaf(op, scope.path(fpc.fieldVariable()), solved);
    }

    /**
     * {@code add} under a string match or unknown operator. Only a folded
     * {@code add(value, value)} against a field translates; everything else throws.
     */
    private Predicate addFoldOrError(String op, List<Operand> operands, Scope scope) {
        Operand addExprOperand = null;
        Operand otherOperand = null;
        for (Operand o : operands) {
            if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "add".equals(o.getExpression().getOperator())) {
                addExprOperand = o;
            } else {
                otherOperand = o;
            }
        }
        if (otherOperand == null) {
            throw Refusals.unsupported(
                    op + " between two add() expressions is not supported: got "
                            + Refusals.describeOperand(operands.get(0)) + " and "
                            + Refusals.describeOperand(operands.get(1))
                            + "; one operand must be a mapped attribute or constant");
        }
        List<Operand> addOperands = addExprOperand.getExpression().getOperandsList();
        if (addOperands.size() != 2) {
            throw Refusals.malformed("add requires exactly 2 operands");
        }
        Operand addLeft = addOperands.get(0);
        Operand addRight = addOperands.get(1);
        if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
            Object folded = PlanValues.foldAdd(
                    PlanValues.protoValueToJava(addLeft.getValue()),
                    PlanValues.protoValueToJava(addRight.getValue()));
            if (otherOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw Refusals.unsupported(
                        "add(const, const) compared to a non-field operand is not supported");
            }
            return leaf.applyLeaf(op, scope.path(otherOperand.getVariable()), folded);
        }
        throw Refusals.unsupported(
                "add comparison with a field reference only supports eq/ne (got " + op + ")");
    }

    /**
     * Reports an operand shape no case accepts. Reads the raw operands left to right so each
     * shape keeps its pinned message.
     */
    private IllegalArgumentException leafOperandError(String op, List<Operand> operands) {
        String variable = null;
        for (Operand o : operands) {
            switch (o.getNodeCase()) {
                case VARIABLE -> variable = o.getVariable();
                // Converting a malformed VALUE throws here, in operand order.
                case VALUE -> PlanValues.protoValueToJava(o.getValue());
                case EXPRESSION -> {
                    String innerOp = o.getExpression().getOperator();
                    // map() is only supported inside hasIntersection.
                    if ("map".equals(innerOp)) {
                        throw Refusals.unsupported(
                                "Direct comparison of map(...) to a value is not supported "
                                        + "(operator: " + op + "). Wrap the map() expression in "
                                        + "hasIntersection(map(...), [...]) instead.");
                    }
                    // eq(except(variable, list), list): the comparison form of except().
                    if ("except".equals(innerOp)) {
                        throw Refusals.exceptUnsupported();
                    }
                    // A cast, index, lambda or nested timestamp(): legal CEL with no column
                    // shape.
                    throw Refusals.unsupported(
                            "Unexpected " + innerOp + "() expression in leaf operand of " + op);
                }
                default -> throw Refusals.malformed(
                        "Unexpected operand type in leaf expression: " + o.getNodeCase());
            }
        }
        // Two constants under an operator the constant fold does not cover. The planner
        // evaluates these itself.
        if (variable == null) {
            return Refusals.malformed("Missing variable operand for " + op);
        }
        return Refusals.malformed("Missing value operand for " + op);
    }

    /**
     * Evaluates a comparison of two constants to always-true ({@code 1=1}) or always-false
     * ({@code 1=0}). Numbers compare as doubles and strings lexicographically; other types
     * support only eq/ne, and ordering them throws.
     *
     * <p>Uses the primitive operators, not {@link Double#compare}, which ranks NaN above every
     * number. A NaN constant (e.g. an unfolded {@code 0.0/0.0}) must compare false, as in CEL.
     */
    Predicate constantComparison(String op, Object left, Object right) {
        if ("eq".equals(op) || "ne".equals(op)) {
            boolean equal = (left instanceof Number ln && right instanceof Number rn)
                    ? ln.doubleValue() == rn.doubleValue()
                    : Objects.equals(left, right);
            return constant("eq".equals(op) == equal);
        }
        if (left instanceof Number ln && right instanceof Number rn) {
            double l = ln.doubleValue();
            double r = rn.doubleValue();
            return constant(switch (op) {
                case "lt" -> l < r;
                case "gt" -> l > r;
                case "le" -> l <= r;
                case "ge" -> l >= r;
                default -> throw Refusals.internal(
                        "Unsupported constant comparison operator: " + op);
            });
        }
        if (left instanceof String ls && right instanceof String rs) {
            return constant(holds(op, ls.compareTo(rs)));
        }
        // CEL cannot order two booleans or a string against a number, so the planner would
        // have rejected the policy.
        throw Refusals.malformed(
                "Cannot order constant operands of " + op + ": "
                        + PlanValues.typeName(left) + " vs " + PlanValues.typeName(right));
    }

    /** Applies {@code op} to a {@code compareTo} result. Not used for numbers. */
    private static boolean holds(String op, int cmp) {
        return switch (op) {
            case "eq" -> cmp == 0;
            case "ne" -> cmp != 0;
            case "lt" -> cmp < 0;
            case "gt" -> cmp > 0;
            case "le" -> cmp <= 0;
            case "ge" -> cmp >= 0;
            default -> throw Refusals.internal(
                    "Unsupported constant comparison operator: " + op);
        };
    }

    private Predicate constant(boolean result) {
        return result ? cb.conjunction() : cb.disjunction();
    }

    /**
     * Compares or pattern-matches two columns. Source order is kept: {@link NormalizedBinary}
     * never swaps two variables.
     */
    private Predicate fieldToFieldComparison(String op, String leftVar, String rightVar,
                                             Scope scope) {
        Expression<?> left = scope.path(leftVar);
        Expression<?> right = scope.path(rightVar);
        if (Temporal.class.isAssignableFrom(left.getJavaType())
                || Temporal.class.isAssignableFrom(right.getJavaType())) {
            throw Refusals.unsupported("Bare temporal comparison cannot preserve CEL string "
                    + "equality; use timestamp() explicitly");
        }
        boolean leftExplicit = leaf.isExplicitNull(leftVar, scope);
        boolean rightExplicit = leaf.isExplicitNull(rightVar, scope);
        if (!LeafTranslator.compatibleTypes(left.getJavaType(), right.getJavaType())) {
            return "eq".equals(op) || "ne".equals(op)
                    ? leaf.definiteEquality(op, left, right, leftExplicit, rightExplicit)
                    : tri.unknown();
        }
        // A NULL on the explicit-null side needs a definite answer and a NULL on the other
        // side needs UNKNOWN: the definite equality, made UNKNOWN wherever the omitted side is
        // NULL, whatever the explicit side holds.
        if (("eq".equals(op) || "ne".equals(op)) && leftExplicit != rightExplicit) {
            Expression<?> omitted = leftExplicit ? right : left;
            Predicate equality = tri.baseUnlessUnknown(
                    leaf.definiteEquality("eq", left, right, leftExplicit, rightExplicit),
                    () -> cb.isNull(omitted));
            return "ne".equals(op) ? tri.not(equality) : equality;
        }
        if (("eq".equals(op) || "ne".equals(op)) && leftExplicit && rightExplicit) {
            return leaf.definiteEquality(op, left, right, leftExplicit, rightExplicit);
        }
        if (COMPARISON_OPS.contains(op)) {
            return comparePredicate(op, left, right);
        }
        StringMatch match = StringMatch.of(op);
        if (match != null) {
            return fieldToFieldLike(left, right, match);
        }
        throw Refusals.unsupported(
                "Field-to-field comparison is not supported for operator '" + op + "': "
                        + leftVar + " vs " + rightVar);
    }

    /**
     * Compares two SQL expressions. A constant right-hand side is bound as a value instead
     * (see {@link ArithmeticTranslator#numericComparison}).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    Predicate comparePredicate(String op,
                               Expression left,
                               Expression right) {
        return switch (op) {
            case "eq" -> cb.equal(left, right);
            case "ne" -> cb.notEqual(left, right);
            case "lt" -> cb.lessThan(left, right);
            case "gt" -> cb.greaterThan(left, right);
            case "le" -> cb.lessThanOrEqualTo(left, right);
            case "ge" -> cb.greaterThanOrEqualTo(left, right);
            default -> throw Refusals.internal(
                    "Unsupported arithmetic comparison operator: " + op);
        };
    }

    // Escaped in this order: the escape character itself must go first.
    private static final List<String> LIKE_METACHARACTERS = List.of("\\", "%", "_", "[");

    /**
     * {@code haystack LIKE pattern(needle)} for a column needle. The needle's LIKE
     * metacharacters are escaped with nested {@code REPLACE}, as {@link PlanValues#escapeLike}
     * does for constants. {@code [} is escaped because SQL Server treats it as a character
     * class.
     *
     * <p>A NULL needle gives a NULL pattern, so the LIKE is UNKNOWN under both polarities, as
     * CEL's missing-attribute error denies under both. It also stops a dialect whose
     * {@code CONCAT} treats NULL as {@code ''} from building a match-all {@code '%%'}.
     */
    private Predicate fieldToFieldLike(Expression<?> haystack, Expression<?> needle,
                                       StringMatch match) {
        Expression<String> pattern = needle.as(String.class);
        for (String metacharacter : LIKE_METACHARACTERS) {
            pattern = cb.function("replace", String.class,
                    pattern, cb.literal(metacharacter), cb.literal("\\" + metacharacter));
        }
        if (match.leadingWildcard) {
            pattern = cb.concat(cb.literal("%"), pattern);
        }
        if (match.trailingWildcard) {
            pattern = cb.concat(pattern, cb.literal("%"));
        }
        Expression<String> guardedPattern =
                cb.<String>selectCase()
                        .when(cb.isNull(needle), cb.nullLiteral(String.class))
                        .otherwise(pattern);
        return cb.like(haystack.as(String.class), guardedPattern, '\\');
    }
}
