package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.From;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;

import com.google.protobuf.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Translates a Cerbos {@code PlanResources} response into a Spring Data JPA
 * {@link org.springframework.data.jpa.domain.Specification} that can be executed by any
 * {@code JpaSpecificationExecutor}.
 */
public final class SpringDataQueryPlanAdapter {

    private SpringDataQueryPlanAdapter() {}

    // -- PlanResourcesResult overloads --

    public static <T> Result<T> toSpecification(
            PlanResourcesResult planResult, Map<String, AttributeMapping> mapper) {
        return toSpecification(planResult, mapper, Map.of());
    }

    public static <T> Result<T> toSpecification(
            PlanResourcesResult planResult,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides) {
        if (planResult.isAlwaysAllowed()) {
            return new Result.AlwaysAllowed<>();
        }
        if (planResult.isAlwaysDenied()) {
            return new Result.AlwaysDenied<>();
        }
        Operand condition = planResult.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Conditional plan has no condition"));
        return new Result.Conditional<>((root, query, cb) ->
                new Translator(cb, overrides).traverse(condition, Scope.root(root, query, mapper)));
    }

    // -- PlanResourcesResponse overloads --

    public static <T> Result<T> toSpecification(
            PlanResourcesResponse response, Map<String, AttributeMapping> mapper) {
        return toSpecification(response, mapper, Map.of());
    }

    public static <T> Result<T> toSpecification(
            PlanResourcesResponse response,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides) {
        PlanResourcesFilter filter = response.getFilter();
        return switch (filter.getKind()) {
            case KIND_ALWAYS_ALLOWED -> new Result.AlwaysAllowed<>();
            case KIND_ALWAYS_DENIED -> new Result.AlwaysDenied<>();
            case KIND_CONDITIONAL -> {
                Operand cond = filter.getCondition();
                if (cond.getNodeCase() == Operand.NodeCase.NODE_NOT_SET) {
                    throw new IllegalArgumentException("Conditional plan has no condition");
                }
                yield new Result.Conditional<T>((root, query, cb) ->
                        new Translator(cb, overrides).traverse(cond, Scope.root(root, query, mapper)));
            }
            default -> throw new IllegalArgumentException("Unknown filter kind: " + filter.getKind());
        };
    }

    // -- Internal translator --

    private static final class Translator {
        private final CriteriaBuilder cb;
        private final Map<String, OperatorFunction> overrides;
        private final HierarchyTranslator hierarchy;

        Translator(CriteriaBuilder cb, Map<String, OperatorFunction> overrides) {
            this.cb = cb;
            this.overrides = overrides;
            this.hierarchy = new HierarchyTranslator(cb);
        }

        Predicate traverse(Operand operand, Scope scope) {
            return switch (operand.getNodeCase()) {
                case EXPRESSION -> traverseExpression(operand.getExpression(), scope);
                case VARIABLE -> handleBareVariable(operand.getVariable(), scope);
                default -> throw new IllegalArgumentException("Unexpected operand type: " + operand.getNodeCase());
            };
        }

        private Predicate handleBareVariable(String variable, Scope scope) {
            Path<?> path = scope.resolvePath(variable);
            return applyLeaf("eq", path, true);
        }

        /**
         * Logical negation with a junction barrier. Hibernate 6's SQM negation is stateful for
         * comparison predicates: {@code cb.not(cb.not(p))} stays negated instead of toggling
         * back (verified against Hibernate 6.6.18 — a double-negated {@code eq} still renders
         * a single {@code NOT}). Wrapping in a single-element conjunction gives each {@code not}
         * a fresh node to negate, so nested negations compose correctly.
         */
        private Predicate negate(Predicate p) {
            return cb.not(cb.and(p));
        }

        private Predicate traverseExpression(PlanResourcesFilter.Expression expression, Scope scope) {
            String op = expression.getOperator();
            List<Operand> operands = expression.getOperandsList();

            return switch (op) {
                case "and" -> cb.and(operands.stream()
                        .map(o -> traverse(o, scope)).toArray(Predicate[]::new));
                case "or" -> cb.or(operands.stream()
                        .map(o -> traverse(o, scope)).toArray(Predicate[]::new));
                case "not" -> {
                    if (operands.size() != 1) {
                        throw new IllegalArgumentException("not requires exactly 1 operand");
                    }
                    yield negate(traverse(operands.get(0), scope));
                }
                case "exists", "exists_one", "all", "except", "filter" ->
                        handleCollectionOperator(op, operands, scope);
                // has_intersection is the deprecated pre-camelCase alias still accepted by the PDP.
                case "hasIntersection", "has_intersection" -> handleHasIntersection(operands, scope);
                case "isSet" -> handleIsSet(operands, scope);
                case "in" -> handleIn(operands, scope);
                case "if" -> handleBareTernary(operands, scope);
                case "overlaps" -> hierarchy.handleOverlaps(operands, scope);
                case "ancestorOf" -> hierarchy.handleAncestorDescendant(operands, scope, true);
                case "descendentOf" -> hierarchy.handleAncestorDescendant(operands, scope, false);
                default -> {
                    Predicate ternaryPred = tryTernaryComparison(op, operands, scope);
                    if (ternaryPred != null) {
                        yield ternaryPred;
                    }
                    NormalizedBinary nb = NormalizedBinary.of(op, operands);
                    Predicate sizePred = trySizeComparison(nb.op(), nb.operands(), scope);
                    if (sizePred != null) {
                        yield sizePred;
                    }
                    Predicate arithPred = tryArithmeticComparison(nb.op(), nb.operands(), scope);
                    if (arithPred != null) {
                        yield arithPred;
                    }
                    yield handleLeafOperator(nb.op(), nb.operands(), scope);
                }
            };
        }

        /**
         * A binary expression normalized to field-side-first. The planner preserves policy source
         * order, so a constant may precede the field it constrains ({@code 5 < R.attr.x} arrives
         * as {@code lt(value(5), variable(x))}). Normalizing once here — most field-like operand
         * first (variable > nested expression > constant value), mirroring directional operators
         * when swapping — lets every downstream handler assume field-first order. A consequence
         * is that {@link OperatorFunction} overrides are consulted under the mirrored operator:
         * a value-first {@code lt} is looked up as {@code gt}.
         *
         * <p>Only operators whose semantics survive a swap are reordered: symmetric ones
         * ({@code eq}/{@code ne}/{@code in}/{@code hasIntersection}) and the mirrorable
         * inequalities ({@code lt}/{@code gt}/{@code le}/{@code ge}). The CEL string-match
         * methods ({@code contains}/{@code startsWith}/{@code endsWith}) are RECEIVER-SENSITIVE:
         * {@code "a,b".contains(R.attr.x)} arrives as {@code contains(value, variable)} where
         * the constant is the haystack — swapping it would silently invert haystack and needle
         * (translating {@code x LIKE '%a,b%'} instead of testing whether {@code "a,b"} contains
         * the column value). Those keep planner source order and are handled positionally by
         * {@link #handleLeafOperator}.
         */
        private record NormalizedBinary(String op, List<Operand> operands) {

            /** Operators whose operands may be reordered without changing meaning. */
            private static final Set<String> ORDER_NORMALIZABLE = Set.of(
                    "eq", "ne", "lt", "gt", "le", "ge",
                    "in", "hasIntersection", "has_intersection");

            static NormalizedBinary of(String op, List<Operand> operands) {
                if (ORDER_NORMALIZABLE.contains(op)
                        && operands.size() == 2
                        && rank(operands.get(0)) < rank(operands.get(1))) {
                    return new NormalizedBinary(mirror(op), List.of(operands.get(1), operands.get(0)));
                }
                return new NormalizedBinary(op, operands);
            }

            private static int rank(Operand o) {
                return switch (o.getNodeCase()) {
                    case VARIABLE -> 2;
                    case EXPRESSION -> 1;
                    default -> 0;
                };
            }

            /** lt/le/gt/ge mirror when their operands swap sides; symmetric operators are unchanged. */
            private static String mirror(String op) {
                return switch (op) {
                    case "lt" -> "gt";
                    case "gt" -> "lt";
                    case "le" -> "ge";
                    case "ge" -> "le";
                    default -> op;
                };
            }
        }

        // -- if (CEL ternary) --

        /**
         * The orderable/equality comparison operators (eq/ne/lt/gt/le/ge) — shared by the
         * ternary rewrite, the arithmetic path, and the constant-vs-constant fold.
         */
        private static final Set<String> COMPARISON_OPS =
                Set.of("eq", "ne", "lt", "gt", "le", "ge");

        /**
         * A comparison wrapping a CEL ternary — {@code cmp(if(c, a, b), other)}. Each branch is
         * substituted back into the comparison and recursed through {@link #traverseExpression},
         * so a ternary branch behaves identically to the same comparison written directly (see
         * {@link #translateTernary} for the rewrite and its null semantics). Recursion also
         * handles nested ternaries and a ternary on the other side for free.
         *
         * @return the rewritten predicate, or {@code null} if this comparison involves no ternary
         */
        private Predicate tryTernaryComparison(String op, List<Operand> operands, Scope scope) {
            if (!COMPARISON_OPS.contains(op) || operands.size() != 2) {
                return null;
            }
            int idx;
            if (isIfExpression(operands.get(0))) {
                idx = 0;
            } else if (isIfExpression(operands.get(1))) {
                idx = 1;
            } else {
                return null;
            }
            List<Operand> ifOps = operands.get(idx).getExpression().getOperandsList();
            return translateTernary(ifOps,
                    branch -> traverseExpression(substituteOperand(op, operands, idx, branch), scope),
                    scope);
        }

        private static boolean isIfExpression(Operand o) {
            return o.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "if".equals(o.getExpression().getOperator());
        }

        /**
         * Rewrite a CEL ternary {@code if(c, a, b)} into a pure predicate:
         *
         * <pre>{@code (pred(c) AND branch(a)) OR (NOT pred(c) AND branch(b)) OR NOT(pred(c) OR NOT pred(c))}</pre>
         *
         * where {@code branch} is supplied by the caller — comparison substitution for
         * {@link #tryTernaryComparison}, {@link #booleanBranchPredicate} for
         * {@link #handleBareTernary}. We rewrite instead of emitting {@code CASE WHEN}
         * ({@code cb.selectCase}) because this translator is predicate-only: every existing typed
         * leaf path — field-first normalization, size() handling, add-fold, fractional
         * double-space comparison — operates on comparison predicates, and routing the branches
         * back through those exact paths keeps them identical to the same condition written
         * directly.
         *
         * <p>A constant boolean condition folds to a single branch — only that branch is
         * translated, so an untranslatable dead branch cannot fail the whole plan.
         *
         * <p>Null semantics: a null/missing condition in a CEL ternary is an evaluation error
         * and the check denies, so the SQL must evaluate to UNKNOWN — never FALSE — when the
         * condition column is NULL. The two branch arms alone are not enough: with both branch
         * predicates false they evaluate {@code (NULL AND FALSE) OR (NULL AND FALSE) = FALSE},
         * which {@code not(...)} flips to TRUE and leaks rows the PDP denies. The third arm
         * ({@link #unknownWhenConditionUnknown}) restores the missing UNKNOWN: it is FALSE for a
         * known condition (no effect on the OR) and UNKNOWN for a NULL one, driving the whole OR
         * to UNKNOWN so the row is excluded under BOTH polarities.
         *
         * <p>The condition is translated fresh for each arm: Hibernate 6 negation is stateful
         * (see {@link #negate}), so sharing one Predicate node between the positive and negated
         * arms is unsafe.
         */
        private Predicate translateTernary(List<Operand> ifOps,
                                           java.util.function.Function<Operand, Predicate> branchTranslator,
                                           Scope scope) {
            if (ifOps.size() != 3) {
                throw new IllegalArgumentException(
                        "if (ternary) requires exactly 3 operands (condition, then, else), got "
                                + ifOps.size());
            }
            Operand condition = ifOps.get(0);
            Operand thenBranch = ifOps.get(1);
            Operand elseBranch = ifOps.get(2);

            if (condition.getNodeCase() == Operand.NodeCase.VALUE) {
                Boolean known = constantBooleanOrNull(condition);
                if (known == null) {
                    throw new IllegalArgumentException(
                            "if (ternary) condition must be a boolean expression");
                }
                return branchTranslator.apply(known ? thenBranch : elseBranch);
            }

            return cb.or(
                    cb.and(traverse(condition, scope), branchTranslator.apply(thenBranch)),
                    cb.and(negate(traverse(condition, scope)), branchTranslator.apply(elseBranch)),
                    unknownWhenConditionUnknown(condition, scope));
        }

        /**
         * UNKNOWN exactly when {@code condition} is UNKNOWN, FALSE when it is known:
         * {@code NOT(c OR NOT c)}. Truth table: condition TRUE → {@code NOT(TRUE OR FALSE)} =
         * FALSE; condition FALSE → {@code NOT(FALSE OR TRUE)} = FALSE; condition UNKNOWN →
         * {@code NOT(UNKNOWN OR UNKNOWN)} = UNKNOWN. As the last arm of the ternary OR it
         * therefore vanishes for known conditions and forces the whole predicate to UNKNOWN for
         * NULL-derived ones — matching the CEL evaluation error (deny) under both polarities.
         * The condition is translated fresh for each occurrence (Hibernate 6 negation is
         * stateful — see {@link #negate}).
         */
        private Predicate unknownWhenConditionUnknown(Operand condition, Scope scope) {
            return negate(cb.or(traverse(condition, scope), negate(traverse(condition, scope))));
        }

        /**
         * A CEL ternary in boolean position — {@code if(c, a, b)} used directly as a condition,
         * so both branches are themselves boolean and translate through
         * {@link #booleanBranchPredicate}. Same rewrite, rationale and null semantics as
         * {@link #translateTernary}.
         */
        private Predicate handleBareTernary(List<Operand> operands, Scope scope) {
            return translateTernary(operands, branch -> booleanBranchPredicate(branch, scope), scope);
        }

        /**
         * A ternary branch in boolean position: a boolean VALUE folds to the always-true /
         * always-false predicate (the same collapse the unsolvable add-solve cases use); anything
         * else translates as a normal boolean operand (bare variables become {@code path = true}).
         */
        private Predicate booleanBranchPredicate(Operand branch, Scope scope) {
            if (branch.getNodeCase() == Operand.NodeCase.VALUE) {
                Boolean constant = constantBooleanOrNull(branch);
                if (constant == null) {
                    throw new IllegalArgumentException(
                            "if (ternary) branch in boolean position must be a boolean");
                }
                return constant ? cb.conjunction() : cb.disjunction();
            }
            return traverse(branch, scope);
        }

        /** Rebuild {@code op(operands...)} with the operand at {@code idx} replaced. */
        private static PlanResourcesFilter.Expression substituteOperand(
                String op, List<Operand> operands, int idx, Operand replacement) {
            PlanResourcesFilter.Expression.Builder b =
                    PlanResourcesFilter.Expression.newBuilder().setOperator(op);
            for (int i = 0; i < operands.size(); i++) {
                b.addOperands(i == idx ? replacement : operands.get(i));
            }
            return b.build();
        }

        /** The operand's boolean constant, or {@code null} if it is not a boolean VALUE. */
        private static Boolean constantBooleanOrNull(Operand o) {
            return PlanValues.protoValueToJava(o.getValue()) instanceof Boolean b ? b : null;
        }

        // -- Leaf operators (eq/ne/lt/gt/le/ge/contains/startsWith/endsWith) --

        /** The receiver-sensitive CEL string-match methods (see {@link NormalizedBinary}). */
        private static final Set<String> STRING_MATCH_OPS =
                Set.of("contains", "startsWith", "endsWith");

        /** Operands must already be normalized field-first (see {@link NormalizedBinary}). */
        private Predicate handleLeafOperator(String op, List<Operand> operands, Scope scope) {
            // Every leaf operator is binary. Extra operands are a malformed plan and must fail
            // loudly: the collector below would otherwise silently DROP one.
            if (operands.size() != 2) {
                throw new IllegalArgumentException(
                        op + " requires exactly 2 operands, got " + operands.size());
            }

            // Constant-vs-constant comparisons are statically evaluated. The planner never emits
            // them directly, but ternary substitution produces them — the else branch of
            // `(aBool ? aNumber : 0) > 0` becomes gt(value(0), value(0)).
            if (COMPARISON_OPS.contains(op)
                    && operands.get(0).getNodeCase() == Operand.NodeCase.VALUE
                    && operands.get(1).getNodeCase() == Operand.NodeCase.VALUE) {
                return constantComparison(op,
                        PlanValues.protoValueToJava(operands.get(0).getValue()),
                        PlanValues.protoValueToJava(operands.get(1).getValue()));
            }

            // Constant-receiver string matches: `"a,b".contains(R.attr.x)` arrives as
            // contains(value, variable) — the CONSTANT is the haystack and the COLUMN the
            // needle (NormalizedBinary deliberately leaves these in source order). This must
            // be checked BEFORE the add-detection below so an unfolded concat receiver
            // (`("a" + "b").contains(R.attr.x)`) routes here too, not into the add-solve path
            // (which would fold the constant and translate the INVERTED column-haystack LIKE).
            if (STRING_MATCH_OPS.contains(op)
                    && operands.get(1).getNodeCase() == Operand.NodeCase.VARIABLE) {
                Object receiver = constantReceiverOrNull(operands.get(0));
                if (receiver != null) {
                    if (!(receiver instanceof String haystack)) {
                        throw new IllegalArgumentException(
                                op + " requires a string receiver, got " + typeName(receiver));
                    }
                    Path<?> needle = scope.resolvePath(operands.get(1).getVariable());
                    // The needle is a column, so it is escaped dynamically; a NULL needle is
                    // a missing attribute → CEL error → deny (fieldToFieldLike guards it).
                    return switch (op) {
                        case "contains" -> fieldToFieldLike(cb.literal(haystack), needle, true, true);
                        case "startsWith" -> fieldToFieldLike(cb.literal(haystack), needle, false, true);
                        case "endsWith" -> fieldToFieldLike(cb.literal(haystack), needle, true, false);
                        default -> throw new IllegalArgumentException(
                                "Unsupported string-match operator: " + op);
                    };
                }
            }

            // Detect leaf comparisons where one side is an 'add' expression (e.g. string
            // concatenation: `aString == "prefix:" + R.attr.id`). We fold constants and solve for
            // the field side when possible — same algorithm as the Prisma adapter.
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
            if (addExprOperand != null) {
                if (otherOperand == null) {
                    throw new IllegalArgumentException("add comparison requires a second operand");
                }
                return handleAddComparison(op, addExprOperand.getExpression(), otherOperand, scope);
            }

            String variable = null;
            String secondVariable = null;
            Object value = null;
            boolean valueSeen = false;
            for (Operand o : operands) {
                switch (o.getNodeCase()) {
                    case VARIABLE -> {
                        if (variable != null) {
                            secondVariable = o.getVariable();
                        } else {
                            variable = o.getVariable();
                        }
                    }
                    case VALUE -> {
                        value = PlanValues.protoValueToJava(o.getValue());
                        valueSeen = true;
                    }
                    case EXPRESSION -> {
                        // H3: map() compositions are only accepted inside hasIntersection.
                        // A direct comparison like eq(map(...), [...]) reaches here; point users
                        // at the supported shape rather than throwing a generic operand error.
                        String innerOp = o.getExpression().getOperator();
                        if ("map".equals(innerOp)) {
                            throw new IllegalArgumentException(
                                    "Direct comparison of map(...) to a value is not supported "
                                            + "(operator: " + op + "). Wrap the map() expression in "
                                            + "hasIntersection(map(...), [...]) instead.");
                        }
                        throw new IllegalArgumentException(
                                "Unexpected " + innerOp + "() expression in leaf operand of " + op);
                    }
                    default -> throw new IllegalArgumentException(
                            "Unexpected operand type in leaf expression: " + o.getNodeCase());
                }
            }
            if (variable == null) {
                throw new IllegalArgumentException("Missing variable operand for " + op);
            }
            if (secondVariable != null) {
                return fieldToFieldComparison(op, variable, secondVariable, scope);
            }
            if (!valueSeen) {
                throw new IllegalArgumentException("Missing value operand for " + op);
            }

            Path<?> path = scope.resolvePath(variable);

            if (value == null) {
                // A registered override owns the operator's full translation, including a null RHS.
                return withOverride(op, path, null, () -> switch (op) {
                    case "eq" -> cb.isNull(path);
                    case "ne" -> cb.isNotNull(path);
                    default -> throw new IllegalArgumentException(
                            "Null values are only supported with eq and ne operators (got " + op + ")");
                });
            }

            return applyLeaf(op, path, value);
        }

        /**
         * Statically evaluate a comparison between two plan constants and collapse it to an
         * always-true ({@code 1=1}) or always-false ({@code 1=0}) predicate — the same collapse
         * the unsolvable {@code add}-solve cases use. Numbers compare in double space: protobuf
         * {@code Value.getNumberValue()} is a double, and {@link PlanValues#protoValueToJava}
         * only splits Long/Double for whole-number cosmetics, not semantics. Strings compare
         * lexicographically; booleans (and mixed incomparable types) support eq/ne only —
         * eq → false, ne → true — while ordering them is a planner bug and throws.
         */
        private Predicate constantComparison(String op, Object left, Object right) {
            boolean result;
            if ("eq".equals(op) || "ne".equals(op)) {
                boolean equal = (left instanceof Number ln && right instanceof Number rn)
                        ? ln.doubleValue() == rn.doubleValue()
                        : Objects.equals(left, right);
                result = "eq".equals(op) == equal;
            } else {
                int cmp;
                if (left instanceof Number ln && right instanceof Number rn) {
                    cmp = Double.compare(ln.doubleValue(), rn.doubleValue());
                } else if (left instanceof String ls && right instanceof String rs) {
                    cmp = ls.compareTo(rs);
                } else {
                    throw new IllegalArgumentException(
                            "Cannot order constant operands of " + op + ": "
                                    + typeName(left) + " vs " + typeName(right));
                }
                result = switch (op) {
                    case "lt" -> cmp < 0;
                    case "gt" -> cmp > 0;
                    case "le" -> cmp <= 0;
                    case "ge" -> cmp >= 0;
                    default -> throw new IllegalArgumentException(
                            "Unsupported constant comparison operator: " + op);
                };
            }
            return result ? cb.conjunction() : cb.disjunction();
        }

        private static String typeName(Object o) {
            return o == null ? "null" : o.getClass().getSimpleName();
        }

        /**
         * The constant value of a string-match RECEIVER operand: a plain VALUE, or an
         * {@code add(value, value)} concatenation folded to its constant. Returns {@code null}
         * when the operand is not a constant (e.g. a variable or a field-bearing expression),
         * in which case the caller falls through to the ordinary leaf paths.
         */
        private static Object constantReceiverOrNull(Operand o) {
            return switch (o.getNodeCase()) {
                case VALUE -> PlanValues.protoValueToJava(o.getValue());
                case EXPRESSION -> {
                    PlanResourcesFilter.Expression e = o.getExpression();
                    if ("add".equals(e.getOperator())
                            && e.getOperandsCount() == 2
                            && e.getOperands(0).getNodeCase() == Operand.NodeCase.VALUE
                            && e.getOperands(1).getNodeCase() == Operand.NodeCase.VALUE) {
                        yield PlanValues.foldAdd(
                                PlanValues.protoValueToJava(e.getOperands(0).getValue()),
                                PlanValues.protoValueToJava(e.getOperands(1).getValue()));
                    }
                    yield null;
                }
                default -> null;
            };
        }

        /**
         * Compare two mapped columns directly (eq/ne/lt/gt/le/ge) or pattern-match one column
         * against another (contains/startsWith/endsWith). Operand source order is preserved —
         * two variables rank equally, so {@link NormalizedBinary} never swaps them.
         */
        private Predicate fieldToFieldComparison(String op, String leftVar, String rightVar,
                                                 Scope scope) {
            jakarta.persistence.criteria.Expression<?> left = scope.resolvePath(leftVar);
            jakarta.persistence.criteria.Expression<?> right = scope.resolvePath(rightVar);
            return switch (op) {
                case "eq", "ne", "lt", "gt", "le", "ge" -> comparePredicate(op, left, right);
                case "contains" -> fieldToFieldLike(left, right, true, true);
                case "startsWith" -> fieldToFieldLike(left, right, false, true);
                case "endsWith" -> fieldToFieldLike(left, right, true, false);
                default -> throw new IllegalArgumentException(
                        "Field-to-field comparison is not supported for operator '" + op + "': "
                                + leftVar + " vs " + rightVar);
            };
        }

        /**
         * Raw-typed comparison of two SQL expressions — the shared dispatch of field-to-field
         * comparisons and arithmetic expression-vs-expression comparisons. Constant-RHS shapes
         * do NOT route here: they bind through the plain-value overloads on purpose (double
         * bind parameters — see {@link #tryArithmeticComparison}).
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        private Predicate comparePredicate(String op,
                                           jakarta.persistence.criteria.Expression left,
                                           jakarta.persistence.criteria.Expression right) {
            return switch (op) {
                case "eq" -> cb.equal(left, right);
                case "ne" -> cb.notEqual(left, right);
                case "lt" -> cb.lessThan(left, right);
                case "gt" -> cb.greaterThan(left, right);
                case "le" -> cb.lessThanOrEqualTo(left, right);
                case "ge" -> cb.greaterThanOrEqualTo(left, right);
                default -> throw new IllegalArgumentException(
                        "Unsupported arithmetic comparison operator: " + op);
            };
        }

        /**
         * {@code haystackColumn LIKE wildcards(escape(needleColumn))} — the column-to-column
         * analogue of the constant LIKE path in {@link #defaultLeaf}. The needle is data, so its
         * LIKE metacharacters are escaped dynamically with nested {@code REPLACE} (portable:
         * H2/Postgres/MySQL/Oracle/SQL Server): {@code \} first, then {@code %} and {@code _},
         * mirroring {@link PlanValues#escapeLike} and the same explicit {@code '\'} escape char.
         *
         * <p>The explicit {@code IS NOT NULL} guard on the needle matches CEL (a missing
         * attribute is an evaluation error → deny) and also defends against dialects whose
         * {@code CONCAT} treats NULL as {@code ''}, which would otherwise turn a NULL needle
         * into a match-anything {@code '%%'} pattern.
         */
        private Predicate fieldToFieldLike(jakarta.persistence.criteria.Expression<?> haystack,
                                           jakarta.persistence.criteria.Expression<?> needle,
                                           boolean leadingWildcard, boolean trailingWildcard) {
            jakarta.persistence.criteria.Expression<String> escaped =
                    needle.as(String.class);
            escaped = cb.function("replace", String.class,
                    escaped, cb.literal("\\"), cb.literal("\\\\"));
            escaped = cb.function("replace", String.class,
                    escaped, cb.literal("%"), cb.literal("\\%"));
            escaped = cb.function("replace", String.class,
                    escaped, cb.literal("_"), cb.literal("\\_"));
            jakarta.persistence.criteria.Expression<String> pattern = escaped;
            if (leadingWildcard) {
                pattern = cb.concat(cb.literal("%"), pattern);
            }
            if (trailingWildcard) {
                pattern = cb.concat(pattern, cb.literal("%"));
            }
            return cb.and(
                    cb.isNotNull(needle),
                    cb.like(haystack.as(String.class), pattern, '\\'));
        }

        /**
         * Apply a scalar leaf operator, consulting the per-operator {@code overrides} hook first so a
         * registered {@link OperatorFunction} wins on EVERY path that produces this operator — direct
         * comparison, {@code add}-folded comparison, and bare-boolean — not just the direct one.
         */
        private Predicate applyLeaf(String op, Path<?> path, Object value) {
            return withOverride(op, path, value, () -> defaultLeaf(op, path, value));
        }

        /**
         * Route a scalar (field, value) translation through the per-operator {@code overrides}
         * hook: a registered {@link OperatorFunction} owns the operator's full translation
         * (mirrored operators are consulted under the mirrored name — see
         * {@link NormalizedBinary}); otherwise the supplied default applies.
         */
        private Predicate withOverride(String op, jakarta.persistence.criteria.Expression<?> field,
                                       Object value, Supplier<Predicate> dflt) {
            OperatorFunction override = overrides.get(op);
            if (override != null) {
                return override.apply(cb, field, value);
            }
            return dflt.get();
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Predicate defaultLeaf(String op, Path<?> path, Object value) {
            // Fractional constants compare in double space: protoValueToJava yields Double only
            // for non-whole numbers, and Hibernate refuses to coerce e.g. 1.5 into an
            // Integer-typed path ("not a whole number") — but `intColumn >= 1.5` is legal CEL
            // that the planner emits verbatim.
            jakarta.persistence.criteria.Expression raw =
                    (value instanceof Double) ? path.as(Double.class) : path;
            return switch (op) {
                case "eq" -> cb.equal(raw, value);
                case "ne" -> cb.notEqual(raw, value);
                case "lt" -> cb.lessThan(raw, (Comparable) value);
                case "gt" -> cb.greaterThan(raw, (Comparable) value);
                case "le" -> cb.lessThanOrEqualTo(raw, (Comparable) value);
                case "ge" -> cb.greaterThanOrEqualTo(raw, (Comparable) value);
                case "contains" -> cb.like(path.as(String.class),
                        "%" + PlanValues.escapeLike(String.valueOf(value)) + "%", '\\');
                case "startsWith" -> cb.like(path.as(String.class),
                        PlanValues.escapeLike(String.valueOf(value)) + "%", '\\');
                case "endsWith" -> cb.like(path.as(String.class),
                        "%" + PlanValues.escapeLike(String.valueOf(value)), '\\');
                default -> throw new IllegalArgumentException("Unsupported operator: " + op);
            };
        }

        // -- arithmetic (add/sub/mult/div) as a comparison operand --

        /** CEL arithmetic operators that can appear as an operand of a comparison. */
        private static final Set<String> ARITHMETIC_OPS = Set.of("add", "sub", "mult", "div", "mod");

        /**
         * Translate {@code cmp(arith(...), other)} — e.g. {@code R.attr.aNumber + 1.0 > 2.0}
         * arriving as {@code gt(add(variable, value(1)), value(2))} — by emitting the arithmetic
         * on the SQL side ({@code cb.sum}/{@code diff}/{@code prod}/{@code quot}) and comparing.
         *
         * <p>Everything is computed and compared in DOUBLE space. This is not a convenience:
         * Cerbos attribute values are protobuf {@code Value} numbers, i.e. ALWAYS CEL doubles at
         * check time, so the only arithmetic that can evaluate without a no-overload error is
         * double-typed — verified against a live PDP: {@code R.attr.n + 1} (int literal) denies
         * every row, {@code + 1.0} works, and {@code / 2.0} is true double division
         * ({@code 5 / 2.0 == 2.5}). Integer truncation is therefore never observable through the
         * check API, and the wire plan erases the int/double distinction anyway (both arrive as
         * {@code number_value}). Emitting the arithmetic (rather than solving algebraically)
         * also means multiplication/division by negative constants needs no inequality flipping.
         *
         * <p>DOUBLE space must be enforced explicitly, because DB decimal arithmetic is not
         * IEEE double arithmetic (see {@link #resolveNumericOperand}): columns are CAST, plan
         * constants are folded in Java or bound as double parameters, and pure-constant
         * comparisons are evaluated statically in Java (full CEL fidelity, Infinity/NaN
         * included).
         *
         * <p>{@code mod} stays unsupported: CEL {@code %} has no double overload, so on
         * attribute values it always errors (deny) — translating it to SQL {@code MOD} would
         * fabricate rows the PDP denies.
         *
         * <p>{@link OperatorFunction} overrides win here like on every other scalar path when
         * the comparison has a plan constant on one side: the arithmetic SQL expression is
         * passed as the field argument and the folded constant (always a {@link Double} — the
         * arithmetic path is double-space end to end) as the value. Expression-vs-expression
         * comparisons (arithmetic against arithmetic or against another column) have no
         * (field, value) pair and are not consulted — the same exclusion as field-to-field
         * comparisons.
         *
         * @return the predicate, or {@code null} if this comparison involves no arithmetic
         *         expression or the shape is owned by the {@code add} fold/solve path (which
         *         also handles string concatenation and the override hooks)
         */
        private Predicate tryArithmeticComparison(String op, List<Operand> operands, Scope scope) {
            if (!COMPARISON_OPS.contains(op) || operands.size() != 2) {
                return null;
            }
            boolean hasArith = operands.stream().anyMatch(o ->
                    o.getNodeCase() == Operand.NodeCase.EXPRESSION
                            && ARITHMETIC_OPS.contains(o.getExpression().getOperator()));
            if (!hasArith) {
                return null;
            }
            if (addFoldSolveOwns(op, operands.get(0), operands.get(1))
                    || addFoldSolveOwns(op, operands.get(1), operands.get(0))) {
                return null;
            }
            NumericOperand left = resolveNumericOperand(operands.get(0), scope);
            NumericOperand right = resolveNumericOperand(operands.get(1), scope);

            // Both sides folded to constants (e.g. ternary substitution producing
            // gt(add(1.0, 2.0), 4.0)) — evaluate statically with IEEE semantics.
            if (left instanceof NumericOperand.Constant lc
                    && right instanceof NumericOperand.Constant rc) {
                return constantComparison(op, lc.value(), rc.value());
            }
            // Keep the SQL side on the left (mirroring the operator) so a constant right side
            // can bind through the plain-Number overloads. Normalization usually guarantees
            // this already, but an expression that FOLDS to a constant (add(1.0, 2.0)) ranks
            // as an expression and can still arrive first.
            if (left instanceof NumericOperand.Constant) {
                NumericOperand tmp = left;
                left = right;
                right = tmp;
                op = NormalizedBinary.mirror(op);
            }
            jakarta.persistence.criteria.Expression<Double> lhs =
                    ((NumericOperand.Sql) left).expr();

            if (right instanceof NumericOperand.Constant rc) {
                // Plain-value overloads bind the constant as a genuine double PARAMETER; a
                // cb.literal would inline `0.3`, which H2/Postgres type as exact NUMERIC and
                // drag the comparison out of IEEE space (see resolveNumericOperand).
                String cmpOp = op;
                double v = rc.value();
                return withOverride(cmpOp, lhs, rc.value(), () -> switch (cmpOp) {
                    case "eq" -> cb.equal(lhs, v);
                    case "ne" -> cb.notEqual(lhs, v);
                    case "lt" -> cb.lt(lhs, v);
                    case "gt" -> cb.gt(lhs, v);
                    case "le" -> cb.le(lhs, v);
                    case "ge" -> cb.ge(lhs, v);
                    default -> throw new IllegalArgumentException(
                            "Unsupported arithmetic comparison operator: " + cmpOp);
                });
            }

            jakarta.persistence.criteria.Expression<Double> rhs =
                    ((NumericOperand.Sql) right).expr();
            return comparePredicate(op, lhs, rhs);
        }

        /**
         * Whether {@code cmp(candidate, other)} is a shape {@link #handleAddComparison} already
         * translates — those keep their existing path (constant folding, string concat
         * solving, and the {@link OperatorFunction} override hooks): {@code add(value, value)}
         * against a field for any operator, and {@code add} of one field and one value against
         * a value for eq/ne.
         */
        private static boolean addFoldSolveOwns(String op, Operand candidate, Operand other) {
            if (candidate.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"add".equals(candidate.getExpression().getOperator())
                    || candidate.getExpression().getOperandsCount() != 2) {
                return false;
            }
            Operand l = candidate.getExpression().getOperands(0);
            Operand r = candidate.getExpression().getOperands(1);
            boolean bothValues = l.getNodeCase() == Operand.NodeCase.VALUE
                    && r.getNodeCase() == Operand.NodeCase.VALUE;
            if (bothValues && other.getNodeCase() == Operand.NodeCase.VARIABLE) {
                return true; // fold path
            }
            boolean oneFieldOneValue =
                    (l.getNodeCase() == Operand.NodeCase.VARIABLE
                            && r.getNodeCase() == Operand.NodeCase.VALUE)
                    || (l.getNodeCase() == Operand.NodeCase.VALUE
                            && r.getNodeCase() == Operand.NodeCase.VARIABLE);
            return ("eq".equals(op) || "ne".equals(op))
                    && oneFieldOneValue
                    && other.getNodeCase() == Operand.NodeCase.VALUE; // solve path
        }

        /**
         * A resolved arithmetic operand: either a pure-constant subtree folded in Java —
         * genuine IEEE double semantics, exactly matching CEL, including division by zero
         * yielding ±Infinity/NaN — or a SQL expression forced into double space.
         */
        private sealed interface NumericOperand {
            record Constant(double value) implements NumericOperand {}
            record Sql(jakarta.persistence.criteria.Expression<Double> expr)
                    implements NumericOperand {}
        }

        /**
         * Resolve a comparison operand to double space. DB decimal arithmetic is NOT IEEE
         * double arithmetic: H2 (and Postgres) type a bare {@code 0.1} literal as exact
         * NUMERIC and evaluate {@code intCol * 0.1} decimally, so {@code aNumber * 0.1 == 0.3}
         * matched rows the PDP (IEEE: {@code 0.30000000000000004}) denies. Verified against
         * H2 2.3: only {@code CAST(col AS DOUBLE) * CAST(0.1 AS DOUBLE)} diverges from
         * {@code 0.3}; {@code Expression.as(Double.class)} renders NO SQL cast (it is a type
         * marker only) and {@code cb.toDouble(literal)} elides the cast on a node already
         * Double-typed, both leaving the arithmetic decimal. Therefore:
         * <ul>
         *   <li>columns go through {@code cb.toDouble} (renders {@code cast(col as float(53))});</li>
         *   <li>constant subtrees fold in Java ({@link NumericOperand.Constant});</li>
         *   <li>constants mixed into SQL arithmetic bind through the plain-{@code Number}
         *       CriteriaBuilder overloads, which emit genuine double-typed bind parameters
         *       instead of decimal literals.</li>
         * </ul>
         *
         * <p>Division guard: SQL raises an error on a zero divisor — a data-dependent runtime
         * failure of the WHOLE query — while CEL double division is defined (±Infinity, or NaN
         * for 0/0). Portable Infinity semantics are not expressible in SQL, so a column
         * divisor is wrapped in {@code NULLIF(d, 0)}: zero-divisor rows become UNKNOWN →
         * EXCLUDED. Documented divergence, under-inclusive in the safe direction: CEL would
         * ALLOW rows where the comparison against ±Infinity holds (e.g. {@code x/0 > 1} with
         * {@code x > 0}); the adapter denies them, and the query survives. For 0/0 CEL yields
         * NaN, whose comparisons are all false (deny) — the exclusion matches exactly.
         * Constant divisors are decided statically: non-zero skips the guard, zero collapses
         * the division to a NULL literal (UNKNOWN for every row).
         */
        private NumericOperand resolveNumericOperand(Operand operand, Scope scope) {
            switch (operand.getNodeCase()) {
                case VARIABLE -> {
                    @SuppressWarnings("unchecked")
                    jakarta.persistence.criteria.Expression<? extends Number> path =
                            (jakarta.persistence.criteria.Expression<? extends Number>)
                                    scope.resolvePath(operand.getVariable());
                    return new NumericOperand.Sql(cb.toDouble(path));
                }
                case VALUE -> {
                    Object v = PlanValues.protoValueToJava(operand.getValue());
                    if (!(v instanceof Number n)) {
                        throw new IllegalArgumentException(
                                "Arithmetic comparison requires numeric operands, got "
                                        + typeName(v));
                    }
                    return new NumericOperand.Constant(n.doubleValue());
                }
                case EXPRESSION -> {
                    PlanResourcesFilter.Expression expr = operand.getExpression();
                    String op = expr.getOperator();
                    if ("mod".equals(op)) {
                        throw new IllegalArgumentException(
                                "mod is not supported in comparisons: CEL % is integer-only and "
                                        + "attribute values are always doubles at check time, so "
                                        + "the condition can never be satisfied by the PDP");
                    }
                    if (!ARITHMETIC_OPS.contains(op)) {
                        throw new IllegalArgumentException(
                                "Unexpected " + op + "() expression inside an arithmetic "
                                        + "comparison operand");
                    }
                    if (expr.getOperandsCount() != 2) {
                        throw new IllegalArgumentException(op + " requires exactly 2 operands");
                    }
                    NumericOperand l = resolveNumericOperand(expr.getOperands(0), scope);
                    NumericOperand r = resolveNumericOperand(expr.getOperands(1), scope);
                    if (l instanceof NumericOperand.Constant lc
                            && r instanceof NumericOperand.Constant rc) {
                        return new NumericOperand.Constant(switch (op) {
                            case "add" -> lc.value() + rc.value();
                            case "sub" -> lc.value() - rc.value();
                            case "mult" -> lc.value() * rc.value();
                            case "div" -> lc.value() / rc.value(); // IEEE: ±Infinity, 0/0 = NaN
                            default -> throw new IllegalArgumentException(
                                    "Unsupported arithmetic operator: " + op);
                        });
                    }
                    return new NumericOperand.Sql(arithmeticSql(op, l, r));
                }
                default -> throw new IllegalArgumentException(
                        "Unexpected operand type in arithmetic comparison: "
                                + operand.getNodeCase());
            }
        }

        /**
         * Emit one SQL arithmetic node; at least one side is a SQL expression. Constants go
         * through the plain-{@code Number} overloads (double bind parameters — see
         * {@link #resolveNumericOperand}).
         */
        private jakarta.persistence.criteria.Expression<Double> arithmeticSql(
                String op, NumericOperand l, NumericOperand r) {
            jakarta.persistence.criteria.Expression<Double> le =
                    l instanceof NumericOperand.Sql s ? s.expr() : null;
            jakarta.persistence.criteria.Expression<Double> re =
                    r instanceof NumericOperand.Sql s ? s.expr() : null;
            Double lc = l instanceof NumericOperand.Constant c ? c.value() : null;
            Double rc = r instanceof NumericOperand.Constant c ? c.value() : null;
            return switch (op) {
                case "add" -> le == null ? cb.sum(lc, re)
                        : re == null ? cb.sum(le, rc) : cb.sum(le, re);
                case "sub" -> le == null ? cb.diff(lc, re)
                        : re == null ? cb.diff(le, rc) : cb.diff(le, re);
                case "mult" -> le == null ? cb.prod(lc, re)
                        : re == null ? cb.prod(le, rc) : cb.prod(le, re);
                case "div" -> divisionSql(le, lc, re, rc);
                default -> throw new IllegalArgumentException(
                        "Unsupported arithmetic operator: " + op);
            };
        }

        /** Division with the NULLIF zero-divisor guard (see {@link #resolveNumericOperand}). */
        private jakarta.persistence.criteria.Expression<Double> divisionSql(
                jakarta.persistence.criteria.Expression<Double> le, Double lc,
                jakarta.persistence.criteria.Expression<Double> re, Double rc) {
            if (rc != null) {
                // Constant divisor, numerator is a SQL expression (both-constant subtrees
                // fold before reaching here). Zero → UNKNOWN for every row; non-zero → no
                // guard needed.
                if (rc == 0.0) {
                    return cb.nullLiteral(Double.class);
                }
                return cb.quot(le, rc).as(Double.class);
            }
            jakarta.persistence.criteria.Expression<Double> guarded = cb.nullif(re, 0.0);
            return (lc != null ? cb.quot(lc, guarded) : cb.quot(le, guarded)).as(Double.class);
        }

        // -- add (fold + solve for string concat / numeric translation) --

        /** Operands must already be normalized field-first (see {@link NormalizedBinary}). */
        private Predicate handleAddComparison(String op, PlanResourcesFilter.Expression addExpr,
                                              Operand otherOperand, Scope scope) {
            List<Operand> addOperands = addExpr.getOperandsList();
            if (addOperands.size() != 2) {
                throw new IllegalArgumentException("add requires exactly 2 operands");
            }
            Operand addLeft = addOperands.get(0);
            Operand addRight = addOperands.get(1);

            // Case 1: add(value, value) — fold the two constants, then compare to the field.
            // Normalization guarantees the field variable sits on the left of the comparison,
            // so the folded constant compares as `field op folded`.
            if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                    && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
                Object folded = PlanValues.foldAdd(
                        PlanValues.protoValueToJava(addLeft.getValue()),
                        PlanValues.protoValueToJava(addRight.getValue()));
                if (otherOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                    throw new IllegalArgumentException(
                            "add(const, const) compared to a non-field operand is not supported");
                }
                Path<?> path = scope.resolvePath(otherOperand.getVariable());
                return applyLeaf(op, path, folded);
            }

            // Case 2: add(field, value) or add(value, field) — solve for the field.
            // Only eq/ne are supported; lt/gt/etc. against a synthesized expression would require
            // emitting more complex predicates we don't try to support here.
            if (!"eq".equals(op) && !"ne".equals(op)) {
                throw new IllegalArgumentException(
                        "add comparison with a field reference only supports eq/ne (got " + op + ")");
            }
            if (otherOperand.getNodeCase() != Operand.NodeCase.VALUE) {
                throw new IllegalArgumentException(
                        "add(field, value) requires a value on the other side of the comparison");
            }
            Object otherValue = PlanValues.protoValueToJava(otherOperand.getValue());

            Operand fieldOp;
            Object addConst;
            boolean fieldIsLeft;
            if (addLeft.getNodeCase() == Operand.NodeCase.VARIABLE
                    && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
                fieldOp = addLeft;
                addConst = PlanValues.protoValueToJava(addRight.getValue());
                fieldIsLeft = true;
            } else if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                    && addRight.getNodeCase() == Operand.NodeCase.VARIABLE) {
                fieldOp = addRight;
                addConst = PlanValues.protoValueToJava(addLeft.getValue());
                fieldIsLeft = false;
            } else {
                throw new IllegalArgumentException(
                        "add requires exactly one field reference and one value, or two values");
            }

            Object solved = PlanValues.solveAdd(otherValue, addConst, fieldIsLeft);
            if (solved == null) {
                // No solution exists (e.g. "projects:123" == "users:" + R.id can never be true).
                // eq → always-false. ne is NOT always-true: a missing attribute makes the
                // concatenation a CEL evaluation error ("users:" + null) → deny, so NULL rows
                // must stay excluded — IS NOT NULL, not an unconditional 1=1 (which leaked
                // exactly the rows the PDP denies).
                if ("eq".equals(op)) {
                    return cb.disjunction();
                }
                return cb.isNotNull(scope.resolvePath(fieldOp.getVariable()));
            }
            Path<?> path = scope.resolvePath(fieldOp.getVariable());
            return applyLeaf(op, path, solved);
        }

        // -- isSet --

        private Predicate handleIsSet(List<Operand> operands, Scope scope) {
            if (operands.size() != 2) {
                throw new IllegalArgumentException("isSet requires exactly 2 operands");
            }
            String variable = null;
            Boolean flag = null;
            for (Operand o : operands) {
                if (o.getNodeCase() == Operand.NodeCase.VARIABLE) variable = o.getVariable();
                else if (o.getNodeCase() == Operand.NodeCase.VALUE) {
                    Object v = PlanValues.protoValueToJava(o.getValue());
                    if (!(v instanceof Boolean b)) {
                        throw new IllegalArgumentException("isSet second operand must be a boolean");
                    }
                    flag = b;
                }
            }
            if (variable == null || flag == null) {
                throw new IllegalArgumentException("Invalid isSet operands");
            }
            Path<?> path = scope.resolvePath(variable);
            boolean isSet = flag;
            return withOverride("isSet", path, isSet,
                    () -> isSet ? cb.isNotNull(path) : cb.isNull(path));
        }

        // -- in (set membership or collection membership) --

        /** Wrap a scalar plan constant as a single-element list; lists pass through unchanged. */
        private static List<?> asList(Object val) {
            return (val instanceof List<?> l) ? l : List.of(val);
        }

        private Predicate handleIn(List<Operand> rawOperands, Scope scope) {
            if (rawOperands.size() != 2) {
                throw new IllegalArgumentException("in requires exactly 2 operands");
            }
            // Both shapes — `field in [values]` and `value in collection-field` — resolve the
            // same way once normalized field-first: the mapping kind (Relation vs Field) decides
            // whether this is collection membership or a scalar IN, not the operand order.
            List<Operand> operands = NormalizedBinary.of("in", rawOperands).operands();
            Operand fieldOp = operands.get(0);
            Operand valueOp = operands.get(1);
            if (fieldOp.getNodeCase() != Operand.NodeCase.VARIABLE
                    || valueOp.getNodeCase() != Operand.NodeCase.VALUE) {
                throw new IllegalArgumentException("Unsupported in operand combination: "
                        + rawOperands.get(0).getNodeCase() + "/" + rawOperands.get(1).getNodeCase());
            }
            String var = fieldOp.getVariable();
            Object val = PlanValues.protoValueToJava(valueOp.getValue());

            Scope.ResolvedRelation relRef = scope.resolveRelation(var);
            if (relRef != null) {
                return collectionContainsAny(scope, relRef, asList(val));
            }

            Path<?> path = scope.resolvePath(var);
            return withOverride("in", path, val, () -> {
                if (val instanceof List<?> list) {
                    if (list.isEmpty()) {
                        return cb.disjunction();
                    }
                    return path.in(list);
                }
                return cb.equal(path, val);
            });
        }

        // -- hasIntersection --

        private Predicate handleHasIntersection(List<Operand> rawOperands, Scope scope) {
            if (rawOperands.size() != 2) {
                throw new IllegalArgumentException("hasIntersection requires exactly 2 operands");
            }
            // Intersection is symmetric, and the planner preserves policy source order —
            // `hasIntersection(P.attr.tags, R.attr.tags)` folds the principal side to a value
            // list in the FIRST position. Normalization puts the field/map side first.
            List<Operand> operands = NormalizedBinary.of("hasIntersection", rawOperands).operands();
            Operand first = operands.get(0);
            Operand second = operands.get(1);

            if (first.getNodeCase() == Operand.NodeCase.VARIABLE
                    && second.getNodeCase() == Operand.NodeCase.VALUE) {
                String var = first.getVariable();
                Object val = PlanValues.protoValueToJava(second.getValue());
                List<?> values = asList(val);

                Scope.ResolvedRelation relRef = scope.resolveRelation(var);
                if (relRef != null) {
                    return collectionContainsAny(scope, relRef, values);
                }
                Path<?> path = scope.resolvePath(var);
                // hasIntersection(field, []) is always false; avoid a dialect-dependent empty `IN ()`.
                if (values.isEmpty()) {
                    return cb.disjunction();
                }
                return path.in(values);
            }

            if (first.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "map".equals(first.getExpression().getOperator())) {
                if (second.getNodeCase() != Operand.NodeCase.VALUE) {
                    throw new IllegalArgumentException(
                            "hasIntersection second operand must be a value list when used with map()");
                }
                Object val = PlanValues.protoValueToJava(second.getValue());
                return handleMapIntersection(first.getExpression(), asList(val), scope);
            }

            throw new IllegalArgumentException(
                    "Unsupported hasIntersection operand shape: " + first.getNodeCase());
        }

        /** A parsed CEL lambda operand: its body and the name of its iteration variable. */
        private record ParsedLambda(Operand body, String varName) {}

        /**
         * Validate and unpack a {@code lambda(body, var)} operand — an EXPRESSION with operator
         * {@code lambda}, exactly two operands, the second a VARIABLE. Error messages are
         * caller-supplied so each operator keeps its exact wording.
         */
        private static ParsedLambda parseLambda(Operand lambdaOperand, String notLambdaMessage,
                                                String arityMessage, String varMessage) {
            if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
                throw new IllegalArgumentException(notLambdaMessage);
            }
            List<Operand> lambdaOps = lambdaOperand.getExpression().getOperandsList();
            if (lambdaOps.size() != 2) {
                throw new IllegalArgumentException(arityMessage);
            }
            Operand varOp = lambdaOps.get(1);
            if (varOp.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException(varMessage);
            }
            return new ParsedLambda(lambdaOps.get(0), varOp.getVariable());
        }

        /** Translate {@code hasIntersection(map(collection, lambda), values)}. */
        private Predicate handleMapIntersection(PlanResourcesFilter.Expression mapExpr,
                                                List<?> values, Scope scope) {
            // hasIntersection(map(...), []) is always false; short-circuit before the subquery.
            if (values.isEmpty()) {
                return cb.disjunction();
            }

            List<Operand> mapOperands = mapExpr.getOperandsList();
            if (mapOperands.size() != 2) {
                throw new IllegalArgumentException("map requires exactly 2 operands");
            }
            Operand collectionOperand = mapOperands.get(0);
            Operand lambdaOperand = mapOperands.get(1);

            if (collectionOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException("map first operand must be a variable");
            }
            String collectionVar = collectionOperand.getVariable();

            ParsedLambda lambda = parseLambda(lambdaOperand,
                    "map second operand must be a lambda",
                    "map lambda requires exactly 2 operands (body, variable)",
                    "map lambda body must be a simple variable projection");
            // map()'s extra shape constraint: the body must project a plain member variable.
            Operand projection = lambda.body();
            if (projection.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException("map lambda body must be a simple variable projection");
            }
            String memberField = Scope.extractLambdaSuffix(projection.getVariable(), lambda.varName());

            // Resolve the collection to its owner-anchored join chain. Single Relations and
            // dotted chains ("request.resource.attr.categories.subCategories") share one path:
            // the subquery correlates the OWNING From and joins through every hop, so the
            // projection ranges over the flattened tail elements.
            Scope.ResolvedRelation ref = scope.resolveRelation(collectionVar);
            if (ref == null) {
                scope.resolveMapping(collectionVar); // throws "Unknown attribute" when unmapped
                throw new IllegalArgumentException(
                        "map can only be applied to a collection mapped as Relation: " + collectionVar);
            }
            return mapIntersectionWithNullGuard(
                    existsSubquery(scope, ref, (sub, tailJoin, rebased) ->
                            Scope.memberPath(tailJoin, ref.tail(), memberField).in(values)),
                    () -> existsSubquery(scope, ref, (sub, tailJoin, rebased) ->
                            cb.isNull(Scope.memberPath(tailJoin, ref.tail(), memberField))));
        }

        /**
         * CEL {@code map()} has no error absorption: a NULL projected column is a missing element
         * attribute, so the whole {@code hasIntersection(map(...), values)} is an evaluation
         * error (deny) even when another element would intersect. Truth table:
         * <ul>
         *   <li>no NULL projection → {@code (in AND TRUE) OR (FALSE AND UNKNOWN)} = in-EXISTS</li>
         *   <li>≥1 NULL projection → {@code (in AND FALSE) OR (TRUE AND UNKNOWN)} = UNKNOWN (deny)</li>
         * </ul>
         * The null-witness EXISTS is supplied as a factory and built fresh per occurrence
         * (Hibernate 6 negation is stateful — see {@link #negate}); {@code IS NULL} itself is
         * two-valued, so both EXISTS legs are safe to compose.
         */
        private Predicate mapIntersectionWithNullGuard(Predicate inExists,
                                                       Supplier<Predicate> nullProjectionExists) {
            return cb.or(
                    cb.and(inExists, negate(nullProjectionExists.get())),
                    cb.and(nullProjectionExists.get(), unknownPredicate()));
        }

        private Predicate collectionContainsAny(Scope scope, Scope.ResolvedRelation ref, List<?> values) {
            // Intersection with an empty value set is always false — and an EXISTS wrapping an
            // empty `IN ()` is dialect-dependent — so short-circuit before building the subquery.
            if (values.isEmpty()) {
                return cb.disjunction();
            }
            return existsSubquery(scope, ref, (sub, tailJoin, rebased) -> {
                Path<?> field = Scope.memberPath(tailJoin, ref.tail(), null);
                if (values.size() == 1) {
                    return cb.equal(field, values.get(0));
                }
                return field.in(values);
            });
        }

        // -- size(collection) <op> N --

        /** Operands must already be normalized field-first (see {@link NormalizedBinary}). */
        private Predicate trySizeComparison(String op, List<Operand> operands, Scope scope) {
            // Detect the size() operand first: every ordinary leaf comparison probes through
            // here, and converting the VALUE operand up front would materialize lists/structs
            // only to discard them when no size() expression is present.
            PlanResourcesFilter.Expression sizeExpr = null;
            for (Operand o : operands) {
                if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                        && "size".equals(o.getExpression().getOperator())) {
                    sizeExpr = o.getExpression();
                }
            }
            if (sizeExpr == null) {
                return null;
            }
            Double numRaw = null;
            for (Operand o : operands) {
                if (o.getNodeCase() == Operand.NodeCase.VALUE
                        && o.getValue().getKindCase() == Value.KindCase.NUMBER_VALUE) {
                    numRaw = o.getValue().getNumberValue();
                }
            }
            if (numRaw == null) {
                return null;
            }

            // Fractional thresholds: COUNT/LENGTH are integral, so a fractional constant f can
            // never be hit exactly. Truncating (`>= 1.5` becoming `>= 1`) over-included rows
            // the PDP denies. Correct integer-count semantics:
            //   eq f      → always-false
            //   ne f      → always-true (Field-mapping NULL caveat handled below: a NULL
            //               string column is a missing attribute → CEL error → deny)
            //   ge f/gt f → ge ceil(f)   (the count being integral makes gt and ge coincide)
            //   le f/lt f → le floor(f)
            // Integral thresholds keep the operator untouched. The always-true/false collapses
            // flow through the same constant predicates the other static folds use
            // (cb.conjunction()/cb.disjunction()), so the size(filter(...)) unknown-element
            // machinery below still wraps them.
            String cmpOp = op;
            long numValue;
            Boolean fractionalCollapse = null; // TRUE → always-true, FALSE → always-false
            if (numRaw != Math.rint(numRaw)) {
                switch (op) {
                    case "eq" -> fractionalCollapse = Boolean.FALSE;
                    case "ne" -> fractionalCollapse = Boolean.TRUE;
                    case "gt", "ge" -> cmpOp = "ge";
                    case "lt", "le" -> cmpOp = "le";
                    default -> throw new IllegalArgumentException(
                            "Unsupported size comparison operator: " + op);
                }
                numValue = "ge".equals(cmpOp)
                        ? (long) Math.ceil(numRaw)
                        : (long) Math.floor(numRaw);
            } else {
                numValue = numRaw.longValue();
            }
            List<Operand> sizeOps = sizeExpr.getOperandsList();
            if (sizeOps.size() != 1) {
                throw new IllegalArgumentException("Unsupported size() expression");
            }
            Operand sizeArg = sizeOps.get(0);
            String var;
            Operand lambdaBody = null;
            String lambdaVarName = null;
            if (sizeArg.getNodeCase() == Operand.NodeCase.VARIABLE) {
                var = sizeArg.getVariable();
            } else if (sizeArg.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "filter".equals(sizeArg.getExpression().getOperator())) {
                // size(coll.filter(x, pred)) — count only the elements matching the lambda.
                List<Operand> filterOps = sizeArg.getExpression().getOperandsList();
                if (filterOps.size() != 2
                        || filterOps.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
                    throw new IllegalArgumentException("Unsupported size(filter(...)) expression");
                }
                var = filterOps.get(0).getVariable();
                ParsedLambda lambda = parseLambda(filterOps.get(1),
                        "Unsupported size(filter(...)) expression",
                        "lambda requires exactly 2 operands",
                        "lambda requires exactly 2 operands");
                lambdaBody = lambda.body();
                lambdaVarName = lambda.varName();
            } else {
                throw new IllegalArgumentException("Unsupported size() expression");
            }
            Scope.ResolvedRelation ref = scope.resolveRelation(var);
            if (ref == null) {
                AttributeMapping mapping = scope.resolveMapping(var);
                if (!(mapping instanceof AttributeMapping.Field)) {
                    throw new IllegalArgumentException(
                            "size() requires a collection (Relation) mapping for " + var);
                }
                // size(string) — CEL string length → LENGTH(column) <op> N.
                if (lambdaBody != null) {
                    throw new IllegalArgumentException(
                            "size(filter(...)) requires a collection (Relation) mapping for " + var);
                }
                Path<?> path = scope.resolvePath(var);
                if (fractionalCollapse != null) {
                    // ne f is vacuously true only for a PRESENT string: a NULL column is a
                    // missing attribute → CEL error → deny, so it must stay excluded —
                    // IS NOT NULL, never an unconditional 1=1. eq f excludes everything.
                    return fractionalCollapse ? cb.isNotNull(path) : cb.disjunction();
                }
                return compareCount(cb.length(path.as(String.class)), cmpOp, (int) numValue);
            }
            final Operand fBody = lambdaBody;
            final String fVar = lambdaVarName;
            SubqueryBodyBuilder bodyBuilder = (sub, tailJoin, rebased) ->
                    fBody == null ? cb.conjunction()
                            : traverse(fBody, Scope.lambda(tailJoin, sub, ref.tail(), fVar, rebased));

            Predicate base;
            if (fractionalCollapse != null) {
                // A Relation count is always defined (an empty join is count 0), so the
                // fractional eq/ne collapse is unconditional here. Falls through to the
                // size(filter(...)) unknown-element guard below so an erroring lambda body
                // still denies the row.
                base = fractionalCollapse ? cb.conjunction() : cb.disjunction();
            } else {
                boolean nonEmpty = ("gt".equals(cmpOp) && numValue == 0L)
                        || ("ge".equals(cmpOp) && numValue == 1L);
                boolean empty = ("eq".equals(cmpOp) && numValue == 0L)
                        || ("le".equals(cmpOp) && numValue == 0L)
                        || ("lt".equals(cmpOp) && numValue == 1L);
                if (nonEmpty) {
                    base = existsSubquery(scope, ref, bodyBuilder);
                } else if (empty) {
                    base = negate(existsSubquery(scope, ref, bodyBuilder));
                } else {
                    // Arbitrary N → correlated (SELECT COUNT(...)) <op> N, same shape as
                    // exists_one. For a multi-hop chain the COUNT joins through every hop, so it
                    // counts the FLATTENED tail elements — the same element set the EXISTS
                    // shortcuts range over.
                    ChainSubquery<Long> cs = countSubquery(scope, ref);
                    if (fBody != null) {
                        cs.sub().where(bodyBuilder.build(cs.sub(), cs.tailJoin(), cs.rebasedOuter()));
                    }
                    base = compareCount(cs.sub(), cmpOp, numValue);
                }
            }
            if (fBody == null) {
                // size(collection) counts rows without evaluating a lambda — no element can be
                // UNKNOWN, so the plain comparison is already exact.
                return base;
            }
            // size(coll.filter(x, pred)): CEL filter has NO error absorption — any element whose
            // predicate errors (NULL-derived UNKNOWN body) errors the whole expression (deny),
            // even when the count comparison would otherwise hold. Same combinator as exists_one:
            //   ≥1 UNKNOWN element → (base AND FALSE) OR (TRUE AND UNKNOWN) = UNKNOWN (deny)
            //   no UNKNOWN element → (base AND TRUE) OR (FALSE AND …)       = base
            return cb.or(
                    cb.and(base, negate(unknownElementExists(scope, ref, bodyBuilder))),
                    cb.and(unknownElementExists(scope, ref, bodyBuilder), unknownPredicate()));
        }

        /** Compare a numeric size expression (COUNT subquery or LENGTH) against a constant. */
        private <N extends Number & Comparable<N>> Predicate compareCount(
                jakarta.persistence.criteria.Expression<N> count, String op, N n) {
            return switch (op) {
                case "eq" -> cb.equal(count, n);
                case "ne" -> cb.notEqual(count, n);
                case "lt" -> cb.lessThan(count, n);
                case "gt" -> cb.greaterThan(count, n);
                case "le" -> cb.lessThanOrEqualTo(count, n);
                case "ge" -> cb.greaterThanOrEqualTo(count, n);
                default -> throw new IllegalArgumentException(
                        "Unsupported size comparison operator: " + op);
            };
        }

        // -- exists / exists_one / all / except / filter --

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
         * {@code not(...)} flips to TRUE, an authorization leak. Building blocks:
         * {@code EXISTS(elem WHERE body)} (true witness), {@code EXISTS(elem WHERE NOT body)}
         * (false witness), {@link #unknownElementExists} (any UNKNOWN-body element) and
         * {@link #unknownPredicate} (a constant SQL UNKNOWN to compose with).
         *
         * <p>{@code filter}/{@code except} in boolean position are kept consistent with the
         * {@code exists} family. Cost note: the unknown machinery is always emitted — each
         * {@link #unknownElementExists} probe is two correlated COUNT subqueries, and
         * {@code exists_one} (like the arbitrary-N {@code size(filter(...))} shape) emits the
         * probe twice, i.e. five correlated subqueries including the base COUNT — the attribute
         * mapping carries no column-nullability metadata, so a NULL-free lambda body cannot be
         * detected statically.
         */
        private Predicate handleCollectionOperator(String op, List<Operand> operands, Scope scope) {
            if (operands.size() != 2) {
                throw new IllegalArgumentException(op + " requires exactly 2 operands");
            }
            Operand listOperand = operands.get(0);
            Operand lambdaOperand = operands.get(1);

            if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException(op + " first operand must be a variable");
            }
            if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
                throw new IllegalArgumentException(op + " second operand must be a lambda");
            }

            String collectionVar = listOperand.getVariable();
            // Owner-anchored chain resolution: multi-hop chains join through every hop, and a
            // relation referenced from inside a lambda anchors to the scope that owns it.
            Scope.ResolvedRelation ref = scope.resolveRelation(collectionVar);
            if (ref == null) {
                scope.resolveMapping(collectionVar); // throws "Unknown attribute" when unmapped
                throw new IllegalArgumentException(
                        op + " requires a Relation mapping for " + collectionVar);
            }

            ParsedLambda lambda = parseLambda(lambdaOperand,
                    op + " second operand must be a lambda",
                    "lambda requires exactly 2 operands",
                    "lambda variable must be a variable operand");
            Operand body = lambda.body();
            String lambdaVarName = lambda.varName();

            // Every invocation re-traverses the body, so each occurrence gets a fresh Predicate
            // tree (Hibernate 6 negation is stateful — see negate()).
            SubqueryBodyBuilder bodyBuilder = (sub, tailJoin, rebased) -> traverse(body,
                    Scope.lambda(tailJoin, sub, ref.tail(), lambdaVarName, rebased));
            SubqueryBodyBuilder negatedBodyBuilder = (sub, tailJoin, rebased) ->
                    negate(bodyBuilder.build(sub, tailJoin, rebased));

            return switch (op) {
                // exists (and filter): true-witness OR (unknown-element AND UNKNOWN).
                //   any TRUE element            → TRUE OR …                    = TRUE (absorbed)
                //   no TRUE, ≥1 UNKNOWN element → FALSE OR (TRUE AND UNKNOWN)  = UNKNOWN (deny)
                //   no TRUE, no UNKNOWN         → FALSE OR (FALSE AND UNKNOWN) = FALSE
                case "exists", "filter" -> cb.or(
                        existsSubquery(scope, ref, bodyBuilder),
                        cb.and(unknownElementExists(scope, ref, bodyBuilder), unknownPredicate()));
                // except is "some element fails the body" — the exists table with the false
                // witness in the true-witness seat; an UNKNOWN body is UNKNOWN under NOT too.
                case "except" -> cb.or(
                        existsSubquery(scope, ref, negatedBodyBuilder),
                        cb.and(unknownElementExists(scope, ref, bodyBuilder), unknownPredicate()));
                // all: NOT false-witness AND (NOT unknown-element OR UNKNOWN).
                //   any FALSE element            → FALSE AND …                   = FALSE (absorbed)
                //   no FALSE, ≥1 UNKNOWN element → TRUE AND (FALSE OR UNKNOWN)   = UNKNOWN (deny)
                //   no FALSE, no UNKNOWN         → TRUE AND (TRUE OR UNKNOWN)    = TRUE
                case "all" -> cb.and(
                        negate(existsSubquery(scope, ref, negatedBodyBuilder)),
                        cb.or(negate(unknownElementExists(scope, ref, bodyBuilder)), unknownPredicate()));
                // exists_one: (COUNT(body) = 1 AND NOT unknown-element) OR (unknown-element AND UNKNOWN).
                //   ≥1 UNKNOWN element → (… AND FALSE) OR (TRUE AND UNKNOWN) = UNKNOWN (deny)
                //   no UNKNOWN element → (COUNT=1 AND TRUE) OR (FALSE AND …) = COUNT=1
                case "exists_one" -> {
                    ChainSubquery<Long> cs = countSubquery(scope, ref);
                    cs.sub().where(bodyBuilder.build(cs.sub(), cs.tailJoin(), cs.rebasedOuter()));
                    yield cb.or(
                            cb.and(cb.equal(cs.sub(), 1L),
                                    negate(unknownElementExists(scope, ref, bodyBuilder))),
                            cb.and(unknownElementExists(scope, ref, bodyBuilder), unknownPredicate()));
                }
                default -> throw new IllegalArgumentException("Unsupported collection operator: " + op);
            };
        }

        /**
         * TRUE iff the relation holds at least one element whose lambda body evaluates to SQL
         * UNKNOWN (NULL-derived). Not expressible as a single EXISTS: inside a subquery WHERE an
         * UNKNOWN body simply fails to match, so {@code EXISTS(body)} and {@code EXISTS(NOT body)}
         * both skip exactly the rows to be detected. Counting closes the gap — an element is
         * <em>determined</em> iff {@code body OR NOT body} matches it, therefore
         * {@code COUNT(elem) > COUNT(elem WHERE body OR NOT body)} holds iff at least one element
         * is UNKNOWN, including mixed collections where sibling elements are determined
         * true/false. Both COUNTs never yield NULL, so the comparison itself is two-valued and
         * safe to {@link #negate}. The body is translated fresh per occurrence (stateful
         * negation — see {@link #negate}).
         */
        private Predicate unknownElementExists(Scope scope, Scope.ResolvedRelation ref,
                                               SubqueryBodyBuilder bodyBuilder) {
            ChainSubquery<Long> total = countSubquery(scope, ref);

            ChainSubquery<Long> determined = countSubquery(scope, ref);
            determined.sub().where(cb.or(
                    bodyBuilder.build(determined.sub(), determined.tailJoin(), determined.rebasedOuter()),
                    negate(bodyBuilder.build(determined.sub(), determined.tailJoin(), determined.rebasedOuter()))));

            return cb.greaterThan(total.sub(), determined.sub());
        }

        /**
         * A constant SQL UNKNOWN: {@code 1 = NULL} (validated by the unknownBooleanConstantProbe
         * unit test against Hibernate 6). Composes by three-valued logic exactly like CEL error
         * absorption: {@code x AND UNKNOWN} is FALSE when x is FALSE (a false witness absorbs the
         * error) and UNKNOWN when x is TRUE; {@code x OR UNKNOWN} is TRUE when x is TRUE and
         * UNKNOWN when x is FALSE. Its negation is UNKNOWN as well, so predicates carrying it
         * stay excluded under both polarities.
         */
        private Predicate unknownPredicate() {
            return cb.equal(cb.literal(1), cb.nullLiteral(Integer.class));
        }

        @FunctionalInterface
        private interface SubqueryBodyBuilder {
            /**
             * @param sub          the subquery being built
             * @param tailJoin     the join over the chain's TAIL Relation inside the subquery —
             *                     for a single Relation, the join over its collection; for a
             *                     multi-hop chain, the innermost join of the join chain
             * @param rebasedOuter the enclosing scope re-rooted for use inside {@code sub}
             *                     (see {@link Scope#rebaseAt}) — lambda bodies resolve
             *                     non-lambda variables (e.g. {@code request.resource.attr.x})
             *                     through this so outer references stay legal correlation paths
             */
            Predicate build(Subquery<?> sub, Join<?, ?> tailJoin, Scope rebasedOuter);
        }

        /** Correlate {@code outerFrom} (the relation owner's {@code From}) into {@code sub}. */
        @SuppressWarnings("unchecked")
        private static From<?, ?> correlate(Subquery<?> sub, From<?, ?> outerFrom) {
            if (outerFrom instanceof Root<?> r) {
                return sub.correlate(r);
            }
            if (outerFrom instanceof Join<?, ?> j) {
                return sub.correlate((Join<Object, Object>) j);
            }
            throw new IllegalArgumentException("Cannot correlate from non-Root, non-Join scope: " + outerFrom);
        }

        /**
         * A correlated subquery spanning a resolved relation chain: {@code sub} correlates the
         * chain OWNER's {@code From} and joins through every hop to {@code tailJoin}, with
         * {@code rebasedOuter} being the evaluation scope re-rooted inside {@code sub}.
         */
        private record ChainSubquery<T>(Subquery<T> sub, Join<?, ?> tailJoin, Scope rebasedOuter) {}

        /**
         * Build the shared skeleton of every relation subquery. Two invariants fix the two
         * join-anchoring failure modes:
         * <ul>
         *   <li>the correlation anchor is {@code ref.owner().from()} — the {@code From} that
         *       OWNS the first relation attribute — never the evaluation scope's own
         *       {@code from()}, which inside a lambda is the lambda element join and does not
         *       hold outer relations like {@code request.resource.attr.tags};</li>
         *   <li>a multi-hop chain ({@code categories.subCategories}) joins THROUGH every hop
         *       off that anchor, so the subquery ranges over the flattened tail elements —
         *       joining only the tail attribute off the anchor would either fail at query-build
         *       time or silently query a same-named collection on the wrong entity.</li>
         * </ul>
         * EXISTS over the join chain and COUNT over {@code tailJoin} therefore express
         * exists/in/hasIntersection membership and {@code size()} of the flattened union with
         * the same element set, so the tri-state unknown-element machinery composes with chains
         * unchanged (its COUNT subqueries traverse the identical chain).
         */
        private <T> ChainSubquery<T> chainSubquery(Class<T> resultType, Scope scope,
                                                   Scope.ResolvedRelation ref) {
            Subquery<T> sub = scope.parentQuery().subquery(resultType);
            From<?, ?> correlated = correlate(sub, ref.owner().from());
            Join<?, ?> join = correlated.join(ref.chain().get(0).joinAttribute());
            for (int i = 1; i < ref.chain().size(); i++) {
                join = join.join(ref.chain().get(i).joinAttribute());
            }
            Scope rebased = Scope.rebaseAt(scope, ref.owner(), correlated, sub);
            return new ChainSubquery<>(sub, join, rebased);
        }

        /** A chain subquery seeded to {@code SELECT COUNT(tailJoin)} — the shared seed of every counting shape. */
        private ChainSubquery<Long> countSubquery(Scope scope, Scope.ResolvedRelation ref) {
            ChainSubquery<Long> cs = chainSubquery(Long.class, scope, ref);
            cs.sub().select(cb.count(cs.tailJoin()));
            return cs;
        }

        private Predicate existsSubquery(Scope scope, Scope.ResolvedRelation ref,
                                         SubqueryBodyBuilder bodyBuilder) {
            ChainSubquery<Integer> cs = chainSubquery(Integer.class, scope, ref);
            cs.sub().select(cb.literal(1));
            cs.sub().where(bodyBuilder.build(cs.sub(), cs.tailJoin(), cs.rebasedOuter()));
            return cb.exists(cs.sub());
        }
    }
}
