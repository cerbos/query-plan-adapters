package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Translates the Cerbos hierarchy operators ({@code overlaps} / {@code ancestorOf} /
 * {@code descendentOf}).
 *
 * <p>A Cerbos hierarchy is a delimited path (e.g. {@code "a:b:c"}). Both sides of a hierarchy
 * operator are wrapped in a {@code hierarchy(...)} expression that resolves to one of:
 * <ul>
 *   <li>a constant string split into segments,</li>
 *   <li>a single field whose column holds the whole delimited string, or</li>
 *   <li>a {@code list(...)} of segments, each a constant or a field.</li>
 * </ul>
 * The translations mirror the Prisma adapter so behaviour is consistent across adapters.
 */
final class HierarchyTranslator {

    private final CriteriaBuilder cb;

    HierarchyTranslator(CriteriaBuilder cb) {
        this.cb = cb;
    }

    /** A resolved {@code hierarchy(...)} operand: a constant path, a whole-column field, or a list of segments. */
    private sealed interface Hierarchy permits Hierarchy.Constant, Hierarchy.FieldRef, Hierarchy.Segmented {
        /** A literal delimited path split into segments. */
        record Constant(List<String> segments, String delimiter) implements Hierarchy {}

        /** A single column holding the whole delimited string. */
        record FieldRef(Path<?> path, String delimiter) implements Hierarchy {}

        /** A {@code list(...)} of segments, each a constant or a field. */
        record Segmented(List<Seg> segments) implements Hierarchy {}
    }

    /** One segment of a {@link Hierarchy.Segmented}: either a literal value or a field reference. */
    private sealed interface Seg permits Seg.Const, Seg.FieldSeg {
        record Const(String value) implements Seg {}

        record FieldSeg(Path<?> path) implements Seg {}
    }

    Predicate handleOverlaps(List<Operand> operands, Scope scope) {
        Hierarchy[] both = extractHierarchyOperands("overlaps", operands, scope);
        Hierarchy left = both[0];
        Hierarchy right = both[1];

        if (left instanceof Hierarchy.FieldRef || right instanceof Hierarchy.FieldRef) {
            return handleFieldOverlaps(left, right);
        }

        List<Seg> leftSegs = toSegments(left);
        List<Seg> rightSegs = toSegments(right);

        List<Predicate> leftPrefixOfRight = checkPrefixConditions(leftSegs, rightSegs);
        List<Predicate> rightPrefixOfLeft = checkPrefixConditions(rightSegs, leftSegs);

        List<List<Predicate>> valid = new ArrayList<>();
        if (leftPrefixOfRight != null) valid.add(leftPrefixOfRight);
        if (rightPrefixOfLeft != null) valid.add(rightPrefixOfLeft);

        if (valid.isEmpty()) {
            // Neither side can be a prefix of the other. If a field is involved the overlap is
            // simply never satisfiable (always-false); two incompatible constants are a planner bug.
            boolean hasField = containsFieldSegment(leftSegs) || containsFieldSegment(rightSegs);
            if (hasField) {
                return cb.disjunction();
            }
            throw new IllegalArgumentException("Cannot determine hierarchy overlap: no field references found");
        }
        // An empty condition list means every compared segment was a matching constant — overlap
        // holds unconditionally.
        for (List<Predicate> c : valid) {
            if (c.isEmpty()) {
                return cb.conjunction();
            }
        }
        // Both directions (equal-length hierarchies) compare the same segment pairs, so either
        // condition set is equivalent; use the first.
        List<Predicate> chosen = valid.get(0);
        return chosen.size() == 1 ? chosen.get(0) : cb.and(chosen.toArray(Predicate[]::new));
    }

    private Predicate handleFieldOverlaps(Hierarchy left, Hierarchy right) {
        if (left instanceof Hierarchy.FieldRef && right instanceof Hierarchy.FieldRef) {
            throw new IllegalArgumentException("overlaps: cannot compare two field-reference hierarchies");
        }
        Hierarchy.FieldRef field = (left instanceof Hierarchy.FieldRef f) ? f : (Hierarchy.FieldRef) right;
        Hierarchy other = (left instanceof Hierarchy.FieldRef) ? right : left;
        if (!(other instanceof Hierarchy.Constant constant)) {
            throw new IllegalArgumentException(
                    "overlaps: segmented hierarchies with field hierarchies are not supported");
        }

        String delimiter = field.delimiter();
        String otherRaw = String.join(delimiter, constant.segments());
        List<String> strictPrefixes = getStrictPrefixes(constant.segments(), delimiter);

        List<Predicate> conditions = new ArrayList<>();
        // field is an ancestor of the constant...
        if (!strictPrefixes.isEmpty()) {
            conditions.add(field.path().in(strictPrefixes));
        }
        // ...or equal to it...
        conditions.add(cb.equal(field.path(), otherRaw));
        // ...or a descendant of it.
        conditions.add(startsWithLiteral(field.path(), otherRaw + delimiter));

        return cb.or(conditions.toArray(Predicate[]::new));
    }

    Predicate handleAncestorDescendant(List<Operand> operands, Scope scope, boolean isAncestor) {
        String opName = isAncestor ? "ancestorOf" : "descendentOf";
        Hierarchy[] both = extractHierarchyOperands(opName, operands, scope);
        // ancestorOf(A, B) ⇔ A is a strict prefix of B; descendentOf(A, B) ⇔ B is a strict prefix of A.
        Hierarchy ancestor = isAncestor ? both[0] : both[1];
        Hierarchy descendant = isAncestor ? both[1] : both[0];

        if (ancestor instanceof Hierarchy.Constant a && descendant instanceof Hierarchy.FieldRef d) {
            String prefix = String.join(d.delimiter(), a.segments()) + d.delimiter();
            return startsWithLiteral(d.path(), prefix);
        }
        if (ancestor instanceof Hierarchy.FieldRef a && descendant instanceof Hierarchy.Constant d) {
            List<String> prefixes = getStrictPrefixes(d.segments(), a.delimiter());
            if (prefixes.isEmpty()) {
                return cb.disjunction();
            }
            if (prefixes.size() == 1) {
                return cb.equal(a.path(), prefixes.get(0));
            }
            return a.path().in(prefixes);
        }
        if (ancestor instanceof Hierarchy.Constant a && descendant instanceof Hierarchy.Constant d) {
            if (d.segments().size() > a.segments().size()
                    && isPrefix(a.segments(), d.segments())) {
                return cb.conjunction();
            }
            throw new IllegalArgumentException(
                    opName + ": constant operands do not satisfy the " + (isAncestor ? "ancestor" : "descendant")
                            + " relationship");
        }
        throw new IllegalArgumentException(opName + ": unsupported hierarchy operand combination");
    }

    private Hierarchy[] extractHierarchyOperands(String opName, List<Operand> operands, Scope scope) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException(opName + " requires exactly 2 operands");
        }
        return new Hierarchy[]{
                normalizeHierarchy(resolveHierarchy(opName, operands.get(0), scope)),
                normalizeHierarchy(resolveHierarchy(opName, operands.get(1), scope)),
        };
    }

    private Hierarchy resolveHierarchy(String opName, Operand operand, Scope scope) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"hierarchy".equals(operand.getExpression().getOperator())) {
            throw new IllegalArgumentException(opName + " requires hierarchy(...) operands");
        }
        List<Operand> ops = operand.getExpression().getOperandsList();
        if (ops.size() == 2) {
            Operand strOp = ops.get(0);
            Operand delimOp = ops.get(1);
            if (delimOp.getNodeCase() != Operand.NodeCase.VALUE) {
                throw new IllegalArgumentException("hierarchy delimiter must be a value");
            }
            String delimiter = String.valueOf(PlanValues.protoValueToJava(delimOp.getValue()));
            if (strOp.getNodeCase() == Operand.NodeCase.VALUE) {
                String raw = String.valueOf(PlanValues.protoValueToJava(strOp.getValue()));
                return new Hierarchy.Constant(splitLiteral(raw, delimiter), delimiter);
            }
            if (strOp.getNodeCase() == Operand.NodeCase.VARIABLE) {
                return new Hierarchy.FieldRef(scope.resolvePath(strOp.getVariable()), delimiter);
            }
            throw new IllegalArgumentException("hierarchy(string, delimiter) requires a value or field operand");
        }
        if (ops.size() == 1) {
            Operand inner = ops.get(0);
            return switch (inner.getNodeCase()) {
                case VALUE -> new Hierarchy.Constant(
                        splitLiteral(String.valueOf(PlanValues.protoValueToJava(inner.getValue())), "."), ".");
                case VARIABLE -> new Hierarchy.FieldRef(scope.resolvePath(inner.getVariable()), ".");
                case EXPRESSION -> {
                    if (!"list".equals(inner.getExpression().getOperator())) {
                        throw new IllegalArgumentException("hierarchy requires a value, field, or list operand");
                    }
                    List<Seg> segs = new ArrayList<>();
                    for (Operand seg : inner.getExpression().getOperandsList()) {
                        switch (seg.getNodeCase()) {
                            case VALUE -> segs.add(new Seg.Const(
                                    String.valueOf(PlanValues.protoValueToJava(seg.getValue()))));
                            case VARIABLE -> segs.add(new Seg.FieldSeg(scope.resolvePath(seg.getVariable())));
                            default -> throw new IllegalArgumentException(
                                    "hierarchy list segment must be a value or field, got " + seg.getNodeCase());
                        }
                    }
                    yield new Hierarchy.Segmented(segs);
                }
                default -> throw new IllegalArgumentException(
                        "hierarchy requires a value, field, or list operand, got " + inner.getNodeCase());
            };
        }
        throw new IllegalArgumentException("hierarchy requires 1 or 2 operands");
    }

    /** Collapse an all-constant segmented hierarchy to a plain Constant (default delimiter). */
    private Hierarchy normalizeHierarchy(Hierarchy h) {
        if (!(h instanceof Hierarchy.Segmented seg)) {
            return h;
        }
        List<String> values = new ArrayList<>();
        for (Seg s : seg.segments()) {
            if (s instanceof Seg.Const c) {
                values.add(c.value());
            } else {
                return h;
            }
        }
        return new Hierarchy.Constant(values, ".");
    }

    private List<Seg> toSegments(Hierarchy h) {
        if (h instanceof Hierarchy.Constant c) {
            return c.segments().stream().map(s -> (Seg) new Seg.Const(s)).toList();
        }
        if (h instanceof Hierarchy.Segmented s) {
            return s.segments();
        }
        throw new IllegalArgumentException("Cannot enumerate segments of a field-reference hierarchy");
    }

    /**
     * If {@code shorter} is a prefix of {@code longer}, return the predicates that must hold for
     * the field segments to line up (an empty list = unconditionally true). Returns {@code null}
     * if {@code shorter} cannot be a prefix of {@code longer}.
     */
    private List<Predicate> checkPrefixConditions(List<Seg> shorter, List<Seg> longer) {
        if (shorter.size() > longer.size()) {
            return null;
        }
        List<Predicate> conditions = new ArrayList<>();
        for (int i = 0; i < shorter.size(); i++) {
            Seg s = shorter.get(i);
            Seg l = longer.get(i);
            if (s instanceof Seg.Const sc && l instanceof Seg.Const lc) {
                if (!sc.value().equals(lc.value())) {
                    return null;
                }
            } else if (s instanceof Seg.FieldSeg sf && l instanceof Seg.Const lc) {
                conditions.add(cb.equal(sf.path(), lc.value()));
            } else if (s instanceof Seg.Const sc && l instanceof Seg.FieldSeg lf) {
                conditions.add(cb.equal(lf.path(), sc.value()));
            } else {
                throw new IllegalArgumentException(
                        "Cannot compare two field references in a hierarchy overlap");
            }
        }
        return conditions;
    }

    private Predicate startsWithLiteral(Path<?> path, String prefix) {
        return cb.like(path.as(String.class), PlanValues.escapeLike(prefix) + "%", '\\');
    }

    private static boolean containsFieldSegment(List<Seg> segs) {
        return segs.stream().anyMatch(s -> s instanceof Seg.FieldSeg);
    }

    private static boolean isPrefix(List<String> shorter, List<String> longer) {
        for (int i = 0; i < shorter.size(); i++) {
            if (!shorter.get(i).equals(longer.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** All proper (strict) ancestor prefixes of a segment list, joined with {@code delimiter}. */
    private static List<String> getStrictPrefixes(List<String> segments, String delimiter) {
        if (segments.size() <= 1) {
            return List.of();
        }
        List<String> prefixes = new ArrayList<>();
        String current = segments.get(0);
        prefixes.add(current);
        for (int i = 1; i < segments.size() - 1; i++) {
            current = current + delimiter + segments.get(i);
            prefixes.add(current);
        }
        return prefixes;
    }

    /**
     * Split on a literal delimiter (not a regex), keeping trailing empty segments — the
     * {@code split(Pattern.quote(delimiter), -1)} semantics without compiling a Pattern per
     * call ({@code \Q..\E} defeats String.split's single-char fast path).
     */
    private static List<String> splitLiteral(String raw, String delimiter) {
        if (delimiter.isEmpty()) {
            // Zero-width delimiter: defer to the regex engine's empty-match semantics.
            return List.of(raw.split(Pattern.quote(delimiter), -1));
        }
        List<String> parts = new ArrayList<>();
        int start = 0;
        int idx;
        while ((idx = raw.indexOf(delimiter, start)) >= 0) {
            parts.add(raw.substring(start, idx));
            start = idx + delimiter.length();
        }
        parts.add(raw.substring(start));
        return List.copyOf(parts);
    }
}
