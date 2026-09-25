/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Translates {@code R.attr.s.matches("re")} without a regex predicate, which JPA Criteria lacks
 * and whose SQL dialects are not RE2. A pattern is translated only when its language can be
 * spelled exactly with {@code LIKE}, {@code =} and {@code REPLACE}:
 *
 * <ul>
 *   <li>each top-level alternative is a finite set of strings, optionally anchored with
 *       {@code ^} and {@code $}: {@code = s}, {@code LIKE 's%'}, {@code LIKE '%s'} or
 *       {@code LIKE '%s%'} for each string;</li>
 *   <li>in an alternative anchored at both ends, {@code .*} and {@code .+} between finite parts
 *       become {@code %} and {@code _%}, with {@code NOT LIKE '%\n%'} because RE2's {@code .}
 *       excludes a newline (the finite parts must hold none);</li>
 *   <li>{@code ^[set]*$} and {@code ^[set]+$} hold when deleting every member of the set with
 *       {@code REPLACE} leaves the empty string.</li>
 * </ul>
 *
 * <p>The finite parts are literals, escapes, character classes (ranges, {@code \d \w \s},
 * POSIX classes), groups, alternation and bounded repetition; a leading {@code (?i)} expands
 * each ASCII letter to its Unicode simple case-fold orbit ({@code k} also matches U+212A KELVIN
 * SIGN, {@code s} also U+017F LATIN SMALL LETTER LONG S). A pattern RE2 rejects (a lookaround, a
 * backreference) is a CEL error, so it is UNKNOWN. Anything else is refused: negated classes, a
 * lone {@code .} ({@code _} counts UTF-16 units on H2, where RE2 counts code points), other flags,
 * and expansions past {@value #MAX_STRINGS} strings.
 *
 * <p>The column must be a String; any other type is a CEL no-overload error, so UNKNOWN. A NULL
 * column is UNKNOWN too. Like every string predicate here, the result assumes a byte-exact
 * collation.
 */
final class RegexTranslator {

    /** The most strings one alternative may expand to. */
    static final int MAX_STRINGS = 256;

    private static final int MAX_REPEAT = 16;

    private final CriteriaBuilder cb;
    private final TriPredicate tri;

    RegexTranslator(CriteriaBuilder cb, TriPredicate tri) {
        this.cb = cb;
        this.tri = tri;
    }

    /** {@code path.matches(pattern)}. */
    Predicate matches(Path<?> path, String pattern) {
        if (!String.class.equals(path.getJavaType())) {
            return tri.unknown();
        }
        Node regex;
        try {
            regex = new Parser(pattern).parse();
        } catch (InvalidRe2 invalid) {
            // RE2 rejects the pattern, so CEL's matches() errors and the PDP denies.
            return tri.unknown();
        }
        @SuppressWarnings("unchecked")
        Expression<String> column = (Expression<String>) path;
        List<Node> alternatives = regex instanceof Node.Alt alt ? alt.options() : List.of(regex);
        List<Predicate> any = new ArrayList<>();
        for (Node alternative : alternatives) {
            Predicate p = alternative(column, alternative, pattern);
            if (p == null) {
                // An alternative that matches every string.
                return tri.baseUnlessUnknown(cb.conjunction(), () -> cb.isNull(column));
            }
            any.add(p);
        }
        return any.size() == 1 ? any.get(0) : cb.or(any.toArray(Predicate[]::new));
    }

    /** One top-level alternative, or {@code null} when it matches every string. */
    private Predicate alternative(Expression<String> column, Node alternative, String pattern) {
        List<Node> parts = new ArrayList<>(alternative instanceof Node.Cat cat
                ? cat.parts() : List.of(alternative));
        boolean start = false;
        boolean end = false;
        while (!parts.isEmpty() && parts.get(0) instanceof Node.Begin) {
            parts.remove(0);
            start = true;
        }
        while (!parts.isEmpty() && parts.get(parts.size() - 1) instanceof Node.End) {
            parts.remove(parts.size() - 1);
            end = true;
        }
        for (Node part : parts) {
            if (containsAnchor(part)) {
                throw unsupported(pattern, "an anchor inside the pattern");
            }
        }

        // ^[set]*$ and ^[set]+$: every character is a member.
        if (start && end && parts.size() == 1 && parts.get(0) instanceof Node.Rep rep
                && rep.max() < 0 && rep.node() instanceof Node.Cls cls && rep.min() <= 1) {
            Expression<String> rest = column;
            for (int member : cls.members()) {
                rest = cb.function("replace", String.class, rest,
                        cb.literal(Character.toString(member)), cb.literal(""));
            }
            Predicate onlyMembers = cb.equal(rest, "");
            return rep.min() == 0 ? onlyMembers : cb.and(onlyMembers, cb.notEqual(column, ""));
        }

        // Finite segments separated by .* / .+ wildcards.
        List<Set<String>> segments = new ArrayList<>();
        List<String> wildcards = new ArrayList<>();
        List<Node> pending = new ArrayList<>();
        for (Node part : parts) {
            if (part instanceof Node.Rep rep && rep.node() instanceof Node.Dot && rep.max() < 0
                    && rep.min() <= 1) {
                segments.add(expand(new Node.Cat(pending), pattern));
                pending = new ArrayList<>();
                wildcards.add(rep.min() == 0 ? "%" : "_%");
            } else {
                pending.add(part);
            }
        }
        segments.add(expand(new Node.Cat(pending), pattern));

        if (wildcards.isEmpty()) {
            Set<String> strings = segments.get(0);
            if (strings.contains("") && !(start && end)) {
                return null;
            }
            if (start && end) {
                return strings.size() == 1 ? cb.equal(column, strings.iterator().next())
                        : column.in(strings);
            }
            List<Predicate> likes = new ArrayList<>();
            for (String s : strings) {
                likes.add(cb.like(column, (start ? "" : "%") + PlanValues.escapeLike(s)
                        + (end ? "" : "%"), '\\'));
            }
            return likes.size() == 1 ? likes.get(0) : cb.or(likes.toArray(Predicate[]::new));
        }

        if (!start || !end) {
            throw unsupported(pattern, "an unanchored .* or .+");
        }
        List<String> likePatterns = List.of("");
        for (int i = 0; i < segments.size(); i++) {
            List<String> next = new ArrayList<>();
            for (String prefix : likePatterns) {
                for (String s : segments.get(i)) {
                    if (s.indexOf('\n') >= 0) {
                        throw unsupported(pattern, "a newline beside .* or .+");
                    }
                    next.add(prefix + PlanValues.escapeLike(s)
                            + (i < wildcards.size() ? wildcards.get(i) : ""));
                }
            }
            if (next.size() > MAX_STRINGS) {
                throw unsupported(pattern, "more than " + MAX_STRINGS + " alternatives");
            }
            likePatterns = next;
        }
        List<Predicate> likes = new ArrayList<>();
        for (String like : likePatterns) {
            likes.add(cb.like(column, like, '\\'));
        }
        Predicate shape = likes.size() == 1 ? likes.get(0) : cb.or(likes.toArray(Predicate[]::new));
        // RE2's . excludes a newline, and no finite part holds one.
        return cb.and(shape, tri.not(cb.like(column, "%\n%")));
    }

    private static boolean containsAnchor(Node node) {
        if (node instanceof Node.Begin || node instanceof Node.End) {
            return true;
        }
        if (node instanceof Node.Cat cat) {
            return cat.parts().stream().anyMatch(RegexTranslator::containsAnchor);
        }
        if (node instanceof Node.Alt alt) {
            return alt.options().stream().anyMatch(RegexTranslator::containsAnchor);
        }
        return node instanceof Node.Rep rep && containsAnchor(rep.node());
    }

    /** The finite set of strings {@code node} matches. */
    private static Set<String> expand(Node node, String pattern) {
        Set<String> out;
        if (node instanceof Node.Lit lit) {
            out = Set.of(Character.toString(lit.codePoint()));
        } else if (node instanceof Node.Cls cls) {
            out = new LinkedHashSet<>();
            for (int member : cls.members()) {
                out.add(Character.toString(member));
            }
        } else if (node instanceof Node.Cat cat) {
            Set<String> acc = new LinkedHashSet<>(Set.of(""));
            for (Node part : cat.parts()) {
                Set<String> tail = expand(part, pattern);
                Set<String> next = new LinkedHashSet<>();
                for (String a : acc) {
                    for (String b : tail) {
                        next.add(a + b);
                    }
                }
                if (next.size() > MAX_STRINGS) {
                    throw unsupported(pattern, "more than " + MAX_STRINGS + " alternatives");
                }
                acc = next;
            }
            out = acc;
        } else if (node instanceof Node.Alt alt) {
            out = new LinkedHashSet<>();
            for (Node option : alt.options()) {
                out.addAll(expand(option, pattern));
            }
        } else if (node instanceof Node.Rep rep) {
            if (rep.max() < 0 || rep.max() > MAX_REPEAT) {
                throw unsupported(pattern, "an unbounded repetition");
            }
            out = new LinkedHashSet<>();
            for (int k = rep.min(); k <= rep.max(); k++) {
                List<Node> copies = new ArrayList<>();
                for (int i = 0; i < k; i++) {
                    copies.add(rep.node());
                }
                out.addAll(expand(new Node.Cat(copies), pattern));
            }
        } else if (node instanceof Node.Dot) {
            throw unsupported(pattern, "a lone .");
        } else {
            throw unsupported(pattern, "an anchor inside the pattern");
        }
        if (out.size() > MAX_STRINGS) {
            throw unsupported(pattern, "more than " + MAX_STRINGS + " alternatives");
        }
        return out;
    }

    private static UnsupportedPlanShapeException unsupported(String pattern, String what) {
        return Refusals.unsupported("matches() is translated only where LIKE can spell the"
                + " pattern exactly, and this one has " + what + ". JPA Criteria has no RE2"
                + " predicate; register an OperatorFunction for 'matches' to use a database"
                + " regex dialect instead (pattern length " + pattern.length() + ")");
    }

    // -- the RE2 subset --------------------------------------------------------------------

    private sealed interface Node {
        record Lit(int codePoint) implements Node {}

        record Cls(Set<Integer> members) implements Node {}

        record Dot() implements Node {}

        record Begin() implements Node {}

        record End() implements Node {}

        record Cat(List<Node> parts) implements Node {}

        record Alt(List<Node> options) implements Node {}

        /** {@code max} is -1 when unbounded. */
        record Rep(Node node, int min, int max) implements Node {}
    }

    /** RE2 rejects the pattern: matches() is a CEL error. */
    private static final class InvalidRe2 extends RuntimeException {
        InvalidRe2() {
            super(null, null, false, false);
        }
    }

    private static final class Parser {
        private final String src;
        private int pos;
        private boolean foldCase;

        Parser(String src) {
            this.src = src;
        }

        Node parse() {
            if (src.startsWith("(?i)")) {
                foldCase = true;
                pos = 4;
            }
            Node node = alternation();
            if (pos != src.length()) {
                // An unbalanced ')'.
                throw new InvalidRe2();
            }
            return node;
        }

        private Node alternation() {
            List<Node> options = new ArrayList<>();
            options.add(concatenation());
            while (peek() == '|') {
                pos++;
                options.add(concatenation());
            }
            return options.size() == 1 ? options.get(0) : new Node.Alt(options);
        }

        private Node concatenation() {
            List<Node> parts = new ArrayList<>();
            while (pos < src.length() && peek() != '|' && peek() != ')') {
                parts.add(repetition());
            }
            return parts.size() == 1 ? parts.get(0) : new Node.Cat(parts);
        }

        private Node repetition() {
            Node atom = atom();
            while (pos < src.length()) {
                char c = peek();
                int min;
                int max;
                if (c == '*') {
                    min = 0;
                    max = -1;
                    pos++;
                } else if (c == '+') {
                    min = 1;
                    max = -1;
                    pos++;
                } else if (c == '?') {
                    min = 0;
                    max = 1;
                    pos++;
                } else if (c == '{') {
                    int[] bounds = braces();
                    if (bounds == null) {
                        return atom;
                    }
                    min = bounds[0];
                    max = bounds[1];
                } else {
                    return atom;
                }
                if (atom instanceof Node.Begin || atom instanceof Node.End
                        || atom instanceof Node.Rep) {
                    // A repeated anchor or a stacked quantifier: RE2 rejects both (a**, ^*).
                    throw new InvalidRe2();
                }
                // A lazy suffix accepts the same language.
                if (pos < src.length() && peek() == '?') {
                    pos++;
                }
                atom = new Node.Rep(atom, min, max);
            }
            return atom;
        }

        /** {@code {n}}, {@code {n,}} or {@code {n,m}}; {@code null} for a literal brace. */
        private int[] braces() {
            int close = src.indexOf('}', pos);
            if (close < 0) {
                return null;
            }
            String body = src.substring(pos + 1, close);
            if (!body.matches("[0-9]+(,[0-9]*)?")) {
                return null;
            }
            String[] bounds = body.split(",", -1);
            int min = Integer.parseInt(bounds[0]);
            int max = bounds.length == 1 ? min
                    : bounds[1].isEmpty() ? -1 : Integer.parseInt(bounds[1]);
            if (min > 1000 || max > 1000 || (max >= 0 && max < min)) {
                throw new InvalidRe2();
            }
            pos = close + 1;
            return new int[] {min, max};
        }

        private Node atom() {
            char c = peek();
            switch (c) {
                case '(' -> {
                    pos++;
                    if (src.startsWith("?:", pos)) {
                        pos += 2;
                    } else if (src.startsWith("?P<", pos)) {
                        int close = src.indexOf('>', pos);
                        if (close < 0) {
                            throw new InvalidRe2();
                        }
                        pos = close + 1;
                    } else if (src.startsWith("?=", pos) || src.startsWith("?!", pos)
                            || src.startsWith("?<=", pos) || src.startsWith("?<!", pos)) {
                        throw new InvalidRe2();
                    } else if (peek() == '?') {
                        throw unsupported(src, "an inline flag other than a leading (?i)");
                    }
                    Node inner = alternation();
                    if (pos >= src.length() || peek() != ')') {
                        throw new InvalidRe2();
                    }
                    pos++;
                    return inner;
                }
                case '[' -> {
                    return characterClass();
                }
                case '.' -> {
                    pos++;
                    return new Node.Dot();
                }
                case '^' -> {
                    pos++;
                    return new Node.Begin();
                }
                case '$' -> {
                    pos++;
                    return new Node.End();
                }
                case '\\' -> {
                    Set<Integer> escaped = escape(false);
                    return escaped.size() == 1 ? literal(escaped.iterator().next())
                            : folded(escaped);
                }
                case '*', '+', '?' -> throw new InvalidRe2();
                case '{' -> {
                    int start = pos;
                    if (braces() != null) {
                        // A repetition with nothing to repeat.
                        throw new InvalidRe2();
                    }
                    pos = start + 1;
                    return new Node.Lit('{');
                }
                default -> {
                    int cp = src.codePointAt(pos);
                    pos += Character.charCount(cp);
                    return literal(cp);
                }
            }
        }

        private Node literal(int cp) {
            Set<Integer> orbit = fold(cp);
            return orbit.size() == 1 ? new Node.Lit(cp) : new Node.Cls(orbit);
        }

        private Node folded(Set<Integer> members) {
            Set<Integer> all = new TreeSet<>();
            members.forEach(m -> all.addAll(fold(m)));
            return new Node.Cls(all);
        }

        /** {@code cp} and, under (?i), the rest of its simple case-fold orbit. */
        private Set<Integer> fold(int cp) {
            if (!foldCase || !Character.isLetter(cp)) {
                return Set.of(cp);
            }
            if (cp > 0x7f) {
                throw unsupported(src, "(?i) over a non-ASCII letter");
            }
            Set<Integer> orbit = new TreeSet<>(List.of(
                    Character.toLowerCase(cp), Character.toUpperCase(cp)));
            if (Character.toLowerCase(cp) == 'k') {
                orbit.add(0x212A);
            }
            if (Character.toLowerCase(cp) == 's') {
                orbit.add(0x017F);
            }
            return orbit;
        }

        private Node characterClass() {
            pos++;
            if (pos < src.length() && peek() == '^') {
                throw unsupported(src, "a negated character class");
            }
            Set<Integer> members = new TreeSet<>();
            boolean first = true;
            while (true) {
                if (pos >= src.length()) {
                    throw new InvalidRe2();
                }
                char c = peek();
                if (c == ']' && !first) {
                    pos++;
                    break;
                }
                first = false;
                if (src.startsWith("[:", pos)) {
                    int close = src.indexOf(":]", pos + 2);
                    if (close < 0) {
                        throw new InvalidRe2();
                    }
                    members.addAll(posix(src.substring(pos + 2, close)));
                    pos = close + 2;
                    continue;
                }
                Set<Integer> low = c == '\\' ? escape(true) : Set.of(nextCodePoint());
                if (low.size() == 1 && pos + 1 < src.length() && peek() == '-'
                        && src.charAt(pos + 1) != ']') {
                    pos++;
                    Set<Integer> high = peek() == '\\' ? escape(true) : Set.of(nextCodePoint());
                    if (high.size() != 1) {
                        throw new InvalidRe2();
                    }
                    int lo = low.iterator().next();
                    int hi = high.iterator().next();
                    if (hi < lo) {
                        throw new InvalidRe2();
                    }
                    if (hi - lo > 128) {
                        throw unsupported(src, "a character range wider than 128");
                    }
                    for (int cp = lo; cp <= hi; cp++) {
                        members.add(cp);
                    }
                } else if (low.size() > 1 && pos + 1 < src.length() && peek() == '-'
                        && src.charAt(pos + 1) != ']') {
                    // A class escape beside a '-': RE2 reads the '-' as a literal here, and
                    // this parser does not reproduce that.
                    throw unsupported(src, "a '-' after a class escape");
                } else {
                    members.addAll(low);
                }
            }
            Set<Integer> all = new TreeSet<>();
            members.forEach(m -> all.addAll(fold(m)));
            return new Node.Cls(all);
        }

        private int nextCodePoint() {
            int cp = src.codePointAt(pos);
            pos += Character.charCount(cp);
            return cp;
        }

        /** The code points an escape matches; {@code pos} is at the backslash. */
        private Set<Integer> escape(boolean inClass) {
            pos++;
            if (pos >= src.length()) {
                throw new InvalidRe2();
            }
            char c = src.charAt(pos++);
            return switch (c) {
                case 'd' -> range('0', '9');
                case 'w' -> {
                    Set<Integer> w = new TreeSet<>(range('0', '9'));
                    w.addAll(range('A', 'Z'));
                    w.addAll(range('a', 'z'));
                    w.add((int) '_');
                    yield w;
                }
                case 's' -> Set.of((int) '\t', (int) '\n', (int) '\f', (int) '\r', (int) ' ');
                case 'n' -> Set.of((int) '\n');
                case 't' -> Set.of((int) '\t');
                case 'r' -> Set.of((int) '\r');
                case 'f' -> Set.of((int) '\f');
                case 'v' -> Set.of(0x0B);
                case 'a' -> Set.of(0x07);
                case '1', '2', '3', '4', '5', '6', '7', '8', '9' -> throw new InvalidRe2();
                default -> {
                    if (c < 0x80 && !Character.isLetterOrDigit(c)) {
                        // An escaped punctuation character is itself.
                        yield Set.of((int) c);
                    }
                    throw unsupported(src, "the escape \\" + c);
                }
            };
        }

        private Set<Integer> posix(String name) {
            return switch (name) {
                case "digit" -> range('0', '9');
                case "lower" -> range('a', 'z');
                case "upper" -> range('A', 'Z');
                case "alpha" -> {
                    Set<Integer> s = new TreeSet<>(range('a', 'z'));
                    s.addAll(range('A', 'Z'));
                    yield s;
                }
                case "alnum" -> {
                    Set<Integer> s = new TreeSet<>(range('a', 'z'));
                    s.addAll(range('A', 'Z'));
                    s.addAll(range('0', '9'));
                    yield s;
                }
                case "xdigit" -> {
                    Set<Integer> s = new TreeSet<>(range('0', '9'));
                    s.addAll(range('a', 'f'));
                    s.addAll(range('A', 'F'));
                    yield s;
                }
                default -> throw unsupported(src, "the POSIX class [:" + name + ":]");
            };
        }

        private static Set<Integer> range(char lo, char hi) {
            Set<Integer> s = new TreeSet<>();
            for (int c = lo; c <= hi; c++) {
                s.add(c);
            }
            return s;
        }

        /** The next character, or U+FFFF (never an operator) past the end. */
        private char peek() {
            return pos < src.length() ? src.charAt(pos) : '\uFFFF';
        }
    }
}
