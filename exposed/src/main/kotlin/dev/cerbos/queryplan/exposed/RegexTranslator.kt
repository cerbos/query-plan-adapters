package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.LikeEscaping
import org.jetbrains.exposed.v1.core.CustomFunction
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.LikeEscapeOp
import org.jetbrains.exposed.v1.core.NeqOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.TextColumnType
import org.jetbrains.exposed.v1.core.stringParam
import java.util.TreeSet

/**
 * `R.attr.s.matches("re")` without a regex predicate: no SQL engine implements RE2, so a pattern is
 * translated only when its language can be spelled EXACTLY with `LIKE`, `=` and `REPLACE`:
 *
 *  - each top-level alternative is a finite set of strings, optionally anchored with `^` and `$`:
 *    `= s`, `LIKE 's%'`, `LIKE '%s'` or `LIKE '%s%'` for each string (RE2's `$` without the `m`
 *    flag matches only at the end of the text, so `= s` is exact);
 *  - in an alternative anchored at both ends, `.*` and `.+` between finite parts become `%` and
 *    `_%`, with `NOT LIKE '%\n%'`, because RE2's `.` excludes a newline (the finite parts must
 *    hold none). `_%` is "at least one character", which is the same in code points and in UTF-16
 *    units, so it holds on H2 too;
 *  - `^[set]*$` and `^[set]+$` hold when deleting every member of the set with `REPLACE` (exact
 *    and case-sensitive on every dialect here) leaves the empty string.
 *
 * The finite parts are literals, escapes, character classes (ranges, `\d \w \s`, POSIX classes),
 * groups, alternation and bounded repetition; a leading `(?i)` expands each ASCII letter to its
 * Unicode simple case-fold orbit (`k` also matches U+212A KELVIN SIGN, `s` also U+017F LATIN
 * SMALL LETTER LONG S), as RE2 does. A pattern RE2 rejects (a lookaround, a backreference, a
 * stacked quantifier, a repeat count past 1000) makes CEL's `matches()` raise on every row, so it
 * is UNKNOWN. Anything else is refused: a negated class, a lone `.` (`_` counts UTF-16 units on
 * H2, where RE2 counts code points), other flags and escapes, and expansions past [MAX_STRINGS]
 * strings.
 *
 * The receiver must be a text column; a number or boolean has no `matches()` overload, so it is
 * UNKNOWN, and any other kind is refused. A NULL column is UNKNOWN. Like every string predicate
 * here, the result assumes the byte-exact collation the README requires.
 */
internal class RegexTranslator(private val translation: Translation) {

    fun matches(target: Resolution.Scalar, pattern: String): Op<Boolean> {
        if (translation.leaf.lacksTextOverload(target)) return TriLogic.unknown()
        translation.leaf.requireText("matches", target)
        val column = target.expression
        val regex = try {
            Parser(pattern).parse()
        } catch (_: InvalidRe2) {
            // RE2 rejects the pattern, so CEL's matches() raises and the PDP denies.
            return TriLogic.unknown()
        }
        val alternatives = if (regex is Node.Alt) regex.options else listOf(regex)
        val any = mutableListOf<Op<Boolean>>()
        for (alternative in alternatives) {
            // An alternative that matches every string decides the whole pattern for a present
            // value; a NULL one is still a missing attribute.
            val predicate = alternative(column, alternative, pattern)
                ?: return TriLogic.baseUnlessUnknown(Op.TRUE, IsNullOp(column))
            any += predicate
        }
        return TriLogic.or(any)
    }

    /** One top-level alternative, or `null` when it matches every string. */
    private fun alternative(column: Expression<*>, alternative: Node, pattern: String): Op<Boolean>? {
        val parts = (if (alternative is Node.Cat) alternative.parts else listOf(alternative)).toMutableList()
        var start = false
        var end = false
        while (parts.isNotEmpty() && parts.first() is Node.Begin) {
            parts.removeAt(0)
            start = true
        }
        while (parts.isNotEmpty() && parts.last() is Node.End) {
            parts.removeAt(parts.size - 1)
            end = true
        }
        if (parts.any(::containsAnchor)) throw unsupported(pattern, "an anchor inside the pattern")

        // ^[set]*$ and ^[set]+$: every character is a member.
        val only = parts.singleOrNull()
        if (start && end && only is Node.Rep && only.max < 0 && only.node is Node.Cls && only.min <= 1) {
            var rest: Expression<*> = column
            for (member in only.node.members) {
                rest = CustomFunction<String>(
                    "REPLACE",
                    TextColumnType(),
                    rest,
                    stringParam(String(Character.toChars(member))),
                    stringParam(""),
                )
            }
            val onlyMembers = EqOp(rest, stringParam(""))
            return if (only.min == 0) onlyMembers else TriLogic.and(onlyMembers, NeqOp(column, stringParam("")))
        }

        // Finite segments separated by .* / .+ wildcards.
        val segments = mutableListOf<Set<String>>()
        val wildcards = mutableListOf<String>()
        var pending = mutableListOf<Node>()
        for (part in parts) {
            if (part is Node.Rep && part.node is Node.Dot && part.max < 0 && part.min <= 1) {
                segments += expand(Node.Cat(pending), pattern)
                pending = mutableListOf()
                wildcards += if (part.min == 0) "%" else "_%"
            } else {
                pending += part
            }
        }
        segments += expand(Node.Cat(pending), pattern)

        if (wildcards.isEmpty()) {
            val strings = segments[0]
            if ("" in strings && !(start && end)) return null
            if (start && end) return TriLogic.or(strings.map { EqOp(column, stringParam(it)) })
            return TriLogic.or(
                strings.map { like(column, (if (start) "" else "%") + PlanValues.escapeLike(it) + (if (end) "" else "%")) },
            )
        }

        if (!start || !end) throw unsupported(pattern, "an unanchored .* or .+")
        var likePatterns = listOf("")
        segments.forEachIndexed { index, strings ->
            val next = mutableListOf<String>()
            for (prefix in likePatterns) {
                for (s in strings) {
                    if ('\n' in s) throw unsupported(pattern, "a newline beside .* or .+")
                    next += prefix + PlanValues.escapeLike(s) + (wildcards.getOrNull(index) ?: "")
                }
            }
            if (next.size > MAX_STRINGS) throw unsupported(pattern, "more than $MAX_STRINGS alternatives")
            likePatterns = next
        }
        val shape = TriLogic.or(likePatterns.map { like(column, it) })
        // RE2's . excludes a newline, and no finite part holds one.
        return TriLogic.and(shape, TriLogic.not(like(column, "%\n%")))
    }

    private fun like(column: Expression<*>, pattern: String): Op<Boolean> =
        LikeEscapeOp(column, stringParam(pattern), true, LikeEscaping.ESCAPE_CHAR)

    private fun containsAnchor(node: Node): Boolean = when (node) {
        is Node.Begin, is Node.End -> true
        is Node.Cat -> node.parts.any(::containsAnchor)
        is Node.Alt -> node.options.any(::containsAnchor)
        is Node.Rep -> containsAnchor(node.node)
        else -> false
    }

    /** The finite set of strings [node] matches. */
    private fun expand(node: Node, pattern: String): Set<String> {
        val out: Set<String> = when (node) {
            is Node.Lit -> setOf(String(Character.toChars(node.codePoint)))
            is Node.Cls -> node.members.mapTo(LinkedHashSet()) { String(Character.toChars(it)) }
            is Node.Cat -> {
                var acc: Set<String> = linkedSetOf("")
                for (part in node.parts) {
                    val tail = expand(part, pattern)
                    val next = LinkedHashSet<String>()
                    for (a in acc) for (b in tail) next += a + b
                    if (next.size > MAX_STRINGS) throw unsupported(pattern, "more than $MAX_STRINGS alternatives")
                    acc = next
                }
                acc
            }
            is Node.Alt -> node.options.flatMapTo(LinkedHashSet()) { expand(it, pattern) }
            is Node.Rep -> {
                if (node.max < 0 || node.max > MAX_REPEAT) throw unsupported(pattern, "an unbounded repetition")
                (node.min..node.max).flatMapTo(LinkedHashSet()) { k -> expand(Node.Cat(List(k) { node.node }), pattern) }
            }
            is Node.Dot -> throw unsupported(pattern, "a lone .")
            is Node.Begin, is Node.End -> throw unsupported(pattern, "an anchor inside the pattern")
        }
        if (out.size > MAX_STRINGS) throw unsupported(pattern, "more than $MAX_STRINGS alternatives")
        return out
    }

    // -- the RE2 subset ---------------------------------------------------------------------------

    private sealed interface Node {
        class Lit(val codePoint: Int) : Node
        class Cls(val members: Set<Int>) : Node
        object Dot : Node
        object Begin : Node
        object End : Node
        class Cat(val parts: List<Node>) : Node
        class Alt(val options: List<Node>) : Node

        /** [max] is -1 when unbounded. */
        class Rep(val node: Node, val min: Int, val max: Int) : Node
    }

    /** RE2 rejects the pattern: `matches()` is a CEL error. */
    private class InvalidRe2 : RuntimeException(null, null, false, false)

    private class Parser(private val src: String) {
        private var pos = 0
        private var foldCase = false

        fun parse(): Node {
            if (src.startsWith("(?i)")) {
                foldCase = true
                pos = 4
            }
            val node = alternation()
            // An unbalanced ')'.
            if (pos != src.length) throw InvalidRe2()
            return node
        }

        private fun alternation(): Node {
            val options = mutableListOf(concatenation())
            while (peek() == '|') {
                pos++
                options += concatenation()
            }
            return options.singleOrNull() ?: Node.Alt(options)
        }

        private fun concatenation(): Node {
            val parts = mutableListOf<Node>()
            while (pos < src.length && peek() != '|' && peek() != ')') parts += repetition()
            return parts.singleOrNull() ?: Node.Cat(parts)
        }

        private fun repetition(): Node {
            var atom = atom()
            while (pos < src.length) {
                val min: Int
                val max: Int
                when (peek()) {
                    '*' -> { min = 0; max = -1; pos++ }
                    '+' -> { min = 1; max = -1; pos++ }
                    '?' -> { min = 0; max = 1; pos++ }
                    '{' -> {
                        val bounds = braces() ?: return atom
                        min = bounds.first
                        max = bounds.second
                    }
                    else -> return atom
                }
                // A repeated anchor or a stacked quantifier: RE2 rejects both (a**, ^*).
                if (atom is Node.Begin || atom is Node.End || atom is Node.Rep) throw InvalidRe2()
                // A lazy suffix accepts the same language.
                if (pos < src.length && peek() == '?') pos++
                atom = Node.Rep(atom, min, max)
            }
            return atom
        }

        /** `{n}`, `{n,}` or `{n,m}`; `null` for a literal brace. */
        private fun braces(): Pair<Int, Int>? {
            val close = src.indexOf('}', pos)
            if (close < 0) return null
            val body = src.substring(pos + 1, close)
            if (!body.matches(Regex("[0-9]+(,[0-9]*)?"))) return null
            val bounds = body.split(",")
            val min = bounds[0].toIntOrNull() ?: throw InvalidRe2()
            val max = when {
                bounds.size == 1 -> min
                bounds[1].isEmpty() -> -1
                else -> bounds[1].toIntOrNull() ?: throw InvalidRe2()
            }
            if (min > 1000 || max > 1000 || (max in 0 until min)) throw InvalidRe2()
            pos = close + 1
            return min to max
        }

        private fun atom(): Node = when (peek()) {
            '(' -> {
                pos++
                when {
                    src.startsWith("?:", pos) -> pos += 2
                    src.startsWith("?P<", pos) -> {
                        val close = src.indexOf('>', pos)
                        if (close < 0) throw InvalidRe2()
                        pos = close + 1
                    }
                    src.startsWith("?=", pos) || src.startsWith("?!", pos) ||
                        src.startsWith("?<=", pos) || src.startsWith("?<!", pos) -> throw InvalidRe2()
                    peek() == '?' -> throw unsupported(src, "an inline flag other than a leading (?i)")
                }
                val inner = alternation()
                if (pos >= src.length || peek() != ')') throw InvalidRe2()
                pos++
                inner
            }
            '[' -> characterClass()
            '.' -> { pos++; Node.Dot }
            '^' -> { pos++; Node.Begin }
            '$' -> { pos++; Node.End }
            '\\' -> {
                val escaped = escape()
                if (escaped.size == 1) literal(escaped.first()) else folded(escaped)
            }
            '*', '+', '?' -> throw InvalidRe2()
            '{' -> {
                val start = pos
                // A repetition with nothing to repeat.
                if (braces() != null) throw InvalidRe2()
                pos = start + 1
                Node.Lit('{'.code)
            }
            else -> literal(nextCodePoint())
        }

        private fun literal(cp: Int): Node {
            val orbit = fold(cp)
            return if (orbit.size == 1) Node.Lit(cp) else Node.Cls(orbit)
        }

        private fun folded(members: Set<Int>): Node = Node.Cls(members.flatMapTo(TreeSet()) { fold(it) })

        /** [cp] and, under (?i), the rest of its simple case-fold orbit. */
        private fun fold(cp: Int): Set<Int> {
            if (!foldCase || !Character.isLetter(cp)) return setOf(cp)
            if (cp > 0x7f) throw unsupported(src, "(?i) over a non-ASCII letter")
            val orbit = sortedSetOf(Character.toLowerCase(cp), Character.toUpperCase(cp))
            if (Character.toLowerCase(cp) == 'k'.code) orbit += 0x212A
            if (Character.toLowerCase(cp) == 's'.code) orbit += 0x017F
            return orbit
        }

        private fun characterClass(): Node {
            pos++
            if (pos < src.length && peek() == '^') throw unsupported(src, "a negated character class")
            val members = TreeSet<Int>()
            var first = true
            while (true) {
                if (pos >= src.length) throw InvalidRe2()
                val c = peek()
                if (c == ']' && !first) {
                    pos++
                    break
                }
                first = false
                if (src.startsWith("[:", pos)) {
                    val close = src.indexOf(":]", pos + 2)
                    if (close < 0) throw InvalidRe2()
                    members += posix(src.substring(pos + 2, close))
                    pos = close + 2
                    continue
                }
                val low = if (c == '\\') escape() else setOf(nextCodePoint())
                if (low.size == 1 && pos + 1 < src.length && peek() == '-' && src[pos + 1] != ']') {
                    pos++
                    val high = if (peek() == '\\') escape() else setOf(nextCodePoint())
                    if (high.size != 1) throw InvalidRe2()
                    val lo = low.first()
                    val hi = high.first()
                    if (hi < lo) throw InvalidRe2()
                    if (hi - lo > 128) throw unsupported(src, "a character range wider than 128")
                    members += lo..hi
                } else if (low.size > 1 && pos + 1 < src.length && peek() == '-' && src[pos + 1] != ']') {
                    // A class escape beside a '-': RE2 reads the '-' as a literal here, and this
                    // parser does not reproduce that.
                    throw unsupported(src, "a '-' after a class escape")
                } else {
                    members += low
                }
            }
            return Node.Cls(members.flatMapTo(TreeSet()) { fold(it) })
        }

        private fun nextCodePoint(): Int {
            val cp = src.codePointAt(pos)
            pos += Character.charCount(cp)
            return cp
        }

        /** The code points an escape matches; [pos] is at the backslash. */
        private fun escape(): Set<Int> {
            pos++
            if (pos >= src.length) throw InvalidRe2()
            return when (val c = src[pos++]) {
                'd' -> range('0', '9')
                'w' -> range('0', '9') + range('A', 'Z') + range('a', 'z') + '_'.code
                's' -> setOf('\t'.code, '\n'.code, 0x0C, '\r'.code, ' '.code)
                'n' -> setOf('\n'.code)
                't' -> setOf('\t'.code)
                'r' -> setOf('\r'.code)
                'f' -> setOf(0x0C)
                'v' -> setOf(0x0B)
                'a' -> setOf(0x07)
                in '1'..'9' -> throw InvalidRe2()
                // An escaped punctuation character is itself.
                else -> if (c.code < 0x80 && !c.isLetterOrDigit()) setOf(c.code) else throw unsupported(src, "the escape \\$c")
            }
        }

        private fun posix(name: String): Set<Int> = when (name) {
            "digit" -> range('0', '9')
            "lower" -> range('a', 'z')
            "upper" -> range('A', 'Z')
            "alpha" -> range('a', 'z') + range('A', 'Z')
            "alnum" -> range('a', 'z') + range('A', 'Z') + range('0', '9')
            "xdigit" -> range('0', '9') + range('a', 'f') + range('A', 'F')
            else -> throw unsupported(src, "the POSIX class [:$name:]")
        }

        private fun range(lo: Char, hi: Char): Set<Int> = (lo.code..hi.code).toSet()

        /** The next character, or U+FFFF (never an operator) past the end. */
        private fun peek(): Char = if (pos < src.length) src[pos] else '￿'
    }

    companion object {
        /** The most strings one alternative may expand to. */
        const val MAX_STRINGS: Int = 256

        private const val MAX_REPEAT = 16

        private fun unsupported(pattern: String, what: String): UnsupportedPlanShapeException = Refusals.unsupported(
            "matches() is translated only where LIKE can spell the pattern exactly, and this one has " +
                "$what. CEL matches with RE2, which no SQL engine implements (pattern length " +
                "${pattern.length})",
        )
    }
}
