package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.ExactString
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.LikeEscaping
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IsNullOp
import org.jetbrains.exposed.v1.core.LikeEscapeOp
import org.jetbrains.exposed.v1.core.NeqOp
import org.jetbrains.exposed.v1.core.Op

/**
 * `overlaps`, `ancestorOf` and `descendentOf` over Cerbos hierarchies. Every relation is decided by
 * prefix, enumerated from the delimiter, so the delimiter must be known at translation time.
 */
internal class HierarchyTranslator(private val translation: Translation) {

    fun translate(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> {
        // `hierarchy()` has no overload for a number or a boolean, so a path read from such a
        // column raises before any relation is asked, on every row: UNKNOWN, under both polarities.
        if (operands.any { readsNonTextPath(it, scope) }) return TriLogic.unknown()
        if (operator in setOf("overlaps", "ancestorOf", "descendentOf")) {
            val (first, second) = extract(operator, operands, scope)
            if (isPerCharacter(first) || isPerCharacter(second)) {
                return perCharacterRelation(operator, first, second)
            }
        }
        return translateDelimited(operator, operands, scope)
    }

    private fun translateDelimited(operator: String, operands: List<Operand>, scope: Scope): Op<Boolean> = when (operator) {
        "overlaps" -> overlaps(operands, scope)
        "ancestorOf" -> ancestorOrDescendant(operands, scope, isAncestor = true)
        "descendentOf" -> ancestorOrDescendant(operands, scope, isAncestor = false)
        else -> throw Refusals.internal("Unsupported hierarchy operator: $operator")
    }

    /** Whether a `hierarchy(...)` operand reads its path, or a `list()` segment, from a number or boolean column. */
    private fun readsNonTextPath(operand: Operand, scope: Scope): Boolean {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator != "hierarchy") return false
        val path = operand.expression.operandsList.firstOrNull() ?: return false
        val variables = when {
            path.nodeCase == Operand.NodeCase.VARIABLE -> listOf(path.variable)
            path.nodeCase == Operand.NodeCase.EXPRESSION && path.expression.operator == "list" ->
                path.expression.operandsList.filter { it.nodeCase == Operand.NodeCase.VARIABLE }.map { it.variable }
            else -> emptyList()
        }
        return variables.any { variable ->
            (scope.resolve(variable) as? Resolution.Scalar)?.let(translation.leaf::lacksTextOverload) == true
        }
    }

    /** A resolved `hierarchy(...)` operand. */
    private sealed interface Hierarchy {
        /** A literal delimited path, split into segments. */
        class Constant(val segments: List<String>, val delimiter: String) : Hierarchy

        /** A single column holding the whole delimited string. */
        class FieldRef(val expression: Expression<*>, val delimiter: String) : Hierarchy

        /** A `list(...)` of segments. */
        class Segmented(val segments: List<Segment>) : Hierarchy
    }

    /** One segment of a [Hierarchy.Segmented]: a literal, or a column. */
    private sealed interface Segment {
        class Literal(val value: String) : Segment

        class FieldSegment(val expression: Expression<*>) : Segment
    }

    private fun overlaps(operands: List<Operand>, scope: Scope): Op<Boolean> {
        val (left, right) = extract("overlaps", operands, scope)

        if (left is Hierarchy.FieldRef || right is Hierarchy.FieldRef) {
            return fieldOverlaps(left, right)
        }

        val leftSegments = segmentsOf(left)
        val rightSegments = segmentsOf(right)
        val leftPrefixOfRight = prefixConditions(leftSegments, rightSegments)
        val rightPrefixOfLeft = prefixConditions(rightSegments, leftSegments)
        val valid = listOfNotNull(leftPrefixOfRight, rightPrefixOfLeft)

        if (valid.isEmpty()) {
            // Neither side can be a prefix of the other. With a column involved the overlap is
            // simply never satisfiable — but only for a row whose column is PRESENT: CEL raises a
            // missing-attribute error for a NULL one, which denies under both polarities, and a
            // definite FALSE would be flipped to TRUE by an enclosing negation.
            val columns = (leftSegments + rightSegments).filterIsInstance<Segment.FieldSegment>()
            if (columns.isNotEmpty()) {
                return TriLogic.baseUnlessUnknown(Op.FALSE, TriLogic.or(columns.map { IsNullOp(it.expression) }))
            }
            // Two incompatible constants are a comparison the planner folds away before shipping.
            throw Refusals.malformed("Cannot determine hierarchy overlap: no field references found")
        }
        // An empty condition list means every compared segment was a matching constant, so the
        // overlap holds for every row that HAS the segments — and the two directions compare the
        // same segment pairs when the lengths are equal, so either condition set is equivalent.
        val base = if (valid.any { it.isEmpty() }) Op.TRUE else TriLogic.and(valid.first())
        return requirePresent(base, leftSegments, rightSegments)
    }

    /** [base], made UNKNOWN when a column segment past the compared prefix is NULL, since CEL raises there. */
    private fun requirePresent(
        base: Op<Boolean>,
        leftSegments: List<Segment>,
        rightSegments: List<Segment>,
    ): Op<Boolean> {
        val compared = minOf(leftSegments.size, rightSegments.size)
        val unread = (leftSegments.drop(compared) + rightSegments.drop(compared))
            .filterIsInstance<Segment.FieldSegment>()
        if (unread.isEmpty()) return base
        return TriLogic.baseUnlessUnknown(base, TriLogic.or(unread.map { IsNullOp(it.expression) }))
    }

    private fun fieldOverlaps(left: Hierarchy, right: Hierarchy): Op<Boolean> {
        if (left is Hierarchy.FieldRef && right is Hierarchy.FieldRef) {
            throw Refusals.unsupported(
                "overlaps: cannot compare two column hierarchies — an overlap is a prefix test in " +
                    "either direction, and neither column's segment count is known at translation time",
            )
        }
        val field = (left as? Hierarchy.FieldRef) ?: (right as Hierarchy.FieldRef)
        val other = if (left is Hierarchy.FieldRef) right else left
        if (other !is Hierarchy.Constant) {
            throw Refusals.unsupported(
                "overlaps: a segmented hierarchy cannot be compared with a column hierarchy — the " +
                    "column holds the whole delimited path and the segments would have to be " +
                    "reassembled with a store-dependent concatenation",
            )
        }

        val delimiter = field.delimiter
        val whole = other.segments.joinToString(delimiter)
        return TriLogic.or(
            buildList {
                // The column is an ancestor of the constant...
                strictPrefixes(other.segments, delimiter).forEach { add(EqOp(field.expression, ExactString.param(it))) }
                // ...or equal to it...
                add(EqOp(field.expression, ExactString.param(whole)))
                // ...or a descendant of it.
                add(startsWith(field.expression, whole + delimiter))
            },
        )
    }

    private fun ancestorOrDescendant(operands: List<Operand>, scope: Scope, isAncestor: Boolean): Op<Boolean> {
        val name = if (isAncestor) "ancestorOf" else "descendentOf"
        val (first, second) = extract(name, operands, scope)
        // ancestorOf(A, B) holds when A is a strict prefix of B; descendentOf(A, B) is the mirror.
        val ancestor = if (isAncestor) first else second
        val descendant = if (isAncestor) second else first

        if (ancestor is Hierarchy.Constant && descendant is Hierarchy.FieldRef) {
            return startsWith(
                descendant.expression,
                ancestor.segments.joinToString(descendant.delimiter) + descendant.delimiter,
            )
        }
        if (ancestor is Hierarchy.FieldRef && descendant is Hierarchy.Constant) {
            val prefixes = strictPrefixes(descendant.segments, ancestor.delimiter)
            // No strict prefix exists, so no present value satisfies it — and a NULL column must
            // still be UNKNOWN rather than FALSE, or a negation readmits the row.
            if (prefixes.isEmpty()) {
                return TriLogic.baseUnlessUnknown(Op.FALSE, IsNullOp(ancestor.expression))
            }
            return TriLogic.or(prefixes.map { EqOp(ancestor.expression, ExactString.param(it)) })
        }
        if (ancestor is Hierarchy.Constant && descendant is Hierarchy.Constant) {
            if (descendant.segments.size > ancestor.segments.size && isPrefix(ancestor.segments, descendant.segments)) {
                return Op.TRUE
            }
            // Two constants that fail the relationship are a comparison the planner folds to false
            // before it ever reaches an adapter.
            throw Refusals.malformed(
                "$name: constant operands do not satisfy the " +
                    "${if (isAncestor) "ancestor" else "descendant"} relationship",
            )
        }
        throw Refusals.unsupported(
            "$name: a segmented or column-to-column hierarchy relation is not supported — the " +
                "prefixes this lowering compares against are enumerated at translation time and " +
                "neither side's segments are known then",
        )
    }

    private fun extract(name: String, operands: List<Operand>, scope: Scope): Pair<Hierarchy, Hierarchy> {
        if (operands.size != 2) throw Refusals.malformed("$name requires exactly 2 operands")
        return normalize(resolveHierarchy(name, operands[0], scope)) to
            normalize(resolveHierarchy(name, operands[1], scope))
    }

    private fun resolveHierarchy(name: String, operand: Operand, scope: Scope): Hierarchy {
        if (operand.nodeCase != Operand.NodeCase.EXPRESSION || operand.expression.operator != "hierarchy") {
            throw Refusals.malformed("$name requires hierarchy(...) operands")
        }
        val operands = operand.expression.operandsList
        return when (operands.size) {
            2 -> {
                val delimiterOperand = operands[1]
                if (delimiterOperand.nodeCase != Operand.NodeCase.VALUE) {
                    // A delimiter read from a column is legal CEL; the prefixes below are built
                    // from a delimiter known at translation time.
                    throw Refusals.unsupported("hierarchy delimiter must be a value")
                }
                val delimiter = PlanValues.toKotlin(delimiterOperand.value).toString()
                // An empty delimiter is kept as-is: [perCharacterRelation] owns that case.
                pathOperand(name, operands[0], delimiter, scope)
            }
            1 -> when (val inner = operands[0].nodeCase) {
                Operand.NodeCase.VALUE, Operand.NodeCase.VARIABLE -> pathOperand(name, operands[0], ".", scope)
                Operand.NodeCase.EXPRESSION -> {
                    if (operands[0].expression.operator != "list") {
                        throw Refusals.unsupported("hierarchy requires a value, field, or list operand")
                    }
                    Hierarchy.Segmented(
                        operands[0].expression.operandsList.map { segment ->
                            when (segment.nodeCase) {
                                Operand.NodeCase.VALUE ->
                                    Segment.Literal(PlanValues.toKotlin(segment.value).toString())
                                Operand.NodeCase.VARIABLE ->
                                    Segment.FieldSegment(pathColumn(name, segment.variable, scope))
                                // A computed segment: legal CEL, no prefix to build from.
                                else -> throw Refusals.unsupported(
                                    "hierarchy list segment must be a value or field, got ${segment.nodeCase}",
                                )
                            }
                        },
                    )
                }
                else -> throw Refusals.malformed("hierarchy requires a value, field, or list operand, got $inner")
            }
            else -> throw Refusals.malformed("hierarchy requires 1 or 2 operands")
        }
    }

    private fun pathOperand(
        name: String,
        operand: Operand,
        delimiter: String,
        scope: Scope,
    ): Hierarchy = when (operand.nodeCase) {
        Operand.NodeCase.VALUE ->
            Hierarchy.Constant(splitLiteral(PlanValues.toKotlin(operand.value).toString(), delimiter), delimiter)
        Operand.NodeCase.VARIABLE ->
            Hierarchy.FieldRef(pathColumn(name, operand.variable, scope), delimiter)
        // A path computed by an expression (a concatenation, a ternary) has no prefix the LIKE
        // below can be built from.
        else -> throw Refusals.unsupported("hierarchy(string, delimiter) requires a value or field operand")
    }

    /** A column a hierarchy path is read from; it must be text, as for string matches. */
    private fun pathColumn(name: String, variable: String, scope: Scope): Expression<*> {
        val target = scope.scalar(variable)
        translation.leaf.requireText(name, target)
        return target.expression
    }

    /** Collapses an all-constant segmented hierarchy to a plain constant under the default delimiter. */
    private fun normalize(hierarchy: Hierarchy): Hierarchy {
        if (hierarchy !is Hierarchy.Segmented) return hierarchy
        val literals = hierarchy.segments.map { (it as? Segment.Literal)?.value ?: return hierarchy }
        return Hierarchy.Constant(literals, ".")
    }

    private fun segmentsOf(hierarchy: Hierarchy): List<Segment> = when (hierarchy) {
        is Hierarchy.Constant -> hierarchy.segments.map { Segment.Literal(it) }
        is Hierarchy.Segmented -> hierarchy.segments
        // overlaps() routes every FieldRef to fieldOverlaps before calling here.
        is Hierarchy.FieldRef -> throw Refusals.internal("Cannot enumerate the segments of a column hierarchy")
    }

    /**
     * The predicates that must hold for [shorter] to be a prefix of [longer] — an empty list means
     * unconditionally — or `null` when it cannot be one.
     */
    private fun prefixConditions(shorter: List<Segment>, longer: List<Segment>): List<Op<Boolean>>? {
        if (shorter.size > longer.size) return null
        val conditions = mutableListOf<Op<Boolean>>()
        shorter.forEachIndexed { index, segment ->
            val other = longer[index]
            when {
                segment is Segment.Literal && other is Segment.Literal ->
                    if (segment.value != other.value) return null
                segment is Segment.FieldSegment && other is Segment.Literal ->
                    conditions.add(EqOp(segment.expression, ExactString.param(other.value)))
                segment is Segment.Literal && other is Segment.FieldSegment ->
                    conditions.add(EqOp(other.expression, ExactString.param(segment.value)))
                else -> throw Refusals.unsupported(
                    "Cannot compare two column segments in a hierarchy overlap: neither side's " +
                        "value is known at translation time",
                )
            }
        }
        return conditions
    }

    private fun startsWith(expression: Expression<*>, prefix: String): Op<Boolean> = LikeEscapeOp(
        expression,
        ExactString.param(PlanValues.escapeLike(prefix) + "%"),
        true,
        LikeEscaping.ESCAPE_CHAR,
    )

    private fun isPrefix(shorter: List<String>, longer: List<String>): Boolean =
        shorter.indices.all { shorter[it] == longer[it] }

    /** Every proper (strict) ancestor prefix of a segment list, joined with [delimiter]. */
    private fun strictPrefixes(segments: List<String>, delimiter: String): List<String> {
        if (segments.size <= 1) return emptyList()
        val prefixes = mutableListOf<String>()
        var current = segments[0]
        prefixes.add(current)
        for (index in 1 until segments.size - 1) {
            current = current + delimiter + segments[index]
            prefixes.add(current)
        }
        return prefixes
    }

    /**
     * Splits on a LITERAL delimiter, keeping trailing empty segments. An EMPTY delimiter splits
     * as Go's `strings.Split` does: one segment per code point (never per UTF-16 unit, which would
     * cut an astral character in two), and none at all for the empty string.
     */
    private fun splitLiteral(raw: String, delimiter: String): List<String> =
        if (delimiter.isEmpty()) {
            raw.codePoints().toArray().map { String(Character.toChars(it)) }
        } else {
            raw.split(delimiter)
        }

    private fun isPerCharacter(hierarchy: Hierarchy): Boolean = when (hierarchy) {
        is Hierarchy.Constant -> hierarchy.delimiter.isEmpty()
        is Hierarchy.FieldRef -> hierarchy.delimiter.isEmpty()
        is Hierarchy.Segmented -> false
    }

    /**
     * A column against a constant, both split on the empty delimiter, so a segment prefix is a string
     * prefix. Other empty-delimiter pairings are refused.
     */
    private fun perCharacterRelation(operator: String, first: Hierarchy, second: Hierarchy): Op<Boolean> {
        val field = (first as? Hierarchy.FieldRef) ?: (second as? Hierarchy.FieldRef)
        val constant = (first as? Hierarchy.Constant) ?: (second as? Hierarchy.Constant)
        if (field == null || constant == null || !isPerCharacter(first) || !isPerCharacter(second)) {
            throw Refusals.unsupported(
                "$operator: a hierarchy split on the empty delimiter is only supported between a " +
                    "column and a constant that both split per character",
            )
        }
        val fieldFirst = first === field
        val characters = constant.segments
        val whole = characters.joinToString("")
        val column = field.expression
        val prefixes = (0..characters.size).map { characters.subList(0, it).joinToString("") }
        fun isOneOf(values: List<String>): Op<Boolean> = if (values.isEmpty()) {
            TriLogic.baseUnlessUnknown(Op.FALSE, IsNullOp(column))
        } else {
            TriLogic.or(values.map { EqOp(column, ExactString.param(it)) })
        }
        val descendant = TriLogic.and(startsWith(column, whole), NeqOp(column, ExactString.param(whole)))
        return when (operator) {
            "overlaps" -> TriLogic.or(isOneOf(prefixes), startsWith(column, whole))
            // ancestorOf(first, second): first is the strict ancestor.
            "ancestorOf" -> if (fieldFirst) isOneOf(prefixes.dropLast(1)) else descendant
            "descendentOf" -> if (fieldFirst) descendant else isOneOf(prefixes.dropLast(1))
            else -> throw Refusals.internal("Unsupported hierarchy operator: $operator")
        }
    }
}
