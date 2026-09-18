package dev.cerbos.queryplan.exposed

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * `overlaps`, `ancestorOf` and `descendentOf`, executed.
 *
 * A hierarchy relation is a PREFIX test, and the prefixes are enumerated at translation time from
 * the declared delimiter. Two things can go wrong invisibly: the direction (an ancestor test that
 * reads as a descendant test still returns rows, the wrong ones), and the boundary (a prefix
 * `LIKE` that forgets the delimiter matches a sibling whose name merely starts the same way).
 */
class HierarchyTranslatorTest {

    @Test
    fun `ancestorOf compares a constant ancestor against a column descendant`() =
        // `"dept.eng".ancestorOf(scope)` — a STRICT prefix, so "dept.eng" itself is not its own
        // ancestor and only the deeper row matches.
        assertSelects("hier-ancestor-cf", "r3")

    @Test
    fun `ancestorOf compares a column ancestor against a constant descendant`() =
        // The column must be one of the constant's strict prefixes: "dept" or "dept.eng".
        assertSelects("hier-ancestor-ff", "r1", "r4", "r6", "f1", "c1")

    @Test
    fun `descendentOf is ancestorOf with the operands read the other way round`() {
        assertSelects("hier-descendent-cf", "r1", "r4", "r6", "f1", "c1")
        assertSelects("hier-descendent-ff", "r3")
    }

    @Test
    fun `overlaps holds in either direction, so it is prefixes, equality and descendants`() {
        assertSelects("hier-overlaps-cf", "r1", "r3", "r4", "r6", "f1", "c1")
        assertSelects("hier-overlaps-ff", "r1", "r3", "r4", "r6", "f1", "c1")
    }

    @Test
    fun `a list() hierarchy compares segment by segment`() =
        // `hierarchy(["projects", R.id])` against the constant "projects:f1": the literal segments
        // match, so what survives is an equality on the key.
        assertSelects("hier-list-id", "f1")

    @Test
    fun `LIKE metacharacters in a hierarchy constant are escaped`() {
        // The descendant lowering is a prefix LIKE, so an unescaped "50%" would match every scope
        // starting "50", and "a_b" would match "aXb".
        assertSelects("hier-meta-like")
        assertSelects("hier-overlaps-meta")
        assertSelects("hier-bracket")
        // The ancestor direction lowers to equality instead, where the metacharacters are inert;
        // asserted so a change of lowering cannot silently start matching.
        assertSelects("hier-meta-in")
    }

    @Test
    fun `an empty delimiter is refused rather than emitted with the wrong boundary`() {
        // Cerbos splits on an empty delimiter per CHARACTER, so the relation becomes a strict
        // string-prefix test — and `LIKE prefix || '' || '%'` also matches the path ITSELF, which
        // is never its own descendant.
        val error = assertThrows<UnsupportedPlanShapeException> { Scalars.ids("hier-empty-delim") }
        assertTrue(
            error.message!!.startsWith("hierarchy delimiter must be a non-empty string"),
            error.message,
        )
    }
}
