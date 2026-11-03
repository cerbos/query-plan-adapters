package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.MAPPING
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.RelResources
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.VISIBILITY_FILTERED
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.ids
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.translate
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.wireFixture
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * `exists`, `all` and `exists_one` over a real store, against hand-picked rows that reach every CEL
 * state: an EMPTY collection, one whose element makes the body UNDETERMINED, and each under BOTH
 * polarities.
 *
 * The undetermined rows are the point. Under a plain `EXISTS` an element whose body touches a NULL
 * silently fails to match, the macro reads FALSE, and the negation readmits the row — so every
 * assertion here names which rows the negation must NOT bring back.
 */
class CollectionMacroTest {

    @Test
    fun `exists is false over the empty collection and unknown over an undetermined element`() {
        // r1/r7/r8 empty, r3 no match, r4 a NULL name (UNDETERMINED), r5 a match beside a NULL.
        assertEquals(listOf("r2", "r5", "r6"), ids(translate("collection/exists/empty-collection")))
    }

    @Test
    fun `the negation of exists keeps the undetermined rows out`() {
        // `!tags.exists(t, t.name == "private")`. r4's only tag has a NULL name and r5 holds one
        // beside a non-matching tag, so CEL errors for both and denies under BOTH polarities. A
        // two-valued EXISTS would read them FALSE and `NOT` would return them.
        assertEquals(listOf("r1", "r2", "r7", "r8"), ids(translate("collection/exists/negated")))
    }

    @Test
    fun `all is vacuously true over the empty collection and unknown over an undetermined element`() {
        // r5 has a matching tag AND a NULL one: no element is determined-false, so CEL errors.
        assertEquals(listOf("r1", "r2", "r7", "r8"), ids(translate("collection/all/empty-collection")))
    }

    @Test
    fun `the negation of all keeps the undetermined rows out`() {
        // Only r3 and r6 hold a determined-false element. r4 and r5 are undetermined.
        assertEquals(listOf("r3", "r6"), ids(translate("null/all/negated-null-element")))
    }

    @Test
    fun `all absorbs an undetermined element once another element is determined-false`() {
        // `!tags.all(t, t.name != "public")`: r2, r5 and r6 hold a "public" tag, which is a
        // determined FALSE for the body and decides the universal whatever its siblings do — r5's
        // NULL-named sibling included.
        assertEquals(listOf("r2", "r5", "r6"), ids(translate("null/all/negated-null-element-with-false-sibling")))
    }

    @Test
    fun `an undetermined element with no determined-false sibling still denies`() {
        // `tags.all(t, t.name != "x")` holds for every present name, so only the NULL-named
        // elements decide anything — and what they decide is "error".
        assertEquals(listOf("r1", "r2", "r3", "r6", "r7", "r8"), ids(translate("null/all/null-element-with-true-sibling")))
    }

    @Test
    fun `exists_one is strict, so one undetermined element denies whatever the count says`() {
        // r2 is the only row with exactly one match. r5 would also count one, but its NULL-named
        // sibling errors the macro; r6 counts two.
        assertEquals(listOf("r2"), ids(translate("collection/exists-one/several-matches")))
    }

    @Test
    fun `the negation of exists_one keeps every undetermined row out`() {
        assertEquals(listOf("r1", "r3", "r6", "r7", "r8"), ids(translate("null/exists-one/negated-null-element")))
    }

    @Test
    fun `a universal quantifies over the rows the application reads, not the rows in the table`() {
        // r6 holds a third tag named "private" that the application's own read hides. Examined, it
        // is a determined FALSE and `all` excludes r6; hidden, r6 comes back.
        assertEquals(listOf("r1", "r2", "r6", "r7", "r8"), ids(translate("collection/all/empty-collection", VISIBILITY_FILTERED)))
    }

    @Test
    fun `a macro over a chain requires its leading hop, so an absent parent stays excluded`() {
        // r2's category holds "finance", r5's holds "finance" and "tech", r7's category has no
        // children at all. r1, r4, r6 and r8 have NO category, which on the check side is a missing
        // path: an unguarded chain reads the empty scan as "no match" and agrees by accident.
        assertEquals(listOf("r2", "r5"), ids(translate("relation/exists/to-one-chain")))
        // The discriminating half: only r3 (a present parent with no matching child) and r7 (a
        // present parent with no children) may come back.
        assertEquals(listOf("r3", "r7"), ids(translate("relation/exists/negated-to-one-chain")))
    }

    @Test
    fun `a universal over a chain is vacuously true only where the parent exists`() {
        // r7's category has no sub-categories, so `all` is vacuously TRUE there. The four rows with
        // no category at all are a missing-path error and must not join it.
        assertEquals(listOf("r2", "r7"), ids(translate("relation/all/to-one-chain")))
    }

    @Test
    fun `a nested macro resolves each lambda variable against its own element`() {
        // `categories.exists(c, c.subCategories.exists(s, s.name == "finance" && R.attr.aBool))`.
        // The middle level proves `c.subCategories` correlates against the categories ALIAS rather
        // than the bare table, and the outer attribute reaches the subquery as a correlation.
        assertEquals(listOf("r5"), ids(translate("collection/exists/nested-with-outer-attribute")))
    }

    @Test
    fun `a macro three levels deep resolves each level against its own element`() {
        // `categories.exists(c, c.subCategories.exists(s, s.labels.exists(l, l.name == "gold")))`.
        // r5's second sub-category carries the label; its first carries none, which is what the
        // universal spelling below turns into a determined FALSE.
        assertEquals(listOf("r2", "r5"), ids(translate("collection/exists/nested-three-levels")))
        assertEquals(listOf("r1", "r3", "r4", "r6", "r7", "r8"), ids(translate("collection/exists/negated-nested-three-levels")))
        // r7's category has no sub-categories at all, so the middle universal is vacuously TRUE.
        assertEquals(listOf("r2", "r7"), ids(translate("collection/all/nested-three-levels-middle")))
    }

    @Test
    fun `a size comparison inside a lambda counts the element's own children`() {
        // `categories.exists(c, size(c.subCategories) == 1)` — the count has to correlate against
        // the categories ALIAS, not against the resource row.
        assertEquals(listOf("r2", "r3"), ids(translate("collection/exists/nested-collection-size")))
    }

    @Test
    fun `a macro inside a lambda over an outer relation stays anchored to the outer scope`() {
        // `categories.exists(c, size(c.subCategories) == 1 && R.attr.tags.exists(t, …))`.
        assertEquals(listOf("r2"), ids(translate("collection/exists/outer-collection-inside-lambda")))
    }

    @Test
    fun `a membership test inside a lambda reads the element column`() {
        assertEquals(listOf("r2", "r5", "r6"), ids(translate("collection/exists/element-in-principal-list")))
    }

    @Test
    fun `a negated existential over an empty collection returns the empty rows`() {
        assertEquals(listOf("r1", "r3", "r7", "r8"), ids(translate("collection/exists/negated-empty-collection")))
    }

    @Test
    fun `a macro over a literal value list is folded into an or chain`() {
        // The planner ships the list itself once the collection passes its own unroll threshold, so
        // the adapter applies the same fold: the translation must not depend on which side of that
        // threshold the collection fell.
        assertEquals(listOf("r1", "r3", "r5"), ids(translate("principal/exists/long-list")))
    }

    @Test
    fun `a universal over a literal value list folds to an and chain`() {
        assertEquals(listOf("r2", "r4", "r6", "r7", "r8"), ids(translate("principal/all/long-list")))
    }

    @Test
    fun `an unmapped collection is refused rather than guessed at a table`() {
        val error = runCatching { translate("collection/exists/empty-collection", cerbosMapping { }) }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals("Unknown attribute: request.resource.attr.tags", error?.message)
    }

    @Test
    fun `a collection mapped as a column is refused`() {
        val scalarOnly = cerbosMapping { "request.resource.attr.tags" to RelResources.aString }
        val error = runCatching { translate("collection/exists/empty-collection", scalarOnly) }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals(
            "request.resource.attr.tags is mapped as a column, but this position needs a relation",
            error?.message,
        )
    }

    @Test
    fun `nesting past maxMacroDepth is refused rather than emitted`() {
        // KIND 2 — a caller-supplied argument the corpus structurally cannot vary, and therefore
        // permanent: maxMacroDepth is an `Options` setting, and actions.json classifies every action
        // against one set of options, so no action can vary it. The PLAN is a corpus wire fixture;
        // only the option is the test's own.
        val error = runCatching {
            ExposedQueryPlanAdapter.toFilter(
                wireFixture("collection/exists/nested-with-outer-attribute"),
                Options.of(MAPPING).withMaxMacroDepth(1),
            )
        }.exceptionOrNull()
        assertEquals(UnsupportedPlanShapeException::class.java, error?.javaClass)
        assertEquals("exists is nested 2 collection macros deep, past maxMacroDepth=1", error?.message)
    }
}
