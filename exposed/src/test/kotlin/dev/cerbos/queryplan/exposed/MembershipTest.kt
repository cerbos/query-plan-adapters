package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.RelResources
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.ids
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.render
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.translate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * `in` and `hasIntersection`, over a scalar column, a stored collection and a `map()` projection.
 *
 * The two VIEWS of a NULL member column are what these suites separate. Under the scalar-projection
 * view a NULL member IS the explicitly-null element, so `null in tagNames` has to be TRUE for it;
 * under member ACCESS through `map()` it is a missing element attribute, so the whole intersection
 * errors even when another element would match.
 */
class MembershipTest {

    @Test
    fun `membership in a stored collection is an existence test over the element column`() {
        // `null in R.attr.tagNames`: r4's only tag name is NULL and r5 holds one beside "public".
        assertEquals(listOf("r4", "r5"), ids(translate("in-null-elem-rel")))
    }

    @Test
    fun `a null list element becomes IS NULL rather than a never-matching IN`() {
        // SQL's `IN (…, NULL)` silently excludes a NULL column and makes the negation UNKNOWN for
        // every non-matching row, so the whole filter would return nothing.
        val sql = render(translate("in-null-elem-rel"))
        assertTrue(sql.contains("IS NULL"), sql)
        assertTrue(!sql.contains("= NULL"), sql)
    }

    @Test
    fun `the negation of collection membership is exact over the empty collection`() {
        // `!(null in R.attr.tagNames)`. An empty collection is a definite non-match in CEL too, so
        // r1, r7 and r8 come back; only r4 and r5 hold a null element.
        assertEquals(listOf("r1", "r2", "r3", "r6", "r7", "r8"), ids(translate("in-null-elem-rel-neg")))
    }

    @Test
    fun `hasIntersection over a stored collection matches any element`() {
        // `hasIntersection(R.attr.tagNames, ["public", null])`: the null constant matches the NULL
        // member of r4 and r5, "public" matches r2, r5 and r6.
        assertEquals(listOf("r2", "r4", "r5", "r6"), ids(translate("in-null-elem-hasint")))
    }

    @Test
    fun `a value-first hasIntersection is mirrored rather than read backwards`() {
        // The operator is commutative, so an inversion is invisible in the answer — but the two
        // operands are not interchangeable in the emitted query, and reading the constant list as
        // the collection would refuse a supported shape.
        assertEquals(listOf("r2", "r5", "r6"), ids(translate("vf-hasint")))
    }

    @Test
    fun `attribute-in-attribute compares the member column against the element column`() {
        // `R.attr.owner in R.attr.tagNames`, with `owner` declared explicit-null: r2's and r6's
        // owner "public" matches one of their tags, and r4's NULL owner matches its NULL tag name.
        assertEquals(listOf("r2", "r4", "r6"), ids(translate("in-var-var")))
    }

    @Test
    fun `attribute-in-attribute is definite for a null member under the explicit convention`() {
        // CEL holds a null VALUE there, so `null in ["private"]` is a definite FALSE and the
        // negation has to return r3 — which plain `=` cannot say, because it answers UNKNOWN.
        assertEquals(listOf("r1", "r3", "r5", "r7", "r8"), ids(translate("in-var-var-neg")))
    }

    @Test
    fun `membership over a chain requires its leading hop`() {
        // `"finance" in R.attr.mainCategory.subNames`. r1, r4, r6 and r8 have no category, which is
        // a missing path on the check side: a bare NOT EXISTS would return every one of them
        // (cerbos/query-plan-adapters#315).
        assertEquals(listOf("r2", "r5"), ids(translate("w1-in-chain")))
        assertEquals(listOf("r3", "r7"), ids(translate("w1-not-in-chain")))
    }

    @Test
    fun `hasIntersection over a chain requires its leading hop`() {
        assertEquals(listOf("r3", "r7"), ids(translate("w1-not-hasint-chain")))
    }

    @Test
    fun `a map projection is strict, so a NULL projected column errors the whole intersection`() {
        // `hasIntersection(R.attr.tags.map(t, t.name), [...])`. r5 holds a matching tag AND a
        // NULL-named one: member ACCESS reads the NULL as a missing element attribute, which errors
        // the expression even though another element intersects.
        assertEquals(listOf("r2", "r6"), ids(translate("p-hasintersection-map")))
    }

    @Test
    fun `scalar membership folds to the equalities CEL itself evaluates`() {
        // `R.attr.aString in {"one": 1, "same": 2}` — the planner already folds the map to its key
        // list, and each key goes through the ordinary scalar leaf.
        assertEquals(listOf("r1"), ids(translate("in-map-keys")))
    }

    @Test
    fun `a collection mapped without an element column is refused rather than guessed`() {
        val noElement = cerbosMapping {
            "request.resource.attr.tagNames" to many(
                SubquerySupportTest.Companion.RelTags,
                from = RelResources.id,
                to = SubquerySupportTest.Companion.RelTags.resourceId,
            )
        }
        val error = runCatching { translate("in-null-elem-rel", noElement) }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals(
            "request.resource.attr.tagNames denotes the elements of rel_tags themselves, but the " +
                "relation declares no element column; map it with element = <column>",
            error?.message,
        )
    }

    @Test
    fun `attribute-in-attribute over a scalar container is refused`() {
        val scalarContainer = cerbosMapping {
            "request.resource.attr.owner" to RelResources.owner
            "request.resource.attr.tagNames" to RelResources.aString
        }
        val error = runCatching { translate("in-var-var", scalarContainer) }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals(
            "in tests membership of request.resource.attr.tagNames, which resolves to a scalar " +
                "column; collection membership needs a relation mapping",
            error?.message,
        )
    }
}
