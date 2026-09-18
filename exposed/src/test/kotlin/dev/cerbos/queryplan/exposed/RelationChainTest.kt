package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.RelResources
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.RelTags
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.WITH_PARENT
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.ids
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.render
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.translate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * A scalar reached THROUGH a to-one relation: the shape that separates "the parent is absent" from
 * "the parent is present and its column does not match".
 *
 * A correlated SCALAR subquery needs no hop guard, which is why there is none in what these tests
 * pin: a subquery with no correlated row IS SQL NULL, which is already CEL's missing-path error,
 * and NULL propagates through every comparison and negation built on it. The guard the COLLECTION
 * chains carry exists because `EXISTS` is two-valued and collapses "absent parent" onto "no
 * matching child"; a scalar projection never makes that collapse.
 *
 * | id | parent                        | inner              |
 * |----|-------------------------------|--------------------|
 * | r2 | aString "One", 2.0, aBool T   | none               |
 * | r4 | aString NULL, 1.0, aBool F    | "inner4", aBool T  |
 * | r8 | aString "Two", 5.0, aBool F   | "inner8", aBool F  |
 *
 * Every other row has no parent at all.
 */
class RelationChainTest {

    @Test
    fun `a to-one column is read as a correlated scalar subquery with no hop guard`() {
        val sql = render(translate("rel-eq-hop", WITH_PARENT))
        assertTrue(sql.contains("(SELECT cerbos_1.A_STRING FROM REL_PARENTS cerbos_1"), sql)
        assertTrue(sql.contains("cerbos_1.RESOURCE_ID = REL_RESOURCES.ID"), sql)
        // No CASE guard and no EXISTS: the subquery's own emptiness is the missing-path error.
        assertTrue(!sql.contains("CASE WHEN"), sql)
        assertTrue(!sql.contains("EXISTS"), sql)
    }

    @Test
    fun `an absent parent is excluded under both polarities`() {
        assertEquals(listOf("r2"), ids(translate("rel-bool-hop", WITH_PARENT)))
        // `!R.attr.parent.aBool`: r4 and r8 have a parent whose flag is FALSE, so the negation is a
        // determined TRUE for them. The five parentless rows are a missing-path error and stay out,
        // which a two-valued reading of the absent hop would not manage.
        assertEquals(listOf("r4", "r8"), ids(translate("rel-not-bool-hop", WITH_PARENT)))
    }

    @Test
    fun `ordering comparisons through a hop keep the parentless rows out`() {
        assertEquals(listOf("r2"), ids(translate("rel-eq-num-hop", WITH_PARENT)))
        assertEquals(listOf("r4"), ids(translate("rel-lt-hop", WITH_PARENT)))
        assertEquals(listOf("r2", "r4"), ids(translate("rel-le-hop", WITH_PARENT)))
        assertEquals(listOf("r8"), ids(translate("rel-gt-hop", WITH_PARENT)))
        assertEquals(listOf("r2", "r8"), ids(translate("rel-ge-hop", WITH_PARENT)))
        // Two comparisons over the same hop, each reading its own subquery.
        assertEquals(listOf("r8"), ids(translate("rel-range-hop", WITH_PARENT)))
    }

    @Test
    fun `a two-level to-one chain nulls out at whichever level is missing`() {
        // r2 HAS a parent and no inner, so the SECOND level is what makes it UNKNOWN — the case a
        // guard on the first hop alone would miss.
        assertEquals(listOf("r4"), ids(translate("rel-bool-hop2", WITH_PARENT)))
        val sql = render(translate("rel-bool-hop2", WITH_PARENT))
        assertTrue(sql.contains("REL_PARENTS cerbos_1 INNER JOIN REL_INNERS cerbos_2"), sql)
        assertTrue(sql.contains("cerbos_2.PARENT_ID"), sql)
    }

    @Test
    fun `a missing hop makes only its own branch unknown, not the whole disjunction`() {
        // `R.attr.parent.inner.aBool == true || R.attr.categories.exists(c, c.name == "business")`.
        // r2 and r5 hold a "business" category and r5 has no parent at all; if an absent hop removed
        // the row from the WHOLE query rather than leaving one branch UNKNOWN, both would be lost.
        assertEquals(listOf("r2", "r4", "r5"), ids(translate("rel-hop2-or-exists", WITH_PARENT)))
    }

    @Test
    fun `a column reached through a to-many relation is refused rather than aggregated`() {
        val error = runCatching { translate("cs-eq", MEMBER_OF_TO_MANY) }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals(
            "request.resource.attr.aString reads a column through the to-many relation on rel_tags, " +
                "which has one value per element rather than one per row; reach it through a " +
                "collection macro",
            error?.message,
        )
    }

    @Test
    fun `an unmapped member of a mapped relation is refused rather than guessed at a column name`() {
        val error = runCatching { translate("rel-eq-hop", PARENT_WITHOUT_MEMBERS) }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals("Unknown attribute: request.resource.attr.parent.aString", error?.message)
    }

    companion object {
        /** `aString` mapped only as a member of a to-many relation: one value per element. */
        private val MEMBER_OF_TO_MANY = cerbosMapping {
            "request.resource.attr" to many(
                RelTags,
                from = RelResources.id,
                to = RelTags.resourceId,
            ) {
                "aString" to RelTags.name
            }
        }

        private val PARENT_WITHOUT_MEMBERS = cerbosMapping {
            "request.resource.attr.parent" to one(
                SubquerySupportTest.Companion.RelParents,
                from = RelResources.id,
                to = SubquerySupportTest.Companion.RelParents.resourceId,
            )
        }
    }
}
