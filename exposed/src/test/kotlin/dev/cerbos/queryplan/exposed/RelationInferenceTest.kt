package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.AttributeMapping.Relation.Cardinality
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.dao.id.IntIdTable
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

/**
 * `relation(...)` reads keys and cardinality from the Exposed schema. The corpus harness proves the
 * inferred mapping returns the PDP's rows; this pins what the schema is NOT allowed to imply: a
 * to-one is inferred only from a provably unique key, and a key that is missing, composite or
 * ambiguous is a construction error rather than a guess.
 */
class RelationInferenceTest {

    private object Docs : Table("infer_docs") {
        val id = varchar("id", 32)
        val ownerId = varchar("owner_id", 32).references(Users.id)
        override val primaryKey = PrimaryKey(id)
    }

    private object Users : Table("infer_users") {
        val id = varchar("id", 32)
        val name = varchar("name", 64)
        override val primaryKey = PrimaryKey(id)
    }

    private object Tags : Table("infer_tags") {
        val id = varchar("id", 32)
        val docId = varchar("doc_id", 32).references(Docs.id)
        val name = varchar("name", 64)
        val position = integer("position")
    }

    private object UniqueParents : Table("infer_unique_parents") {
        val docId = varchar("doc_id", 32).references(Docs.id).uniqueIndex()
        val label = varchar("label", 64)
    }

    private object LooseParents : Table("infer_loose_parents") {
        val docId = varchar("doc_id", 32).references(Docs.id)
        val label = varchar("label", 64)
    }

    private object PartialParents : Table("infer_partial_parents") {
        val docId = varchar("doc_id", 32).references(Docs.id)
        val deleted = bool("deleted")

        init {
            uniqueIndex("infer_partial_live", docId, filterCondition = { deleted eq false })
        }
    }

    private object PairParents : Table("infer_pair_parents") {
        val docId = varchar("doc_id", 32).references(Docs.id)
        val kind = varchar("kind", 8)

        init {
            uniqueIndex(docId, kind)
        }
    }

    private object Reviews : Table("infer_reviews") {
        val authorDocId = varchar("author_doc_id", 32).references(Docs.id)
        val subjectDocId = varchar("subject_doc_id", 32).references(Docs.id)
    }

    private object Nodes : Table("infer_nodes") {
        val id = varchar("id", 32)
        val parentId = varchar("parent_id", 32).references(id).nullable()
        override val primaryKey = PrimaryKey(id)
    }

    private object Composite : Table("infer_composite") {
        val a = varchar("a", 8)
        val b = varchar("b", 8)
        override val primaryKey = PrimaryKey(a, b)
    }

    private object CompositeChildren : Table("infer_composite_children") {
        val a = varchar("a", 8)
        val b = varchar("b", 8)

        init {
            foreignKey(a to Composite.a, b to Composite.b)
        }
    }

    private object Unrelated : Table("infer_unrelated") {
        val id = varchar("id", 32)
    }

    private object Authors : IntIdTable("infer_authors")

    private object Books : IntIdTable("infer_books") {
        val author = reference("author_id", Authors)
    }

    private fun relationOf(mapping: AttributeMappings, reference: String = "r") =
        mapping.resolve(reference) as AttributeMapping.Relation

    @Test
    fun `a child table pointing back at the scope is a collection`() {
        val tags = relationOf(cerbosMapping(root = Docs) { "r" to relation(Tags, element = Tags.name, orderBy = Tags.position) })
        assertEquals(Cardinality.MANY, tags.cardinality)
        assertSame(Docs.id, tags.from)
        assertSame(Tags.docId, tags.to)
        assertSame(Tags.name, tags.element!!.column)
        assertSame(Tags.position, tags.orderBy)
    }

    @Test
    fun `a child key with a unique index is to-one`() {
        val parent = relationOf(cerbosMapping(root = Docs) { "r" to relation(UniqueParents) })
        assertEquals(Cardinality.ONE, parent.cardinality)
        assertSame(Docs.id, parent.from)
        assertSame(UniqueParents.docId, parent.to)
    }

    @Test
    fun `a child key without a unique index stays a collection`() {
        assertEquals(Cardinality.MANY, relationOf(cerbosMapping(root = Docs) { "r" to relation(LooseParents) }).cardinality)
    }

    @Test
    fun `a partial unique index does not make a key to-one`() {
        assertEquals(Cardinality.MANY, relationOf(cerbosMapping(root = Docs) { "r" to relation(PartialParents) }).cardinality)
    }

    @Test
    fun `a composite unique index does not make one of its columns to-one`() {
        assertEquals(Cardinality.MANY, relationOf(cerbosMapping(root = Docs) { "r" to relation(PairParents) }).cardinality)
    }

    @Test
    fun `a reference from the scope is to-one through the referenced primary key`() {
        val owner = relationOf(cerbosMapping(root = Docs) { "r" to relation(Users) { "name" to Users.name } })
        assertEquals(Cardinality.ONE, owner.cardinality)
        assertSame(Docs.ownerId, owner.from)
        assertSame(Users.id, owner.to)
    }

    @Test
    fun `DAO id tables infer through their entity id columns`() {
        val author = relationOf(cerbosMapping(root = Books) { "r" to relation(Authors) })
        assertEquals(Cardinality.ONE, author.cardinality)
        assertSame(Books.author, author.from)
        assertSame(Authors.id, author.to)

        val books = relationOf(cerbosMapping(root = Authors) { "r" to relation(Books) })
        assertEquals(Cardinality.MANY, books.cardinality)
        assertSame(Authors.id, books.from)
        assertSame(Books.author, books.to)
    }

    @Test
    fun `nested relations infer against the enclosing relation's table`() {
        val chain = relationOf(
            cerbosMapping(root = Users) {
                "r" to relation(Docs) { "tags" to relation(Tags) }
            },
        )
        assertEquals(Cardinality.MANY, chain.cardinality)
        val tags = chain.fields.getValue("tags") as AttributeMapping.Relation
        assertSame(Docs.id, tags.from)
        assertSame(Tags.docId, tags.to)
    }

    @Test
    fun `two keys to the same table are ambiguous until one is named`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping(root = Docs) { "r" to relation(Reviews) }
        }
        assertEquals(
            "Several foreign keys link infer_docs and infer_reviews (infer_reviews.author_doc_id = infer_docs.id (MANY), " +
                "infer_reviews.subject_doc_id = infer_docs.id (MANY)): name one with relation(via = ...), " +
                "or declare the relation with one(...) or many(...)",
            error.message,
        )
        val authored = relationOf(cerbosMapping(root = Docs) { "r" to relation(via = Reviews.authorDocId) })
        assertSame(Reviews.authorDocId, authored.to)
        assertSame(Docs.id, authored.from)
    }

    @Test
    fun `a self reference is ambiguous by table and directed by its column`() {
        assertThrows(IllegalArgumentException::class.java) { cerbosMapping(root = Nodes) { "r" to relation(Nodes) } }
        val parent = relationOf(cerbosMapping(root = Nodes) { "r" to relation(via = Nodes.parentId) })
        assertEquals(Cardinality.ONE, parent.cardinality)
        assertSame(Nodes.parentId, parent.from)
        assertSame(Nodes.id, parent.to)
    }

    @Test
    fun `a composite key is not inferred`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping(root = Composite) { "r" to relation(CompositeChildren) }
        }
        assertEquals(
            "A foreign key between infer_composite and infer_composite_children is composite: declare the relation with one(...) or many(...)",
            error.message,
        )
    }

    @Test
    fun `no key is a construction error`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping(root = Docs) { "r" to relation(Unrelated) }
        }
        assertEquals(
            "No foreign key links infer_docs and infer_unrelated: declare the relation with one(...) or many(...)",
            error.message,
        )
    }

    @Test
    fun `a via column unrelated to the scope is refused`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping(root = Users) { "r" to relation(via = Tags.docId) }
        }
        assertEquals("infer_tags.doc_id neither belongs to nor references infer_users", error.message)
    }

    @Test
    fun `a top-level relation needs the root table`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping { "r" to relation(Tags) }
        }
        assertEquals(
            "relation(infer_tags) at the top level needs the root table: build the mapping with cerbosMapping(root = ...)",
            error.message,
        )
    }

    @Test
    fun `a to-one takes no element or order`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping(root = Docs) { "r" to relation(UniqueParents, element = UniqueParents.label) }
        }
        assertEquals(
            "infer_unique_parents.doc_id = infer_docs.id (ONE) is to-one, so it takes no element or orderBy",
            error.message,
        )
    }

    @Test
    fun `a declared root checks every top-level key`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping(root = Users) { "r" to many(Tags, from = Docs.id, to = Tags.docId) }
        }
        assertEquals("Relation key infer_docs.id is not a column of infer_users", error.message)
    }
}
