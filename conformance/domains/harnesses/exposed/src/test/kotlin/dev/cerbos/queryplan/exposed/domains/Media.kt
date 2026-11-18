package dev.cerbos.queryplan.exposed.domains

import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.EXPLICIT
import dev.cerbos.queryplan.exposed.NullAttributeRepresentation.OMITTED
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

internal object Media : ExposedDomain {
    object Authors : Table("authors") {
        val id = varchar("id", 255)
        val displayName = varchar("display_name", 255)
        val staff = bool("staff")
        override val primaryKey = PrimaryKey(id)
    }

    object Sections : Table("sections") {
        val slug = varchar("slug", 255)
        val name = varchar("name", 255)
        val desk = varchar("desk", 255)
        override val primaryKey = PrimaryKey(slug)
    }

    object Articles : Table("articles") {
        val id = varchar("id", 255)
        val slug = varchar("slug", 255)
        val sectionSlug = varchar("section_slug", 255).references(Sections.slug)
        val authorId = varchar("author_id", 255).references(Authors.id)
        val status = varchar("status", 255)
        val language = varchar("language", 255)
        val embargoUntil = timestamp("embargo_until").nullable()
        val publishedAt = timestamp("published_at").nullable()
        val paywalled = bool("paywalled")
        val wordCount = integer("word_count")
        // text[] on PostgreSQL, a JSON array elsewhere; no relation can read its elements.
        val keywords = text("keywords")
        override val primaryKey = PrimaryKey(id)
    }

    object Contributors : Table("article_contributors") {
        val articleId = varchar("article_id", 255).references(Articles.id)
        val authorId = varchar("author_id", 255).references(Authors.id)
        override val primaryKey = PrimaryKey(articleId, authorId)
    }

    override val name = "media"
    override val tables = listOf(Authors, Sections, Articles, Contributors)
    override val root = Articles
    override val id = Articles.id

    private const val A = "request.resource.attr"
    private val mapping = cerbosMapping(root = Articles) {
        "$A.slug" to Articles.slug
        "$A.status" to Articles.status
        "$A.language" to Articles.language
        "$A.embargoUntil" to field(Articles.embargoUntil, OMITTED)
        "$A.publishedAt" to field(Articles.publishedAt, OMITTED)
        "$A.paywalled" to Articles.paywalled
        "$A.wordCount" to Articles.wordCount
        "$A.keywords" to Articles.keywords
        "$A.section" to relation(Sections) {
            "slug" to Sections.slug
            "desk" to Sections.desk
        }
        "$A.author" to relation(via = Articles.authorId) {
            "id" to Authors.id
            "staff" to Authors.staff
        }
        // The join-table rows are the elements; an author's own columns (staff) are out of reach.
        "$A.contributors" to relation(Contributors, orderBy = Contributors.authorId) {
            "id" to Contributors.authorId
        }
    }

    override fun options() = Options.of(mapping)
}
