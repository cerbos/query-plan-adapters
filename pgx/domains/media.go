package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

// keywords is a native text[] column, which no Entry can read: it stays unmapped.
var Media = func() Domain {
	section := join{"sections", "section_slug", "slug"}
	author := join{"authors", "author_id", "id"}
	a := newAttrs(cerbospgx.NullConventionOmitted).
		col("id", "id", str).
		col("attr.slug", "slug", str).
		col("attr.status", "status", str).
		col("attr.language", "language", str).
		col("attr.embargoUntil", "embargo_until", ts).
		col("attr.publishedAt", "published_at", ts).
		col("attr.paywalled", "paywalled", boo).
		col("attr.wordCount", "word_count", num).
		via("section.slug", path(section), "slug", str).
		via("section.desk", path(section), "desk", str).
		via("author.id", path(author), "id", str).
		via("author.staff", path(author), "staff", boo).
		objects("contributors", path(join{"article_contributors", "id", "article_id"}, author), map[string]cerbospgx.Entry{
			"id": f("id", str), "staff": f("staff", boo),
		})
	return Domain{Name: "media", Table: "articles", ID: "id", Nulls: cerbospgx.NullOmitted, Mapper: a.m}
}()
