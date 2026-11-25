from _kit import BOOL, NUM, STR, TS, Attrs, f, path

ROOT, ID = "articles_v", "id"

# keywords is a native VARCHAR[] column, so it maps as it is stored.
VIEWS = """
CREATE VIEW articles_v AS
SELECT a.*,
    (SELECT coalesce(list({'id': au.id, 'staff': au.staff} ORDER BY au.id), [])
        FROM article_contributors c JOIN authors au ON au.id = c.author_id
        WHERE c.article_id = a.id) AS contributors
FROM articles a
"""


def mapping(nulls):
    section = ("sections", "section_slug", "slug")
    author = ("authors", "author_id", "id")
    return (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.slug", "slug", STR)
        .col("attr.status", "status", STR)
        .col("attr.language", "language", STR)
        .col("attr.embargoUntil", "embargo_until", TS)
        .col("attr.publishedAt", "published_at", TS)
        .col("attr.paywalled", "paywalled", BOOL)
        .col("attr.wordCount", "word_count", NUM)
        .values("keywords", "keywords", STR)
        .via("section.slug", path(section), "slug", STR)
        .via("section.desk", path(section), "desk", STR)
        .via("author.id", path(author), "id", STR)
        .via("author.staff", path(author), "staff", BOOL)
        .objects(
            "contributors",
            "contributors",
            {"id": f("id", STR), "staff": f("staff", BOOL)},
        )
        .m
    )
