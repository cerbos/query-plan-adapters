from _frames import many, one

R = "request.resource.attr."
ID = "id"


def frame(t):
    a = one(
        t["articles"],
        "section_slug",
        t["sections"],
        "slug",
        "section",
        slug="slug",
        desk="desk",
    )
    a = one(a, "author_id", t["authors"], "id", "author", id="id", staff="staff")
    contributors = t["article_contributors"].join(
        t["authors"], left_on="author_id", right_on="id", how="inner"
    )
    return many(
        a,
        "id",
        contributors,
        "article_id",
        "contributors",
        "author_id",
        id="author_id",
        staff="staff",
    )


ATTR_MAP = {
    R + "embargoUntil": "embargo_until",
    R + "publishedAt": "published_at",
    R + "wordCount": "word_count",
}
