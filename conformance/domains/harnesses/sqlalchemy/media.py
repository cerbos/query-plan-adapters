from _types import KEY, BOOL, INT, STR, STRLIST, TS, scalar
from sqlalchemy import Column, ForeignKey, func, literal_column
from sqlalchemy.orm import aliased, declarative_base, relationship

from cerbos_sqlalchemy import CollectionColumn, Relation, relation_overrides

Base = declarative_base()


class Author(Base):
    __tablename__ = "authors"
    id = Column(KEY, primary_key=True)
    display_name = Column(STR, nullable=False)
    staff = Column(BOOL, nullable=False)


class Section(Base):
    __tablename__ = "sections"
    slug = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)
    desk = Column(STR, nullable=False)


class ArticleContributor(Base):
    __tablename__ = "article_contributors"
    article_id = Column(KEY, ForeignKey("articles.id"), primary_key=True)
    author_id = Column(KEY, ForeignKey("authors.id"), primary_key=True)


class Article(Base):
    __tablename__ = "articles"
    id = Column(KEY, primary_key=True)
    slug = Column(STR, nullable=False)
    section_slug = Column(KEY, ForeignKey("sections.slug"), nullable=False)
    author_id = Column(KEY, ForeignKey("authors.id"), nullable=False)
    status = Column(STR, nullable=False)
    language = Column(STR, nullable=False)
    embargo_until = Column(TS, nullable=True)
    published_at = Column(TS, nullable=True)
    paywalled = Column(BOOL, nullable=False)
    word_count = Column(INT, nullable=False)
    keywords = Column(STRLIST, nullable=False)
    section = relationship(Section, viewonly=True)
    author = relationship(Author, viewonly=True)
    contributors = relationship(
        Author, secondary=ArticleContributor.__table__, viewonly=True
    )


ROOT = Article
R = "request.resource.attr."


def _keywords_column(dialect):
    storage = "pgArray" if dialect == "postgresql" else "json"
    return {R + "keywords": CollectionColumn(Article.keywords, storage)}


A = Article
Contributor = aliased(Author)
CONTRIBUTORS = Relation(
    [
        ArticleContributor.article_id == A.id,
        Contributor.id == ArticleContributor.author_id,
    ],
    [A],
)


def _keyword_elements(dialect):
    if dialect == "postgresql":
        return func.unnest(A.keywords).table_valued("value").render_derived()
    if dialect == "sqlite":
        return func.json_each(A.keywords).table_valued("value")
    if dialect == "mysql":
        columns = literal_column("'$[*]' COLUMNS (value VARCHAR(255) PATH '$')")
        return func.json_table(A.keywords, columns).table_valued("value").alias("kw")
    return None


def _declared(dialect):
    mapping = {
        R + "slug": A.slug,
        R + "status": A.status,
        R + "language": A.language,
        R + "embargoUntil": A.embargo_until,
        R + "publishedAt": A.published_at,
        R + "paywalled": A.paywalled,
        R + "wordCount": A.word_count,
        R + "section.slug": A.section_slug,
        R + "section.desk": scalar(A, Section.desk, Section.slug == A.section_slug),
        R + "author.id": A.author_id,
        R + "author.staff": scalar(A, Author.staff, Author.id == A.author_id),
        R + "contributors": CONTRIBUTORS,
        "c": CONTRIBUTORS,
        "c.id": Contributor.id,
        "c.staff": Contributor.staff,
    }
    elements = _keyword_elements(dialect)
    if elements is not None:
        keywords = Relation([elements.c.value.is_not(None)], [A], member=elements.c.value)
        mapping.update({R + "keywords": keywords, "k": elements.c.value})
    return mapping


def mapping(style, dialect):
    if style == "inferred":
        return {"attr_map": {}, "collection_columns": _keywords_column(dialect)}
    return {
        "attr_map": _declared(dialect),
        "operator_override_fns": relation_overrides(),
        "collection_columns": _keywords_column(dialect),
    }
