import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function media(k: Kit): DomainMapping {
  const authors = k.table("authors", {
    id: k.text("id").primaryKey(),
    displayName: k.text("display_name").notNull(),
    staff: k.bool("staff").notNull(),
  });

  const sections = k.table("sections", {
    slug: k.text("slug").primaryKey(),
    name: k.text("name").notNull(),
    desk: k.text("desk").notNull(),
  });

  const articles = k.table("articles", {
    id: k.text("id").primaryKey(),
    slug: k.text("slug").notNull(),
    sectionSlug: k.text("section_slug").notNull().references(() => sections.slug),
    authorId: k.text("author_id").notNull().references(() => authors.id),
    status: k.text("status").notNull(),
    language: k.text("language").notNull(),
    embargoUntil: k.timestamp("embargo_until"),
    publishedAt: k.timestamp("published_at"),
    paywalled: k.bool("paywalled").notNull(),
    wordCount: k.int("word_count").notNull(),
    keywords: k.stringArray("keywords").notNull(),
  });

  const articleContributors = k.table(
    "article_contributors",
    {
      articleId: k.text("article_id").notNull().references(() => articles.id),
      authorId: k.text("author_id").notNull().references(() => authors.id),
    },
    ["articleId", "authorId"]
  );

  const schema = {
    authors,
    sections,
    articles,
    articleContributors,
    articlesRelations: relations(articles, ({ one, many }) => ({
      section: one(sections, { fields: [articles.sectionSlug], references: [sections.slug] }),
      author: one(authors, { fields: [articles.authorId], references: [authors.id] }),
      contributors: many(articleContributors),
    })),
    articleContributorsRelations: relations(articleContributors, ({ one }) => ({
      article: one(articles, { fields: [articleContributors.articleId], references: [articles.id] }),
      author: one(authors, { fields: [articleContributors.authorId], references: [authors.id] }),
    })),
  };

  return {
    table: articles,
    id: articles.id,
    schema,
    mapper: {
      "request.resource.attr.embargoUntil": { column: articles.embargoUntil, valueType: "timestamp" },
      "request.resource.attr.publishedAt": { column: articles.publishedAt, valueType: "timestamp" },
      "request.resource.attr.keywords": {
        column: articles.keywords,
        indexable: k.dialect === "postgres" ? "pgArray" : "json",
      },
      "request.resource.attr.contributors": {
        relation: {
          name: "contributors",
          fields: {
            id: articleContributors.authorId,
            staff: { relation: { name: "author", field: authors.staff } },
          },
        },
      },
    },
  };
}
