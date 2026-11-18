package dev.cerbos.queryplan.jpa.domains;

import static dev.cerbos.queryplan.jpa.AttributeMapping.field;
import static dev.cerbos.queryplan.jpa.AttributeMapping.relation;
import static dev.cerbos.queryplan.jpa.domains.DomainMapping.omitted;

import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

class Media implements DomainMapping {

    @Entity @Table(name = "authors")
    public static class Author {
        @Id String id;
        String displayName;
        boolean staff;
    }

    @Entity @Table(name = "sections")
    public static class Section {
        @Id String slug;
        String name;
        String desk;
    }

    @Entity @Table(name = "articles")
    public static class Article {
        @Id String id;
        String slug;
        @ManyToOne @JoinColumn(name = "section_slug") Section section;
        @ManyToOne Author author;
        String status;
        String language;
        Instant embargoUntil;
        Instant publishedAt;
        boolean paywalled;
        int wordCount;
        // text[] on PostgreSQL, a JSON array elsewhere: never read, only referenced by the mapping.
        @Basic(fetch = FetchType.LAZY) String keywords;
        @ManyToMany
        @JoinTable(name = "article_contributors",
                joinColumns = @JoinColumn(name = "article_id"),
                inverseJoinColumns = @JoinColumn(name = "author_id"))
        List<Author> contributors;
    }

    public String domain() { return "media"; }

    public List<Class<?>> entities() {
        return List.of(Author.class, Section.class, Article.class);
    }

    public Class<?> root() { return Article.class; }

    public Options options() {
        String a = "request.resource.attr.";
        return Options.of(Map.ofEntries(
                Map.entry(a + "slug", field("slug")),
                Map.entry(a + "status", field("status")),
                Map.entry(a + "language", field("language")),
                Map.entry(a + "embargoUntil", omitted("embargoUntil")),
                Map.entry(a + "publishedAt", omitted("publishedAt")),
                Map.entry(a + "paywalled", field("paywalled")),
                Map.entry(a + "wordCount", field("wordCount")),
                Map.entry(a + "keywords", field("keywords")),
                Map.entry(a + "section.slug", field("section.slug")),
                Map.entry(a + "section.desk", field("section.desk")),
                Map.entry(a + "author.id", field("author.id")),
                Map.entry(a + "author.staff", field("author.staff")),
                Map.entry(a + "contributors", relation("contributors", Map.of()))));
    }
}
