using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Media : Domain<Article>
{
    public override string Name => "media";
    public override DbContext CreateContext(DbContextOptions options) => new MediaContext(options);
    public override Expression<Func<Article, string>> Id => a => a.Id;

    public override AttributeMap<Article> Map { get; } = new AttributeMap<Article>()
        .Field("request.resource.id", a => a.Id)
        .Field("request.resource.attr.slug", a => a.Slug)
        .Field("request.resource.attr.status", a => a.Status)
        .Field("request.resource.attr.language", a => a.Language)
        .Field("request.resource.attr.embargoUntil", a => a.EmbargoUntil)
        .Field("request.resource.attr.publishedAt", a => a.PublishedAt)
        .Field("request.resource.attr.paywalled", a => a.Paywalled)
        .Field("request.resource.attr.wordCount", a => a.WordCount)
        .List("request.resource.attr.keywords", a => a.Keywords, k => k)
        .Field("request.resource.attr.section.slug", a => a.Section.Slug)
        .Field("request.resource.attr.section.desk", a => a.Section.Desk)
        .Field("request.resource.attr.author.id", a => a.Author.Id)
        .Field("request.resource.attr.author.staff", a => a.Author.Staff)
        .List("request.resource.attr.contributors", a => a.Contributors, c => c
            .Field("id", x => x.Id)
            .Field("staff", x => x.Staff));
}

public class Author
{
    public string Id { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public bool Staff { get; set; }
}

public class Section
{
    public string Slug { get; set; } = "";
    public string Name { get; set; } = "";
    public string Desk { get; set; } = "";
}

public class Article
{
    public string Id { get; set; } = "";
    public string Slug { get; set; } = "";
    public string SectionSlug { get; set; } = "";
    public Section Section { get; set; } = null!;
    public string AuthorId { get; set; } = "";
    public Author Author { get; set; } = null!;
    public string Status { get; set; } = "";
    public string Language { get; set; } = "";
    public DateTime? EmbargoUntil { get; set; }
    public DateTime? PublishedAt { get; set; }
    public bool Paywalled { get; set; }
    public int WordCount { get; set; }
    public List<string> Keywords { get; set; } = [];
    public List<Author> Contributors { get; set; } = [];
}

public sealed class MediaContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<Author>().ToTable("authors");
        b.Entity<Section>().ToTable("sections").HasKey(x => x.Slug);
        b.Entity<Article>(e =>
        {
            e.ToTable("articles");
            e.HasOne(x => x.Author).WithMany().HasForeignKey(x => x.AuthorId);
            e.HasMany(x => x.Contributors).WithMany().UsingEntity(
                "article_contributors",
                r => r.HasOne(typeof(Author)).WithMany().HasForeignKey("author_id"),
                l => l.HasOne(typeof(Article)).WithMany().HasForeignKey("article_id"));
        });
    }
}
