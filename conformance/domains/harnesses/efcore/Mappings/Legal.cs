using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Legal : Domain<LegalDocument>
{
    public override string Name => "legal";
    public override DbContext CreateContext(DbContextOptions options) => new LegalContext(options);
    public override Expression<Func<LegalDocument, string>> Id => d => d.Id;
    public override NullConvention Nulls => NullConvention.Explicit;

    public override AttributeMap<LegalDocument> Map { get; } = new AttributeMap<LegalDocument>()
        .Field("request.resource.id", d => d.Id)
        .Field("request.resource.attr.title", d => d.Title)
        .Field("request.resource.attr.privilege", d => d.Privilege)
        .Field("request.resource.attr.classification", d => d.Classification)
        .Field("request.resource.attr.legalHold", d => d.LegalHold)
        .Field("request.resource.attr.createdBy", d => d.CreatedBy)
        .Field("request.resource.attr.retentionUntil", d => d.RetentionUntil)
        .List("request.resource.attr.shares", d => d.Shares, s => s
            .Field("partyType", x => x.PartyType)
            .Field("partyId", x => x.PartyId))
        .Field("request.resource.attr.matter.status", d => d.Matter.Status)
        .Field("request.resource.attr.matter.practiceArea", d => d.Matter.PracticeArea)
        .Field("request.resource.attr.matter.leadPartnerId", d => d.Matter.LeadPartnerId)
        .List("request.resource.attr.matter.team", d => d.Matter.Team, t => t.UserId)
        .List("request.resource.attr.matter.screened", d => d.Matter.Screens, s => s.UserId)
        .Field("request.resource.attr.matter.client.id", d => d.Matter.Client.Id)
        .Field("request.resource.attr.matter.client.industry", d => d.Matter.Client.Industry);
}

public class Client
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string? Industry { get; set; }
}

public class Matter
{
    public string Id { get; set; } = "";
    public string ClientId { get; set; } = "";
    public Client Client { get; set; } = null!;
    public string PracticeArea { get; set; } = "";
    public string Status { get; set; } = "";
    public DateTime OpenedAt { get; set; }
    public string? LeadPartnerId { get; set; }
    public List<MatterTeam> Team { get; set; } = [];
    public List<MatterScreen> Screens { get; set; } = [];
}

public class MatterTeam
{
    public string MatterId { get; set; } = "";
    public string UserId { get; set; } = "";
}

public class MatterScreen
{
    public string MatterId { get; set; } = "";
    public string UserId { get; set; } = "";
}

public class LegalDocument
{
    public string Id { get; set; } = "";
    public string MatterId { get; set; } = "";
    public Matter Matter { get; set; } = null!;
    public string Title { get; set; } = "";
    public string? Privilege { get; set; }
    public int Classification { get; set; }
    public bool LegalHold { get; set; }
    public string CreatedBy { get; set; } = "";
    public DateTime? RetentionUntil { get; set; }
    public List<DocumentShare> Shares { get; set; } = [];
}

public class DocumentShare
{
    public string Id { get; set; } = "";
    public string DocumentId { get; set; } = "";
    public string PartyType { get; set; } = "";
    public string PartyId { get; set; } = "";
}

public sealed class LegalContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<Client>().ToTable("clients");
        b.Entity<Matter>(e =>
        {
            e.ToTable("matters");
            e.HasMany(x => x.Team).WithOne().HasForeignKey(x => x.MatterId);
            e.HasMany(x => x.Screens).WithOne().HasForeignKey(x => x.MatterId);
        });
        b.Entity<MatterTeam>().ToTable("matter_team").HasKey(x => new { x.MatterId, x.UserId });
        b.Entity<MatterScreen>().ToTable("matter_screens").HasKey(x => new { x.MatterId, x.UserId });
        b.Entity<LegalDocument>().ToTable("documents").HasMany(x => x.Shares).WithOne().HasForeignKey(x => x.DocumentId);
        b.Entity<DocumentShare>().ToTable("document_shares");
    }
}
