using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Government : Domain<BenefitCase>
{
    public override string Name => "government";
    public override DbContext CreateContext(DbContextOptions options) => new GovernmentContext(options);
    public override Expression<Func<BenefitCase, string>> Id => c => c.Id;
    public override NullConvention Nulls => NullConvention.Explicit;

    public override AttributeMap<BenefitCase> Map { get; } = new AttributeMap<BenefitCase>()
        .Field("request.resource.id", c => c.Id)
        .Field("request.resource.attr.caseNumber", c => c.CaseNumber)
        .Field("request.resource.attr.status", c => c.Status)
        .Field("request.resource.attr.clearanceLevel", c => c.ClearanceLevel)
        .Field("request.resource.attr.program", c => c.Program)
        .Field("request.resource.attr.openedOn", c => c.OpenedOn)
        .Field("request.resource.attr.closedOn", c => c.ClosedOn)
        .Field("request.resource.attr.pii", c => c.Pii)
        .Field("request.resource.attr.assignedTo", c => c.AssignedTo)
        .Field("request.resource.attr.monthlyAmount", c => (double?)c.MonthlyAmount)
        .List("request.resource.attr.flags", c => c.Flags, f => f.Flag)
        .Field("request.resource.attr.agency.code", c => c.Agency.Code)
        .Field("request.resource.attr.agency.state", c => c.Agency.State)
        .Field("request.resource.attr.applicant.countyFips", c => c.Applicant.CountyFips)
        .Field("request.resource.attr.applicant.householdSize", c => c.Applicant.HouseholdSize)
        .Field("request.resource.attr.applicant.citizenship", c => c.Applicant.Citizenship);
}

public class Agency
{
    public string Code { get; set; } = "";
    public string Name { get; set; } = "";
    public string State { get; set; } = "";
}

public class Applicant
{
    public string Id { get; set; } = "";
    public string CountyFips { get; set; } = "";
    public int HouseholdSize { get; set; }
    public string? Citizenship { get; set; }
}

public class BenefitCase
{
    public string Id { get; set; } = "";
    public string CaseNumber { get; set; } = "";
    public string AgencyCode { get; set; } = "";
    public Agency Agency { get; set; } = null!;
    public string ApplicantId { get; set; } = "";
    public Applicant Applicant { get; set; } = null!;
    public string Status { get; set; } = "";
    public int ClearanceLevel { get; set; }
    public string Program { get; set; } = "";
    public DateTime OpenedOn { get; set; }
    public DateTime? ClosedOn { get; set; }
    public bool Pii { get; set; }
    public string? AssignedTo { get; set; }
    public decimal? MonthlyAmount { get; set; }
    public List<CaseFlag> Flags { get; set; } = [];
}

public class CaseFlag
{
    public string CaseId { get; set; } = "";
    public string Flag { get; set; } = "";
}

public sealed class GovernmentContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<Agency>().ToTable("agencies").HasKey(x => x.Code);
        b.Entity<Applicant>().ToTable("applicants");
        b.Entity<BenefitCase>().ToTable("cases").HasMany(x => x.Flags).WithOne().HasForeignKey(x => x.CaseId);
        b.Entity<CaseFlag>().ToTable("case_flags").HasKey(x => new { x.CaseId, x.Flag });
    }
}
