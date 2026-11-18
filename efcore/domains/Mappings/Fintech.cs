using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Fintech : Domain<Payment>
{
    public override string Name => "fintech";
    public override DbContext CreateContext(DbContextOptions options) => new FintechContext(options);
    public override Expression<Func<Payment, string>> Id => p => p.Id;

    public override AttributeMap<Payment> Map { get; } = new AttributeMap<Payment>()
        .Field("request.resource.id", p => p.Id)
        .Field("request.resource.attr.amount", p => (double)p.Amount)
        .Field("request.resource.attr.currency", p => p.Currency)
        .Field("request.resource.attr.status", p => p.Status)
        .Field("request.resource.attr.riskScore", p => p.RiskScore)
        .Field("request.resource.attr.createdAt", p => p.CreatedAt)
        .Field("request.resource.attr.initiatedBy", p => p.InitiatedBy)
        .Field("request.resource.attr.counterparty.name", p => p.Counterparty.Name)
        .Field("request.resource.attr.counterparty.iban", p => p.Counterparty.Iban)
        .Field("request.resource.attr.counterparty.country", p => p.Counterparty.Country)
        .List("request.resource.attr.flags", p => p.Flags, f => f.Flag)
        .List("request.resource.attr.approvals", p => p.Approvals, a => a
            .Field("approverId", x => x.ApproverId)
            .Field("decision", x => x.Decision))
        .Field("request.resource.attr.account.currency", p => p.Account.Currency)
        .Field("request.resource.attr.account.status", p => p.Account.Status)
        .Field("request.resource.attr.account.overdraftLimit", p => (double?)p.Account.OverdraftLimit)
        .Field("request.resource.attr.account.customer.id", p => p.Account.Customer.Id)
        .Field("request.resource.attr.account.customer.kycLevel", p => p.Account.Customer.KycLevel)
        .Field("request.resource.attr.account.customer.pep", p => p.Account.Customer.Pep)
        .Field("request.resource.attr.account.customer.branch.code", p => p.Account.Customer.Branch.Code)
        .Field("request.resource.attr.account.customer.branch.country", p => p.Account.Customer.Branch.Country);
}

public class Branch
{
    public string Code { get; set; } = "";
    public string Country { get; set; } = "";
}

public class Customer
{
    public string Id { get; set; } = "";
    public string LegalName { get; set; } = "";
    public int KycLevel { get; set; }
    public string BranchCode { get; set; } = "";
    public Branch Branch { get; set; } = null!;
    public bool? Pep { get; set; }
}

public class Account
{
    public string Id { get; set; } = "";
    public string CustomerId { get; set; } = "";
    public Customer Customer { get; set; } = null!;
    public string Currency { get; set; } = "";
    public string Status { get; set; } = "";
    public decimal? OverdraftLimit { get; set; }
}

public class Counterparty
{
    public string Name { get; set; } = "";
    public string Iban { get; set; } = "";
    public string Country { get; set; } = "";
}

public class Payment
{
    public string Id { get; set; } = "";
    public string AccountId { get; set; } = "";
    public Account Account { get; set; } = null!;
    public decimal Amount { get; set; }
    public string Currency { get; set; } = "";
    public string Status { get; set; } = "";
    public double? RiskScore { get; set; }
    public DateTime CreatedAt { get; set; }
    public Counterparty Counterparty { get; set; } = null!;
    public string InitiatedBy { get; set; } = "";
    public List<PaymentFlag> Flags { get; set; } = [];
    public List<PaymentApproval> Approvals { get; set; } = [];
}

public class PaymentFlag
{
    public string PaymentId { get; set; } = "";
    public string Flag { get; set; } = "";
}

public class PaymentApproval
{
    public string Id { get; set; } = "";
    public string PaymentId { get; set; } = "";
    public string ApproverId { get; set; } = "";
    public string Decision { get; set; } = "";
}

public sealed class FintechContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<Branch>().ToTable("branches").HasKey(x => x.Code);
        b.Entity<Customer>().ToTable("customers");
        b.Entity<Account>().ToTable("accounts");
        b.Entity<Payment>(e =>
        {
            e.ToTable("payments");
            e.OwnsOne(x => x.Counterparty, c =>
            {
                c.Property(x => x.Name).HasColumnName("cp_name");
                c.Property(x => x.Iban).HasColumnName("cp_iban");
                c.Property(x => x.Country).HasColumnName("cp_country");
            });
            e.Navigation(x => x.Counterparty).IsRequired();
            e.HasMany(x => x.Flags).WithOne().HasForeignKey(x => x.PaymentId);
            e.HasMany(x => x.Approvals).WithOne().HasForeignKey(x => x.PaymentId);
        });
        b.Entity<PaymentFlag>().ToTable("payment_flags").HasKey(x => new { x.PaymentId, x.Flag });
        b.Entity<PaymentApproval>().ToTable("payment_approvals");
    }
}
