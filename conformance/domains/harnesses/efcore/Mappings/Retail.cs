using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Retail : Domain<Order>
{
    public override string Name => "retail";
    public override DbContext CreateContext(DbContextOptions options) => new RetailContext(options);
    public override Expression<Func<Order, string>> Id => o => o.Id;

    public override AttributeMap<Order> Map { get; } = new AttributeMap<Order>()
        .Field("request.resource.id", o => o.Id)
        .Field("request.resource.attr.status", o => o.Status)
        .Field("request.resource.attr.channel", o => o.Channel)
        .Field("request.resource.attr.totalCents", o => o.TotalCents)
        .Field("request.resource.attr.currency", o => o.Currency)
        .Field("request.resource.attr.couponCode", o => o.CouponCode)
        .Field("request.resource.attr.placedAt", o => o.PlacedAt)
        .Field("request.resource.attr.isGift", o => o.Gift)
        .Field("request.resource.attr.customer.id", o => o.Customer.Id)
        .Field("request.resource.attr.customer.tier", o => o.Customer.Tier)
        .Field("request.resource.attr.customer.loyaltyPoints", o => o.Customer.LoyaltyPoints)
        .Field("request.resource.attr.store.region", o => o.Store!.Region)
        .Field("request.resource.attr.store.country", o => o.Store!.Country)
        .List("request.resource.attr.lines", o => o.Lines, l => l
            .Field("sku", x => x.Sku)
            .Field("category", x => x.Category)
            .Field("quantity", x => x.Quantity)
            .Field("unitPriceCents", x => x.UnitPriceCents));
}

public class RetailStore
{
    public string Id { get; set; } = "";
    public string Region { get; set; } = "";
    public string Country { get; set; } = "";
}

public class RetailCustomer
{
    public string Id { get; set; } = "";
    public string Email { get; set; } = "";
    public string Tier { get; set; } = "";
    public int LoyaltyPoints { get; set; }
}

public class Order
{
    public string Id { get; set; } = "";
    public string CustomerId { get; set; } = "";
    public RetailCustomer Customer { get; set; } = null!;
    public string? StoreId { get; set; }
    public RetailStore? Store { get; set; }
    public string Status { get; set; } = "";
    public string Channel { get; set; } = "";
    public long TotalCents { get; set; }
    public string Currency { get; set; } = "";
    public string? CouponCode { get; set; }
    public DateTime PlacedAt { get; set; }
    public bool Gift { get; set; }
    public List<OrderLine> Lines { get; set; } = [];
}

public class OrderLine
{
    public string Id { get; set; } = "";
    public string OrderId { get; set; } = "";
    public int LineNo { get; set; }
    public string Sku { get; set; } = "";
    public string Category { get; set; } = "";
    public int Quantity { get; set; }
    public long UnitPriceCents { get; set; }
}

public sealed class RetailContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<RetailStore>().ToTable("stores");
        b.Entity<RetailCustomer>().ToTable("customers");
        b.Entity<Order>().ToTable("order").HasMany(x => x.Lines).WithOne().HasForeignKey(x => x.OrderId);
        b.Entity<OrderLine>().ToTable("order_line");
    }
}
