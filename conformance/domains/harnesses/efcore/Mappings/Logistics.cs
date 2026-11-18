using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Logistics : Domain<Shipment>
{
    public override string Name => "logistics";
    public override DbContext CreateContext(DbContextOptions options) => new LogisticsContext(options);
    public override Expression<Func<Shipment, string>> Id => s => s.Id;

    public override AttributeMap<Shipment> Map { get; } = new AttributeMap<Shipment>()
        .Field("request.resource.id", s => s.Id)
        .Field("request.resource.attr.trackingNumber", s => s.TrackingNo)
        .Field("request.resource.attr.status", s => s.Status)
        .Field("request.resource.attr.weightKg", s => s.WeightKg)
        .Field("request.resource.attr.hazmat", s => s.Hazmat)
        .Field("request.resource.attr.customsHold", s => s.CustomsHold)
        .Field("request.resource.attr.shipper", s => s.ShipperAccount)
        .Field("request.resource.attr.eta", s => s.Eta)
        .Field("request.resource.attr.declaredValue", s => s.DeclaredValue)
        .Field("request.resource.attr.origin.code", s => s.OriginHub.Code)
        .Field("request.resource.attr.origin.country", s => s.OriginHub.Country)
        .Field("request.resource.attr.origin.region", s => s.OriginHub.Region)
        .Field("request.resource.attr.destination.code", s => s.DestinationHub.Code)
        .Field("request.resource.attr.destination.country", s => s.DestinationHub.Country)
        .Field("request.resource.attr.destination.region", s => s.DestinationHub.Region)
        .Field("request.resource.attr.carrier.scac", s => s.Carrier!.Scac)
        .List("request.resource.attr.handling", s => s.Handling, h => h.Code)
        .List("request.resource.attr.checkpoints", s => s.Checkpoints, c => c
            .Field("event", x => x.Event)
            .Field("scannedAt", x => x.ScannedAt)
            .Field("hub.code", x => x.Hub.Code));
}

public class Hub
{
    public string Id { get; set; } = "";
    public string Code { get; set; } = "";
    public string Country { get; set; } = "";
    public string Region { get; set; } = "";
}

public class Carrier
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Scac { get; set; } = "";
}

public class Shipment
{
    public string Id { get; set; } = "";
    public string TrackingNo { get; set; } = "";
    public string Status { get; set; } = "";
    public string OriginHubId { get; set; } = "";
    public Hub OriginHub { get; set; } = null!;
    public string DestinationHubId { get; set; } = "";
    public Hub DestinationHub { get; set; } = null!;
    public string? CarrierId { get; set; }
    public Carrier? Carrier { get; set; }
    public double WeightKg { get; set; }
    public bool Hazmat { get; set; }
    public bool? CustomsHold { get; set; }
    public string ShipperAccount { get; set; } = "";
    public DateTime? Eta { get; set; }
    public decimal DeclaredValue { get; set; }
    public List<ShipmentHandling> Handling { get; set; } = [];
    public List<Checkpoint> Checkpoints { get; set; } = [];
}

public class ShipmentHandling
{
    public string ShipmentId { get; set; } = "";
    public string Code { get; set; } = "";
}

public class Checkpoint
{
    public string Id { get; set; } = "";
    public string ShipmentId { get; set; } = "";
    public string HubId { get; set; } = "";
    public Hub Hub { get; set; } = null!;
    public string Event { get; set; } = "";
    public DateTime ScannedAt { get; set; }
}

public sealed class LogisticsContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<Hub>().ToTable("hubs");
        b.Entity<Carrier>().ToTable("carriers");
        b.Entity<Shipment>(e =>
        {
            e.ToTable("shipments");
            e.HasOne(x => x.OriginHub).WithMany().HasForeignKey(x => x.OriginHubId).OnDelete(DeleteBehavior.NoAction);
            e.HasOne(x => x.DestinationHub).WithMany().HasForeignKey(x => x.DestinationHubId).OnDelete(DeleteBehavior.NoAction);
            e.HasMany(x => x.Handling).WithOne().HasForeignKey(x => x.ShipmentId);
            e.HasMany(x => x.Checkpoints).WithOne().HasForeignKey(x => x.ShipmentId);
        });
        b.Entity<ShipmentHandling>().ToTable("shipment_handling").HasKey(x => new { x.ShipmentId, x.Code });
        b.Entity<Checkpoint>().ToTable("checkpoints");
    }
}
