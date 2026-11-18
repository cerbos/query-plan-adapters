using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public sealed class Iot : Domain<Device>
{
    public override string Name => "iot";
    public override DbContext CreateContext(DbContextOptions options) => new IotContext(options);
    public override Expression<Func<Device, string>> Id => d => d.Id;

    public override AttributeMap<Device> Map { get; } = new AttributeMap<Device>()
        .Field("request.resource.id", d => d.Id)
        .Field("request.resource.attr.serial", d => d.Serial)
        .Field("request.resource.attr.model", d => d.Model)
        .Field("request.resource.attr.firmware", d => d.Firmware)
        .Field("request.resource.attr.lastSeenAt", d => d.LastSeenAt)
        .Field("request.resource.attr.online", d => d.Online)
        .Field("request.resource.attr.batteryPct", d => d.BatteryPct)
        .Field("request.resource.attr.labels.env", d => d.Labels.Env)
        .Field("request.resource.attr.labels.tier", d => d.Labels.Tier)
        .Field("request.resource.attr.labels.restricted", d => d.Labels.Restricted)
        .Field("request.resource.attr.decommissioned", d => d.Decommissioned)
        .Field("request.resource.attr.site.region", d => d.Site.Region)
        .Field("request.resource.attr.site.org.id", d => d.Site.Org.Id)
        .Field("request.resource.attr.site.org.plan", d => d.Site.Org.Plan)
        .List("request.resource.attr.alerts", d => d.Alerts, a => a
            .Field("severity", x => x.Severity)
            .Field("acknowledged", x => x.Acknowledged)
            .Field("code", x => x.Code));
}

public class Org
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Plan { get; set; } = "";
}

public class Site
{
    public string Id { get; set; } = "";
    public string OrgId { get; set; } = "";
    public Org Org { get; set; } = null!;
    public string Region { get; set; } = "";
    public string Name { get; set; } = "";
}

public class DeviceLabels
{
    public string? Env { get; set; }
    public string? Tier { get; set; }
    public string? Restricted { get; set; }
}

public class Device
{
    public string Id { get; set; } = "";
    public string Serial { get; set; } = "";
    public string Model { get; set; } = "";
    public string Firmware { get; set; } = "";
    public string SiteId { get; set; } = "";
    public Site Site { get; set; } = null!;
    public DateTime? LastSeenAt { get; set; }
    public bool Online { get; set; }
    public int? BatteryPct { get; set; }
    public DeviceLabels Labels { get; set; } = new();
    public bool Decommissioned { get; set; }
    public List<DeviceAlert> Alerts { get; set; } = [];
}

public class DeviceAlert
{
    public string Id { get; set; } = "";
    public string DeviceId { get; set; } = "";
    public int Severity { get; set; }
    public bool Acknowledged { get; set; }
    public string Code { get; set; } = "";
}

public sealed class IotContext(DbContextOptions options) : DomainContext(options)
{
    protected override void Configure(ModelBuilder b)
    {
        b.Entity<Org>().ToTable("orgs");
        b.Entity<Site>().ToTable("sites");
        b.Entity<Device>(e =>
        {
            e.ToTable("devices");
            e.OwnsOne(x => x.Labels, l => l.ToJson("labels"));
            e.Navigation(x => x.Labels).IsRequired();
            e.HasMany(x => x.Alerts).WithOne().HasForeignKey(x => x.DeviceId);
        });
        b.Entity<DeviceAlert>().ToTable("device_alerts");
    }
}
