// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Demo-domain example for the EF Core adapter. It proves plumbing, not semantics: the packed
// package restores, and its predicate composes with Where, OrderBy and Skip/Take. All data comes
// from demo/seeds.json.

using System.Text.Json;
using System.Text.Json.Nodes;
using Cerbos.QueryPlan.EntityFrameworkCore;
using Cerbos.Sdk.Builder;
using PlanResourcesFilter = Cerbos.Api.V1.Engine.PlanResourcesFilter;
using Principal = Cerbos.Sdk.Builder.Principal;
using Resource = Cerbos.Sdk.Builder.Resource;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

var demoDir = Environment.GetEnvironmentVariable("DEMO_DIR") ?? Path.Combine(AppContext.BaseDirectory, "../../../../demo");
var cerbosHost = Environment.GetEnvironmentVariable("CERBOS_HOST") is { Length: > 0 } host ? host
    : throw new InvalidOperationException("CERBOS_HOST is not set — run this example through demo/scripts/run-example.sh efcore");

var seeds = JsonNode.Parse(File.ReadAllText(Path.Combine(demoDir, "seeds.json")))!;

using var connection = new SqliteConnection("Data Source=:memory:");
connection.Open();
var dbOptions = new DbContextOptionsBuilder<DemoContext>().UseSqlite(connection).Options;
using (var seed = new DemoContext(dbOptions))
{
    seed.Database.EnsureCreated();
    foreach (var row in seeds["documents"]!.AsArray())
    {
        seed.Documents.Add(new Document
        {
            Id = (string)row!["id"]!,
            OwnerId = (string)row["ownerId"]!,
            IsPublic = (bool)row["public"]!,
            Region = (string)row["region"]!,
            Archived = (bool)row["archived"]!,
        });
    }
    seed.SaveChanges();
}

// Maps Cerbos attributes to columns. `public` is stored as IsPublic.
var attributes = new AttributeMap<Document>()
    .Field("request.resource.attr.ownerId", d => d.OwnerId)
    .Field("request.resource.attr.public", d => d.IsPublic);
var adapterOptions = new QueryPlanOptions<Document>(DatabaseProvider.Sqlite, attributes);

var client = CerbosClientBuilder.ForTarget("http://" + cerbosHost).WithPlaintext().Build();

var shapes = new JsonObject
{
    ["filtered"] = new JsonObject
    {
        ["alice/view"] = Filtered("alice", "view"),
        ["bob/view"] = Filtered("bob", "view"),
    },
    // 2. Always allowed: the predicate is `true`, through the same code path.
    ["alwaysAllowed"] = new JsonObject { ["admin/admin-view"] = Filtered("admin", "admin-view") },
    // 3. Always denied: the policy has no rule for this action.
    ["alwaysDenied"] = new JsonObject { ["alice/publish"] = Filtered("alice", "publish") },
    ["paginated"] = new JsonObject
    {
        ["alice/view"] = Paginated("alice", "view", 2),
        ["admin/admin-view"] = Paginated("admin", "admin-view", 3),
    },
    ["composed"] = new JsonObject
    {
        ["alice/view"] = Composed("alice", "view"),
        ["bob/view"] = Composed("bob", "view"),
        ["admin/admin-view"] = Composed("admin", "admin-view"),
        ["alice/publish"] = Composed("alice", "publish"),
    },
};

Console.WriteLine(new JsonObject { ["adapter"] = "efcore", ["shapes"] = shapes }.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));

// Read principals from demo/seeds.json rather than copying them here.
Principal PrincipalFor(string id)
{
    var found = seeds["principals"]!.AsArray().FirstOrDefault(p => (string)p!["id"]! == id)
        ?? throw new InvalidOperationException($"demo/seeds.json declares no principal \"{id}\"");
    return Principal.NewInstance(id, [.. found["roles"]!.AsArray().Select(r => (string)r!)]);
}

QueryPlanFilter<Document> Plan(string principalId, string action)
{
    var response = client.PlanResources(PlanResourcesRequest.NewInstance()
        .WithPrincipal(PrincipalFor(principalId))
        .WithResource(Resource.NewInstance("document"))
        .WithActions([action]));
    return QueryPlanAdapter.ToFilter(response, adapterOptions);
}

static string KindName(FilterKind kind) =>
    PlanResourcesFilter.Descriptor.EnumTypes[0].FindValueByNumber((int)(kind switch
    {
        FilterKind.AlwaysAllowed => PlanResourcesFilter.Types.Kind.AlwaysAllowed,
        FilterKind.AlwaysDenied => PlanResourcesFilter.Types.Kind.AlwaysDenied,
        _ => PlanResourcesFilter.Types.Kind.Conditional,
    })).Name;

static JsonArray Ids(IEnumerable<string> ids) => [.. ids.Order(StringComparer.Ordinal).Select(i => (JsonNode)i)];

// 1. A plain filtered list: the adapter's predicate is the whole query.
JsonObject Filtered(string principalId, string action)
{
    var filter = Plan(principalId, action);
    using var db = new DemoContext(dbOptions);
    return new JsonObject { ["kind"] = KindName(filter.Kind), ["ids"] = Ids(db.Documents.Where(filter.Predicate).Select(d => d.Id).ToList()) };
}

// 4. Pagination. Reports page sizes and all ids sorted, not per-page order, which comes from
//    OrderBy rather than the filter.
JsonObject Paginated(string principalId, string action, int pageSize)
{
    var filter = Plan(principalId, action);
    using var db = new DemoContext(dbOptions);
    var query = db.Documents.Where(filter.Predicate).OrderBy(d => d.Id);
    var pageSizes = new JsonArray();
    var collected = new List<string>();
    for (var offset = 0; ; offset += pageSize)
    {
        var page = query.Skip(offset).Take(pageSize).Select(d => d.Id).ToList();
        if (page.Count == 0) break;
        pageSizes.Add(page.Count);
        collected.AddRange(page);
        if (page.Count < pageSize) break;
    }
    return new JsonObject { ["kind"] = KindName(filter.Kind), ["pageSize"] = pageSize, ["pageSizes"] = pageSizes, ["ids"] = Ids(collected) };
}

// 5. The adapter's predicate ANDed with the application's own. An always-denied plan still runs
//    the query, to show the application's Where cannot bring a denied row back.
JsonObject Composed(string principalId, string action)
{
    var filter = Plan(principalId, action);
    var archived = (bool)seeds["applicationFilter"]!["archived"]!;
    var region = (string)seeds["applicationFilter"]!["region"]!;
    using var db = new DemoContext(dbOptions);
    var ids = db.Documents.Where(filter.Predicate).Where(d => d.Archived == archived && d.Region == region).Select(d => d.Id).ToList();
    return new JsonObject { ["kind"] = KindName(filter.Kind), ["ids"] = Ids(ids) };
}

internal sealed class Document
{
    public required string Id { get; set; }
    public required string OwnerId { get; set; }
    public bool IsPublic { get; set; }
    public required string Region { get; set; }
    public bool Archived { get; set; }
}

internal sealed class DemoContext(DbContextOptions<DemoContext> options) : DbContext(options)
{
    public DbSet<Document> Documents => Set<Document>();
}
