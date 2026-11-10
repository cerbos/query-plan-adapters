// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Xunit;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Tests;

/// <summary>
/// The store a harness leg runs on, chosen by <c>ADAPTER_TEST_DB</c>: <c>sqlite</c> (the default),
/// <c>postgres</c> or <c>sqlserver</c>. The server stores start from the pinned images in efcore/*_IMAGE.
/// </summary>
public sealed class Store : IAsyncLifetime
{
    public static readonly string Name = Environment.GetEnvironmentVariable("ADAPTER_TEST_DB") is { Length: > 0 } db ? db : "sqlite";

    private SqliteConnection? _sqlite;
    private MsSqlContainer? _sqlServer;
    private PostgreSqlContainer? _postgres;
    private DbContextOptions<CorpusContext>? _options;
    private string? _collation;

    public DatabaseProvider Provider { get; private set; }

    /// <summary>The seeds this store cannot hold: each requires a capability the ledger lacks.</summary>
    public HashSet<string> Unheld { get; } = [];

    public CorpusContext Context() => new(_options!, _collation);

    public async ValueTask InitializeAsync()
    {
        var builder = new DbContextOptionsBuilder<CorpusContext>();
        switch (Name)
        {
            case "sqlite":
                Provider = DatabaseProvider.Sqlite;
                _sqlite = new SqliteConnection("Data Source=:memory:");
                await _sqlite.OpenAsync();
                builder.UseSqlite(_sqlite);
                break;
            case "postgres":
                Provider = DatabaseProvider.PostgreSql;
                // CEL orders strings by code point; the image's default libc collation orders them linguistically.
                _postgres = new PostgreSqlBuilder(Image("POSTGRES_IMAGE"))
                    .WithEnvironment("POSTGRES_INITDB_ARGS", Environment.GetEnvironmentVariable("ADAPTER_TEST_POSTGRES_INITDB_ARGS") ?? "--lc-collate=C")
                    .Build();
                await _postgres.StartAsync();
                builder.UseNpgsql(_postgres.GetConnectionString());
                break;
            case "sqlserver":
                Provider = DatabaseProvider.SqlServer;
                // The default collation is case-insensitive; CEL compares strings byte for byte.
                _collation = Environment.GetEnvironmentVariable("ADAPTER_TEST_SQLSERVER_COLLATION") ?? "Latin1_General_100_BIN2_UTF8";
                _sqlServer = new MsSqlBuilder(Image("SQLSERVER_IMAGE")).Build();
                await _sqlServer.StartAsync();
                // A database of its own, so EnsureCreated creates it with the collation.
                builder.UseSqlServer(new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(_sqlServer.GetConnectionString()) { InitialCatalog = "conformance" }.ConnectionString);
                break;
            default:
                throw new InvalidOperationException($"Unknown ADAPTER_TEST_DB '{Name}' (expected sqlite, postgres or sqlserver)");
        }
        _options = builder.Options;
        await using var db = Context();
        await db.Database.EnsureCreatedAsync();
        await Seed(db);
    }

    private static string Image(string file) => File.ReadAllText(Path.Combine(Corpus.AdapterDir, file)).Trim();

    public async ValueTask DisposeAsync()
    {
        if (_sqlite is not null) await _sqlite.DisposeAsync();
        if (_sqlServer is not null) await _sqlServer.DisposeAsync();
        if (_postgres is not null) await _postgres.DisposeAsync();
    }

    private async Task Seed(CorpusContext db)
    {
        var lacks = Corpus.ReadLedger().Lacks?.Where(l => l.Value.AppliesTo(Name)).Select(l => l.Key).ToHashSet() ?? [];
        var all = Corpus.Seeds();
        foreach (var s in all.Where(s => s.Requires?.Any(lacks.Contains) == true)) Unheld.Add(s.Id);
        var seeds = all.Where(s => !Unheld.Contains(s.Id)).ToList();
        var byId = seeds.ToDictionary(s => s.Id);
        var derived = Corpus.DerivedFields();
        Corpus.Seed? ParentOf(Corpus.Seed? s) => s?.ParentSeedId is { } p ? byId[p] : null;

        var catSeq = 0;
        var mainCategories = new Dictionary<string, string>();
        foreach (var s in seeds)
        {
            var d = derived[s.Id];
            var r = new Resource
            {
                Id = s.Id,
                ABool = s.ABool,
                AString = s.AString,
                ANumber = s.ANumber,
                ADouble = d.ADouble,
                AOptionalString = s.AOptionalString,
                CreatedBy = d.CreatedBy,
                Scope = d.Scope,
                CreatedAt = Instant(d.CreatedAt),
                UpdatedAt = Instant(d.UpdatedAt),
            };
            var position = 0;
            foreach (var t in s.Tags) r.Tags.Add(new Tag { TagId = t.Id, Name = t.Name, Position = position++ });
            position = 0;
            foreach (var n in s.ANumberList ?? []) r.NumberList.Add(new NumberElement { Element = n, Position = position++ });
            position = 0;
            foreach (var b in s.ABoolList ?? []) r.BoolList.Add(new BoolElement { Element = b, Position = position++ });
            // One category holding every subcategory name (conformance/README.md, "The dataset").
            if (s.SubCategoryNames.Count > 0)
            {
                catSeq++;
                var category = new Category { Id = $"cat-{catSeq}", Name = "business" };
                foreach (var subName in s.SubCategoryNames)
                {
                    var sub = new SubCategory { Id = $"cat-{catSeq}-sub-{category.SubCategories.Count + 1}", Name = subName };
                    foreach (var label in d.Labels) sub.Labels.Add(new Label { Id = $"{sub.Id}-label-{sub.Labels.Count + 1}", Name = label });
                    category.SubCategories.Add(sub);
                }
                r.Categories.Add(category);
                mainCategories[s.Id] = category.Id;
            }
            // The to-one chain, one owned row per level: no parent seed, no row.
            if (ParentOf(s) is { } ps)
            {
                r.Parent = new Parent { Id = s.Id + "-parent", ABool = ps.ABool, AString = ps.AString, ANumber = ps.ANumber, AOptionalString = ps.AOptionalString };
                if (ParentOf(ps) is { } inner)
                {
                    r.Parent.Inner = new Inner { Id = s.Id + "-inner", ABool = inner.ABool, AString = inner.AString, ANumber = inner.ANumber, AOptionalString = inner.AOptionalString };
                }
            }
            db.Resources.Add(r);
        }
        await db.SaveChangesAsync();
        // A second pass: the resource and its category each hold a key to the other.
        foreach (var r in db.Resources.Local)
        {
            if (mainCategories.TryGetValue(r.Id, out var category)) r.MainCategoryId = category;
        }
        await db.SaveChangesAsync();
    }

    private static DateTime? Instant(string? text) => text is null
        ? null
        : DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
}
