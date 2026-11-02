// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Cerbos.Api.V1.Engine;
using Google.Protobuf;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Tests;

/// <summary>
/// Reads the shared conformance corpus and this adapter's ledger. Every adapter carries its own copy of
/// this loader on purpose (docs/adr/0007-adapters-share-data-not-code.md).
/// </summary>
public static class Corpus
{
    private static readonly JsonSerializerOptions Strict = new()
    {
        AllowDuplicateProperties = false,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Skip,
    };

    public static readonly string RepositoryRoot = FindRoot();

    public static string ConformanceDir => Path.Combine(RepositoryRoot, "conformance");

    public static string AdapterDir => Path.Combine(RepositoryRoot, "efcore");

    private static string FindRoot()
    {
        foreach (var start in new[] { AppContext.BaseDirectory, Environment.CurrentDirectory })
        {
            for (var dir = new DirectoryInfo(start); dir is not null; dir = dir.Parent)
            {
                if (File.Exists(Path.Combine(dir.FullName, "conformance", "pdp-versions.json"))) return dir.FullName;
            }
        }
        throw new InvalidOperationException("No conformance/ directory above " + AppContext.BaseDirectory);
    }

    public static JsonNode Read(string path) =>
        JsonNode.Parse(File.ReadAllText(path), documentOptions: new JsonDocumentOptions { AllowDuplicateProperties = false })!;

    public static T Read<T>(string path) => JsonSerializer.Deserialize<T>(File.ReadAllText(path), Strict)!;

    // -- the PDPs and their goldens ---------------------------------------------------------------

    /// <summary>The tags of pdp-versions.json, current first.</summary>
    public static IReadOnlyList<string> PdpTags()
    {
        var versions = Read(Path.Combine(ConformanceDir, "pdp-versions.json"));
        return [versions["current"]!["tag"]!.GetValue<string>(), versions["previous"]!["tag"]!.GetValue<string>()];
    }

    public static readonly string? FuzzDir = Environment.GetEnvironmentVariable("QPA_FUZZ_DIR") is { Length: > 0 } d ? d : null;

    public static bool Fuzzing => FuzzDir is not null;

    /// <summary>Every golden file recorded for a tag (in fuzz mode, the run's), by case id.</summary>
    public static SortedDictionary<string, JsonNode> Goldens(string tag) => GoldenCache.GetOrAdd(tag, ReadGoldens);

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, SortedDictionary<string, JsonNode>> GoldenCache = new();

    private static SortedDictionary<string, JsonNode> ReadGoldens(string tag)
    {
        var dir = Fuzzing ? Path.Combine(FuzzDir!, tag) : Path.Combine(ConformanceDir, "golden", tag);
        var result = new SortedDictionary<string, JsonNode>(StringComparer.Ordinal);
        foreach (var file in Directory.EnumerateFiles(dir, "*.json", SearchOption.AllDirectories))
        {
            var golden = Read(file);
            result.Add(golden["id"]!.GetValue<string>(), golden);
        }
        return result;
    }

    private const string NowMinus24H = "__NOW_MINUS_24H__";

    /// <summary>A golden's plan, with the folded <c>now() - duration("24h")</c> filled in.</summary>
    public static PlanResourcesFilter Plan(JsonNode golden)
    {
        var plan = golden["plan"] ?? throw new InvalidOperationException(golden["id"] + " records no plan");
        var now = DateTime.UtcNow.AddHours(-24).ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff", CultureInfo.InvariantCulture) + "00Z";
        return JsonParser.Default.Parse<PlanResourcesFilter>(plan.ToJsonString().Replace(NowMinus24H, now, StringComparison.Ordinal));
    }

    // -- efcore/conformance-ledger.json -----------------------------------------------------------

    public sealed record LedgerEntry(string Status, string Reason, string? Issue, List<string>? Pdp)
    {
        public bool AppliesTo(string tag) => Pdp is null || Pdp.Contains(tag);
    }

    public sealed record Ledger(string Adapter, Dictionary<string, string>? Lacks, Dictionary<string, LedgerEntry> Cases);

    private static readonly JsonSerializerOptions LedgerOptions = new(Strict) { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow };

    private static readonly JsonSerializerOptions ResultOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static Ledger ReadLedger()
    {
        var file = Path.Combine(AdapterDir, "conformance-ledger.json");
        var ledger = JsonSerializer.Deserialize<Ledger>(File.ReadAllText(file), LedgerOptions)!;
        if (ledger.Adapter != "efcore" || ledger.Cases is null) throw new InvalidOperationException(file + " is not the efcore ledger");
        return ledger;
    }

    // -- the dataset ------------------------------------------------------------------------------

    public sealed record SeedTag(string Id, string? Name);

    public sealed record Seed(
        string Id, bool? ABool, string? AString, double? ANumber, string? AOptionalString,
        List<double?>? ANumberList, List<bool?>? ABoolList, List<SeedTag> Tags, List<string> SubCategoryNames,
        string? ParentSeedId, List<string>? Requires);

    public sealed record SeedsFile(List<Seed> Seeds);

    public sealed record Derived(string? CreatedBy, double? ADouble, string? CreatedAt, string? UpdatedAt, string? Scope, List<string?> Labels);

    public sealed record DerivedFile(Dictionary<string, Derived> Derived);

    public static List<Seed> Seeds() => Read<SeedsFile>(Path.Combine(ConformanceDir, "seeds.json")).Seeds;

    public static Dictionary<string, Derived> DerivedFields() => Read<DerivedFile>(Path.Combine(ConformanceDir, "derived-fields.json")).Derived;

    // -- fuzz mode --------------------------------------------------------------------------------

    public sealed record FuzzVerdict(string Pdp, string Id, string Verdict, List<string>? Expected, List<string>? Returned, string? Detail)
    {
        public bool Ok => Verdict is "match" or "refused";
    }

    public static void WriteFuzzResults(string adapter, string store, IEnumerable<FuzzVerdict> results)
    {
        if (!Fuzzing) return;
        var dir = Path.Combine(FuzzDir!, "results");
        Directory.CreateDirectory(dir);
        var sorted = results.OrderBy(r => r.Id, StringComparer.Ordinal).ThenBy(r => r.Pdp, StringComparer.Ordinal).ToList();
        var json = JsonSerializer.Serialize(new { adapter, store, results = sorted }, ResultOptions);
        File.WriteAllText(Path.Combine(dir, $"{adapter}-{store}.json"), json + "\n");
    }
}
