using System.Text.Json;
using System.Text.Json.Nodes;
using Cerbos.Api.V1.Engine;
using Cerbos.QueryPlan.EntityFrameworkCore;
using Cerbos.QueryPlan.EntityFrameworkCore.Domains;
using Microsoft.EntityFrameworkCore;
using Google.Protobuf;

var store = new StoreLeg(args.Length > 0 ? args[0] : "sqlite");
var only = args.Skip(1).ToHashSet();
var root = FindRoot();
var domainsDir = Path.Combine(root, "conformance", "domains");

IDomain[] domains =
[
    new Healthcare(), new Fintech(), new Logistics(), new Education(), new Legal(),
    new Retail(), new Hr(), new Media(), new Iot(), new Government(),
];

var results = new List<JsonObject>();
foreach (var domain in domains.Where(d => only.Count == 0 || only.Contains(d.Name)))
{
    var dir = Path.Combine(domainsDir, domain.Name);
    var (options, owner) = store.Open(domain, dir);
    using (owner)
    {
        var goldens = Directory.EnumerateFiles(Path.Combine(dir, "golden"), "*.json", SearchOption.AllDirectories)
            .Select(f => JsonNode.Parse(File.ReadAllText(f))!)
            .OrderBy(g => (string)g["pdp"]!, StringComparer.Ordinal)
            .ThenBy(g => (string)g["principalName"]!, StringComparer.Ordinal)
            .ThenBy(g => (string)g["action"]!, StringComparer.Ordinal);
        foreach (var g in goldens)
        {
            var allowed = g["allowed"]!.AsArray().Select(n => (string)n!).Order(StringComparer.Ordinal).ToList();
            List<string> returned = [];
            string verdict, detail = "";
            try
            {
                var plan = JsonParser.Default.Parse<PlanResourcesFilter>(g["plan"]!.ToJsonString());
                using var db = domain.CreateContext(options);
                var query = domain.Query(db, plan, store.Provider);
                if (Environment.GetEnvironmentVariable("DOMAINS_SHOW") == $"{domain.Name}/{g["pdp"]}/{g["principalName"]}/{g["action"]}") Console.WriteLine(query.ToQueryString());
                returned = [.. query.AsEnumerable().Order(StringComparer.Ordinal)];
                if (returned.Except(allowed).Any()) verdict = "over-grant";
                else if (returned.Count == allowed.Count && returned.SequenceEqual(allowed)) verdict = "pass";
                else if (returned.Distinct().Count() < returned.Count) { verdict = "error"; detail = "duplicate ids returned"; }
                else verdict = "under-grant";
            }
            catch (QueryPlanRefusedException e)
            {
                verdict = "refused";
                detail = $"{e.GetType().Name}: {e.Message}";
            }
            catch (Exception e)
            {
                verdict = "error";
                detail = $"{e.GetType().Name}: {First(e.Message)}" + (e.InnerException is { } inner ? $" <- {inner.GetType().Name}: {First(inner.Message)}" : "");
            }
            results.Add(new JsonObject
            {
                ["domain"] = domain.Name,
                ["pdp"] = (string)g["pdp"]!,
                ["principal"] = (string)g["principalName"]!,
                ["action"] = (string)g["action"]!,
                ["verdict"] = verdict,
                ["returned"] = new JsonArray([.. returned.Select(i => (JsonNode)i!)]),
                ["allowed"] = new JsonArray([.. allowed.Select(i => (JsonNode)i!)]),
                ["detail"] = detail,
            });
            if (verdict != "pass") Console.WriteLine($"{domain.Name} {g["pdp"]} {g["principalName"]} {g["action"]}: {verdict} {detail}");
        }
    }
}

var outDir = Path.Combine(domainsDir, "results", "efcore");
Directory.CreateDirectory(outDir);
var file = new JsonObject { ["adapter"] = "efcore", ["store"] = store.Name, ["results"] = new JsonArray([.. results]) };
File.WriteAllText(Path.Combine(outDir, store.Name + ".json"), file.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) + "\n");
foreach (var grp in results.GroupBy(r => (string)r["verdict"]!)) Console.WriteLine($"{grp.Key}: {grp.Count()}");

static string First(string message) => message.Split('\n')[0].Trim();

static string FindRoot()
{
    for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
    {
        if (File.Exists(Path.Combine(dir.FullName, "conformance", "domains", "README.md"))) return dir.FullName;
    }
    throw new InvalidOperationException("no conformance/domains/ above " + AppContext.BaseDirectory);
}
