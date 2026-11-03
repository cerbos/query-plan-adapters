// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Xunit;

[assembly: AssemblyFixture(typeof(Cerbos.QueryPlan.EntityFrameworkCore.Tests.Store))]

namespace Cerbos.QueryPlan.EntityFrameworkCore.Tests;

/// <summary>
/// The conformance harness (conformance/README.md, "The harness contract"): every recorded plan of both
/// pinned PDPs, translated and run against a real store, must return exactly the ids <c>check()</c>
/// allowed, unless the ledger says the adapter refuses it or still diverges.
/// </summary>
public sealed class ConformanceTests(Store store)
{
    /// <summary>
    /// The one mapping every case is translated with. Each scalar resources.json omits when its column is
    /// NULL is omitted-when-NULL, and so is a NULL field of a list element (a tag's or label's name);
    /// <c>owner</c> and <c>coOwner</c> send an explicit null. The call-level convention, explicit, covers
    /// what is left, the elements of scalar lists, which the dataset sends as null values.
    /// </summary>
    public static readonly AttributeMap<Resource> Mapping = new AttributeMap<Resource>()
        .Field("request.resource.id", r => r.Id)
        .Field("request.resource.attr.aBool", r => r.ABool, NullConvention.Omitted)
        .Field("request.resource.attr.aString", r => r.AString, NullConvention.Omitted)
        .Field("request.resource.attr.aNumber", r => r.ANumber, NullConvention.Omitted)
        .Field("request.resource.attr.aDouble", r => r.ADouble, NullConvention.Omitted)
        .Field("request.resource.attr.aOptionalString", r => r.AOptionalString, NullConvention.Omitted)
        .Field("request.resource.attr.createdBy", r => r.CreatedBy, NullConvention.Omitted)
        .Field("request.resource.attr.scope", r => r.Scope, NullConvention.Omitted)
        .Field("request.resource.attr.createdAt", r => r.CreatedAt, NullConvention.Omitted)
        .Field("request.resource.attr.updatedAt", r => r.UpdatedAt, NullConvention.Omitted)
        .Field("request.resource.attr.obj.inner", r => r.AString, NullConvention.Omitted)
        .Field("request.resource.attr.parent.aBool", r => r.Parent!.ABool, NullConvention.Omitted)
        .Field("request.resource.attr.parent.aString", r => r.Parent!.AString, NullConvention.Omitted)
        .Field("request.resource.attr.parent.aNumber", r => r.Parent!.ANumber, NullConvention.Omitted)
        .Field("request.resource.attr.parent.aOptionalString", r => r.Parent!.AOptionalString, NullConvention.Omitted)
        .Field("request.resource.attr.parent.inner.aBool", r => r.Parent!.Inner!.ABool, NullConvention.Omitted)
        .Field("request.resource.attr.parent.inner.aString", r => r.Parent!.Inner!.AString, NullConvention.Omitted)
        .Field("request.resource.attr.parent.inner.aNumber", r => r.Parent!.Inner!.ANumber, NullConvention.Omitted)
        .Field("request.resource.attr.parent.inner.aOptionalString", r => r.Parent!.Inner!.AOptionalString, NullConvention.Omitted)
        .Field("request.resource.attr.owner", r => r.AOptionalString, NullConvention.Explicit)
        .Field("request.resource.attr.coOwner", r => r.Scope, NullConvention.Explicit)
        .List("request.resource.attr.tagNames", r => r.Tags, t => t.Name, t => t.Position)
        .List("request.resource.attr.aNumberList", r => r.NumberList, e => e.Element, e => e.Position)
        .List("request.resource.attr.aBoolList", r => r.BoolList, e => e.Element, e => e.Position)
        .List("request.resource.attr.tags", r => r.Tags, t => t
            .Field("id", x => x.TagId)
            .Field("name", x => x.Name, NullConvention.Omitted), t => t.Position)
        .List("request.resource.attr.categories", r => r.Categories, c => c
            .Field("name", x => x.Name)
            .List("subCategories", x => x.SubCategories, s => s
                .Field("name", x => x.Name)
                .List("labels", x => x.Labels, l => l.Field("name", x => x.Name, NullConvention.Omitted))))
        .Field("request.resource.attr.mainCategory.name", r => r.MainCategory!.Name, NullConvention.Omitted)
        .List("request.resource.attr.mainCategory.subCategories", r => r.MainCategory!.SubCategories, s => s
            .Field("name", x => x.Name))
        .List("request.resource.attr.mainCategory.subNames", r => r.MainCategory!.SubCategories, s => s.Name);

    private static readonly Corpus.Ledger Ledger = Corpus.ReadLedger();

    private static readonly ConcurrentBag<Corpus.FuzzVerdict> FuzzResults = [];

    public static IEnumerable<TheoryDataRow<string, string>> Cases() =>
        Corpus.PdpTags().SelectMany(tag => Corpus.Goldens(tag).Keys.Select(id => new TheoryDataRow<string, string>(tag, id)));

    [Theory]
    [MemberData(nameof(Cases))]
    public void Golden(string tag, string id)
    {
        var golden = Corpus.Goldens(tag)[id];
        Assert.Equal(tag, golden["pdp"]!.GetValue<string>());
        Assert.True(golden.AsObject().ContainsKey("plannerDivergence"), id + ": golden has no plannerDivergence key");
        if (golden["plannerDivergence"] is not null)
        {
            Assert.Skip("planner divergence: " + golden["plannerDivergence"]!["reason"]);
        }
        var allowed = golden["allowed"]!.AsArray().Select(n => n!.GetValue<string>())
            .Where(i => !store.Unheld.Contains(i)).Order(StringComparer.Ordinal).ToList();

        if (Corpus.Fuzzing)
        {
            var verdict = FuzzVerdict(tag, id, allowed, golden);
            FuzzResults.Add(verdict);
            Corpus.WriteFuzzResults("efcore", Environment.GetEnvironmentVariable("QPA_HARNESS_STORE") ?? Store.Name, FuzzResults);
            Assert.True(verdict.Ok, verdict.ToString());
            return;
        }

        var status = Ledger.Cases.TryGetValue(id, out var entry) && entry.AppliesTo(tag) ? entry.Status : "pass";
        switch (status)
        {
            case "pass":
                {
                    var ids = Ids(golden);
                    if (!ids.SequenceEqual(allowed))
                    {
                        Assert.Fail($"over-grant: [{string.Join(", ", ids.Except(allowed))}] under-grant: [{string.Join(", ", allowed.Except(ids))}] returned: [{string.Join(", ", ids)}]");
                    }
                    break;
                }
            case "unsupported":
                Assert.ThrowsAny<QueryPlanRefusedException>(() => Filter(golden));
                break;
            case "divergent":
                Assert.NotEqual(allowed, Ids(golden));
                break;
            default:
                Assert.Fail($"{id}: unknown ledger status {status}");
                break;
        }
    }

    [Fact]
    public void EveryLedgerEntryNamesAGoldenCase()
    {
        Assert.SkipWhen(Corpus.Fuzzing, "a fuzz run is judged without the ledger");
        var tags = Corpus.PdpTags();
        var recorded = tags.SelectMany(t => Corpus.Goldens(t).Keys).ToHashSet();
        foreach (var id in Ledger.Cases.Keys) Assert.True(recorded.Contains(id), id + " has no golden file");
        foreach (var (id, entry) in Ledger.Cases)
        {
            Assert.True(entry.Status is "unsupported" or "divergent", id + ": unknown status " + entry.Status);
            Assert.False(string.IsNullOrWhiteSpace(entry.Reason), id);
            Assert.True(entry.Status != "divergent" || entry.Issue is not null, id + ": a divergent entry needs an issue");
            Assert.True(entry.Pdp is null || (entry.Pdp.Count > 0 && entry.Pdp.All(tags.Contains)), id + ": pdp scope is not within " + string.Join(",", tags));
        }
    }

    /// <summary>The ids the filter selects, sorted ordinally: the database's collation must not decide the order.</summary>
    private List<string> Ids(JsonNode golden)
    {
        var filter = Filter(golden);
        using var db = store.Context();
        return [.. db.Resources.AsNoTracking().Where(filter.Predicate).Select(r => r.Id).AsEnumerable().Order(StringComparer.Ordinal)];
    }

    /// <summary>The refusal comes from <c>ToFilter</c>, before any query exists: EF's own translation error is not one.</summary>
    private QueryPlanFilter<Resource> Filter(JsonNode golden) =>
        QueryPlanAdapter.ToFilter(Corpus.Plan(golden), new QueryPlanOptions<Resource>(store.Provider, Mapping) { Nulls = NullConvention.Explicit });

    private Corpus.FuzzVerdict FuzzVerdict(string tag, string id, List<string> expected, JsonNode golden)
    {
        List<string> returned;
        try
        {
            returned = Ids(golden);
        }
        catch (QueryPlanRefusedException)
        {
            return new(tag, id, "refused", null, null, null);
        }
#pragma warning disable CA1031
        catch (Exception e)
#pragma warning restore CA1031
        {
            return new(tag, id, "error", null, null, $"{e.GetType().Name}: {e.Message}");
        }
        if (returned.SequenceEqual(expected)) return new(tag, id, "match", null, null, null);
        var verdict = returned.Except(expected).Any() ? "over-grant" : expected.Except(returned).Any() ? "under-grant" : "error";
        return new(tag, id, verdict, expected, returned, verdict == "error" ? "the right rows, but some of them repeated" : null);
    }
}
