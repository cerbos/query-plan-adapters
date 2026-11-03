// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Tests;

public class Resource
{
    public required string Id { get; set; }
    public bool? ABool { get; set; }
    public string? AString { get; set; }
    public double? ANumber { get; set; }
    public double? ADouble { get; set; }
    public string? AOptionalString { get; set; }
    public string? CreatedBy { get; set; }
    public string? Scope { get; set; }
    public DateTime? CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }
    public List<Tag> Tags { get; set; } = [];
    public List<NumberElement> NumberList { get; set; } = [];
    public List<BoolElement> BoolList { get; set; } = [];
    public List<Category> Categories { get; set; } = [];
    public string? MainCategoryId { get; set; }
    public Category? MainCategory { get; set; }
    public Parent? Parent { get; set; }
}

public class Tag
{
    public int Pk { get; set; }
    public required string TagId { get; set; }
    public string? Name { get; set; }
    public int Position { get; set; }
    public string ResourceId { get; set; } = "";
}

public class NumberElement
{
    public int Pk { get; set; }
    public double? Element { get; set; }
    public int Position { get; set; }
    public string ResourceId { get; set; } = "";
}

public class BoolElement
{
    public int Pk { get; set; }
    public bool? Element { get; set; }
    public int Position { get; set; }
    public string ResourceId { get; set; } = "";
}

public class Category
{
    public required string Id { get; set; }
    public required string Name { get; set; }
    public string ResourceId { get; set; } = "";
    public List<SubCategory> SubCategories { get; set; } = [];
}

public class SubCategory
{
    public required string Id { get; set; }
    public required string Name { get; set; }
    public string CategoryId { get; set; } = "";
    public List<Label> Labels { get; set; } = [];
}

public class Label
{
    public required string Id { get; set; }
    public string? Name { get; set; }
    public string SubCategoryId { get; set; } = "";
}

/// <summary>The resource's own copy of its parent seed's scalars, one row per resource.</summary>
public class Parent
{
    public required string Id { get; set; }
    public string ResourceId { get; set; } = "";
    public bool? ABool { get; set; }
    public string? AString { get; set; }
    public double? ANumber { get; set; }
    public string? AOptionalString { get; set; }
    public Inner? Inner { get; set; }
}

public class Inner
{
    public required string Id { get; set; }
    public string ParentId { get; set; } = "";
    public bool? ABool { get; set; }
    public string? AString { get; set; }
    public double? ANumber { get; set; }
    public string? AOptionalString { get; set; }
}

public class CorpusContext(DbContextOptions<CorpusContext> options, string? collation) : DbContext(options)
{
    public DbSet<Resource> Resources => Set<Resource>();

    /// <summary>
    /// On SQL Server, strings are varchar under a UTF-8 binary collation: nvarchar orders by UTF-16 code
    /// unit and LEN counts code units, where CEL orders and counts by code point.
    /// </summary>
    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        if (collation is not null) configurationBuilder.Properties<string>().AreUnicode(false);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        if (collation is not null) modelBuilder.UseCollation(collation);
        modelBuilder.Entity<Tag>().HasKey(t => t.Pk);
        modelBuilder.Entity<NumberElement>().HasKey(e => e.Pk);
        modelBuilder.Entity<BoolElement>().HasKey(e => e.Pk);
        modelBuilder.Entity<Resource>(r =>
        {
            r.HasMany(x => x.Tags).WithOne().HasForeignKey(t => t.ResourceId);
            r.HasMany(x => x.NumberList).WithOne().HasForeignKey(t => t.ResourceId);
            r.HasMany(x => x.BoolList).WithOne().HasForeignKey(t => t.ResourceId);
            r.HasMany(x => x.Categories).WithOne().HasForeignKey(c => c.ResourceId);
            r.HasOne(x => x.MainCategory).WithMany().HasForeignKey(x => x.MainCategoryId).OnDelete(DeleteBehavior.NoAction);
            r.HasOne(x => x.Parent).WithOne().HasForeignKey<Parent>(p => p.ResourceId);
        });
        modelBuilder.Entity<Category>().HasMany(c => c.SubCategories).WithOne().HasForeignKey(s => s.CategoryId);
        modelBuilder.Entity<SubCategory>().HasMany(s => s.Labels).WithOne().HasForeignKey(l => l.SubCategoryId);
        modelBuilder.Entity<Parent>().HasOne(p => p.Inner).WithOne().HasForeignKey<Inner>(i => i.ParentId);
    }
}
