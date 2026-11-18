using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

/// <summary>A snake_case naming convention, as an application on an existing schema would use, plus DECIMAL(18,2).</summary>
public abstract class DomainContext(DbContextOptions options) : DbContext(options)
{
    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder) =>
        configurationBuilder.Properties<decimal>().HavePrecision(18, 2);

    protected sealed override void OnModelCreating(ModelBuilder modelBuilder)
    {
        Configure(modelBuilder);
        foreach (var et in modelBuilder.Model.GetEntityTypes())
        {
            foreach (var p in et.GetProperties())
            {
                if (et.IsMappedToJson())
                {
                    if (!p.IsShadowProperty() && !p.IsKey()) p.SetJsonPropertyName(Snake(p.Name));
                    continue;
                }
                if (et.IsOwned() && p.IsShadowProperty()) continue;
                if (p.FindAnnotation(RelationalAnnotationNames.ColumnName) is null) p.SetColumnName(Snake(p.Name));
            }
        }
    }

    protected abstract void Configure(ModelBuilder b);

    private static string Snake(string name)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            if (char.IsUpper(name[i]) && i > 0) sb.Append('_');
            sb.Append(char.ToLowerInvariant(name[i]));
        }
        return sb.ToString();
    }
}
