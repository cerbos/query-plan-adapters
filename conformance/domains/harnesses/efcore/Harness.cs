using System.Globalization;
using System.Linq.Expressions;
using System.Text.Json;
using Cerbos.Api.V1.Engine;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql;

namespace Cerbos.QueryPlan.EntityFrameworkCore.Domains;

public interface IDomain
{
    string Name { get; }
    DbContext CreateContext(DbContextOptions options);
    IQueryable<string> Query(DbContext db, PlanResourcesFilter plan, DatabaseProvider provider);
}

public abstract class Domain<TRoot> : IDomain where TRoot : class
{
    public abstract string Name { get; }
    public abstract DbContext CreateContext(DbContextOptions options);
    public abstract AttributeMap<TRoot> Map { get; }
    public abstract Expression<Func<TRoot, string>> Id { get; }
    public virtual NullConvention Nulls => NullConvention.Omitted;

    public IQueryable<string> Query(DbContext db, PlanResourcesFilter plan, DatabaseProvider provider)
    {
        var filter = QueryPlanAdapter.ToFilter(plan, new QueryPlanOptions<TRoot>(provider, Map) { Nulls = Nulls });
        return db.Set<TRoot>().AsNoTracking().Where(filter.Predicate).Select(Id);
    }
}

/// <summary>One store leg: a fresh database per domain, loaded the way the store's application would hold it.</summary>
public sealed class StoreLeg(string name)
{
    public string Name { get; } = name;

    public DatabaseProvider Provider => Name switch
    {
        "sqlite" => DatabaseProvider.Sqlite,
        "postgres" => DatabaseProvider.PostgreSql,
        "sqlserver" => DatabaseProvider.SqlServer,
        _ => throw new InvalidOperationException("unknown store " + Name),
    };

    private static string Env(string key, string fallback) => Environment.GetEnvironmentVariable(key) is { Length: > 0 } v ? v : fallback;

    private static readonly string PgAdmin = Env("DOMAINS_PG", "Host=localhost;Port=55432;Username=postgres;Password=postgres;Database=postgres;Pooling=false");
    private static readonly string MsAdmin = Env("DOMAINS_MSSQL", "Server=localhost,51433;User Id=sa;Password=Domains_Passw0rd;TrustServerCertificate=true;Database=master;Pooling=false");

    /// <summary>Creates and loads the domain's database and returns options for its context, plus a disposer.</summary>
    public (DbContextOptions Options, IDisposable Owner) Open(IDomain domain, string domainDir)
    {
        var db = "dom_" + domain.Name;
        var builder = new DbContextOptionsBuilder();
        switch (Name)
        {
            case "sqlite":
                {
                    var conn = new SqliteConnection("Data Source=:memory:");
                    conn.Open();
                    builder.UseSqlite(conn);
                    using var ctx = domain.CreateContext(builder.Options);
                    ctx.Database.EnsureCreated();
                    RowsLoader.Load(ctx, Path.Combine(domainDir, "generated", "rows.json"));
                    return (builder.Options, conn);
                }
            case "postgres":
                {
                    using (var admin = new NpgsqlConnection(PgAdmin))
                    {
                        admin.Open();
                        Exec(admin, $"DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)");
                        Exec(admin, $"CREATE DATABASE \"{db}\"");
                    }
                    var cs = new NpgsqlConnectionStringBuilder(PgAdmin) { Database = db }.ConnectionString;
                    using (var conn = new NpgsqlConnection(cs))
                    {
                        conn.Open();
                        Exec(conn, File.ReadAllText(Path.Combine(domainDir, "generated", "sql", "postgres.sql")));
                    }
                    builder.UseNpgsql(cs);
                    return (builder.Options, new Nothing());
                }
            case "sqlserver":
                {
                    using (var admin = new SqlConnection(MsAdmin))
                    {
                        admin.Open();
                        Exec(admin, $"IF DB_ID(N'{db}') IS NOT NULL BEGIN ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END");
                        Exec(admin, $"CREATE DATABASE [{db}] COLLATE Latin1_General_100_BIN2_UTF8");
                    }
                    var cs = new SqlConnectionStringBuilder(MsAdmin) { InitialCatalog = db }.ConnectionString;
                    using (var conn = new SqlConnection(cs))
                    {
                        conn.Open();
                        Exec(conn, File.ReadAllText(Path.Combine(domainDir, "generated", "sql", "sqlserver.sql")));
                    }
                    builder.UseSqlServer(cs);
                    return (builder.Options, new Nothing());
                }
            default:
                throw new InvalidOperationException("unknown store " + Name);
        }
    }

    private static void Exec(System.Data.Common.DbConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 120;
        cmd.ExecuteNonQuery();
    }

    private sealed class Nothing : IDisposable
    {
        public void Dispose() { }
    }
}

/// <summary>Loads generated/rows.json through the EF model: every table by its mapped name, every column by its mapped column name.</summary>
public static class RowsLoader
{
    public static void Load(DbContext db, string rowsFile)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(rowsFile));
        var types = db.Model.GetEntityTypes().Where(e => !e.IsOwned()).ToList();
        foreach (var table in doc.RootElement.GetProperty("tables").EnumerateArray())
        {
            var name = table.GetProperty("name").GetString()!;
            var et = types.SingleOrDefault(e => e.GetTableName() == name)
                ?? throw new InvalidOperationException($"no entity is mapped to table {name}");
            foreach (var row in table.GetProperty("rows").EnumerateArray())
            {
                if (et.HasSharedClrType)
                {
                    var bag = new Dictionary<string, object>();
                    Fill(et, bag, row);
                    db.Set<Dictionary<string, object>>(et.Name).Add(bag);
                }
                else
                {
                    var entity = Activator.CreateInstance(et.ClrType)!;
                    Fill(et, entity, row);
                    db.Add(entity);
                }
            }
        }
        db.SaveChanges();
        db.ChangeTracker.Clear();
    }

    private static void Fill(IEntityType et, object target, JsonElement row)
    {
        var table = StoreObjectIdentifier.Table(et.GetTableName()!, et.GetSchema());
        foreach (var p in et.GetProperties())
        {
            if (p.IsShadowProperty() && target is not Dictionary<string, object>) continue;
            var column = p.GetColumnName(table);
            if (column is null || !row.TryGetProperty(column, out var value)) continue;
            Set(target, p.Name, p.PropertyInfo, Convert(value, p.ClrType));
        }
        foreach (var nav in et.GetNavigations().Where(n => n.ForeignKey.IsOwnership && n.TargetEntityType.IsOwned() && !n.IsOnDependent))
        {
            var owned = nav.TargetEntityType;
            var instance = Activator.CreateInstance(owned.ClrType)!;
            if (owned.IsMappedToJson())
            {
                if (!row.TryGetProperty(owned.GetContainerColumnName()!, out var json) || json.ValueKind == JsonValueKind.Null) continue;
                foreach (var p in owned.GetProperties().Where(p => !p.IsShadowProperty()))
                {
                    if (json.TryGetProperty(p.GetJsonPropertyName() ?? p.Name, out var value)) p.PropertyInfo!.SetValue(instance, Convert(value, p.ClrType));
                }
            }
            else
            {
                Fill(owned, instance, row);
            }
            nav.PropertyInfo!.SetValue(target, instance);
        }
    }

    private static void Set(object target, string name, System.Reflection.PropertyInfo? info, object? value)
    {
        if (target is Dictionary<string, object> bag) bag[name] = value!;
        else info!.SetValue(target, value);
    }

    private static object? Convert(JsonElement v, Type type)
    {
        if (v.ValueKind == JsonValueKind.Null) return null;
        var t = Nullable.GetUnderlyingType(type) ?? type;
        if (t == typeof(string)) return v.ValueKind == JsonValueKind.String ? v.GetString() : v.GetRawText();
        if (t == typeof(int)) return v.GetInt32();
        if (t == typeof(long)) return v.GetInt64();
        if (t == typeof(double)) return v.GetDouble();
        if (t == typeof(decimal)) return v.GetDecimal();
        if (t == typeof(bool)) return v.GetBoolean();
        if (t == typeof(DateTime)) return DateTime.Parse(v.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        if (t == typeof(List<string>)) return v.EnumerateArray().Select(e => e.GetString()!).ToList();
        throw new InvalidOperationException("no conversion to " + t);
    }
}
