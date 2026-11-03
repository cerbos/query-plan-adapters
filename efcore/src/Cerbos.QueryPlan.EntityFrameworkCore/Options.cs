// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>The database the predicate runs on. Each one needs its own translation of string matching and equality.</summary>
public enum DatabaseProvider
{
    /// <summary>Microsoft SQL Server, through Microsoft.EntityFrameworkCore.SqlServer.</summary>
    SqlServer,

    /// <summary>PostgreSQL, through Npgsql.EntityFrameworkCore.PostgreSQL.</summary>
    PostgreSql,

    /// <summary>SQLite, through Microsoft.EntityFrameworkCore.Sqlite.</summary>
    Sqlite,
}

/// <summary>How a NULL column reaches Cerbos <c>check()</c>.</summary>
public enum NullConvention
{
    /// <summary>The attribute is left out of the resource, so CEL treats it as missing and denies whatever reads it.</summary>
    Omitted,

    /// <summary>The attribute is sent as an explicit <c>null</c> value.</summary>
    Explicit,
}

/// <summary>Whether the plan allows every row, denies every row, or depends on the row.</summary>
public enum FilterKind
{
    /// <summary>Every row is allowed; the predicate is <c>true</c>.</summary>
    AlwaysAllowed,

    /// <summary>No row is allowed; the predicate is <c>false</c>.</summary>
    AlwaysDenied,

    /// <summary>The predicate decides per row.</summary>
    Conditional,
}

/// <summary>What a translation needs besides the plan.</summary>
/// <typeparam name="TEntity">The entity the predicate filters.</typeparam>
public sealed class QueryPlanOptions<TEntity>
{
    /// <summary>Creates options for one provider and one attribute map.</summary>
    public QueryPlanOptions(DatabaseProvider provider, AttributeMap<TEntity> attributes)
    {
        ArgumentNullException.ThrowIfNull(attributes);
        Provider = provider;
        Attributes = attributes;
    }

    /// <summary>The database the predicate runs on.</summary>
    public DatabaseProvider Provider { get; }

    /// <summary>Plan variables mapped onto the entity.</summary>
    public AttributeMap<TEntity> Attributes { get; }

    /// <summary>The convention for every field that does not declare its own. Defaults to <see cref="NullConvention.Omitted"/>.</summary>
    public NullConvention Nulls { get; init; } = NullConvention.Omitted;
}
