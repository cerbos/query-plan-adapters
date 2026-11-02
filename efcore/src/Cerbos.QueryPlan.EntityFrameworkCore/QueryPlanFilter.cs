// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Linq.Expressions;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>
/// A translated plan. <see cref="Predicate"/> is always safe to apply: it is <c>false</c> when the plan
/// denies every row and <c>true</c> when it allows every row.
/// </summary>
/// <typeparam name="TEntity">The entity the predicate filters.</typeparam>
public sealed class QueryPlanFilter<TEntity>
{
    internal QueryPlanFilter(FilterKind kind, Expression<Func<TEntity, bool>> predicate)
    {
        Kind = kind;
        Predicate = predicate;
    }

    /// <summary>Lets a caller skip the query when no row can be allowed.</summary>
    public FilterKind Kind { get; }

    /// <summary>The predicate to pass to <c>Where</c>.</summary>
    public Expression<Func<TEntity, bool>> Predicate { get; }
}
