// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Linq.Expressions;
using Cerbos.Api.V1.Engine;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>Translates a Cerbos <c>PlanResources</c> filter into an EF Core predicate.</summary>
public static class QueryPlanAdapter
{
    /// <summary>Translates the filter of an SDK <c>PlanResources</c> response.</summary>
    /// <exception cref="QueryPlanRefusedException">The plan holds a shape the adapter cannot translate faithfully.</exception>
    public static QueryPlanFilter<TEntity> ToFilter<TEntity>(Cerbos.Sdk.Response.PlanResourcesResponse response, QueryPlanOptions<TEntity> options)
    {
        ArgumentNullException.ThrowIfNull(response);
        return ToFilter(response.Filter, options);
    }

    /// <summary>Translates a <c>PlanResources</c> filter.</summary>
    /// <exception cref="QueryPlanRefusedException">The plan holds a shape the adapter cannot translate faithfully.</exception>
    public static QueryPlanFilter<TEntity> ToFilter<TEntity>(PlanResourcesFilter filter, QueryPlanOptions<TEntity> options)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentNullException.ThrowIfNull(options);
        var root = Expression.Parameter(typeof(TEntity), "r");
        switch (filter.Kind)
        {
            case PlanResourcesFilter.Types.Kind.AlwaysAllowed:
                return new QueryPlanFilter<TEntity>(FilterKind.AlwaysAllowed, Expression.Lambda<Func<TEntity, bool>>(Ex.True, root));
            case PlanResourcesFilter.Types.Kind.AlwaysDenied:
                return new QueryPlanFilter<TEntity>(FilterKind.AlwaysDenied, Expression.Lambda<Func<TEntity, bool>>(Ex.False, root));
            case PlanResourcesFilter.Types.Kind.Conditional:
                if (filter.Condition is null)
                {
                    throw new QueryPlanRefusedException("A conditional plan carries no condition.");
                }
                var translator = new Translator(options.Provider, options.Nulls, root, options.Attributes.Entries);
                var body = translator.TranslateCondition(filter.Condition);
                return new QueryPlanFilter<TEntity>(FilterKind.Conditional, Expression.Lambda<Func<TEntity, bool>>(body, root));
            default:
                throw new QueryPlanRefusedException($"The plan has kind {filter.Kind}, which the adapter does not know.");
        }
    }
}
