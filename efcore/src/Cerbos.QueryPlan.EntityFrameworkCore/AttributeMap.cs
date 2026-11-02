// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Linq.Expressions;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>
/// Maps plan variables onto an entity. At the root the key is the full variable name
/// (<c>request.resource.attr.status</c>, <c>request.resource.id</c>); inside a list of objects it is the
/// element's field name (<c>name</c>), as a lambda body reads it (<c>t.name</c>).
/// </summary>
/// <remarks>
/// A path may go through to-one navigations (<c>r =&gt; r.Owner.Name</c>). A navigation that is null is a
/// missing attribute, which Cerbos denies under both polarities, and the predicate follows it.
/// </remarks>
/// <typeparam name="TEntity">The entity, or the list element, the paths start from.</typeparam>
public sealed class AttributeMap<TEntity>
{
    private readonly Dictionary<string, Mapping> _entries = new(StringComparer.Ordinal);

    internal IReadOnlyDictionary<string, Mapping> Entries => _entries;

    /// <summary>Maps a scalar attribute to a column.</summary>
    /// <param name="name">The plan variable, or the element field name inside a list of objects.</param>
    /// <param name="path">The column. A string, number, boolean, <see cref="DateTime"/> or <see cref="DateTimeOffset"/>.</param>
    /// <param name="nulls">How a NULL in this column reaches <c>check()</c>; omit it to use <see cref="QueryPlanOptions{TEntity}.Nulls"/>.</param>
    public AttributeMap<TEntity> Field<TValue>(string name, Expression<Func<TEntity, TValue>> path, NullConvention? nulls = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(path);
        var kind = ScalarKinds.Of(typeof(TValue))
            ?? throw new ArgumentException($"Field '{name}' maps a {typeof(TValue).Name}; a field must be a string, number, boolean, DateTime or DateTimeOffset.", nameof(path));
        _entries.Add(name, new FieldMapping(path, kind, nulls));
        return this;
    }

    /// <summary>Maps a list attribute whose elements are scalars, each stored as one related row.</summary>
    /// <param name="name">The plan variable, or the element field name inside a list of objects.</param>
    /// <param name="path">The collection navigation.</param>
    /// <param name="element">The column of a related row holding the element. A NULL column is a null element.</param>
    /// <param name="position">The column holding the element's zero-based index in the list sent to <c>check()</c>, so <c>R.attr.list[0]</c> translates.</param>
    public AttributeMap<TEntity> List<TElement, TValue>(string name, Expression<Func<TEntity, IEnumerable<TElement>>> path, Expression<Func<TElement, TValue>> element, Expression<Func<TElement, int>>? position = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(element);
        var kind = ScalarKinds.Of(typeof(TValue))
            ?? throw new ArgumentException($"List '{name}' has {typeof(TValue).Name} elements; an element must be a string, number, boolean, DateTime or DateTimeOffset.", nameof(element));
        _entries.Add(name, new ListMapping(path, typeof(TElement), new ScalarElement(element, kind), position));
        return this;
    }

    /// <summary>Maps a list attribute whose elements are objects, each stored as one related row.</summary>
    /// <param name="name">The plan variable, or the element field name inside a list of objects.</param>
    /// <param name="path">The collection navigation.</param>
    /// <param name="fields">The element's fields, keyed by the name a lambda body reads.</param>
    /// <param name="position">The column holding the element's zero-based index in the list sent to <c>check()</c>, so <c>R.attr.list[0]</c> translates.</param>
    public AttributeMap<TEntity> List<TElement>(string name, Expression<Func<TEntity, IEnumerable<TElement>>> path, Action<AttributeMap<TElement>> fields, Expression<Func<TElement, int>>? position = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(fields);
        var members = new AttributeMap<TElement>();
        fields(members);
        _entries.Add(name, new ListMapping(path, typeof(TElement), new ObjectElement(members.Entries), position));
        return this;
    }
}

internal abstract record Mapping(LambdaExpression Path);

internal sealed record FieldMapping(LambdaExpression Path, ScalarKind Kind, NullConvention? Nulls) : Mapping(Path);

internal sealed record ListMapping(LambdaExpression Path, Type ElementType, ElementMapping Element, LambdaExpression? Position) : Mapping(Path);

internal abstract record ElementMapping;

internal sealed record ScalarElement(LambdaExpression Value, ScalarKind Kind) : ElementMapping;

internal sealed record ObjectElement(IReadOnlyDictionary<string, Mapping> Fields) : ElementMapping;
