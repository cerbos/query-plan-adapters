// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>
/// A translated plan node. A boolean is a pair of two-valued predicates, one selecting the rows where
/// CEL says true and one where it says false; a row in neither is a CEL error, which Cerbos denies under
/// both polarities. Negation swaps the pair, so no SQL NOT ever meets a NULL, and the predicate means the
/// same under EF's C# null semantics and under <c>UseRelationalNulls</c>.
/// </summary>
internal abstract record Term;

internal sealed record BoolTerm(Expression True, Expression False) : Term
{
    public static readonly BoolTerm Error = new(Ex.False, Ex.False);

    public BoolTerm Negate() => new(False, True);
}

/// <summary>A plan constant: string, double, bool, <see cref="CelNull"/>, list, map, timestamp or duration.</summary>
internal sealed record ConstTerm(object Value) : Term;

/// <summary>
/// A scalar read from the store. <see cref="Value"/> means something only where <see cref="Defined"/>
/// holds. Where <see cref="Nullable"/> is false, <see cref="Defined"/> already excludes a null value.
/// </summary>
internal sealed record ScalarTerm(Expression Value, Expression Defined, ScalarKind Kind, bool Nullable) : Term
{
    /// <summary>The same value, with a null counted as an error, as every operator but equality does.</summary>
    public ScalarTerm Strict() => Nullable ? this with { Defined = Ex.And(Defined, Ex.NotNull(Value)), Nullable = false } : this;
}

/// <summary>A value every row errors on.</summary>
internal sealed record ErrorTerm : Term
{
    public static readonly ErrorTerm Instance = new();
}

/// <summary>
/// A list held as related rows. <see cref="Element"/> builds the term for one element row, and
/// <see cref="Position"/>, when the mapping declares one, reads the row's index in the list.
/// </summary>
internal sealed record ListTerm(Expression Source, Type ElementType, Func<Expression, Term> Element, Expression Defined, LambdaExpression? Position = null) : Term;

/// <summary>An element of a list of objects, inside a lambda body.</summary>
internal sealed record ObjectTerm(Expression Target, IReadOnlyDictionary<string, Mapping> Fields) : Term;

/// <summary>The value of <c>hierarchy(path, delimiter)</c>.</summary>
internal sealed record HierarchyTerm(Term Path, string Delimiter) : Term;

/// <summary>A list literal with members read from the store. A member that errors makes the whole list an error.</summary>
internal sealed record TermList(IReadOnlyList<Term> Items) : Term
{
    public Expression Defined { get; } = Ex.And([.. Items.Select(DefinedOf)]);

    public static Expression DefinedOf(Term t) => t switch
    {
        ScalarTerm s => s.Defined,
        BoolTerm b => Ex.Or(b.True, b.False),
        ErrorTerm => Ex.False,
        ListTerm l => l.Defined,
        TermList l => l.Defined,
        _ => Ex.True,
    };

    public BoolTerm Guard(BoolTerm b) => new(Ex.And(Defined, b.True), Ex.And(Defined, b.False));
}

/// <summary>Expression-tree helpers that fold constants, so a translated plan stays as small as the plan.</summary>
internal static class Ex
{
    public static readonly ConstantExpression True = Expression.Constant(true);
    public static readonly ConstantExpression False = Expression.Constant(false);

    public static bool IsTrue(Expression e) => e is ConstantExpression { Value: true };

    public static bool IsFalse(Expression e) => e is ConstantExpression { Value: false };

    public static Expression And(params Expression[] items)
    {
        Expression result = True;
        foreach (var item in items)
        {
            if (IsFalse(item)) return False;
            if (IsTrue(item)) continue;
            result = IsTrue(result) ? item : Expression.AndAlso(result, item);
        }
        return result;
    }

    public static Expression Or(params Expression[] items)
    {
        Expression result = False;
        foreach (var item in items)
        {
            if (IsTrue(item)) return True;
            if (IsFalse(item)) continue;
            result = IsFalse(result) ? item : Expression.OrElse(result, item);
        }
        return result;
    }

    /// <summary>Negates a predicate. Only ever applied to a two-valued predicate.</summary>
    public static Expression Not(Expression e) => e switch
    {
        ConstantExpression { Value: true } => False,
        ConstantExpression { Value: false } => True,
        UnaryExpression { NodeType: ExpressionType.Not } u => u.Operand,
        _ => Expression.Not(e),
    };

    public static Expression NotNull(Expression e) =>
        CanBeNull(e.Type) ? Expression.NotEqual(e, Expression.Constant(null, e.Type)) : True;

    public static Expression IsNull(Expression e) =>
        CanBeNull(e.Type) ? Expression.Equal(e, Expression.Constant(null, e.Type)) : False;

    public static bool CanBeNull(Type t) => !t.IsValueType || System.Nullable.GetUnderlyingType(t) != null;

    /// <summary>
    /// A constant EF sends as a query parameter rather than inlining, so one compiled query serves every
    /// value.
    /// </summary>
    public static Expression Parameter(object? value, Type type)
    {
        var holder = typeof(Holder<>).MakeGenericType(type).GetConstructors()[0].Invoke([value]);
        return Expression.Property(Expression.Constant(holder), "Value");
    }

    private sealed class Holder<T>(T value)
    {
        public T Value { get; } = value;
    }

    private static readonly MethodInfo LikeMethod = typeof(DbFunctionsExtensions).GetMethod(
        nameof(DbFunctionsExtensions.Like), [typeof(DbFunctions), typeof(string), typeof(string), typeof(string)])!;

    private static readonly Expression Functions = Expression.Property(null, typeof(EF), nameof(EF.Functions));

    public static Expression Like(Expression value, string pattern) => Like(value, Parameter(pattern, typeof(string)));

    public static Expression Like(Expression value, Expression pattern) =>
        Expression.Call(LikeMethod, Functions, value, pattern, Expression.Constant("\\"));

    private static readonly MethodInfo ConcatMethod = typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)])!;

    public static Expression Concat(Expression a, Expression b) => Expression.Add(a, b, ConcatMethod);

    private static readonly MethodInfo CompareMethod = typeof(string).GetMethod(nameof(string.Compare), [typeof(string), typeof(string)])!;

    public static Expression CompareStrings(Expression a, Expression b) => Expression.Call(CompareMethod, a, b);

    public static Expression Call(string method, Type elementType, params Expression[] args) =>
        Expression.Call(typeof(Enumerable), method, [elementType], args);

    public static Expression Lambda(Expression body, ParameterExpression parameter) =>
        Expression.Lambda(typeof(Func<,>).MakeGenericType(parameter.Type, typeof(bool)), body, parameter);

    /// <summary>Replaces a mapping lambda's parameter with <paramref name="target"/>.</summary>
    public static Expression Rebind(LambdaExpression path, Expression target) =>
        new Rebinder(path.Parameters[0], target).Visit(path.Body);

    private sealed class Rebinder(ParameterExpression from, Expression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) => node == from ? to : base.VisitParameter(node);
    }

    /// <summary>
    /// Every to-one navigation a member chain passes through must be present: a null navigation is a
    /// missing attribute, not a null value.
    /// </summary>
    public static Expression HopsPresent(Expression body)
    {
        var checks = new List<Expression>();
        var current = Unwrap(body);
        while (current is MemberExpression { Expression: { } inner })
        {
            inner = Unwrap(inner);
            if (inner is not ParameterExpression && !inner.Type.IsValueType && inner.Type != typeof(string))
            {
                checks.Add(Expression.NotEqual(inner, Expression.Constant(null, inner.Type)));
            }
            current = inner;
        }
        checks.Reverse();
        return And([.. checks]);
    }

    private static Expression Unwrap(Expression e) =>
        e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u ? Unwrap(u.Operand) : e;

    /// <summary>Normalises a column to the nullable type its kind compares in.</summary>
    public static Expression Normalise(Expression value, ScalarKind kind)
    {
        var target = kind switch
        {
            ScalarKind.String => typeof(string),
            ScalarKind.Double => typeof(double?),
            ScalarKind.Bool => typeof(bool?),
            _ => System.Nullable.GetUnderlyingType(value.Type) is { } u ? typeof(System.Nullable<>).MakeGenericType(u)
                : value.Type.IsValueType ? typeof(System.Nullable<>).MakeGenericType(value.Type) : value.Type,
        };
        return value.Type == target ? value : Expression.Convert(value, target);
    }
}
