// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Immutable;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using Cerbos.Api.V1.Engine;
using Operand = Cerbos.Api.V1.Engine.PlanResourcesFilter.Types.Expression.Types.Operand;
using PlanExpression = Cerbos.Api.V1.Engine.PlanResourcesFilter.Types.Expression;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

internal sealed class Translator
{
    private const int MaxUnrolledElements = 64;

    /// <summary>CEL's int range as doubles: int() of a double at or beyond either bound is an error.</summary>
    private const double IntUpper = 9223372036854775808.0;
    private const double IntLower = -9223372036854775808.0;

    /// <summary>
    /// A bound on an int sum, difference or product computed in double space that proves the exact
    /// result fits an int64: the largest double below 2^63 is 2^63 - 1024, and the rounding of the
    /// double-space result stays within that margin.
    /// </summary>
    private const double IntOverflowMargin = 9223372036854773760.0;

    private readonly DatabaseProvider _provider;
    private readonly NullConvention _defaultNulls;
    private readonly ParameterExpression _root;
    private readonly IReadOnlyDictionary<string, Mapping> _rootMap;
    private readonly CelTimestamp _now;

    public Translator(DatabaseProvider provider, NullConvention defaultNulls, ParameterExpression root, IReadOnlyDictionary<string, Mapping> rootMap, DateTimeOffset now)
    {
        _provider = provider;
        _defaultNulls = defaultNulls;
        _root = root;
        _rootMap = rootMap;
        _now = CelTimestamp.FromDateTimeOffset(now);
    }

    public Expression TranslateCondition(Operand condition) =>
        AsBool(Translate(condition, ImmutableDictionary<string, Term>.Empty)).True;

    private Term Translate(Operand operand, ImmutableDictionary<string, Term> scope) => operand.NodeCase switch
    {
        Operand.NodeOneofCase.Value => new ConstTerm(Cel.FromProto(operand.Value)),
        Operand.NodeOneofCase.Variable => Resolve(operand.Variable, scope),
        Operand.NodeOneofCase.Expression => TranslateExpression(operand.Expression, scope),
        _ => throw new QueryPlanRefusedException($"The plan holds an empty operand ({operand.NodeCase})."),
    };

    private Term TranslateExpression(PlanExpression e, ImmutableDictionary<string, Term> scope)
    {
        var ops = e.Operands;
        Term Arg(int i) => Translate(ops[i], scope);
        void Arity(int n)
        {
            if (ops.Count != n)
            {
                throw new QueryPlanRefusedException($"Operator '{e.Operator}' takes {n} operands, and the plan gives it {ops.Count}.");
            }
        }
        Term Unary(Func<Term, Term> f)
        {
            Arity(1);
            return Over(Arg(0), f);
        }
        Term Binary(Func<Term, Term, Term> f)
        {
            Arity(2);
            return Over(Arg(0), Arg(1), f);
        }
        // size() is a CEL int, which % requires.
        Term SizeAsInt(int i)
        {
            var term = Arg(i);
            if (ops[i].Expression?.Operator != "size") return term;
            return term switch
            {
                ScalarTerm { Kind: ScalarKind.Double } s => s with { Value = Expression.Convert(s.Value, typeof(long?)), Kind = ScalarKind.Int },
                ConstTerm { Value: double d } when Cel.IsIntLiteral(d, out var n) => new ConstTerm(n),
                _ => term,
            };
        }

        switch (e.Operator)
        {
            case "and" or "or":
                {
                    var terms = ops.Select(o => AsBool(Translate(o, scope))).ToList();
                    var trues = terms.Select(t => t.True).ToArray();
                    var falses = terms.Select(t => t.False).ToArray();
                    return e.Operator == "and"
                        ? new BoolTerm(Ex.And(trues), Ex.Or(falses))
                        : new BoolTerm(Ex.Or(trues), Ex.And(falses));
                }
            case "not":
                Arity(1);
                return AsBool(Arg(0)).Negate();
            case "if":
                Arity(3);
                return Ternary(AsBool(Arg(0)), Arg(1), Arg(2));
            case "eq" or "ne" or "lt" or "le" or "gt" or "ge":
                return Binary((a, b) => Compare(e.Operator, a, b));
            case "in":
                return Binary(In);
            case "startsWith" or "endsWith" or "contains":
                return Binary((a, b) => StringMatch(e.Operator, a, b));
            case "matches":
                return Binary(Matches);
            case "size":
                return Unary(Size);
            case "has":
                Arity(1);
                return Has(ops[0], scope);
            case "mod":
                Arity(2);
                return Over(SizeAsInt(0), SizeAsInt(1), (a, b) => Arithmetic(e.Operator, a, b));
            case "add" or "sub" or "mult" or "div":
                return Binary((a, b) => Arithmetic(e.Operator, a, b));
            case "exists" or "all" or "exists_one" or "filter" or "map":
                return Macro(e.Operator, ops, scope);
            case "hasIntersection":
                return Binary(HasIntersection);
            case "except" or "intersect" or "isSubset":
                return Binary((a, b) => ListFunction(e.Operator, a, b));
            case "list":
                {
                    var items = ops.Select(o => Translate(o, scope)).ToList();
                    return items.All(i => i is ConstTerm) ? new ConstTerm(items.Select(i => ((ConstTerm)i).Value).ToList()) : new TermList(items);
                }
            case "struct":
                return Struct(ops, scope);
            case "timestamp":
                return Unary(Timestamp);
            case "timeSince":
                return Unary(TimeSince);
            case "duration":
                Arity(1);
                return Arg(0) is ConstTerm { Value: string s }
                    ? Cel.ParseDuration(s) is { } d ? new ConstTerm(d) : ErrorTerm.Instance
                    : throw Refuse("duration() over a value read from the store has no translation; only a literal duration is supported.");
            case "string":
                return Unary(ToText);
            case "double":
                return Unary(t => t switch
                {
                    ConstTerm { Value: double } c => c,
                    ConstTerm { Value: long l } => new ConstTerm((double)l),
                    ScalarTerm { Kind: ScalarKind.Double } num => num.Strict(),
                    ScalarTerm { Kind: ScalarKind.Int } i => new ScalarTerm(Expression.Convert(i.Strict().Value, typeof(double?)), i.Strict().Defined, ScalarKind.Double, false),
                    _ => throw Refuse("double() is translated only over a number: a SQL CAST of a string accepts spellings CEL rejects as malformed."),
                });
            case "int":
                return Unary(ToInt);
            case "upperAscii" or "lowerAscii":
                return Unary(t => AsciiCase(e.Operator == "upperAscii", t));
            case "get-field":
                Arity(2);
                if (ops[1].NodeCase != Operand.NodeOneofCase.Variable)
                {
                    throw Refuse("get-field() names its field with something other than an identifier.");
                }
                if (ops[0].NodeCase == Operand.NodeOneofCase.Variable && Compound(ops[0].Variable, ops[1].Variable, scope) is { } field)
                {
                    return field;
                }
                return Over(Arg(0), t => Member(t, ops[1].Variable, ops[1].Variable));
            case "index":
                Arity(2);
                if (ops[0].NodeCase == Operand.NodeOneofCase.Variable && ops[1].NodeCase == Operand.NodeOneofCase.Value
                    && ops[1].Value.KindCase == Google.Protobuf.WellKnownTypes.Value.KindOneofCase.StringValue
                    && Compound(ops[0].Variable, ops[1].Value.StringValue, scope) is { } member)
                {
                    return member;
                }
                return Over(Arg(0), Arg(1), Index);
            case "hierarchy":
                return HierarchyOf(ops.Select(o => Translate(o, scope)).ToList());
            case "ancestorOf" or "descendentOf" or "overlaps":
                return Binary((a, b) => HierarchyRelation(e.Operator, a, b));
            case "immediateChildOf" or "immediateParentOf" or "siblingOf" or "commonAncestors":
                throw Refuse($"The hierarchy function {e.Operator}() has no translation in this adapter.");
            case "lambda":
                throw Refuse("A lambda appears outside a macro.");
            default:
                throw Refuse($"Operator '{e.Operator}' has no translation.");
        }
    }

    private static QueryPlanRefusedException Refuse(string message) => new(message);

    // -- conditional values ---------------------------------------------------------------------

    /// <summary>Applies <paramref name="f"/> under the arms of a conditional value.</summary>
    private static Term Over(Term t, Func<Term, Term> f) =>
        t is CaseTerm c ? Cases(c.Arms.Select(a => (a.When, f(a.Then)))) : f(t);

    private static Term Over(Term a, Term b, Func<Term, Term, Term> f) =>
        a is CaseTerm ? Over(a, x => Over(b, y => f(x, y))) : Over(b, y => f(a, y));

    /// <summary>
    /// A conditional value from its arms: nested arms flatten, error arms vanish, and arms that are all
    /// booleans become one boolean, since a boolean already carries a predicate per polarity.
    /// </summary>
    private static Term Cases(IEnumerable<(Expression When, Term Then)> arms)
    {
        var flat = new List<(Expression When, Term Then)>();
        foreach (var (when, then) in arms)
        {
            if (Ex.IsFalse(when)) continue;
            switch (then)
            {
                case ErrorTerm:
                    continue;
                case CaseTerm inner:
                    flat.AddRange(inner.Arms.Select(a => (Ex.And(when, a.When), a.Then)).Where(a => !Ex.IsFalse(a.Item1)));
                    continue;
                default:
                    flat.Add((when, then));
                    continue;
            }
        }
        if (flat.Count == 0) return ErrorTerm.Instance;
        if (flat.Count == 1 && Ex.IsTrue(flat[0].When)) return flat[0].Then;
        if (flat.All(a => IsBoolean(a.Then)))
        {
            var bools = flat.Select(a => (a.When, Bool: AsBool(a.Then))).ToList();
            return new BoolTerm(
                Ex.Or([.. bools.Select(a => Ex.And(a.When, a.Bool.True))]),
                Ex.Or([.. bools.Select(a => Ex.And(a.When, a.Bool.False))]));
        }
        return new CaseTerm(flat);
    }

    // -- variables ------------------------------------------------------------------------------

    private Term Resolve(string name, ImmutableDictionary<string, Term> scope)
    {
        if (!name.StartsWith("request.", StringComparison.Ordinal))
        {
            var parts = name.Split('.');
            if (!scope.TryGetValue(parts[0], out var bound))
            {
                throw new UnmappedAttributeException($"The plan reads '{name}', which is no lambda variable in scope.");
            }
            foreach (var part in parts.Skip(1))
            {
                bound = Over(bound, t => Member(t, part, name));
            }
            return bound;
        }
        if (_rootMap.TryGetValue(name, out var mapping))
        {
            return FromMapping(mapping, _root, name);
        }
        throw new UnmappedAttributeException($"The plan reads '{name}', which the attribute map does not map.");
    }

    /// <summary>
    /// <c>R.attr.obj["inner"]</c> and <c>R.attr.obj.inner</c> read the same attribute: when the map
    /// does not map the object but maps the member, the member is the value.
    /// </summary>
    private Term? Compound(string variable, string field, ImmutableDictionary<string, Term> scope)
    {
        if (!variable.StartsWith("request.", StringComparison.Ordinal) || _rootMap.ContainsKey(variable)) return null;
        var name = variable + "." + field;
        return _rootMap.ContainsKey(name) ? Resolve(name, scope) : null;
    }

    private Term Member(Term target, string field, string variable) => target switch
    {
        ObjectTerm o when o.Fields.TryGetValue(field, out var m) => FromMapping(m, o.Target, variable),
        ObjectTerm => throw new UnmappedAttributeException($"The plan reads '{variable}', and the element mapping has no field '{field}'."),
        ConstTerm { Value: Dictionary<string, object> map } => map.TryGetValue(field, out var v) ? new ConstTerm(v) : ErrorTerm.Instance,
        ConstTerm or ErrorTerm => ErrorTerm.Instance,
        _ => throw new UnmappedAttributeException($"The plan reads '{variable}', a field of a value that is not an object."),
    };

    private Term FromMapping(Mapping mapping, Expression target, string name)
    {
        var body = Ex.Rebind(mapping.Path, target);
        var hops = Ex.HopsPresent(body);
        switch (mapping)
        {
            case FieldMapping f:
                {
                    var value = Ex.Normalise(body, f.Kind);
                    var nulls = f.Nulls ?? _defaultNulls;
                    return nulls == NullConvention.Omitted
                        ? new ScalarTerm(value, Ex.And(hops, Ex.NotNull(value)), f.Kind, false)
                        : new ScalarTerm(value, hops, f.Kind, Ex.CanBeNull(value.Type));
                }
            case ListMapping l:
                return new ListTerm(body, l.ElementType, ElementFactory(l.Element), hops, l.Position);
            default:
                throw new InvalidOperationException($"Unknown mapping for '{name}'.");
        }
    }

    private static Func<Expression, Term> ElementFactory(ElementMapping element) => element switch
    {
        ScalarElement s => e =>
        {
            var value = Ex.Normalise(Ex.Rebind(s.Value, e), s.Kind);
            return new ScalarTerm(value, Ex.True, s.Kind, Ex.CanBeNull(value.Type));
        },
        ObjectElement o => e => new ObjectTerm(e, o.Fields),
        _ => throw new InvalidOperationException("Unknown element mapping."),
    };

    // -- booleans -------------------------------------------------------------------------------

    private static BoolTerm AsBool(Term term) => term switch
    {
        BoolTerm b => b,
        ConstTerm { Value: bool v } => v ? new BoolTerm(Ex.True, Ex.False) : new BoolTerm(Ex.False, Ex.True),
        ScalarTerm { Kind: ScalarKind.Bool } s => new BoolTerm(
            Ex.And(s.Defined, Expression.Equal(s.Value, Expression.Constant(true, typeof(bool?)))),
            Ex.And(s.Defined, Expression.Equal(s.Value, Expression.Constant(false, typeof(bool?))))),
        CaseTerm c => Cases(c.Arms.Select(a => (a.When, (Term)AsBool(a.Then)))) is BoolTerm b ? b : BoolTerm.Error,
        _ => BoolTerm.Error,
    };

    /// <summary>A boolean in value position, such as an operand of <c>==</c>.</summary>
    private static Term AsValue(Term term) => term is BoolTerm b
        ? new ScalarTerm(Expression.Convert(b.True, typeof(bool?)), Ex.Or(b.True, b.False), ScalarKind.Bool, false)
        : term;

    private Term Ternary(BoolTerm condition, Term then, Term otherwise)
    {
        if (Ex.IsTrue(condition.True) && Ex.IsFalse(condition.False)) return then;
        if (Ex.IsFalse(condition.True) && Ex.IsTrue(condition.False)) return otherwise;
        if (IsBoolean(then) && IsBoolean(otherwise))
        {
            var a = AsBool(then);
            var b = AsBool(otherwise);
            return new BoolTerm(
                Ex.Or(Ex.And(condition.True, a.True), Ex.And(condition.False, b.True)),
                Ex.Or(Ex.And(condition.True, a.False), Ex.And(condition.False, b.False)));
        }
        // CEL types both branches alike, so a whole-number literal beside an int branch is an int.
        (then, otherwise) = (IntLiteralBeside(then, otherwise), IntLiteralBeside(otherwise, then));
        var (x, y) = (AsScalar(then, otherwise), AsScalar(otherwise, then));
        if (x is not null && y is not null && x.Kind == y.Kind)
        {
            return new ScalarTerm(
                Expression.Condition(condition.True, x.Value, y.Value),
                Ex.Or(Ex.And(condition.True, x.Defined), Ex.And(condition.False, y.Defined)),
                x.Kind,
                x.Nullable || y.Nullable);
        }
        return Cases([(condition.True, then), (condition.False, otherwise)]);
    }

    private static Term IntLiteralBeside(Term term, Term other) =>
        term is ConstTerm { Value: double d } && KindOf(other) == ScalarKind.Int && Cel.IsIntLiteral(d, out var i) ? new ConstTerm(i) : term;

    private static bool IsBoolean(Term t) => t is BoolTerm or ConstTerm { Value: bool } or ScalarTerm { Kind: ScalarKind.Bool };

    /// <summary>A branch as a scalar, a constant taking the type of the other branch.</summary>
    private ScalarTerm? AsScalar(Term term, Term other) => term switch
    {
        ScalarTerm s => s,
        ErrorTerm when other is ScalarTerm o => new ScalarTerm(Expression.Default(o.Value.Type), Ex.False, o.Kind, false),
        ConstTerm c when other is ScalarTerm o && ConstKind(c.Value) == o.Kind && IsFinite(c.Value) =>
            new ScalarTerm(ConstExpression(c.Value, o.Value.Type), Ex.True, o.Kind, false),
        // Two number literals stay literals: whether CEL typed them as ints or doubles decides their spelling.
        ConstTerm c when other is ConstTerm && ConstKind(c.Value) is { } k and not (ScalarKind.Timestamp or ScalarKind.Double) =>
            new ScalarTerm(ConstExpression(c.Value, ClrType(k)), Ex.True, k, false),
        _ => null,
    };

    private static bool IsFinite(object value) => value is not double d || double.IsFinite(d);

    private static bool IsNaN(Term t) => t is ConstTerm { Value: double d } && double.IsNaN(d);

    private static Type ClrType(ScalarKind kind) => kind switch
    {
        ScalarKind.String => typeof(string),
        ScalarKind.Double => typeof(double?),
        ScalarKind.Bool => typeof(bool?),
        ScalarKind.Int => typeof(long?),
        _ => throw new InvalidOperationException(),
    };

    // -- comparison -----------------------------------------------------------------------------

    private static ScalarKind? ConstKind(object value) => value switch
    {
        string => ScalarKind.String,
        double => ScalarKind.Double,
        long => ScalarKind.Int,
        bool => ScalarKind.Bool,
        CelTimestamp => ScalarKind.Timestamp,
        _ => null,
    };

    private Expression ConstExpression(object value, Type type) => value switch
    {
        double d when !double.IsFinite(d) => throw Refuse("NaN and infinities have no translation: SQL Server stores neither, and no provider compares NaN as CEL does."),
        bool b => Expression.Constant(b, type),
        CelTimestamp t => TimestampExpression(t.ToDateTime(TicksPerUnit).Value, type),
        _ => Ex.Parameter(value, type),
    };

    private static Expression TimestampExpression(DateTime value, Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        var v = underlying == typeof(DateTimeOffset) ? (object)new DateTimeOffset(value, TimeSpan.Zero) : value;
        return Ex.Parameter(v, type);
    }

    /// <summary>The finest instant a provider's timestamp column holds, in ticks.</summary>
    private long TicksPerUnit => _provider == DatabaseProvider.PostgreSql ? 10 : 1;

    private static string Flip(string op) => op switch
    {
        "lt" => "gt",
        "le" => "ge",
        "gt" => "lt",
        "ge" => "le",
        _ => op,
    };

    private static string Negated(string op) => op switch
    {
        "lt" => "ge",
        "le" => "gt",
        "gt" => "le",
        "ge" => "lt",
        "eq" => "ne",
        _ => "eq",
    };

    private static BoolTerm Decided(bool holds) => holds ? new BoolTerm(Ex.True, Ex.False) : new BoolTerm(Ex.False, Ex.True);

    private BoolTerm Compare(string op, Term left, Term right)
    {
        if (left is CaseTerm || right is CaseTerm) return AsBool(Over(left, right, (x, y) => Compare(op, x, y)));
        var a = AsValue(left);
        var b = AsValue(right);
        if (a is ErrorTerm || b is ErrorTerm) return BoolTerm.Error;
        if (a is NumberTextTerm or ShiftedTimestampTerm or SinceTerm || b is NumberTextTerm or ShiftedTimestampTerm or SinceTerm)
        {
            return CompareDerived(op, a, b);
        }
        if (IsList(a) || IsList(b) || a is ObjectTerm || b is ObjectTerm)
        {
            return CompareLists(op, a, b);
        }
        if (a is ConstTerm ca && b is ConstTerm cb) return FoldCompare(op, ca.Value, cb.Value);
        if (a is ConstTerm)
        {
            (a, b) = (b, a);
            op = Flip(op);
        }
        var s = (ScalarTerm)a;
        if (b is ConstTerm c && NumericLiteral(op, s, c) is { } settled) return settled;
        return op switch
        {
            "eq" => Equality(s, b),
            "ne" => Equality(s, b).Negate(),
            _ => Ordering(op, s, b),
        };
    }

    private static bool IsList(Term t) => t is ListTerm or TermList or GuardedList or CountedList or ConcatList or ConstTerm { Value: List<object> };

    /// <summary>
    /// An int column against a double literal, or a double column against an int literal, compared
    /// exactly: the literal is moved to the column's kind where that loses nothing, and otherwise the
    /// comparison is settled or shifted to the nearest value the column can hold.
    /// </summary>
    private BoolTerm? NumericLiteral(string op, ScalarTerm s, ConstTerm c)
    {
        switch (s.Kind, c.Value)
        {
            case (ScalarKind.Int, double d):
                {
                    var strict = s.Strict();
                    if (double.IsNaN(d)) return op == "ne" ? new BoolTerm(strict.Defined, Ex.False) : new BoolTerm(Ex.False, strict.Defined);
                    if (Cel.IsIntLiteral(d, out var exact)) return Compare(op, s, new ConstTerm(exact));
                    if (d >= IntUpper || d < IntLower)
                    {
                        var below = d >= IntUpper;
                        var holds = op switch { "lt" or "le" => below, "gt" or "ge" => !below, "ne" => true, _ => false };
                        return holds ? new BoolTerm(strict.Defined, Ex.False) : new BoolTerm(Ex.False, strict.Defined);
                    }
                    // A fraction: the column is below it exactly when at most its floor, above when at least its ceiling.
                    return op switch
                    {
                        "eq" => new BoolTerm(Ex.False, strict.Defined),
                        "ne" => new BoolTerm(strict.Defined, Ex.False),
                        "lt" or "le" => Compare("le", s, new ConstTerm((long)Math.Floor(d))),
                        _ => Compare("ge", s, new ConstTerm((long)Math.Ceiling(d))),
                    };
                }
            case (ScalarKind.Double, long l):
                {
                    var strict = s.Strict();
                    var nearest = (double)l;
                    if (Cel.CompareIntDouble(l, nearest) == 0) return Compare(op, s, new ConstTerm(nearest));
                    // The int sits between two doubles: the column compares with the rounded one, shifted past it.
                    var roundedDown = Cel.CompareIntDouble(l, nearest) > 0;
                    return op switch
                    {
                        "eq" => new BoolTerm(Ex.False, strict.Defined),
                        "ne" => new BoolTerm(strict.Defined, Ex.False),
                        "lt" or "le" => Compare(roundedDown ? "le" : "lt", s, new ConstTerm(nearest)),
                        _ => Compare(roundedDown ? "gt" : "ge", s, new ConstTerm(nearest)),
                    };
                }
            default:
                return null;
        }
    }

    private static BoolTerm FoldCompare(string op, object a, object b)
    {
        if (op is "lt" or "le" or "gt" or "ge" && (a is double da && double.IsNaN(da) || b is double db && double.IsNaN(db))) return Decided(false);
        bool? result = op switch
        {
            "eq" => Cel.Equal(a, b),
            "ne" => !Cel.Equal(a, b),
            _ => Cel.Compare(a, b) is { } c ? op switch
            {
                "lt" => c < 0,
                "le" => c <= 0,
                "gt" => c > 0,
                _ => c >= 0,
            } : null,
        };
        return result switch
        {
            true => Decided(true),
            false => Decided(false),
            null => BoolTerm.Error,
        };
    }

    private BoolTerm Equality(ScalarTerm s, Term other)
    {
        if (other is ConstTerm { Value: CelNull })
        {
            return s.Nullable
                ? new BoolTerm(Ex.And(s.Defined, Ex.IsNull(s.Value)), Ex.And(s.Defined, Ex.NotNull(s.Value)))
                : new BoolTerm(Ex.False, s.Defined);
        }
        if (other is ConstTerm c)
        {
            var kind = ConstKind(c.Value);
            RefuseTextualTimestamp(s.Kind, kind);
            if (kind != s.Kind)
            {
                return new BoolTerm(Ex.False, s.Defined);
            }
            if (c.Value is double d && !double.IsFinite(d) || (c.Value is CelTimestamp ts && ts.ToDateTime(TicksPerUnit).Truncated))
            {
                return new BoolTerm(Ex.False, s.Defined);
            }
            var equal = ValueEquals(s, ConstExpression(c.Value, s.Value.Type));
            return new BoolTerm(
                Ex.And(s.Defined, Ex.NotNull(s.Value), equal),
                Ex.And(s.Defined, s.Nullable ? Ex.Or(Ex.IsNull(s.Value), Ex.And(Ex.NotNull(s.Value), Ex.Not(equal))) : Ex.Not(equal)));
        }
        var t = (ScalarTerm)other;
        RefuseTextualTimestamp(s.Kind, t.Kind);
        if (s.Kind != t.Kind && s.Kind is ScalarKind.Int or ScalarKind.Double && t.Kind is ScalarKind.Int or ScalarKind.Double)
        {
            throw Refuse("Comparing an int() with a number read from the store has no translation: SQL compares them after rounding the int to a double, which CEL does not.");
        }
        var defined = Ex.And(s.Defined, t.Defined);
        var bothNull = s.Nullable && t.Nullable ? Ex.And(Ex.IsNull(s.Value), Ex.IsNull(t.Value)) : Ex.False;
        if (s.Kind != t.Kind)
        {
            return new BoolTerm(Ex.And(defined, bothNull), Ex.And(defined, Ex.Not(bothNull)));
        }
        var eq = ValueEquals(s, t.Value);
        var present = Ex.And(Ex.NotNull(s.Value), Ex.NotNull(t.Value));
        var oneNull = Ex.Or(
            s.Nullable ? Ex.And(Ex.IsNull(s.Value), Ex.NotNull(t.Value)) : Ex.False,
            t.Nullable ? Ex.And(Ex.NotNull(s.Value), Ex.IsNull(t.Value)) : Ex.False);
        return new BoolTerm(
            Ex.And(defined, Ex.Or(Ex.And(present, eq), bothNull)),
            Ex.And(defined, Ex.Or(Ex.And(present, Ex.Not(eq)), oneNull)));
    }

    private static void RefuseTextualTimestamp(ScalarKind a, ScalarKind? b)
    {
        if ((a == ScalarKind.TimestampString && b is ScalarKind.String or ScalarKind.TimestampString)
            || (b == ScalarKind.TimestampString && a is ScalarKind.String))
        {
            throw Refuse("A timestamp column reaches CEL as an RFC 3339 string whose exact spelling the store does not keep, so comparing it as a string has no translation. Wrap it in timestamp().");
        }
    }

    /// <summary>Equality of two present values of one kind, as CEL means it.</summary>
    private Expression ValueEquals(ScalarTerm s, Expression other)
    {
        var eq = Expression.Equal(s.Value, other);
        if (s.Kind != ScalarKind.String || _provider != DatabaseProvider.SqlServer) return eq;
        // SQL Server's = pads the shorter string with spaces, which a sentinel after each side cannot reach.
        return Ex.And(eq, Expression.Equal(Sentinel(s.Value), Sentinel(other)));
    }

    private BoolTerm Ordering(string op, ScalarTerm s, Term other)
    {
        ScalarKind? kind = other switch
        {
            ConstTerm c => ConstKind(c.Value),
            ScalarTerm t => t.Kind,
            _ => null,
        };
        RefuseTextualTimestamp(s.Kind, kind);
        if (IsNaN(other))
        {
            // CEL orders nothing against NaN, whatever the other side: false, not an error.
            return new BoolTerm(Ex.False, s.Strict().Defined);
        }
        if (kind != s.Kind)
        {
            if (s.Kind is ScalarKind.Int or ScalarKind.Double && kind is ScalarKind.Int or ScalarKind.Double)
            {
                throw Refuse("Comparing an int() with a number read from the store has no translation: SQL compares them after rounding the int to a double, which CEL does not.");
            }
            return BoolTerm.Error;
        }
        if (other is ConstTerm { Value: double special } && !double.IsFinite(special))
        {
            // A column holds only finite numbers: every one is below +Infinity and above -Infinity.
            var finite = s.Strict().Defined;
            var holds = op is "lt" or "le" ? special > 0 : special < 0;
            return holds ? new BoolTerm(finite, Ex.False) : new BoolTerm(Ex.False, finite);
        }
        var strict = s.Strict();
        Expression otherValue;
        var defined = strict.Defined;
        if (other is ConstTerm c2)
        {
            if (c2.Value is CelTimestamp ts)
            {
                var (value, truncated) = ts.ToDateTime(TicksPerUnit);
                otherValue = TimestampExpression(value, s.Value.Type);
                if (truncated)
                {
                    // The column holds no instant between the truncated literal and the literal itself.
                    op = op is "lt" or "le" ? "le" : "gt";
                }
            }
            else
            {
                otherValue = ConstExpression(c2.Value, s.Value.Type);
            }
        }
        else
        {
            var t = ((ScalarTerm)other).Strict();
            otherValue = t.Value;
            defined = Ex.And(defined, t.Defined);
        }
        var negated = Negated(op);
        if (kind == ScalarKind.String && _provider == DatabaseProvider.SqlServer)
        {
            return new BoolTerm(
                Ex.And(defined, PaddedOrder(op, strict.Value, otherValue)),
                Ex.And(defined, PaddedOrder(negated, strict.Value, otherValue)));
        }
        return new BoolTerm(
            Ex.And(defined, CompareValues(op, strict.Value, otherValue, kind.Value)),
            Ex.And(defined, CompareValues(negated, strict.Value, otherValue, kind.Value)));
    }

    /// <summary>
    /// CEL's string order on SQL Server, whose comparison pads the shorter string with spaces. Padding
    /// only decides a comparison where one string is a prefix of the other, and there CEL puts the
    /// shorter first; everywhere else the first differing character decides, as it does in SQL.
    /// </summary>
    private Expression PaddedOrder(string op, Expression a, Expression b)
    {
        var aPrefixOfB = Match("startsWith", b, a, null);
        var bPrefixOfA = Match("startsWith", a, b, null);
        var neither = Ex.And(Ex.Not(aPrefixOfB), Ex.Not(bPrefixOfA));
        var equal = Ex.And(aPrefixOfB, bPrefixOfA);
        var sql = CompareValues(op, a, b, ScalarKind.String);
        return op switch
        {
            "lt" => Ex.Or(Ex.And(aPrefixOfB, Ex.Not(equal)), Ex.And(neither, sql)),
            "le" => Ex.Or(aPrefixOfB, Ex.And(neither, sql)),
            "gt" => Ex.Or(Ex.And(bPrefixOfA, Ex.Not(equal)), Ex.And(neither, sql)),
            _ => Ex.Or(bPrefixOfA, Ex.And(neither, sql)),
        };
    }

    private static Expression CompareValues(string op, Expression a, Expression b, ScalarKind kind)
    {
        if (kind == ScalarKind.Bool)
        {
            // CEL orders false before true; SQL Server has no ordering over bit values.
            Expression Is(Expression v, bool value) => Expression.Equal(v, Expression.Constant(value, typeof(bool?)));
            return op switch
            {
                "lt" => Ex.And(Is(a, false), Is(b, true)),
                "le" => Ex.Or(Is(a, false), Is(b, true)),
                "gt" => Ex.And(Is(a, true), Is(b, false)),
                _ => Ex.Or(Is(a, true), Is(b, false)),
            };
        }
        if (kind == ScalarKind.String)
        {
            a = Ex.CompareStrings(a, b);
            b = Expression.Constant(0);
        }
        return op switch
        {
            "lt" => Expression.LessThan(a, b),
            "le" => Expression.LessThanOrEqual(a, b),
            "gt" => Expression.GreaterThan(a, b),
            _ => Expression.GreaterThanOrEqual(a, b),
        };
    }

    /// <summary>Comparisons over string(), timestamp shifts and timeSince(), each solved against a literal.</summary>
    private BoolTerm CompareDerived(string op, Term a, Term b)
    {
        if (a is ConstTerm)
        {
            (a, b) = (b, a);
            op = Flip(op);
        }
        switch (a, b)
        {
            case (NumberTextTerm text, ConstTerm { Value: string literal }):
                if (op is not ("eq" or "ne")) return BoolTerm.Error;
                return op == "eq" ? TextEquals(text, literal) : TextEquals(text, literal).Negate();
            case (NumberTextTerm text, ConstTerm):
                return Unequal(op, text.Number.Strict().Defined);
            case (NumberTextTerm, _):
                throw Refuse("string() of a number is translated only against a literal: CEL's rendering of a number has no SQL spelling that agrees with it on every provider.");
            case (ShiftedTimestampTerm shifted, ConstTerm { Value: CelTimestamp t }):
                {
                    // x + d CMP t holds exactly when x CMP t - d, where x + d stays inside CEL's range.
                    var ts = shifted.Timestamp.Strict();
                    var bound = t + new CelDuration(-shifted.Nanos);
                    var inRange = shifted.Nanos > 0
                        ? Compare("le", ts, new ConstTerm(CelTimestamp.Max + new CelDuration(-shifted.Nanos))).True
                        : Compare("ge", ts, new ConstTerm(CelTimestamp.Min + new CelDuration(-shifted.Nanos))).True;
                    var solved = Compare(op, ts, new ConstTerm(bound));
                    return new BoolTerm(Ex.And(inRange, solved.True), Ex.And(inRange, solved.False));
                }
            case (ShiftedTimestampTerm shifted, ConstTerm):
                return Unequal(op, shifted.Timestamp.Strict().Defined);
            case (ShiftedTimestampTerm, _):
                throw Refuse("A shifted timestamp is compared only against a timestamp literal.");
            case (SinceTerm since, ConstTerm { Value: CelDuration d }):
                // now - x CMP d holds exactly when x mirror(CMP) now - d.
                return Compare(Flip(op), since.Timestamp.Strict(), new ConstTerm(_now + new CelDuration(-d.Nanos)));
            case (SinceTerm since, ConstTerm):
                return Unequal(op, since.Timestamp.Strict().Defined);
            default:
                throw Refuse("timeSince() is compared only against a duration literal.");
        }
    }

    /// <summary>A value of one type beside a literal of another: unequal where defined, and unordered.</summary>
    private static BoolTerm Unequal(string op, Expression defined) => op switch
    {
        "eq" => new BoolTerm(Ex.False, defined),
        "ne" => new BoolTerm(defined, Ex.False),
        _ => BoolTerm.Error,
    };

    /// <summary>
    /// <c>string(number) == literal</c> holds exactly when the literal is CEL's spelling of a value and
    /// the number is that value: every other literal matches no spelling.
    /// </summary>
    private BoolTerm TextEquals(NumberTextTerm text, string literal)
    {
        var number = text.Number.Strict();
        if (number.Kind == ScalarKind.Int)
        {
            return Cel.IsIntSpelling(literal, out var i) ? Compare("eq", number, new ConstTerm(i)) : new BoolTerm(Ex.False, number.Defined);
        }
        if (literal is "0" or "-0")
        {
            throw Refuse("string() of a number compared with \"0\" or \"-0\" has no translation: CEL spells -0.0 as \"-0\", and SQL cannot read the sign of a stored zero.");
        }
        var d = Cel.ParseDoubleSpelling(literal);
        return d is { } value && double.IsFinite(value) ? Compare("eq", number, new ConstTerm(value)) : new BoolTerm(Ex.False, number.Defined);
    }

    // -- lists compared whole -------------------------------------------------------------------

    private BoolTerm CompareLists(string op, Term a, Term b)
    {
        if (op is not ("eq" or "ne")) return BoolTerm.Error;
        if (a is ConstTerm ca && b is ConstTerm cb) return FoldCompare(op, ca.Value, cb.Value);
        if (!IsList(a) && a is not ObjectTerm) (a, b) = (b, a);
        if (!IsList(b) && b is not ObjectTerm)
        {
            // A list or object beside a scalar: unequal, never an error.
            return Unequal(op, Ex.And(DefinedOf(a), DefinedOf(b)));
        }
        BoolTerm equal;
        switch (a, b)
        {
            case (ListTerm l, ConstTerm { Value: List<object> { Count: 0 } }):
                equal = new BoolTerm(Ex.And(l.Defined, Ex.Not(Ex.Call(nameof(Enumerable.Any), l.ElementType, l.Source))), Ex.And(l.Defined, Ex.Call(nameof(Enumerable.Any), l.ElementType, l.Source)));
                break;
            case (ListTerm { Position: { } } l, ConstTerm { Value: List<object> items }):
                equal = ListEquals(l, items.Select(i => (Term)new ConstTerm(i)).ToList());
                break;
            case (ListTerm, ConstTerm { Value: List<object> }):
                throw Refuse("Comparing a whole list read from the store needs the position of each element: declare a position column in the attribute map, and compare the list itself, since a list filtered by except() or filter() keeps no positions.");
            case (TermList list, ConstTerm { Value: List<object> items }):
                equal = ItemsEqual(list.Items, items.Select(i => (Term)new ConstTerm(i)).ToList(), list.Defined);
                break;
            case (GuardedList g, ConstTerm { Value: List<object> { Count: 0 } }):
                equal = new BoolTerm(Ex.And(g.Defined, Ex.Not(Ex.Or([.. g.Items.Select(i => i.Keep)]))), Ex.And(g.Defined, Ex.Or([.. g.Items.Select(i => i.Keep)])));
                break;
            case (CountedList counted, ConstTerm { Value: List<object> { Count: 0 } }):
                equal = counted.Empty;
                break;
            case (ObjectTerm, _) or (_, ObjectTerm):
                throw Refuse("Comparing a whole object read from the store has no translation.");
            default:
                throw Refuse("Comparing a whole list or object read from the store has no translation: related rows have no order or multiplicity a single comparison can read.");
        }
        return op == "eq" ? equal : equal.Negate();
    }

    private static Expression DefinedOf(Term t) => TermList.DefinedOf(t);

    /// <summary>Ordered, exact equality of related rows with a literal, read through the position column.</summary>
    private BoolTerm ListEquals(ListTerm l, IReadOnlyList<Term> items)
    {
        var count = Expression.Equal(Ex.Call(nameof(Enumerable.Count), l.ElementType, l.Source), Expression.Constant(items.Count));
        var holds = new List<Expression> { count };
        for (var k = 0; k < items.Count; k++)
        {
            var e = Expression.Parameter(l.ElementType, "e");
            var at = Expression.Equal(Ex.Rebind(l.Position!, e), Expression.Constant(k));
            var eq = Compare("eq", l.Element(e), items[k]);
            holds.Add(Any(l, e, Ex.And(at, eq.True)));
        }
        var all = Ex.And([.. holds]);
        return new BoolTerm(Ex.And(l.Defined, all), Ex.And(l.Defined, Ex.Not(all)));
    }

    private BoolTerm ItemsEqual(IReadOnlyList<Term> left, IReadOnlyList<Term> right, Expression defined)
    {
        if (left.Count != right.Count) return new BoolTerm(Ex.False, defined);
        var pairs = left.Zip(right).Select(p => Compare("eq", p.First, p.Second)).ToList();
        return new BoolTerm(
            Ex.And([defined, .. pairs.Select(p => p.True)]),
            Ex.And(defined, Ex.Or([.. pairs.Select(p => p.False)])));
    }

    // -- membership -----------------------------------------------------------------------------

    private BoolTerm In(Term needle, Term haystack)
    {
        if (needle is CaseTerm || haystack is CaseTerm) return AsBool(Over(needle, haystack, In));
        needle = AsValue(needle);
        var needleDefined = needle switch
        {
            ScalarTerm s => s.Defined,
            ErrorTerm => Ex.False,
            _ => Ex.True,
        };
        switch (haystack)
        {
            case ConstTerm { Value: List<object> items }:
                return InItems(needle, needleDefined, items.Select(i => (Term)new ConstTerm(i)));
            case TermList list:
                return list.Guard(InItems(needle, needleDefined, list.Items));
            case GuardedList g:
                {
                    var hits = g.Items.Select(i => (i.Keep, Hit: Compare("eq", needle, i.Item))).ToList();
                    return new BoolTerm(
                        Ex.And(g.Defined, Ex.Or([.. hits.Select(h => Ex.And(h.Keep, h.Hit.True))])),
                        Ex.And([g.Defined, needleDefined, .. hits.Select(h => Ex.Or(Ex.Not(h.Keep), h.Hit.False))]));
                }
            case ConcatList c:
                {
                    var parts = c.Parts.Select(p => In(needle, p)).ToList();
                    return new BoolTerm(Ex.Or([.. parts.Select(p => p.True)]), Ex.And([.. parts.Select(p => p.False)]));
                }
            case ListTerm l:
                {
                    if (needle is ListTerm or TermList or GuardedList or CountedList or ConcatList or ObjectTerm)
                    {
                        throw Refuse("Membership of a list read from the store in another list has no translation.");
                    }
                    var e = Expression.Parameter(l.ElementType, "e");
                    var eq = Compare("eq", l.Element(e), needle);
                    return new BoolTerm(
                        Ex.And(l.Defined, needleDefined, Any(l, e, eq.True)),
                        Ex.And(l.Defined, needleDefined, All(l, e, eq.False)));
                }
            case CountedList:
                throw Refuse("Membership in an intersect() over related rows has no translation.");
            case ConstTerm:
            case ScalarTerm:
            case ErrorTerm:
                return BoolTerm.Error;
            default:
                throw Refuse("Membership in a map or object has no translation.");
        }
    }

    private BoolTerm InItems(Term needle, Expression needleDefined, IEnumerable<Term> items)
    {
        var trues = new List<Expression>();
        var falses = new List<Expression> { needleDefined };
        foreach (var item in items)
        {
            var eq = Compare("eq", needle, item);
            trues.Add(eq.True);
            falses.Add(eq.False);
        }
        return new BoolTerm(Ex.Or([.. trues]), Ex.And([.. falses]));
    }

    private static Expression Any(ListTerm l, ParameterExpression e, Expression body)
    {
        if (Ex.IsFalse(body)) return Ex.False;
        return Ex.Call(nameof(Enumerable.Any), l.ElementType, l.Source, Ex.Lambda(body, e));
    }

    private static Expression All(ListTerm l, ParameterExpression e, Expression body)
    {
        if (Ex.IsTrue(body)) return Ex.True;
        return Ex.Call(nameof(Enumerable.All), l.ElementType, l.Source, Ex.Lambda(body, e));
    }

    private static Expression Count(ListTerm l, ParameterExpression e, Expression body) =>
        Ex.IsFalse(body) ? Expression.Constant(0) : Ex.Call(nameof(Enumerable.Count), l.ElementType, l.Source, Ex.Lambda(body, e));

    private BoolTerm HasIntersection(Term a, Term b)
    {
        if (a is CaseTerm || b is CaseTerm) return AsBool(Over(a, b, HasIntersection));
        if (a is ConstTerm { Value: List<object> x } && b is ConstTerm { Value: List<object> y })
        {
            return Decided(x.Any(i => y.Any(j => Cel.Equal(i, j))));
        }
        if (a is not ListTerm && b is ListTerm) (a, b) = (b, a);
        if (a is ListTerm l && b is ListTerm or TermList or ConstTerm { Value: List<object> })
        {
            var e = Expression.Parameter(l.ElementType, "e");
            var hit = In(l.Element(e), b);
            return new BoolTerm(Ex.And(l.Defined, Any(l, e, hit.True)), Ex.And(l.Defined, All(l, e, hit.False)));
        }
        if (a is TermList && b is ConstTerm { Value: List<object> } or TermList)
        {
            var list = (TermList)a;
            var hits = list.Items.Select(i => In(i, b)).ToList();
            return list.Guard(new BoolTerm(Ex.Or([.. hits.Select(h => h.True)]), Ex.And([.. hits.Select(h => h.False)])));
        }
        if (a is ListTerm or TermList or ConstTerm { Value: List<object> } && b is ConstTerm { Value: List<object> } or TermList or ListTerm)
        {
            return HasIntersection(b, a);
        }
        if (IsList(a) || IsList(b))
        {
            throw Refuse("hasIntersection() over a list built by except(), intersect(), filter() or + has no translation.");
        }
        return BoolTerm.Error;
    }

    // -- list functions -------------------------------------------------------------------------

    /// <summary>
    /// Cerbos's except(), intersect() and isSubset(). Over related rows and a literal they are a filter
    /// or a count; a difference whose order matters, or two lists both read from the store, is refused.
    /// </summary>
    private Term ListFunction(string op, Term a, Term b)
    {
        if (a is CaseTerm || b is CaseTerm) return Over(a, b, (x, y) => ListFunction(op, x, y));
        if (a is ErrorTerm || b is ErrorTerm) return ErrorTerm.Instance;
        if (!IsList(a) || !IsList(b))
        {
            return IsList(a) || IsList(b) || a is ScalarTerm or ConstTerm && b is ScalarTerm or ConstTerm
                ? ErrorTerm.Instance
                : throw Refuse($"{op}() over an object has no translation.");
        }
        if (a is ConstTerm { Value: List<object> x } && b is ConstTerm { Value: List<object> y })
        {
            return FoldListFunction(op, x, y);
        }
        switch (op)
        {
            case "except":
                switch (a, b)
                {
                    case (ListTerm l, ConstTerm { Value: List<object> }):
                        {
                            var e = Expression.Parameter(l.ElementType, "e");
                            var keep = In(l.Element(e), b).False;
                            return new ListTerm(Ex.Call(nameof(Enumerable.Where), l.ElementType, l.Source, Ex.Lambda(keep, e)), l.ElementType, l.Element, l.Defined);
                        }
                    case (ConstTerm { Value: List<object> items }, ListTerm or TermList):
                        {
                            var guarded = items.Select(i => (Term)new ConstTerm(i)).Select(i => (In(i, b).False, i)).ToList();
                            return new GuardedList(guarded, DefinedOf(b));
                        }
                    default:
                        throw Refuse("except() is translated between related rows and a literal, or a literal and a list literal with columns in it.");
                }
            case "intersect":
                switch (a, b)
                {
                    case (ListTerm l, ConstTerm { Value: List<object> items }):
                        return Intersect(l, items, rowsFirst: true);
                    case (ConstTerm { Value: List<object> items }, ListTerm l):
                        return Intersect(l, items, rowsFirst: false);
                    default:
                        throw Refuse("intersect() is translated between related rows and a literal.");
                }
            default:
                switch (a, b)
                {
                    case (ListTerm l, ConstTerm { Value: List<object> }):
                        {
                            var e = Expression.Parameter(l.ElementType, "e");
                            var hit = In(l.Element(e), b);
                            return new BoolTerm(Ex.And(l.Defined, All(l, e, hit.True)), Ex.And(l.Defined, Any(l, e, hit.False)));
                        }
                    case (ConstTerm { Value: List<object> items }, ListTerm or TermList):
                        {
                            var hits = items.Select(i => In(new ConstTerm(i), b)).ToList();
                            return new BoolTerm(
                                Ex.And([DefinedOf(b), .. hits.Select(h => h.True)]),
                                Ex.And(DefinedOf(b), Ex.Or([.. hits.Select(h => h.False)])));
                        }
                    default:
                        throw Refuse("isSubset() is translated between related rows and a literal.");
                }
        }
    }

    private static Term FoldListFunction(string op, List<object> x, List<object> y) => op switch
    {
        "except" => new ConstTerm(x.Where(i => !y.Any(j => Cel.Equal(i, j))).ToList()),
        "intersect" => x.Count > y.Count
            ? new ConstTerm(y.Where(i => x.Any(j => Cel.Equal(i, j))).ToList())
            : new ConstTerm(x.Where(i => y.Any(j => Cel.Equal(i, j))).ToList()),
        _ => Decided(x.All(i => y.Any(j => Cel.Equal(i, j)))),
    };

    /// <summary>
    /// Cerbos's intersect() walks the shorter list and keeps each of its elements found in the other,
    /// duplicates included, so the size depends on which list is shorter; whether it is empty does not.
    /// </summary>
    private CountedList Intersect(ListTerm l, List<object> items, bool rowsFirst)
    {
        var literal = new ConstTerm(items);
        var e = Expression.Parameter(l.ElementType, "e");
        var hit = In(l.Element(e), literal);
        var rowsIn = Count(l, e, hit.True);
        Expression literalIn = Expression.Constant(0);
        foreach (var item in items)
        {
            literalIn = Expression.Add(literalIn, Expression.Condition(In(new ConstTerm(item), l).True, Expression.Constant(1), Expression.Constant(0)));
        }
        var rows = Ex.Call(nameof(Enumerable.Count), l.ElementType, l.Source);
        var rowsShorter = rowsFirst
            ? Expression.LessThanOrEqual(rows, Expression.Constant(items.Count))
            : Expression.LessThan(rows, Expression.Constant(items.Count));
        var count = Expression.Condition(rowsShorter, rowsIn, literalIn);
        var empty = new BoolTerm(Ex.And(l.Defined, All(l, e, hit.False)), Ex.And(l.Defined, Any(l, e, hit.True)));
        return new CountedList(count, l.Defined, empty);
    }

    // -- strings --------------------------------------------------------------------------------

    private BoolTerm StringMatch(string op, Term receiver, Term argument)
    {
        if (receiver is CaseTerm || argument is CaseTerm) return AsBool(Over(receiver, argument, (x, y) => StringMatch(op, x, y)));
        if (receiver is ErrorTerm || argument is ErrorTerm) return BoolTerm.Error;
        if (receiver is ScalarTerm { Kind: ScalarKind.TimestampString } || argument is ScalarTerm { Kind: ScalarKind.TimestampString })
        {
            RefuseTextualTimestamp(ScalarKind.TimestampString, ScalarKind.String);
        }
        if (receiver is ConstTerm { Value: string r } && argument is ConstTerm { Value: string n })
        {
            return Decided(op switch
            {
                "startsWith" => r.StartsWith(n, StringComparison.Ordinal),
                "endsWith" => r.EndsWith(n, StringComparison.Ordinal),
                _ => r.Contains(n, StringComparison.Ordinal),
            });
        }
        if (receiver is NumberTextTerm || argument is NumberTextTerm)
        {
            throw Refuse("string() of a number is translated only against a literal: CEL's rendering of a number has no SQL spelling that agrees with it on every provider.");
        }
        if (receiver is not ScalarTerm { Kind: ScalarKind.String } && receiver is not ConstTerm { Value: string })
        {
            return receiver is ScalarTerm or ConstTerm ? BoolTerm.Error : throw Refuse($"{op}() over a list or object has no translation.");
        }
        if (argument is not ScalarTerm { Kind: ScalarKind.String } && argument is not ConstTerm { Value: string })
        {
            return argument is ScalarTerm or ConstTerm ? BoolTerm.Error : throw Refuse($"{op}() with a list or object argument has no translation.");
        }
        var (value, valueDefined) = StringOperand(receiver);
        var (needle, needleDefined) = StringOperand(argument);
        var match = Match(op, value, needle, (argument as ConstTerm)?.Value as string);
        var defined = Ex.And(valueDefined, needleDefined);
        return new BoolTerm(Ex.And(defined, match), Ex.And(defined, Ex.Not(match)));
    }

    private static (Expression Value, Expression Defined) StringOperand(Term t) => t switch
    {
        ScalarTerm s => (s.Strict().Value, s.Strict().Defined),
        ConstTerm { Value: string c } => (Ex.Parameter(c, typeof(string)), Ex.True),
        _ => throw new InvalidOperationException(),
    };

    private static readonly MethodInfo StringStartsWith = typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!;
    private static readonly MethodInfo StringEndsWith = typeof(string).GetMethod(nameof(string.EndsWith), [typeof(string)])!;
    private static readonly MethodInfo StringContains = typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;
    private static readonly MethodInfo StringReplace = typeof(string).GetMethod(nameof(string.Replace), [typeof(string), typeof(string)])!;
    private static readonly MethodInfo MathTruncate = typeof(Math).GetMethod(nameof(Math.Truncate), [typeof(double)])!;

    /// <summary>
    /// <paramref name="value"/> starts with, ends with or contains <paramref name="needle"/>, by code point and
    /// case-sensitively. <paramref name="literal"/> is the needle when it is a plan constant.
    /// </summary>
    private Expression Match(string op, Expression value, Expression needle, string? literal)
    {
        switch (_provider)
        {
            case DatabaseProvider.SqlServer:
                {
                    // LIKE on varchar ignores the value's trailing spaces, so match against value + "." and
                    // make the pattern end before that sentinel.
                    var (prefix, suffix) = op switch
                    {
                        "startsWith" => ("", "%_"),
                        "endsWith" => ("%", "."),
                        _ => ("%", "%_"),
                    };
                    var pattern = literal is not null
                        ? Ex.Parameter(prefix + Cel.EscapeLike(literal, escapeBracket: true) + suffix, typeof(string))
                        : Ex.Concat(prefix.Length == 0 ? EscapeLikeInQuery(needle) : Ex.Concat(Expression.Constant(prefix), EscapeLikeInQuery(needle)), Expression.Constant(suffix));
                    return Ex.Like(Sentinel(value), pattern);
                }
            case DatabaseProvider.Sqlite when literal is not null:
                {
                    // SQLite's LIKE folds ASCII case unless a connection pragma says otherwise: match by substring.
                    var length = Cel.CodePointLength(literal);
                    return op switch
                    {
                        "startsWith" => Expression.Equal(Substring(value, Expression.Constant(0), Expression.Constant(length)), needle),
                        "endsWith" => Ex.And(
                            Expression.GreaterThanOrEqual(Length(value), Expression.Constant(length)),
                            Expression.Equal(Substring(value, Expression.Subtract(Length(value), Expression.Constant(length)), null), needle)),
                        _ => Expression.Call(value, StringContains, needle),
                    };
                }
            case DatabaseProvider.PostgreSql when literal is not null:
                {
                    var escaped = Cel.EscapeLike(literal, escapeBracket: false);
                    return Ex.Like(value, op switch
                    {
                        "startsWith" => escaped + "%",
                        "endsWith" => "%" + escaped,
                        _ => "%" + escaped + "%",
                    });
                }
            default:
                // PostgreSQL's left/right/strpos and SQLite's substr/instr, which compare exactly.
                return Expression.Call(value, op switch
                {
                    "startsWith" => StringStartsWith,
                    "endsWith" => StringEndsWith,
                    _ => StringContains,
                }, needle);
        }
    }

    /// <summary>Escapes LIKE's metacharacters in a string the query reads, with SQL REPLACE.</summary>
    private static Expression EscapeLikeInQuery(Expression s)
    {
        foreach (var c in new[] { "\\", "%", "_", "[" })
        {
            s = Expression.Call(s, StringReplace, Expression.Constant(c), Expression.Constant("\\" + c));
        }
        return s;
    }

    /// <summary>The value with a "." appended, which SQL Server's padding and trimming rules cannot reach past.</summary>
    private static Expression Sentinel(Expression value) => Ex.Concat(value, Expression.Constant("."));

    /// <summary>
    /// The length in code points, as CEL's size() counts it. On SQL Server the sentinel is nvarchar,
    /// so LEN counts UTF-16 units unless a supplementary-character collation is applied.
    /// </summary>
    private Expression StringLength(Expression value) => _provider == DatabaseProvider.SqlServer
        ? Expression.Subtract(Length(Ex.Collate(Sentinel(value), "Latin1_General_100_CI_AS_SC")), Expression.Constant(1))
        : Length(value);

    private static Expression Length(Expression value) => Expression.Property(value, nameof(string.Length));

    private static Expression Substring(Expression value, Expression start, Expression? length) => length is null
        ? Expression.Call(value, typeof(string).GetMethod(nameof(string.Substring), [typeof(int)])!, start)
        : Expression.Call(value, typeof(string).GetMethod(nameof(string.Substring), [typeof(int), typeof(int)])!, start, length);

    private Term Size(Term term)
    {
        if (term is CaseTerm) return Over(term, Size);
        switch (term)
        {
            case ConstTerm { Value: string s }:
                return new ConstTerm((double)Cel.CodePointLength(s));
            case ConstTerm { Value: List<object> l }:
                return new ConstTerm((double)l.Count);
            case ConstTerm { Value: Dictionary<string, object> m }:
                return new ConstTerm((double)m.Count);
            case ScalarTerm { Kind: ScalarKind.String } s:
                var strict = s.Strict();
                return new ScalarTerm(Expression.Convert(StringLength(strict.Value), typeof(double?)), strict.Defined, ScalarKind.Double, false);
            case ScalarTerm { Kind: ScalarKind.TimestampString }:
                RefuseTextualTimestamp(ScalarKind.TimestampString, ScalarKind.String);
                return ErrorTerm.Instance;
            case ListTerm l:
                return new ScalarTerm(
                    Expression.Convert(Ex.Call(nameof(Enumerable.Count), l.ElementType, l.Source), typeof(double?)),
                    l.Defined, ScalarKind.Double, false);
            case TermList t:
                return new ScalarTerm(Ex.Parameter((double?)t.Items.Count, typeof(double?)), t.Defined, ScalarKind.Double, false);
            case GuardedList g:
                {
                    Expression count = Expression.Constant(0);
                    foreach (var (keep, _) in g.Items)
                    {
                        count = Expression.Add(count, Expression.Condition(keep, Expression.Constant(1), Expression.Constant(0)));
                    }
                    return new ScalarTerm(Expression.Convert(count, typeof(double?)), g.Defined, ScalarKind.Double, false);
                }
            case CountedList c:
                return new ScalarTerm(Expression.Convert(c.Count, typeof(double?)), c.Defined, ScalarKind.Double, false);
            case ConcatList c:
                {
                    var sizes = c.Parts.Select(Size).Cast<ScalarTerm>().ToList();
                    return new ScalarTerm(
                        sizes.Skip(1).Aggregate(sizes[0].Value, (sum, s) => Expression.Add(sum, s.Value)),
                        Ex.And([.. sizes.Select(s => s.Defined)]), ScalarKind.Double, false);
                }
            case NumberTextTerm:
                throw Refuse("size() of string() of a number has no translation: CEL's rendering of a number has no SQL spelling that agrees with it on every provider.");
            case ScalarTerm or ConstTerm or ErrorTerm or BoolTerm or ShiftedTimestampTerm or SinceTerm:
                return ErrorTerm.Instance;
            default:
                throw Refuse("size() of an object has no translation.");
        }
    }

    /// <summary>
    /// <c>upperAscii()</c> and <c>lowerAscii()</c> fold only the 26 ASCII letters, where SQL's UPPER and LOWER
    /// fold every letter: each ASCII letter is replaced on its own, which is exact under a binary collation.
    /// </summary>
    private static Term AsciiCase(bool upper, Term term)
    {
        if (term is CaseTerm) return Over(term, t => AsciiCase(upper, t));
        switch (term)
        {
            case ConstTerm { Value: string s }:
                return new ConstTerm(string.Concat(s.Select(c => c is >= 'a' and <= 'z' && upper ? (char)(c - 32) : c is >= 'A' and <= 'Z' && !upper ? (char)(c + 32) : c)));
            case ScalarTerm { Kind: ScalarKind.String } s:
                {
                    var strict = s.Strict();
                    var value = strict.Value;
                    for (var c = 'a'; c <= 'z'; c++)
                    {
                        var (from, to) = upper ? (c, (char)(c - 32)) : ((char)(c - 32), c);
                        value = Expression.Call(value, StringReplace, Expression.Constant(from.ToString()), Expression.Constant(to.ToString()));
                    }
                    return new ScalarTerm(value, strict.Defined, ScalarKind.String, false);
                }
            case ScalarTerm { Kind: ScalarKind.TimestampString }:
                RefuseTextualTimestamp(ScalarKind.TimestampString, ScalarKind.String);
                return ErrorTerm.Instance;
            case ScalarTerm or ConstTerm or ErrorTerm or BoolTerm:
                return ErrorTerm.Instance;
            default:
                throw Refuse($"{(upper ? "upperAscii" : "lowerAscii")}() over a list or object has no translation.");
        }
    }

    // -- regular expressions --------------------------------------------------------------------

    /// <summary>
    /// <c>matches()</c>, lowered through <see cref="RegexLowering"/> into the adapter's exact string
    /// predicates, never into a provider's own regex dialect, none of which is RE2.
    /// </summary>
    private BoolTerm Matches(Term receiver, Term pattern)
    {
        if (receiver is CaseTerm || pattern is CaseTerm) return AsBool(Over(receiver, pattern, Matches));
        if (receiver is ErrorTerm || pattern is ErrorTerm) return BoolTerm.Error;
        if (pattern is not ConstTerm { Value: string p })
        {
            return pattern is ConstTerm ? BoolTerm.Error : throw Refuse("matches() with a pattern read from the store has no translation: the pattern is lowered when the plan is translated.");
        }
        if (receiver is ScalarTerm { Kind: ScalarKind.TimestampString }) RefuseTextualTimestamp(ScalarKind.TimestampString, ScalarKind.String);
        if (receiver is NumberTextTerm) throw Refuse("string() of a number is translated only against a literal: CEL's rendering of a number has no SQL spelling that agrees with it on every provider.");
        if (receiver is not (ConstTerm { Value: string } or ScalarTerm { Kind: ScalarKind.String }))
        {
            return receiver is ScalarTerm or ConstTerm ? BoolTerm.Error : throw Refuse("matches() over a list or object has no translation.");
        }
        var plans = RegexLowering.Compile(p);
        if (plans is null) return BoolTerm.Error;
        if (receiver is ConstTerm { Value: string value })
        {
            return Decided(plans.Any(plan => RegexLowering.Holds(plan, value)));
        }
        var strict = ((ScalarTerm)receiver).Strict();
        var conditions = plans.Select(plan => LowerRegex(plan, strict)).ToList();
        return new BoolTerm(
            Ex.And(strict.Defined, Ex.Or([.. conditions.Select(c => c.True)])),
            Ex.And([strict.Defined, .. conditions.Select(c => c.False)]));
    }

    private BoolTerm LowerRegex(RegexLowering.Plan plan, ScalarTerm value)
    {
        BoolTerm AnyOf(IEnumerable<BoolTerm> terms)
        {
            var list = terms.ToList();
            return new BoolTerm(Ex.Or([.. list.Select(t => t.True)]), Ex.And([.. list.Select(t => t.False)]));
        }
        BoolTerm AllOf(params BoolTerm[] terms) => new(Ex.And([.. terms.Select(t => t.True)]), Ex.Or([.. terms.Select(t => t.False)]));
        BoolTerm AtLeast(int characters) => characters > 0 ? Compare("ge", Size(value), new ConstTerm((double)characters)) : Decided(true);
        BoolTerm NoNewline() => StringMatch("contains", value, new ConstTerm("\n")).Negate();
        switch (plan)
        {
            case RegexLowering.OneOf e:
                return In(value, new ConstTerm(e.Literals.Cast<object>().ToList()));
            case RegexLowering.Matches m:
                return AnyOf(m.Literals.Select(l => StringMatch(m.Operator, value, new ConstTerm(l))));
            case RegexLowering.AllCharactersIn a:
                {
                    // Remove every allowed character; nothing may be left.
                    var rest = a.Characters.Aggregate(value.Value, (current, c) => Expression.Call(current, StringReplace, Expression.Constant(c), Expression.Constant("")));
                    var remainder = new ScalarTerm(rest, value.Defined, ScalarKind.String, false);
                    return AllOf(Compare("eq", Size(remainder), new ConstTerm(0.0)), AtLeast(a.Min));
                }
            case RegexLowering.NoNewline n:
                return AllOf(NoNewline(), AtLeast(n.Min));
            case RegexLowering.PrefixSuffix p:
                return AllOf(
                    AnyOf(p.Pairs.Select(pair => AllOf(
                        StringMatch("startsWith", value, new ConstTerm(pair.Prefix)),
                        StringMatch("endsWith", value, new ConstTerm(pair.Suffix)),
                        AtLeast(Cel.CodePointLength(pair.Prefix) + p.Min + Cel.CodePointLength(pair.Suffix))))),
                    NoNewline());
            default:
                throw new InvalidOperationException();
        }
    }

    // -- presence -------------------------------------------------------------------------------

    private BoolTerm Has(Operand operand, ImmutableDictionary<string, Term> scope)
    {
        if (operand.NodeCase != Operand.NodeOneofCase.Variable)
        {
            throw Refuse("has() over anything but an attribute has no translation.");
        }
        var name = operand.Variable;
        Mapping? mapping;
        Expression target;
        if (name.StartsWith("request.", StringComparison.Ordinal))
        {
            if (!_rootMap.TryGetValue(name, out mapping))
            {
                return HasPrefix(name) ?? throw new UnmappedAttributeException($"The plan tests has({name}), which the attribute map does not map.");
            }
            target = _root;
        }
        else
        {
            var dot = name.LastIndexOf('.');
            if (dot < 0) throw Refuse($"has({name}) does not select a field.");
            var owner = Resolve(name[..dot], scope);
            var field = name[(dot + 1)..];
            switch (owner)
            {
                case ConstTerm { Value: Dictionary<string, object> map }:
                    return Decided(map.ContainsKey(field));
                case ObjectTerm o when o.Fields.TryGetValue(field, out mapping):
                    target = o.Target;
                    break;
                case ObjectTerm:
                    throw new UnmappedAttributeException($"The plan tests has({name}), and the element mapping has no field '{field}'.");
                default:
                    return BoolTerm.Error;
            }
        }
        var body = Ex.Rebind(mapping.Path, target);
        var hops = Ex.HopsPresent(body);
        if (mapping is FieldMapping f && (f.Nulls ?? _defaultNulls) == NullConvention.Omitted)
        {
            var value = Ex.Normalise(body, f.Kind);
            return new BoolTerm(Ex.And(hops, Ex.NotNull(value)), Ex.And(hops, Ex.IsNull(value)));
        }
        return new BoolTerm(hops, Ex.False);
    }

    /// <summary>
    /// <c>has(R.attr.parent)</c> over an attribute the map holds only members of: the object is present
    /// exactly when the navigations its members pass through are, so the test is the presence of the
    /// owner of any member.
    /// </summary>
    private BoolTerm? HasPrefix(string name)
    {
        var prefix = name + ".";
        var owners = new List<Expression>();
        foreach (var (member, mapping) in _rootMap)
        {
            if (!member.StartsWith(prefix, StringComparison.Ordinal) || member.IndexOf('.', prefix.Length) >= 0) continue;
            var body = Ex.Rebind(mapping.Path, _root);
            owners.Add(Ex.OwnerPresent(body));
        }
        if (owners.Count == 0) return null;
        var present = Ex.Or([.. owners]);
        return new BoolTerm(present, Ex.Not(present));
    }

    // -- arithmetic -----------------------------------------------------------------------------

    private Term Arithmetic(string op, Term left, Term right)
    {
        if (left is CaseTerm || right is CaseTerm) return Over(left, right, (x, y) => Arithmetic(op, x, y));
        if (left is ConstTerm { Value: var a } && right is ConstTerm { Value: var b })
        {
            return FoldArithmetic(op, a, b);
        }
        if (left is ErrorTerm || right is ErrorTerm) return ErrorTerm.Instance;
        if (IsList(left) && IsList(right))
        {
            return op == "add" ? Concat(left, right) : ErrorTerm.Instance;
        }
        if (IsList(left) || IsList(right) || left is ObjectTerm || right is ObjectTerm)
        {
            return left is ListTerm or TermList or ConstTerm or ScalarTerm && right is ListTerm or TermList or ConstTerm or ScalarTerm
                ? ErrorTerm.Instance
                : throw Refuse($"Arithmetic over a list built by except(), intersect(), filter() or + has no translation.");
        }
        if (left is ScalarTerm { Kind: ScalarKind.Timestamp } or ShiftedTimestampTerm && right is ConstTerm { Value: CelDuration d1 } && op is "add" or "sub")
        {
            return Shift(left, op == "add" ? d1.Nanos : -d1.Nanos);
        }
        if (left is ConstTerm { Value: CelDuration d2 } && right is ScalarTerm { Kind: ScalarKind.Timestamp } or ShiftedTimestampTerm && op == "add")
        {
            return Shift(right, d2.Nanos);
        }
        if (left is not (ScalarTerm or ConstTerm) || right is not (ScalarTerm or ConstTerm))
        {
            throw Refuse("Arithmetic over string() of a number, a shifted timestamp or timeSince() has no translation.");
        }
        // A whole-number literal beside an int is an int: CEL has no int + double overload, so the policy compiled with an int literal.
        (left, right) = (IntLiteralBeside(left, right), IntLiteralBeside(right, left));
        var kind = KindOf(left);
        if (kind != KindOf(right) || kind is null)
        {
            if (kind is ScalarKind.Timestamp or ScalarKind.TimestampString || KindOf(right) is ScalarKind.Timestamp or ScalarKind.TimestampString
                || left is ConstTerm { Value: CelDuration } || right is ConstTerm { Value: CelDuration })
            {
                throw Refuse("Timestamp arithmetic over a column has no translation.");
            }
            return ErrorTerm.Instance;
        }
        if (kind == ScalarKind.String) return op == "add" ? Concatenate(left, right) : ErrorTerm.Instance;
        if (kind == ScalarKind.Int) return IntArithmetic(op, left, right);
        if (kind != ScalarKind.Double)
        {
            return kind == ScalarKind.Bool ? ErrorTerm.Instance : throw Refuse("Timestamp arithmetic over a column has no translation.");
        }
        if (op == "mod") return ErrorTerm.Instance;
        if (left is ConstTerm { Value: double x } && !double.IsFinite(x)) return NonFinite(op, x, (ScalarTerm)right, constantFirst: true);
        if (right is ConstTerm { Value: double y } && !double.IsFinite(y)) return NonFinite(op, y, (ScalarTerm)left, constantFirst: false);
        if (op == "div" && right is ConstTerm { Value: 0.0 } or ScalarTerm)
        {
            return Divide(left, right);
        }
        var (l, ld) = Leaf(left, typeof(double?));
        var (r, rd) = Leaf(right, typeof(double?));
        Expression value = op switch
        {
            "add" => Expression.Add(l, r),
            "sub" => Expression.Subtract(l, r),
            "mult" => Expression.Multiply(l, r),
            _ => Expression.Divide(l, r),
        };
        return new ScalarTerm(value, Ex.And(ld, rd), ScalarKind.Double, false);
    }

    private Term Concatenate(Term left, Term right)
    {
        var (l, ld) = Leaf(left, typeof(string));
        var (r, rd) = Leaf(right, typeof(string));
        return new ScalarTerm(Ex.Concat(l, r), Ex.And(ld, rd), ScalarKind.String, false);
    }

    private (Expression, Expression) Leaf(Term t, Type type) => t switch
    {
        ScalarTerm s => (s.Strict().Value, s.Strict().Defined),
        ConstTerm c => (ConstExpression(c.Value, type), Ex.True),
        _ => throw new InvalidOperationException(),
    };

    /// <summary>
    /// A NaN or infinity beside a column: NaN absorbs every operation, an infinity survives a sum or a
    /// difference, and a product or quotient would take the sign of the column, which is refused.
    /// </summary>
    private static Term NonFinite(string op, double constant, ScalarTerm column, bool constantFirst)
    {
        var defined = column.Strict().Defined;
        if (double.IsNaN(constant) || op is "add" or "sub")
        {
            var result = double.IsNaN(constant) ? double.NaN : op == "sub" && !constantFirst ? -constant : constant;
            return Cases([(defined, new ConstTerm(result))]);
        }
        throw Refuse("Multiplying or dividing an infinity by a value read from the store has no translation: the sign of the result is the sign of the column, which includes the sign of a stored zero that SQL cannot read.");
    }

    /// <summary>
    /// Division whose divisor can be zero. CEL divides by zero into NaN or an infinity of the zero's
    /// sign, and SQL raises or yields NULL, so each row is routed by its divisor: a constant zero has a
    /// known sign, and a column equal to the dividend makes only NaN. Any other column divisor is
    /// refused, since SQL cannot read the sign of a stored zero.
    /// </summary>
    private Term Divide(Term dividend, Term divisor)
    {
        if (divisor is ConstTerm { Value: double zero })
        {
            var n = ((ScalarTerm)dividend).Strict();
            var negative = double.IsNegative(zero);
            Expression Sign(string op) => Compare(op, n, new ConstTerm(0.0)).True;
            return Cases([
                (Sign("eq"), new ConstTerm(double.NaN)),
                (Sign("gt"), new ConstTerm(negative ? double.NegativeInfinity : double.PositiveInfinity)),
                (Sign("lt"), new ConstTerm(negative ? double.PositiveInfinity : double.NegativeInfinity)),
            ]);
        }
        var d = ((ScalarTerm)divisor).Strict();
        if (dividend is not ScalarTerm num || !Ex.Same(num.Value, d.Value))
        {
            throw Refuse("Division by a value read from the store has no translation unless it is the dividend itself: CEL divides by zero into an infinity of the zero's sign, which SQL cannot read from a stored zero, and the comparison depends on it.");
        }
        var zeroDivisor = Compare("eq", d, new ConstTerm(0.0));
        var quotient = Expression.Condition(zeroDivisor.False, Expression.Divide(num.Strict().Value, d.Value), Expression.Constant(null, typeof(double?)));
        return Cases([
            (zeroDivisor.True, new ConstTerm(double.NaN)),
            (zeroDivisor.False, new ScalarTerm(quotient, zeroDivisor.False, ScalarKind.Double, false)),
        ]);
    }

    /// <summary>
    /// Int arithmetic, in bigint: + - * are checked against overflow through their double-space value,
    /// and / and % truncate toward zero as CEL does, with a zero divisor and MinInt64 / -1 as errors.
    /// </summary>
    private Term IntArithmetic(string op, Term left, Term right)
    {
        var (l, ld) = Leaf(left, typeof(long?));
        var (r, rd) = Leaf(right, typeof(long?));
        var defined = Ex.And(ld, rd);
        if (op is "div" or "mod")
        {
            var zero = Expression.Equal(r, Expression.Constant(0L, typeof(long?)));
            var overflow = Ex.And(Expression.Equal(l, Expression.Constant(long.MinValue, typeof(long?))), Expression.Equal(r, Expression.Constant(-1L, typeof(long?))));
            var legal = Ex.And(Ex.Not(zero), Ex.Not(overflow));
            var result = op == "div" ? Expression.Divide(l, r) : Expression.Modulo(l, r);
            return new ScalarTerm(Expression.Condition(legal, result, Expression.Constant(null, typeof(long?))), Ex.And(defined, legal), ScalarKind.Int, false);
        }
        Expression value = op switch
        {
            "add" => Expression.Add(l, r),
            "sub" => Expression.Subtract(l, r),
            _ => Expression.Multiply(l, r),
        };
        var (ld2, rd2) = (Expression.Convert(l, typeof(double?)), Expression.Convert(r, typeof(double?)));
        Expression approximate = op switch
        {
            "add" => Expression.Add(ld2, rd2),
            "sub" => Expression.Subtract(ld2, rd2),
            _ => Expression.Multiply(ld2, rd2),
        };
        var fits = Ex.And(
            Expression.GreaterThanOrEqual(approximate, Expression.Constant(-IntOverflowMargin, typeof(double?))),
            Expression.LessThanOrEqual(approximate, Expression.Constant(IntOverflowMargin, typeof(double?))));
        return new ScalarTerm(value, Ex.And(defined, fits), ScalarKind.Int, false);
    }

    private static Term Concat(Term left, Term right)
    {
        if (left is ConstTerm { Value: List<object> x } && right is ConstTerm { Value: List<object> y }) return new ConstTerm(x.Concat(y).ToList());
        var parts = new List<Term>();
        foreach (var side in new[] { left, right })
        {
            if (side is ConcatList c) parts.AddRange(c.Parts);
            else parts.Add(side);
        }
        return new ConcatList(parts);
    }

    private static Term Shift(Term timestamp, BigInteger nanos) => timestamp switch
    {
        ShiftedTimestampTerm s => new ShiftedTimestampTerm(s.Timestamp, s.Nanos + nanos),
        ScalarTerm s => new ShiftedTimestampTerm(s.Strict(), nanos),
        _ => throw new InvalidOperationException(),
    };

    private static ScalarKind? KindOf(Term t) => t switch
    {
        ScalarTerm s => s.Kind,
        ConstTerm c => ConstKind(c.Value),
        _ => null,
    };

    private static Term FoldArithmetic(string op, object a, object b)
    {
        switch (a, b)
        {
            case (double x, double y):
                return op == "mod" ? ErrorTerm.Instance : new ConstTerm(op switch
                {
                    "add" => x + y,
                    "sub" => x - y,
                    "mult" => x * y,
                    _ => x / y,
                });
            case (long x, long y):
                return FoldInt(op, x, y);
            case (long x, double y) when Cel.IsIntLiteral(y, out var yi):
                return FoldInt(op, x, yi);
            case (double x, long y) when Cel.IsIntLiteral(x, out var xi):
                return FoldInt(op, xi, y);
            case (string x, string y) when op == "add":
                return new ConstTerm(x + y);
            case (List<object> x, List<object> y) when op == "add":
                return new ConstTerm(x.Concat(y).ToList());
            case (CelTimestamp t, CelDuration d) when op is "add" or "sub":
                return new ConstTerm(t + new CelDuration(op == "add" ? d.Nanos : -d.Nanos));
            case (CelDuration d, CelTimestamp t) when op is "add":
                return new ConstTerm(t + d);
            case (CelDuration x, CelDuration y) when op is "add" or "sub":
                return new ConstTerm(new CelDuration(op == "add" ? x.Nanos + y.Nanos : x.Nanos - y.Nanos));
            default:
                return ErrorTerm.Instance;
        }
    }

    private static Term FoldInt(string op, long x, long y)
    {
        try
        {
            return new ConstTerm(op switch
            {
                "add" => checked(x + y),
                "sub" => checked(x - y),
                "mult" => checked(x * y),
                "div" => y == 0 ? throw new DivideByZeroException() : checked(x / y),
                _ => y == 0 ? throw new DivideByZeroException() : y == -1 && x == long.MinValue ? throw new OverflowException() : x % y,
            });
        }
        catch (ArithmeticException)
        {
            return ErrorTerm.Instance;
        }
    }

    // -- conversions ----------------------------------------------------------------------------

    private Term ToText(Term term) => term switch
    {
        CaseTerm => Over(term, ToText),
        ConstTerm { Value: string } c => c,
        ConstTerm { Value: bool b } => new ConstTerm(b ? "true" : "false"),
        ConstTerm { Value: long l } => new ConstTerm(l.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        ScalarTerm { Kind: ScalarKind.String } str => str.Strict(),
        ScalarTerm { Kind: ScalarKind.Bool } flag => new ScalarTerm(
            Expression.Condition(Expression.Equal(flag.Value, Expression.Constant(true, typeof(bool?))), Expression.Constant("true"), Expression.Constant("false")),
            flag.Strict().Defined, ScalarKind.String, false),
        BoolTerm flag => new ScalarTerm(
            Expression.Condition(flag.True, Expression.Constant("true"), Expression.Constant("false")),
            Ex.Or(flag.True, flag.False), ScalarKind.String, false),
        ScalarTerm { Kind: ScalarKind.Double or ScalarKind.Int } number => new NumberTextTerm(number.Strict()),
        ConstTerm { Value: double } => throw Refuse("string() of a number literal has no translation: the plan does not say whether CEL typed it as an int, spelled \"1000000\", or a double, spelled \"1e+06\"."),
        _ => throw Refuse("string() is translated only over a string, a boolean or a number: CEL's rendering of a timestamp has no SQL spelling that agrees with it on every provider."),
    };

    /// <summary>
    /// <c>int()</c> of a number read from the store: truncated toward zero, an error outside the int64
    /// range. Over a string column it is refused: a SQL CAST reads a numeric prefix where CEL requires the
    /// whole string.
    /// </summary>
    private Term ToInt(Term term)
    {
        if (term is CaseTerm) return Over(term, ToInt);
        switch (term)
        {
            case ConstTerm { Value: long } c:
                return c;
            case ConstTerm { Value: double d }:
                return double.IsFinite(d) && d > IntLower && d < IntUpper ? new ConstTerm((long)Math.Truncate(d)) : ErrorTerm.Instance;
            case ConstTerm { Value: string s }:
                return Cel.ParseGoInt(s) is { } parsed ? new ConstTerm(parsed) : ErrorTerm.Instance;
            case ScalarTerm { Kind: ScalarKind.Int } i:
                return i.Strict();
            case ScalarTerm { Kind: ScalarKind.Double } n:
                {
                    var strict = n.Strict();
                    var inRange = Ex.And(
                        Expression.GreaterThan(strict.Value, Expression.Constant(IntLower, typeof(double?))),
                        Expression.LessThan(strict.Value, Expression.Constant(IntUpper, typeof(double?))));
                    // SQLite's CAST truncates toward zero; SQL Server and PostgreSQL round, so they truncate first.
                    var truncated = _provider == DatabaseProvider.Sqlite
                        ? strict.Value
                        : Expression.Convert(Expression.Call(MathTruncate, Expression.Convert(strict.Value, typeof(double))), typeof(double?));
                    // SQL Server may run the CAST before a sibling guard and abort the query.
                    Expression value = Expression.Convert(truncated, typeof(long?));
                    if (_provider != DatabaseProvider.Sqlite)
                        value = Expression.Condition(inRange, value, Expression.Constant(null, typeof(long?)));
                    return new ScalarTerm(value, Ex.And(strict.Defined, inRange), ScalarKind.Int, false);
                }
            case ScalarTerm { Kind: ScalarKind.String }:
                throw Refuse("int() over a string column has no translation: a SQL CAST reads a numeric prefix where CEL requires the whole string.");
            case ScalarTerm { Kind: ScalarKind.TimestampString }:
                RefuseTextualTimestamp(ScalarKind.TimestampString, ScalarKind.String);
                return ErrorTerm.Instance;
            case ScalarTerm { Kind: ScalarKind.Timestamp } or ConstTerm { Value: CelTimestamp }:
                throw Refuse("int() of a timestamp has no translation.");
            case ScalarTerm or ConstTerm or ErrorTerm or BoolTerm:
                return ErrorTerm.Instance;
            default:
                throw Refuse("int() over a list or object has no translation.");
        }
    }

    // -- timestamps -----------------------------------------------------------------------------

    private static Term Timestamp(Term term) => term switch
    {
        CaseTerm => Over(term, Timestamp),
        ConstTerm { Value: string s } => CelTimestamp.Parse(s) is { } t ? new ConstTerm(t) : ErrorTerm.Instance,
        ConstTerm { Value: CelTimestamp } c => c,
        ScalarTerm { Kind: ScalarKind.TimestampString } s => s with { Kind = ScalarKind.Timestamp },
        ScalarTerm { Kind: ScalarKind.Timestamp } s => s,
        ScalarTerm { Kind: ScalarKind.String } => throw Refuse("timestamp() over a string column has no translation: a malformed string must stay a CEL error, which no provider's cast reproduces. Map the column as a DateTime."),
        ConstTerm { Value: double } => throw Refuse("timestamp() of a number has no translation."),
        ErrorTerm or ConstTerm or ScalarTerm => ErrorTerm.Instance,
        _ => throw Refuse("timestamp() over a list or object has no translation."),
    };

    private Term TimeSince(Term term) => term switch
    {
        CaseTerm => Over(term, TimeSince),
        ConstTerm { Value: CelTimestamp t } => new ConstTerm(_now - t),
        ScalarTerm { Kind: ScalarKind.Timestamp } s => new SinceTerm(s.Strict()),
        ErrorTerm or ConstTerm or ScalarTerm => ErrorTerm.Instance,
        _ => throw Refuse("timeSince() over anything but a timestamp has no translation."),
    };

    // -- hierarchies ----------------------------------------------------------------------------

    private static Term HierarchyOf(List<Term> args)
    {
        if (args.Count is < 1 or > 2) throw Refuse("hierarchy() takes a path and an optional delimiter.");
        var delimiter = ".";
        if (args.Count == 2)
        {
            delimiter = args[1] is ConstTerm { Value: string d } ? d : throw Refuse("hierarchy() with a delimiter read from the store has no translation.");
        }
        return args[0] switch
        {
            ConstTerm { Value: string } or ScalarTerm { Kind: ScalarKind.String } => new HierarchyTerm(args[0], delimiter),
            ConstTerm { Value: List<object> } or TermList => new HierarchyTerm(args[0], delimiter),
            ScalarTerm { Kind: ScalarKind.TimestampString } => throw Refuse("hierarchy() over a timestamp column has no translation."),
            CaseTerm c => Cases(c.Arms.Select(a => (a.When, HierarchyOf([a.Then, .. args.Skip(1)])))),
            _ => ErrorTerm.Instance,
        };
    }

    /// <summary>Go's strings.Split, which Cerbos splits a hierarchy with: an empty delimiter splits into code points.</summary>
    private static List<string> Segments(string path, string delimiter) => delimiter.Length == 0
        ? [.. path.EnumerateRunes().Select(r => r.ToString())]
        : [.. path.Split(delimiter)];

    /// <summary>The segments of a hierarchy as terms, or null for a path read from the store as one string.</summary>
    private static List<Term>? SegmentTerms(HierarchyTerm h) => h.Path switch
    {
        ConstTerm { Value: string s } => Segments(s, h.Delimiter).Select(x => (Term)new ConstTerm(x)).ToList(),
        ConstTerm { Value: List<object> items } => items.Select(x => (Term)new ConstTerm(x)).ToList(),
        TermList list => [.. list.Items],
        _ => null,
    };

    private BoolTerm HierarchyRelation(string op, Term left, Term right)
    {
        if (left is CaseTerm || right is CaseTerm) return AsBool(Over(left, right, (x, y) => HierarchyRelation(op, x, y)));
        if (left is ErrorTerm || right is ErrorTerm) return BoolTerm.Error;
        if (left is not HierarchyTerm a || right is not HierarchyTerm b)
        {
            return left is HierarchyTerm or ConstTerm or ScalarTerm && right is HierarchyTerm or ConstTerm or ScalarTerm
                ? BoolTerm.Error
                : throw Refuse($"{op}() over a value that is not a hierarchy has no translation.");
        }
        var (sa, sb) = (SegmentTerms(a), SegmentTerms(b));
        if (sa is not null && sb is not null)
        {
            return SegmentRelation(op, sa, sb);
        }
        if (a.Path is ScalarTerm && b.Path is ScalarTerm)
        {
            throw Refuse($"{op}() between two hierarchies read from the store has no translation.");
        }
        var (column, delimiter, literalSegments, relation) = a.Path is ScalarTerm
            ? (a.Path, a.Delimiter, sb!, op)
            : (b.Path, b.Delimiter, sa!, op switch { "ancestorOf" => "descendentOf", "descendentOf" => "ancestorOf", _ => op });
        if (literalSegments.Any(s => s is not ConstTerm { Value: string }))
        {
            throw Refuse($"{op}() between a path read from the store as a string and one built from segments read from the store has no translation.");
        }
        var segments = literalSegments.Select(s => (string)((ConstTerm)s).Value).ToList();
        // The column splits on its delimiter, so a literal segment holding it would split too.
        if (segments.Any(seg => delimiter.Length == 0 ? Cel.CodePointLength(seg) != 1 : seg.Contains(delimiter, StringComparison.Ordinal)))
        {
            throw Refuse($"{op}() against a segment that holds the column's delimiter has no translation.");
        }
        // From the column's side: "ancestor" means the column is an ancestor of the literal.
        var literal = string.Join(delimiter, segments);
        // Every strict ancestor of the literal, as a string; with an empty delimiter that includes "".
        var first = delimiter.Length == 0 ? 0 : 1;
        var ancestors = Enumerable.Range(first, Math.Max(0, segments.Count - first))
            .Select(k => (object)string.Join(delimiter, segments.Take(k))).ToList();
        var isAncestor = In(column, new ConstTerm(ancestors));
        var descendantPrefix = delimiter.Length == 0 ? literal : literal + delimiter;
        var startsWith = StringMatch("startsWith", column, new ConstTerm(descendantPrefix));
        var equal = Compare("eq", column, new ConstTerm(literal));
        var isDescendant = delimiter.Length == 0
            ? new BoolTerm(Ex.And(startsWith.True, equal.False), Ex.Or(startsWith.False, equal.True))
            : startsWith;
        return relation switch
        {
            "ancestorOf" => isAncestor,
            "descendentOf" => isDescendant,
            _ => new BoolTerm(
                Ex.Or(equal.True, isAncestor.True, isDescendant.True),
                Ex.And(equal.False, isAncestor.False, isDescendant.False)),
        };
    }

    /// <summary>A relation between two hierarchies whose segments are each a literal or a column, compared segment by segment.</summary>
    private BoolTerm SegmentRelation(string op, List<Term> a, List<Term> b)
    {
        foreach (var segment in a.Concat(b))
        {
            if (segment is not (ConstTerm { Value: string } or ScalarTerm { Kind: ScalarKind.String }))
            {
                return segment is ConstTerm or ScalarTerm or ErrorTerm ? BoolTerm.Error : throw Refuse("A hierarchy segment that is not a string has no translation.");
            }
        }
        var defined = Ex.And([.. a.Concat(b).Select(TermList.DefinedOf)]);
        BoolTerm Prefix(List<Term> p, List<Term> q, bool strict)
        {
            if (p.Count > q.Count || (strict && p.Count == q.Count)) return new BoolTerm(Ex.False, defined);
            var pairs = p.Zip(q).Select(pair => Compare("eq", pair.First, pair.Second)).ToList();
            return new BoolTerm(Ex.And([defined, .. pairs.Select(x => x.True)]), Ex.And(defined, Ex.Or([.. pairs.Select(x => x.False)])));
        }
        return op switch
        {
            "ancestorOf" => Prefix(a, b, strict: true),
            "descendentOf" => Prefix(b, a, strict: true),
            _ => Prefix(a, b, strict: false) is var ab && Prefix(b, a, strict: false) is var ba
                ? new BoolTerm(Ex.Or(ab.True, ba.True), Ex.And(ab.False, ba.False))
                : throw new InvalidOperationException(),
        };
    }

    // -- literals -------------------------------------------------------------------------------

    private Term Struct(IEnumerable<Operand> ops, ImmutableDictionary<string, Term> scope)
    {
        var map = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (var op in ops)
        {
            if (op.NodeCase != Operand.NodeOneofCase.Expression || op.Expression.Operator != "set-field" || op.Expression.Operands.Count != 2)
            {
                throw Refuse("A map literal holds an entry the adapter cannot read.");
            }
            var key = Translate(op.Expression.Operands[0], scope);
            var value = Translate(op.Expression.Operands[1], scope);
            if (key is not ConstTerm { Value: string k } || value is not ConstTerm { Value: var v })
            {
                throw Refuse("A map literal with an entry read from the store has no translation.");
            }
            map[k] = v;
        }
        return new ConstTerm(map);
    }

    private Term Index(Term target, Term index)
    {
        if (target is CaseTerm || index is CaseTerm) return Over(target, index, Index);
        switch (target, index)
        {
            case (ListTerm { Position: { } position } l, ConstTerm { Value: double or long } c):
                {
                    var i = c.Value is long li ? (double)li : (double)c.Value;
                    if (i < 0 || i != Math.Floor(i) || i > int.MaxValue) return ErrorTerm.Instance;
                    var e = Expression.Parameter(l.ElementType, "e");
                    var at = Ex.Lambda(Expression.Equal(Ex.Rebind(position, e), Expression.Constant((int)i)), e);
                    var row = Ex.Call(nameof(Enumerable.FirstOrDefault), l.ElementType, Ex.Call(nameof(Enumerable.Where), l.ElementType, l.Source, at));
                    // Past the last position is a CEL error, which a missing row is here.
                    var present = Ex.And(l.Defined, Ex.Call(nameof(Enumerable.Any), l.ElementType, l.Source, at));
                    return l.Element(row) switch
                    {
                        ScalarTerm s => s with { Defined = Ex.And(present, s.Defined) },
                        BoolTerm b => new BoolTerm(Ex.And(present, b.True), Ex.And(present, b.False)),
                        var other => other,
                    };
                }
            case (ListTerm, ConstTerm { Value: double or long }):
                throw Refuse("Indexing a list read from the store needs its position column: declare one in the attribute map, since a related table has no row order to read a position from.");
            case (ConstTerm { Value: List<object> list }, ConstTerm { Value: double i }):
                return i >= 0 && i < list.Count && i == Math.Floor(i) ? new ConstTerm(list[(int)i]) : ErrorTerm.Instance;
            case (ConstTerm { Value: List<object> list }, ConstTerm { Value: long i }):
                return i >= 0 && i < list.Count ? new ConstTerm(list[(int)i]) : ErrorTerm.Instance;
            case (ConstTerm { Value: Dictionary<string, object> map }, ConstTerm { Value: var key }):
                return key is string k && map.TryGetValue(k, out var v) ? new ConstTerm(v) : ErrorTerm.Instance;
            case (ConstTerm { Value: Dictionary<string, object> map }, ScalarTerm key):
                // A lookup by a column: one arm per key, and a key the map lacks is an error.
                return Cases(map.Select(entry => (Compare("eq", key, new ConstTerm(entry.Key)).True, (Term)new ConstTerm(entry.Value))));
            case (ConstTerm { Value: List<object> list }, ScalarTerm { Kind: ScalarKind.Int or ScalarKind.Double } position):
                return Cases(list.Select((item, k) => (Compare("eq", position, new ConstTerm((long)k)).True, (Term)new ConstTerm(item))));
            case (ListTerm, _):
                throw Refuse("Indexing a list read from the store has no translation: a related table has no row order to read a position from.");
            case (ErrorTerm, _) or (_, ErrorTerm) or (ConstTerm, ConstTerm) or (ConstTerm, ScalarTerm):
                return ErrorTerm.Instance;
            default:
                throw Refuse("Indexing by a value read from the store has no translation.");
        }
    }

    // -- macros ---------------------------------------------------------------------------------

    private Term Macro(string op, IList<Operand> ops, ImmutableDictionary<string, Term> scope)
    {
        if (ops.Count != 2 || ops[1].NodeCase != Operand.NodeOneofCase.Expression || ops[1].Expression.Operator != "lambda")
        {
            throw Refuse($"{op}() in this form has no translation.");
        }
        var lambda = ops[1].Expression;
        if (lambda.Operands.Count is not (2 or 3) || lambda.Operands.Skip(1).Any(o => o.NodeCase != Operand.NodeOneofCase.Variable))
        {
            throw Refuse($"{op}() in this form has no translation.");
        }
        var body = lambda.Operands[0];
        // With two variables the first is the index (or a map's key) and the second the element.
        var index = lambda.Operands.Count == 3 ? lambda.Operands[1].Variable : null;
        var variable = lambda.Operands[^1].Variable;
        return Over(Translate(ops[0], scope), range => MacroOver(op, range, index, variable, body, scope));
    }

    private Term MacroOver(string op, Term range, string? index, string variable, Operand body, ImmutableDictionary<string, Term> scope)
    {
        switch (range)
        {
            case ListTerm l:
                if (index is not null && l.Position is null)
                {
                    throw Refuse($"A two-variable {op}() over a list read from the store needs its position column: declare one in the attribute map, since a related table has no row order to bind an index to.");
                }
                return MacroOverRows(op, l, index, variable, body, scope);
            case ConstTerm { Value: List<object> items }:
                if (items.Count > MaxUnrolledElements)
                {
                    throw Refuse($"{op}() over a literal list of more than {MaxUnrolledElements} elements is refused, to keep the predicate small.");
                }
                return MacroOverItems(op, items.Select(i => (Term)new ConstTerm(i)).ToList(), index, variable, body, scope);
            case TermList list:
                return MacroOverItems(op, list.Items, index, variable, body, scope, list.Defined);
            case ConstTerm { Value: Dictionary<string, object> }:
                throw Refuse($"{op}() over a map has no translation.");
            case ErrorTerm or ConstTerm or ScalarTerm or BoolTerm:
                return op is "filter" or "map" ? ErrorTerm.Instance : BoolTerm.Error;
            case GuardedList or CountedList or ConcatList:
                throw Refuse($"{op}() over a list built by except(), intersect(), filter() or + has no translation.");
            default:
                throw Refuse($"{op}() over an object has no translation: a macro over a map ranges over its keys, which SQL has no form for.");
        }
    }

    private Term MacroOverRows(string op, ListTerm l, string? index, string variable, Operand body, ImmutableDictionary<string, Term> scope)
    {
        var e = Expression.Parameter(l.ElementType, variable);
        ImmutableDictionary<string, Term> Scope(Expression row)
        {
            var inner = scope.SetItem(variable, l.Element(row));
            return index is null ? inner : inner.SetItem(index, new ScalarTerm(Expression.Convert(Ex.Rebind(l.Position!, row), typeof(long?)), Ex.True, ScalarKind.Int, false));
        }
        var inner = Scope(e);
        switch (op)
        {
            case "exists":
                {
                    var p = AsBool(Translate(body, inner));
                    return new BoolTerm(Ex.And(l.Defined, Any(l, e, p.True)), Ex.And(l.Defined, All(l, e, p.False)));
                }
            case "all":
                {
                    var p = AsBool(Translate(body, inner));
                    return new BoolTerm(Ex.And(l.Defined, All(l, e, p.True)), Ex.And(l.Defined, Any(l, e, p.False)));
                }
            case "exists_one":
                {
                    var p = AsBool(Translate(body, inner));
                    var decided = Ex.And(l.Defined, All(l, e, Ex.Or(p.True, p.False)));
                    var one = Expression.Equal(Count(l, e, p.True), Expression.Constant(1));
                    return new BoolTerm(Ex.And(decided, one), Ex.And(decided, Ex.Not(one)));
                }
            case "filter":
                {
                    var p = AsBool(Translate(body, inner));
                    var source = Ex.Call(nameof(Enumerable.Where), l.ElementType, l.Source, Ex.Lambda(p.True, e));
                    return new ListTerm(source, l.ElementType, l.Element, Ex.And(l.Defined, All(l, e, Ex.Or(p.True, p.False))));
                }
            default:
                {
                    var mapped = Translate(body, inner);
                    var defined = mapped switch
                    {
                        BoolTerm b => Ex.Or(b.True, b.False),
                        ScalarTerm s => s.Defined,
                        ErrorTerm => Ex.False,
                        ConstTerm => Ex.True,
                        CaseTerm c => Ex.Or([.. c.Arms.Select(a => a.When)]),
                        _ => throw Refuse("map() producing lists or objects has no translation."),
                    };
                    Term Element(Expression x) => Translate(body, Scope(x));
                    return new ListTerm(l.Source, l.ElementType, Element, Ex.And(l.Defined, All(l, e, defined)), l.Position);
                }
        }
    }

    private Term MacroOverItems(string op, IReadOnlyList<Term> items, string? index, string variable, Operand body, ImmutableDictionary<string, Term> scope, Expression? listDefined = null)
    {
        var defined = listDefined ?? Ex.True;
        ImmutableDictionary<string, Term> Scope(int k)
        {
            var inner = scope.SetItem(variable, items[k]);
            return index is null ? inner : inner.SetItem(index, new ConstTerm((long)k));
        }
        if (op == "map")
        {
            var mapped = items.Select((_, k) => Translate(body, Scope(k))).ToList();
            if (mapped.Any(m => IsList(m) || m is ObjectTerm)) throw Refuse("map() producing lists or objects has no translation.");
            if (mapped.All(m => m is ConstTerm) && Ex.IsTrue(defined)) return new ConstTerm(mapped.Select(m => ((ConstTerm)m).Value).ToList());
            var list = new TermList(mapped);
            return Ex.IsTrue(defined) ? list : new GuardedList([.. mapped.Select(m => (Ex.True, m))], Ex.And(defined, list.Defined));
        }
        var results = items.Select((_, k) => AsBool(Translate(body, Scope(k)))).ToList();
        if (op == "filter")
        {
            var decided = Ex.And([defined, .. results.Select(r => Ex.Or(r.True, r.False))]);
            return new GuardedList([.. results.Select((r, k) => (r.True, items[k]))], decided);
        }
        BoolTerm result;
        switch (op)
        {
            case "exists":
                result = new BoolTerm(Ex.Or([.. results.Select(r => r.True)]), Ex.And([.. results.Select(r => r.False)]));
                break;
            case "all":
                result = new BoolTerm(Ex.And([.. results.Select(r => r.True)]), Ex.Or([.. results.Select(r => r.False)]));
                break;
            default:
                {
                    var exactlyOne = Ex.Or([.. results.Select((r, i) => Ex.And([r.True, .. results.Where((_, j) => j != i).Select(o => o.False)]))]);
                    var allDecided = Ex.And([.. results.Select(r => Ex.Or(r.True, r.False))]);
                    result = new BoolTerm(exactlyOne, Ex.And(allDecided, Ex.Not(exactlyOne)));
                    break;
                }
        }
        return new BoolTerm(Ex.And(defined, result.True), Ex.And(defined, result.False));
    }
}
