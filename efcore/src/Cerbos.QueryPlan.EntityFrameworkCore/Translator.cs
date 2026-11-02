// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Immutable;
using System.Linq.Expressions;
using System.Reflection;
using Cerbos.Api.V1.Engine;
using Operand = Cerbos.Api.V1.Engine.PlanResourcesFilter.Types.Expression.Types.Operand;
using PlanExpression = Cerbos.Api.V1.Engine.PlanResourcesFilter.Types.Expression;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

internal sealed class Translator
{
    private const int MaxUnrolledElements = 64;

    private readonly DatabaseProvider _provider;
    private readonly NullConvention _defaultNulls;
    private readonly ParameterExpression _root;
    private readonly IReadOnlyDictionary<string, Mapping> _rootMap;

    public Translator(DatabaseProvider provider, NullConvention defaultNulls, ParameterExpression root, IReadOnlyDictionary<string, Mapping> rootMap)
    {
        _provider = provider;
        _defaultNulls = defaultNulls;
        _root = root;
        _rootMap = rootMap;
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
                Arity(2);
                return Compare(e.Operator, Arg(0), Arg(1));
            case "in":
                Arity(2);
                return In(Arg(0), Arg(1));
            case "startsWith" or "endsWith" or "contains":
                Arity(2);
                return StringMatch(e.Operator, Arg(0), Arg(1));
            case "size":
                Arity(1);
                return Size(Arg(0));
            case "has":
                Arity(1);
                return Has(ops[0], scope);
            case "add" or "sub" or "mult" or "div":
                Arity(2);
                return Arithmetic(e.Operator, Arg(0), Arg(1));
            case "exists" or "all" or "exists_one" or "filter" or "map":
                return Macro(e.Operator, ops, scope);
            case "hasIntersection":
                Arity(2);
                return HasIntersection(Arg(0), Arg(1));
            case "list":
                {
                    var items = ops.Select(o => Translate(o, scope)).ToList();
                    return items.All(i => i is ConstTerm) ? new ConstTerm(items.Select(i => ((ConstTerm)i).Value).ToList()) : new TermList(items);
                }
            case "struct":
                return Struct(ops, scope);
            case "timestamp":
                Arity(1);
                return Timestamp(Arg(0));
            case "duration":
                Arity(1);
                return Arg(0) is ConstTerm { Value: string s }
                    ? Cel.ParseDuration(s) is { } d ? new ConstTerm(d) : ErrorTerm.Instance
                    : throw Refuse("duration() over a value read from the store has no translation; only a literal duration is supported.");
            case "string":
                Arity(1);
                return Arg(0) switch
                {
                    ConstTerm { Value: string } c => c,
                    ConstTerm { Value: bool b } => new ConstTerm(b ? "true" : "false"),
                    ScalarTerm { Kind: ScalarKind.String } str => str.Strict(),
                    ScalarTerm { Kind: ScalarKind.Bool } flag => new ScalarTerm(
                        Expression.Condition(Expression.Equal(flag.Value, Expression.Constant(true, typeof(bool?))), Expression.Constant("true"), Expression.Constant("false")),
                        flag.Strict().Defined, ScalarKind.String, false),
                    BoolTerm flag => new ScalarTerm(
                        Expression.Condition(flag.True, Expression.Constant("true"), Expression.Constant("false")),
                        Ex.Or(flag.True, flag.False), ScalarKind.String, false),
                    _ => throw Refuse("string() is translated only over a string or a boolean: CEL's rendering of numbers and timestamps has no SQL spelling that agrees with it on every provider."),
                };
            case "double":
                Arity(1);
                return Arg(0) switch
                {
                    ConstTerm { Value: double } c => c,
                    ScalarTerm { Kind: ScalarKind.Double } num => num.Strict(),
                    _ => throw Refuse("double() is translated only over a number: a SQL CAST of a string accepts spellings CEL rejects as malformed."),
                };
            case "get-field":
                Arity(2);
                if (ops[1].NodeCase != Operand.NodeOneofCase.Variable)
                {
                    throw Refuse("get-field() names its field with something other than an identifier.");
                }
                return Member(Arg(0), ops[1].Variable, ops[1].Variable);
            case "index":
                Arity(2);
                return Index(Arg(0), Arg(1));
            case "matches":
                throw Refuse("matches() has no translation: CEL's RE2 semantics are not those of any provider's regular expressions, and SQLite and SQL Server before 2025 have none.");
            case "int":
                throw Refuse("int() has no translation: a SQL CAST reads a numeric prefix where CEL requires the whole string, and rounds where CEL truncates toward zero.");
            case "hierarchy":
                return HierarchyOf(ops.Select(o => Translate(o, scope)).ToList());
            case "ancestorOf" or "descendentOf" or "overlaps":
                Arity(2);
                return HierarchyRelation(e.Operator, Arg(0), Arg(1));
            case "immediateChildOf" or "immediateParentOf" or "siblingOf" or "commonAncestors":
                throw Refuse($"The hierarchy function {e.Operator}() has no translation in this adapter.");
            case "except" or "intersect" or "isSubset":
                throw Refuse($"{e.Operator}() compares lists element by element, and the adapter has no list-difference translation for related rows.");
            case "mod":
                throw Refuse("% has no translation: CEL's % is integer-only, and every attribute number reaches CEL as a double, so a SQL modulus would answer what CEL rejects.");
            case "upperAscii" or "lowerAscii":
                throw Refuse($"{e.Operator}() has no translation: SQL UPPER and LOWER fold non-ASCII letters, which CEL leaves alone.");
            case "lambda":
                throw Refuse("A lambda appears outside a macro.");
            default:
                throw Refuse($"Operator '{e.Operator}' has no translation.");
        }
    }

    private static QueryPlanRefusedException Refuse(string message) => new(message);

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
                bound = Member(bound, part, name);
            }
            return bound;
        }
        if (_rootMap.TryGetValue(name, out var mapping))
        {
            return FromMapping(mapping, _root, name);
        }
        throw new UnmappedAttributeException($"The plan reads '{name}', which the attribute map does not map.");
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
        if (then is ErrorTerm && otherwise is ErrorTerm) return ErrorTerm.Instance;
        var (x, y) = (AsScalar(then, otherwise), AsScalar(otherwise, then));
        if (x is null || y is null || x.Kind != y.Kind)
        {
            throw Refuse("A ternary whose branches are not both booleans, or both scalars of one type, has no translation.");
        }
        return new ScalarTerm(
            Expression.Condition(condition.True, x.Value, y.Value),
            Ex.Or(Ex.And(condition.True, x.Defined), Ex.And(condition.False, y.Defined)),
            x.Kind,
            x.Nullable || y.Nullable);
    }

    private static bool IsBoolean(Term t) => t is BoolTerm or ConstTerm { Value: bool } or ScalarTerm { Kind: ScalarKind.Bool };

    /// <summary>A branch as a scalar, a constant taking the type of the other branch.</summary>
    private ScalarTerm? AsScalar(Term term, Term other) => term switch
    {
        ScalarTerm s => s,
        ErrorTerm when other is ScalarTerm o => new ScalarTerm(Expression.Default(o.Value.Type), Ex.False, o.Kind, false),
        ConstTerm c when other is ScalarTerm o && ConstKind(c.Value) == o.Kind => new ScalarTerm(ConstExpression(c.Value, o.Value.Type), Ex.True, o.Kind, false),
        ConstTerm c when other is ConstTerm && ConstKind(c.Value) is { } k and not ScalarKind.Timestamp =>
            new ScalarTerm(ConstExpression(c.Value, ClrType(k)), Ex.True, k, false),
        _ => null,
    };

    private static Type ClrType(ScalarKind kind) => kind switch
    {
        ScalarKind.String => typeof(string),
        ScalarKind.Double => typeof(double?),
        ScalarKind.Bool => typeof(bool?),
        _ => throw new InvalidOperationException(),
    };

    // -- comparison -----------------------------------------------------------------------------

    private static ScalarKind? ConstKind(object value) => value switch
    {
        string => ScalarKind.String,
        double => ScalarKind.Double,
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

    private BoolTerm Compare(string op, Term left, Term right)
    {
        var a = AsValue(left);
        var b = AsValue(right);
        if (a is ListTerm or TermList or ObjectTerm || b is ListTerm or TermList or ObjectTerm)
        {
            throw Refuse("Comparing a whole list or object read from the store has no translation: related rows have no order or multiplicity a single comparison can read.");
        }
        if (a is ErrorTerm || b is ErrorTerm) return BoolTerm.Error;
        if (a is ConstTerm ca && b is ConstTerm cb) return FoldCompare(op, ca.Value, cb.Value);
        if (a is ConstTerm)
        {
            (a, b) = (b, a);
            op = Flip(op);
        }
        var s = (ScalarTerm)a;
        return op switch
        {
            "eq" => Equality(s, b),
            "ne" => Equality(s, b).Negate(),
            _ => Ordering(op, s, b),
        };
    }

    private static BoolTerm FoldCompare(string op, object a, object b)
    {
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
            true => new BoolTerm(Ex.True, Ex.False),
            false => new BoolTerm(Ex.False, Ex.True),
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
        if (kind != s.Kind) return BoolTerm.Error;
        if (other is ConstTerm { Value: double special } && !double.IsFinite(special))
        {
            // A column holds only finite numbers: every one is below +Infinity and above -Infinity, and NaN orders against nothing.
            var finite = s.Strict().Defined;
            var holds = !double.IsNaN(special) && (op is "lt" or "le" ? special > 0 : special < 0);
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
        var negated = op switch
        {
            "lt" => "ge",
            "le" => "gt",
            "gt" => "le",
            _ => "lt",
        };
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

    // -- membership -----------------------------------------------------------------------------

    private BoolTerm In(Term needle, Term haystack)
    {
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
            case ListTerm l:
                {
                    if (needle is ListTerm or TermList or ObjectTerm)
                    {
                        throw Refuse("Membership of a list read from the store in another list has no translation.");
                    }
                    var e = Expression.Parameter(l.ElementType, "e");
                    var eq = Compare("eq", l.Element(e), needle);
                    return new BoolTerm(
                        Ex.And(l.Defined, needleDefined, Any(l, e, eq.True)),
                        Ex.And(l.Defined, needleDefined, All(l, e, eq.False)));
                }
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

    private BoolTerm HasIntersection(Term a, Term b)
    {
        if (a is ConstTerm { Value: List<object> x } && b is ConstTerm { Value: List<object> y })
        {
            var hit = x.Any(i => y.Any(j => Cel.Equal(i, j)));
            return hit ? new BoolTerm(Ex.True, Ex.False) : new BoolTerm(Ex.False, Ex.True);
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
        return BoolTerm.Error;
    }

    // -- strings --------------------------------------------------------------------------------

    private BoolTerm StringMatch(string op, Term receiver, Term argument)
    {
        if (receiver is ErrorTerm || argument is ErrorTerm) return BoolTerm.Error;
        if (receiver is ScalarTerm { Kind: ScalarKind.TimestampString } || argument is ScalarTerm { Kind: ScalarKind.TimestampString })
        {
            RefuseTextualTimestamp(ScalarKind.TimestampString, ScalarKind.String);
        }
        if (receiver is ConstTerm { Value: string r } && argument is ConstTerm { Value: string n })
        {
            var result = op switch
            {
                "startsWith" => r.StartsWith(n, StringComparison.Ordinal),
                "endsWith" => r.EndsWith(n, StringComparison.Ordinal),
                _ => r.Contains(n, StringComparison.Ordinal),
            };
            return result ? new BoolTerm(Ex.True, Ex.False) : new BoolTerm(Ex.False, Ex.True);
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

    /// <summary>The length in code points, as CEL's size() counts it.</summary>
    private Expression StringLength(Expression value) => _provider == DatabaseProvider.SqlServer
        ? Expression.Subtract(Length(Sentinel(value)), Expression.Constant(1))
        : Length(value);

    private static Expression Length(Expression value) => Expression.Property(value, nameof(string.Length));

    private static Expression Substring(Expression value, Expression start, Expression? length) => length is null
        ? Expression.Call(value, typeof(string).GetMethod(nameof(string.Substring), [typeof(int)])!, start)
        : Expression.Call(value, typeof(string).GetMethod(nameof(string.Substring), [typeof(int), typeof(int)])!, start, length);

    private Term Size(Term term)
    {
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
            case ScalarTerm or ConstTerm or ErrorTerm or BoolTerm:
                return ErrorTerm.Instance;
            default:
                throw Refuse("size() of an object has no translation.");
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
                throw new UnmappedAttributeException($"The plan tests has({name}), which the attribute map does not map.");
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
                    return map.ContainsKey(field) ? new BoolTerm(Ex.True, Ex.False) : new BoolTerm(Ex.False, Ex.True);
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

    // -- arithmetic -----------------------------------------------------------------------------

    private Term Arithmetic(string op, Term left, Term right)
    {
        if (left is ConstTerm { Value: var a } && right is ConstTerm { Value: var b })
        {
            return FoldArithmetic(op, a, b);
        }
        if (left is ErrorTerm || right is ErrorTerm) return ErrorTerm.Instance;
        var kind = KindOf(left);
        if (kind != KindOf(right) || kind is null)
        {
            if (kind is ScalarKind.Timestamp or ScalarKind.TimestampString || KindOf(right) is ScalarKind.Timestamp or ScalarKind.TimestampString
                || left is ConstTerm { Value: CelDuration } || right is ConstTerm { Value: CelDuration })
            {
                throw Refuse("Timestamp arithmetic over a column has no translation.");
            }
            if (left is ListTerm or TermList || right is ListTerm or TermList)
            {
                throw Refuse("List concatenation over related rows has no translation.");
            }
            return ErrorTerm.Instance;
        }
        if (kind == ScalarKind.String && op != "add") return ErrorTerm.Instance;
        if (kind is not (ScalarKind.String or ScalarKind.Double))
        {
            return kind == ScalarKind.Bool ? ErrorTerm.Instance : throw Refuse("Timestamp arithmetic over a column has no translation.");
        }
        if (op == "div" && right is not ConstTerm { Value: double d and not 0 and not double.NaN })
        {
            throw Refuse("Division by a value read from the store has no translation: CEL divides by zero into an infinity of the zero's sign, which SQL cannot hold.");
        }
        var type = kind == ScalarKind.String ? typeof(string) : typeof(double?);
        var (x, xd) = Operand(left, type);
        var (y, yd) = Operand(right, type);
        Expression value = op switch
        {
            "add" when kind == ScalarKind.String => Ex.Concat(x, y),
            "add" => Expression.Add(x, y),
            "sub" => Expression.Subtract(x, y),
            "mult" => Expression.Multiply(x, y),
            _ => Expression.Divide(x, y),
        };
        return new ScalarTerm(value, Ex.And(xd, yd), kind.Value, false);

        (Expression, Expression) Operand(Term t, Type type) => t switch
        {
            ScalarTerm s => (s.Strict().Value, s.Strict().Defined),
            ConstTerm c => (ConstExpression(c.Value, type), Ex.True),
            _ => throw new InvalidOperationException(),
        };
    }

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
                return new ConstTerm(op switch
                {
                    "add" => x + y,
                    "sub" => x - y,
                    "mult" => x * y,
                    _ => x / y,
                });
            case (string x, string y) when op == "add":
                return new ConstTerm(x + y);
            case (List<object> x, List<object> y) when op == "add":
                return new ConstTerm(x.Concat(y).ToList());
            case (CelTimestamp t, CelDuration d) when op is "add" or "sub":
                {
                    var nanos = new System.Numerics.BigInteger(t.Seconds) * 1_000_000_000 + t.Nanos + (op == "add" ? d.Nanos : -d.Nanos);
                    var seconds = (long)System.Numerics.BigInteger.Divide(nanos, 1_000_000_000);
                    var rem = (int)(nanos - new System.Numerics.BigInteger(seconds) * 1_000_000_000);
                    if (rem < 0)
                    {
                        seconds--;
                        rem += 1_000_000_000;
                    }
                    return new ConstTerm(new CelTimestamp(seconds, rem));
                }
            case (CelDuration x, CelDuration y) when op is "add" or "sub":
                return new ConstTerm(new CelDuration(op == "add" ? x.Nanos + y.Nanos : x.Nanos - y.Nanos));
            default:
                return ErrorTerm.Instance;
        }
    }

    // -- timestamps -----------------------------------------------------------------------------

    private static Term Timestamp(Term term) => term switch
    {
        ConstTerm { Value: string s } => CelTimestamp.Parse(s) is { } t ? new ConstTerm(t) : ErrorTerm.Instance,
        ConstTerm { Value: CelTimestamp } c => c,
        ScalarTerm { Kind: ScalarKind.TimestampString } s => s with { Kind = ScalarKind.Timestamp },
        ScalarTerm { Kind: ScalarKind.Timestamp } s => s,
        ScalarTerm { Kind: ScalarKind.String } => throw Refuse("timestamp() over a string column has no translation: a malformed string must stay a CEL error, which no provider's cast reproduces. Map the column as a DateTime."),
        ConstTerm { Value: double } => throw Refuse("timestamp() of a number has no translation."),
        ErrorTerm or ConstTerm or ScalarTerm => ErrorTerm.Instance,
        _ => throw Refuse("timestamp() over a list or object has no translation."),
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
            ConstTerm { Value: List<object> } or TermList => throw Refuse("hierarchy() over a list of segments has no translation: it compares segment by segment, and the adapter translates only a delimited string."),
            ScalarTerm { Kind: ScalarKind.TimestampString } => throw Refuse("hierarchy() over a timestamp column has no translation."),
            _ => ErrorTerm.Instance,
        };
    }

    /// <summary>Go's strings.Split, which Cerbos splits a hierarchy with: an empty delimiter splits into code points.</summary>
    private static List<string> Segments(string path, string delimiter) => delimiter.Length == 0
        ? [.. path.EnumerateRunes().Select(r => r.ToString())]
        : [.. path.Split(delimiter)];

    private BoolTerm HierarchyRelation(string op, Term left, Term right)
    {
        if (left is ErrorTerm || right is ErrorTerm) return BoolTerm.Error;
        if (left is not HierarchyTerm a || right is not HierarchyTerm b)
        {
            return left is HierarchyTerm or ConstTerm or ScalarTerm && right is HierarchyTerm or ConstTerm or ScalarTerm
                ? BoolTerm.Error
                : throw Refuse($"{op}() over a value that is not a hierarchy has no translation.");
        }
        if (a.Delimiter != b.Delimiter)
        {
            throw Refuse($"{op}() between hierarchies with different delimiters has no translation.");
        }
        if (a.Path is ConstTerm { Value: string x } && b.Path is ConstTerm { Value: string y })
        {
            var (sx, sy) = (Segments(x, a.Delimiter), Segments(y, b.Delimiter));
            bool IsAncestor(List<string> p, List<string> q) => p.Count < q.Count && p.SequenceEqual(q.Take(p.Count), StringComparer.Ordinal);
            var result = op switch
            {
                "ancestorOf" => IsAncestor(sx, sy),
                "descendentOf" => IsAncestor(sy, sx),
                _ => sx.SequenceEqual(sy, StringComparer.Ordinal) || IsAncestor(sx, sy) || IsAncestor(sy, sx),
            };
            return result ? new BoolTerm(Ex.True, Ex.False) : new BoolTerm(Ex.False, Ex.True);
        }
        if (a.Path is ScalarTerm && b.Path is ScalarTerm)
        {
            throw Refuse($"{op}() between two hierarchies read from the store has no translation.");
        }
        // From the column's side: "ancestor" means the column is an ancestor of the literal.
        var (column, literal, relation) = a.Path is ScalarTerm
            ? (a.Path, (string)((ConstTerm)b.Path).Value, op)
            : (b.Path, (string)((ConstTerm)a.Path).Value, op switch { "ancestorOf" => "descendentOf", "descendentOf" => "ancestorOf", _ => op });
        var delimiter = a.Delimiter;
        var segments = Segments(literal, delimiter);
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

    private static Term Index(Term target, Term index)
    {
        switch (target, index)
        {
            case (ListTerm { Position: { } position } l, ConstTerm { Value: double i }):
                {
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
            case (ListTerm, ConstTerm { Value: double }):
                throw Refuse("Indexing a list read from the store needs its position column: declare one in the attribute map, since a related table has no row order to read a position from.");
            case (ConstTerm { Value: List<object> list }, ConstTerm { Value: double i }):
                return i >= 0 && i < list.Count && i == Math.Floor(i) ? new ConstTerm(list[(int)i]) : ErrorTerm.Instance;
            case (ConstTerm { Value: Dictionary<string, object> map }, ConstTerm { Value: var key }):
                return key is string k && map.TryGetValue(k, out var v) ? new ConstTerm(v) : ErrorTerm.Instance;
            case (ListTerm, _):
                throw Refuse("Indexing a list read from the store has no translation: a related table has no row order to read a position from.");
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
        if (lambda.Operands.Count != 2 || lambda.Operands[1].NodeCase != Operand.NodeOneofCase.Variable)
        {
            throw Refuse($"A two-variable {op}() has no translation: a related table has no row order to bind an index to.");
        }
        var body = lambda.Operands[0];
        var variable = lambda.Operands[1].Variable;
        var range = Translate(ops[0], scope);

        switch (range)
        {
            case ListTerm l:
                return MacroOverRows(op, l, variable, body, scope);
            case ConstTerm { Value: List<object> items }:
                if (items.Count > MaxUnrolledElements)
                {
                    throw Refuse($"{op}() over a literal list of more than {MaxUnrolledElements} elements is refused, to keep the predicate small.");
                }
                return MacroOverItems(op, items.Select(i => (Term)new ConstTerm(i)).ToList(), variable, body, scope);
            case TermList list:
                return list.Guard(MacroOverItems(op, list.Items, variable, body, scope));
            case ConstTerm { Value: Dictionary<string, object> }:
                throw Refuse($"{op}() over a map has no translation.");
            case ErrorTerm or ConstTerm or ScalarTerm or BoolTerm:
                return op is "filter" or "map" ? ErrorTerm.Instance : BoolTerm.Error;
            default:
                throw Refuse($"{op}() over an object has no translation: a macro over a map ranges over its keys, which SQL has no form for.");
        }
    }

    private Term MacroOverRows(string op, ListTerm l, string variable, Operand body, ImmutableDictionary<string, Term> scope)
    {
        var e = Expression.Parameter(l.ElementType, variable);
        var inner = scope.SetItem(variable, l.Element(e));
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
                    var count = Ex.IsFalse(p.True)
                        ? (Expression)Expression.Constant(0)
                        : Ex.Call(nameof(Enumerable.Count), l.ElementType, l.Source, Ex.Lambda(p.True, e));
                    var one = Expression.Equal(count, Expression.Constant(1));
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
                        _ => throw Refuse("map() producing lists or objects has no translation."),
                    };
                    Term Element(Expression x) => Translate(body, scope.SetItem(variable, l.Element(x)));
                    return new ListTerm(l.Source, l.ElementType, Element, Ex.And(l.Defined, All(l, e, defined)), l.Position);
                }
        }
    }

    private BoolTerm MacroOverItems(string op, IReadOnlyList<Term> items, string variable, Operand body, ImmutableDictionary<string, Term> scope)
    {
        if (op is "filter" or "map")
        {
            throw Refuse($"{op}() over a literal list has no translation.");
        }
        var results = items.Select(i => AsBool(Translate(body, scope.SetItem(variable, i)))).ToList();
        switch (op)
        {
            case "exists":
                return new BoolTerm(Ex.Or([.. results.Select(r => r.True)]), Ex.And([.. results.Select(r => r.False)]));
            case "all":
                return new BoolTerm(Ex.And([.. results.Select(r => r.True)]), Ex.Or([.. results.Select(r => r.False)]));
            default:
                {
                    var exactlyOne = Ex.Or([.. results.Select((r, i) => Ex.And([r.True, .. results.Where((_, j) => j != i).Select(o => o.False)]))]);
                    var decided = Ex.And([.. results.Select(r => Ex.Or(r.True, r.False))]);
                    return new BoolTerm(exactlyOne, Ex.And(decided, Ex.Not(exactlyOne)));
                }
        }
    }
}
