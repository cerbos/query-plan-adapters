// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Value = Google.Protobuf.WellKnownTypes.Value;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>
/// The CEL type of a column. A <see cref="DateTime"/> column reaches <c>check()</c> as an RFC 3339
/// string, so it is <see cref="TimestampString"/>: a string to CEL that <c>timestamp()</c> reads exactly.
/// </summary>
internal enum ScalarKind
{
    String,
    Double,
    Bool,
    TimestampString,
    Timestamp,
}

internal static class ScalarKinds
{
    public static ScalarKind? Of(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        if (t == typeof(string)) return ScalarKind.String;
        if (t == typeof(bool)) return ScalarKind.Bool;
        if (t == typeof(double) || t == typeof(float) || t == typeof(int) || t == typeof(long) || t == typeof(short)) return ScalarKind.Double;
        if (t == typeof(DateTime) || t == typeof(DateTimeOffset)) return ScalarKind.TimestampString;
        return null;
    }
}

/// <summary>CEL null, distinct from a C# null that means "no value".</summary>
internal sealed class CelNull
{
    public static readonly CelNull Instance = new();

    private CelNull() { }
}

/// <summary>A CEL timestamp: UTC seconds since the epoch and nanoseconds.</summary>
internal readonly record struct CelTimestamp(long Seconds, int Nanos) : IComparable<CelTimestamp>
{
    private static readonly Regex Rfc3339 = new(
        @"^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:([Zz])|([+-])(\d{2}):(\d{2}))$",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    public int CompareTo(CelTimestamp other) =>
        Seconds != other.Seconds ? Seconds.CompareTo(other.Seconds) : Nanos.CompareTo(other.Nanos);

    public static CelTimestamp? Parse(string text)
    {
        var m = Rfc3339.Match(text);
        if (!m.Success) return null;
        int Part(int i) => int.Parse(m.Groups[i].Value, CultureInfo.InvariantCulture);
        DateTime local;
        try
        {
            local = new DateTime(Part(1), Part(2), Part(3), Part(4), Part(5), Part(6), DateTimeKind.Utc);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
        var offset = 0;
        if (!m.Groups[8].Success)
        {
            var hours = Part(10);
            var minutes = Part(11);
            if (hours > 23 || minutes > 59) return null;
            offset = (hours * 60 + minutes) * 60 * (m.Groups[9].Value == "-" ? -1 : 1);
        }
        var nanos = m.Groups[7].Success ? int.Parse(m.Groups[7].Value.PadRight(9, '0'), CultureInfo.InvariantCulture) : 0;
        var seconds = (long)(local - DateTime.UnixEpoch).TotalSeconds - offset;
        return new CelTimestamp(seconds, nanos);
    }

    /// <summary>The instant truncated to <paramref name="ticksPerUnit"/> ticks, and whether anything was cut.</summary>
    public (DateTime Value, bool Truncated) ToDateTime(long ticksPerUnit)
    {
        var ticks = DateTime.UnixEpoch.Ticks + Seconds * TimeSpan.TicksPerSecond + Nanos / 100;
        var truncated = ticks - ticks % ticksPerUnit;
        return (new DateTime(truncated, DateTimeKind.Utc), truncated != ticks || Nanos % 100 != 0);
    }
}

/// <summary>A CEL duration in nanoseconds.</summary>
internal readonly record struct CelDuration(System.Numerics.BigInteger Nanos);

/// <summary>Plan constants and the CEL operations the translator folds over them.</summary>
internal static class Cel
{
    public static object FromProto(Value value) => value.KindCase switch
    {
        Value.KindOneofCase.NullValue => CelNull.Instance,
        Value.KindOneofCase.NumberValue => value.NumberValue,
        Value.KindOneofCase.StringValue => value.StringValue,
        Value.KindOneofCase.BoolValue => value.BoolValue,
        Value.KindOneofCase.ListValue => value.ListValue.Values.Select(FromProto).ToList(),
        Value.KindOneofCase.StructValue => value.StructValue.Fields.ToDictionary(f => f.Key, f => FromProto(f.Value), StringComparer.Ordinal),
        _ => throw new QueryPlanRefusedException($"The plan holds a constant of kind {value.KindCase}, which the adapter cannot read."),
    };

    public static string TypeName(object value) => value switch
    {
        CelNull => "null",
        string => "string",
        double => "double",
        bool => "bool",
        List<object> => "list",
        Dictionary<string, object> => "map",
        CelTimestamp => "timestamp",
        CelDuration => "duration",
        _ => value.GetType().Name,
    };

    /// <summary>CEL heterogeneous equality: values of different types are unequal, never an error.</summary>
    public static bool Equal(object a, object b)
    {
        switch (a, b)
        {
            case (CelNull, CelNull): return true;
            case (string x, string y): return string.Equals(x, y, StringComparison.Ordinal);
            case (double x, double y): return x == y;
            case (bool x, bool y): return x == y;
            case (CelTimestamp x, CelTimestamp y): return x == y;
            case (CelDuration x, CelDuration y): return x == y;
            case (List<object> x, List<object> y): return x.Count == y.Count && x.Zip(y).All(p => Equal(p.First, p.Second));
            case (Dictionary<string, object> x, Dictionary<string, object> y):
                return x.Count == y.Count && x.All(kv => y.TryGetValue(kv.Key, out var v) && Equal(kv.Value, v));
            default: return false;
        }
    }

    /// <summary>The CEL ordering of two values, or null when CEL has no overload for the pair (an error).</summary>
    public static int? Compare(object a, object b) => (a, b) switch
    {
        (string x, string y) => CompareCodePoints(x, y),
        (double x, double y) when !double.IsNaN(x) && !double.IsNaN(y) => x.CompareTo(y),
        (bool x, bool y) => x.CompareTo(y),
        (CelTimestamp x, CelTimestamp y) => x.CompareTo(y),
        (CelDuration x, CelDuration y) => x.Nanos.CompareTo(y.Nanos),
        _ => null,
    };

    public static int CompareCodePoints(string a, string b)
    {
        var x = a.EnumerateRunes().GetEnumerator();
        var y = b.EnumerateRunes().GetEnumerator();
        while (true)
        {
            var hasX = x.MoveNext();
            var hasY = y.MoveNext();
            if (!hasX || !hasY) return hasX.CompareTo(hasY);
            var c = x.Current.Value.CompareTo(y.Current.Value);
            if (c != 0) return c;
        }
    }

    public static int CodePointLength(string s) => s.EnumerateRunes().Count();

    /// <summary>Whether a string is well-formed UTF-16, so every character is one code point a store can hold.</summary>
    public static bool IsWellFormed(string s)
    {
        for (var i = 0; i < s.Length; i++)
        {
            if (char.IsHighSurrogate(s[i]))
            {
                if (i + 1 >= s.Length || !char.IsLowSurrogate(s[i + 1])) return false;
                i++;
            }
            else if (char.IsLowSurrogate(s[i]))
            {
                return false;
            }
        }
        return true;
    }

    private static readonly Regex DurationPart = new(@"(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|ms|s|m|h)", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>Parses a Go duration string, as CEL's <c>duration()</c> does.</summary>
    public static CelDuration? ParseDuration(string text)
    {
        if (text.Length == 0) return null;
        var sign = 1;
        var rest = text;
        if (rest[0] is '+' or '-')
        {
            sign = rest[0] == '-' ? -1 : 1;
            rest = rest[1..];
        }
        if (rest == "0") return new CelDuration(0);
        if (rest.Length == 0) return null;
        var position = 0;
        decimal total = 0;
        foreach (Match m in DurationPart.Matches(rest))
        {
            if (m.Index != position) return null;
            position += m.Length;
            var unit = m.Groups[2].Value switch
            {
                "ns" => 1m,
                "us" or "µs" => 1_000m,
                "ms" => 1_000_000m,
                "s" => 1_000_000_000m,
                "m" => 60_000_000_000m,
                _ => 3_600_000_000_000m,
            };
            total += decimal.Parse(m.Groups[1].Value, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture) * unit;
        }
        if (position != rest.Length) return null;
        return new CelDuration(new System.Numerics.BigInteger(decimal.Truncate(total)) * sign);
    }

    public static string Describe(object value) => value switch
    {
        string s => "\"" + s + "\"",
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        bool b => b ? "true" : "false",
        CelNull => "null",
        _ => TypeName(value),
    };

    internal static string EscapeLike(string s, bool escapeBracket)
    {
        var sb = new StringBuilder(s.Length + 4);
        foreach (var c in s)
        {
            if (c is '%' or '_' or '\\' || (escapeBracket && c == '[')) sb.Append('\\');
            sb.Append(c);
        }
        return sb.ToString();
    }
}
