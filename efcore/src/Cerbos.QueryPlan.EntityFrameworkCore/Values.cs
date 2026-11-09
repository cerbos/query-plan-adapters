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

    /// <summary>A CEL int, which only <c>int()</c> and a lambda's index variable produce: no column maps to one.</summary>
    Int,
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

    /// <summary>CEL's range: 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999999Z.</summary>
    public static readonly CelTimestamp Min = new(-62135596800, 0);
    public static readonly CelTimestamp Max = new(253402300799, 999_999_999);

    public static CelTimestamp operator +(CelTimestamp t, CelDuration d)
    {
        var nanos = new System.Numerics.BigInteger(t.Seconds) * 1_000_000_000 + t.Nanos + d.Nanos;
        var seconds = (long)System.Numerics.BigInteger.Divide(nanos, 1_000_000_000);
        var rem = (int)(nanos - new System.Numerics.BigInteger(seconds) * 1_000_000_000);
        if (rem < 0)
        {
            seconds--;
            rem += 1_000_000_000;
        }
        return new CelTimestamp(seconds, rem);
    }

    public static CelDuration operator -(CelTimestamp a, CelTimestamp b) =>
        new((new System.Numerics.BigInteger(a.Seconds) - b.Seconds) * 1_000_000_000 + (a.Nanos - b.Nanos));

    public static CelTimestamp FromDateTimeOffset(DateTimeOffset t)
    {
        var ticks = t.UtcTicks - DateTime.UnixEpoch.Ticks;
        var seconds = Math.DivRem(ticks, TimeSpan.TicksPerSecond, out var rem);
        if (rem < 0)
        {
            seconds--;
            rem += TimeSpan.TicksPerSecond;
        }
        return new CelTimestamp(seconds, (int)(rem * 100));
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
        long => "int",
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
            case (long x, long y): return x == y;
            case (long x, double y): return !double.IsNaN(y) && CompareIntDouble(x, y) == 0;
            case (double x, long y): return !double.IsNaN(x) && CompareIntDouble(y, x) == 0;
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
        (long x, long y) => x.CompareTo(y),
        (long x, double y) when !double.IsNaN(y) => CompareIntDouble(x, y),
        (double x, long y) when !double.IsNaN(x) => -CompareIntDouble(y, x),
        (bool x, bool y) => x.CompareTo(y),
        (CelTimestamp x, CelTimestamp y) => x.CompareTo(y),
        (CelDuration x, CelDuration y) => x.Nanos.CompareTo(y.Nanos),
        _ => null,
    };

    /// <summary>Exact order of an int and a double, as CEL compares them: neither is rounded to the other.</summary>
    public static int CompareIntDouble(long i, double d)
    {
        if (d >= 9223372036854775808.0) return -1;
        if (d < -9223372036854775808.0) return 1;
        return ((decimal)i).CompareTo((decimal)d);
    }

    /// <summary>Whether a double is a whole number a CEL int can hold, so a plan literal beside an int is that int.</summary>
    public static bool IsIntLiteral(double d, out long value)
    {
        value = 0;
        if (!double.IsFinite(d) || d != Math.Floor(d) || d < -9223372036854775808.0 || d >= 9223372036854775808.0) return false;
        value = (long)d;
        return true;
    }

    /// <summary>Go's strconv.ParseInt in base 10, which CEL's <c>int()</c> reads a string with.</summary>
    public static long? ParseGoInt(string s)
    {
        if (s.Length == 0) return null;
        var body = s[0] is '+' or '-' ? s[1..] : s;
        if (body.Length == 0 || !body.All(char.IsAsciiDigit)) return null;
        return long.TryParse(s, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var v) ? v : null;
    }

    /// <summary>Whether a string is CEL's spelling of an int, strconv.Itoa: no sign on zero, no leading zeros.</summary>
    public static bool IsIntSpelling(string s, out long value) =>
        ParseGoInt(s, out value) && s == value.ToString(CultureInfo.InvariantCulture);

    private static bool ParseGoInt(string s, out long value)
    {
        var parsed = ParseGoInt(s);
        value = parsed ?? 0;
        return parsed is not null;
    }

    /// <summary>
    /// CEL's spelling of a double, strconv.FormatFloat(d, 'g', -1, 64): the shortest digits that read
    /// back exactly, in exponent form when the exponent is below -4 or at least 6.
    /// </summary>
    public static string FormatDouble(double d)
    {
        if (double.IsNaN(d)) return "NaN";
        if (double.IsPositiveInfinity(d)) return "+Inf";
        if (double.IsNegativeInfinity(d)) return "-Inf";
        var negative = d < 0 || (d == 0 && double.IsNegative(d));
        var r = Math.Abs(d).ToString("R", CultureInfo.InvariantCulture);
        var e = r.IndexOf('E');
        var mantissa = e < 0 ? r : r[..e];
        var exponent = e < 0 ? 0 : int.Parse(r[(e + 1)..], CultureInfo.InvariantCulture);
        var point = mantissa.IndexOf('.');
        var intPart = point < 0 ? mantissa : mantissa[..point];
        var fracPart = point < 0 ? "" : mantissa[(point + 1)..];
        var all = intPart + fracPart;
        var stripped = all.TrimStart('0');
        var dp = intPart.Length + exponent - (all.Length - stripped.Length);
        var digits = stripped.TrimEnd('0');
        var sb = new StringBuilder();
        if (negative) sb.Append('-');
        if (digits.Length == 0) return sb.Append('0').ToString();
        var exp = dp - 1;
        if (exp < -4 || exp >= 6)
        {
            sb.Append(digits[0]);
            if (digits.Length > 1) sb.Append('.').Append(digits, 1, digits.Length - 1);
            sb.Append('e').Append(exp < 0 ? '-' : '+');
            var magnitude = Math.Abs(exp).ToString(CultureInfo.InvariantCulture);
            if (magnitude.Length < 2) sb.Append('0');
            return sb.Append(magnitude).ToString();
        }
        if (dp <= 0)
        {
            sb.Append("0.").Append('0', -dp).Append(digits);
        }
        else if (dp >= digits.Length)
        {
            sb.Append(digits).Append('0', dp - digits.Length);
        }
        else
        {
            sb.Append(digits, 0, dp).Append('.').Append(digits, dp, digits.Length - dp);
        }
        return sb.ToString();
    }

    /// <summary>The double a literal spells, when the literal is exactly CEL's spelling of it.</summary>
    public static double? ParseDoubleSpelling(string s)
    {
        if (!double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d)) return null;
        return FormatDouble(d) == s ? d : null;
    }

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
        long l => l.ToString(CultureInfo.InvariantCulture),
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
