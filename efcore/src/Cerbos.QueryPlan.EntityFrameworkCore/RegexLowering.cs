// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.RegularExpressions;

namespace Cerbos.QueryPlan.EntityFrameworkCore;

/// <summary>
/// CEL's <c>matches()</c> without a SQL regex engine. CEL matches with RE2, and no provider's dialect
/// is RE2: PostgreSQL's ARE differs in classes and flags, SQL Server before 2025 and SQLite have no
/// regex operator at all. So a pattern is never handed to the store. It is parsed here and lowered only
/// when what it matches can be said with the adapter's exact string predicates: equality, startsWith,
/// endsWith and contains over a finite set of literals, plus two infinite forms with an exact reading,
/// every character drawn from a small set (<c>^[ab]+$</c>) and a prefix and a suffix around a run of
/// non-newline characters (<c>^a.*b$</c>).
/// </summary>
internal static class RegexLowering
{
    /// <summary>The most literals one pattern may expand to before it is refused rather than enumerated.</summary>
    private const int MaxLiterals = 256;

    /// <summary>The most characters a set may hold for the every-character-in-a-set form.</summary>
    private const int MaxSetSize = 64;

    /// <summary>RE2's bound on the copies a counted repetition, nested ones included, may make.</summary>
    private const int MaxRepeat = 1000;

    internal abstract record Plan;

    /// <summary>The string is one of the literals.</summary>
    internal sealed record OneOf(IReadOnlyList<string> Literals) : Plan;

    /// <summary>Some literal is a prefix, suffix or substring of the string.</summary>
    internal sealed record Matches(string Operator, IReadOnlyList<string> Literals) : Plan;

    /// <summary>Every character of the string is in the set, and there are at least <see cref="Min"/> of them.</summary>
    internal sealed record AllCharactersIn(IReadOnlyList<string> Characters, int Min) : Plan;

    /// <summary>No character is a newline, and there are at least <see cref="Min"/> of them.</summary>
    internal sealed record NoNewline(int Min) : Plan;

    /// <summary>
    /// For some pair, the string starts with the prefix, ends with the suffix, holds at least
    /// prefix + min + suffix characters, and contains no newline.
    /// </summary>
    internal sealed record PrefixSuffix(IReadOnlyList<(string Prefix, string Suffix)> Pairs, int Min) : Plan;

    private abstract record Node;

    private sealed record SetNode(IReadOnlyList<string> Characters) : Node;

    private sealed record DotNode : Node;

    private sealed record GroupNode(IReadOnlyList<IReadOnlyList<Node>> Alternatives) : Node;

    private sealed record RepeatNode(Node Inner, int Min, int? Max) : Node;

    private sealed record BeginNode : Node;

    private sealed record EndNode : Node;

    /// <summary>A pattern RE2 rejects: CEL raises at evaluation, which denies under both polarities.</summary>
    private sealed class InvalidPattern : Exception;

    private static QueryPlanRefusedException Unsupported(string pattern, string why) => new(
        $"Cannot translate matches(\"{pattern}\"): {why}. The adapter lowers only patterns whose matches are a finite set of literals under anchors, every character from a small set, or a prefix and suffix around non-newline characters, because no provider's regex dialect is RE2.");

    private static List<string> Range(int from, int to)
    {
        var characters = new List<string>();
        for (var code = from; code <= to; code++) characters.Add(char.ConvertFromUtf32(code));
        return characters;
    }

    private static readonly List<string> Digits = Range('0', '9');
    private static readonly List<string> Lower = Range('a', 'z');
    private static readonly List<string> Upper = Range('A', 'Z');
    private static readonly List<string> Word = [.. Digits, .. Upper, .. Lower, "_"];
    private static readonly List<string> Space = ["\t", "\n", "\f", "\r", " "];

    /// <summary>RE2's ASCII POSIX classes, <c>[[:name:]]</c>.</summary>
    private static readonly Dictionary<string, List<string>> PosixClasses = new(StringComparer.Ordinal)
    {
        ["alnum"] = [.. Digits, .. Upper, .. Lower],
        ["alpha"] = [.. Upper, .. Lower],
        ["blank"] = [" ", "\t"],
        ["digit"] = Digits,
        ["lower"] = Lower,
        ["space"] = [.. Space, "\v"],
        ["upper"] = Upper,
        ["word"] = Word,
        ["xdigit"] = [.. Digits, .. Range('A', 'F'), .. Range('a', 'f')],
    };

    /// <summary>Perl classes RE2 accepts, outside and inside brackets.</summary>
    private static readonly Dictionary<string, List<string>> PerlClasses = new(StringComparer.Ordinal)
    {
        ["d"] = Digits,
        ["s"] = Space,
        ["w"] = Word,
    };

    private static readonly Dictionary<string, string> LiteralEscapes = new(StringComparer.Ordinal)
    {
        ["a"] = "\a",
        ["f"] = "\f",
        ["n"] = "\n",
        ["r"] = "\r",
        ["t"] = "\t",
        ["v"] = "\v",
    };

    private static readonly Regex AsciiLetter = new("^[a-zA-Z]$", RegexOptions.CultureInvariant);
    private static readonly Regex Punctuation = new("^[!-/:-@[-`{-~]$", RegexOptions.CultureInvariant);
    private static readonly Regex CountedRepeatSyntax = new(@"^\{(0|[1-9]\d*)(,(0|[1-9]\d*)?)?\}", RegexOptions.CultureInvariant);
    private static readonly Regex PosixClassSyntax = new(@"^\[:([a-z]+):\]", RegexOptions.CultureInvariant);
    private static readonly Regex Lookaround = new(@"^\?(=|!|<=|<!)", RegexOptions.CultureInvariant);

    /// <summary>
    /// Every character RE2's <c>(?i)</c> treats as the same as the character. Its case folding follows
    /// Unicode simple folding, so beyond the two ASCII cases <c>k</c> also matches KELVIN SIGN and <c>s</c>
    /// matches LONG S. A non-ASCII letter's orbit is not tabulated here, so it is refused.
    /// </summary>
    private static List<string> FoldCase(string character, string pattern)
    {
        if (character is "k" or "K") return ["k", "K", "K"];
        if (character is "s" or "S") return ["s", "S", "ſ"];
        if (AsciiLetter.IsMatch(character)) return [character.ToLowerInvariant(), character.ToUpperInvariant()];
        if (char.ConvertToUtf32(character, 0) > 0x7f && !string.Equals(character.ToLowerInvariant(), character.ToUpperInvariant(), StringComparison.Ordinal))
        {
            throw Unsupported(pattern, $"case-insensitive matching of '{character}' is not tabulated");
        }
        return [character];
    }

    private static List<string> Unique(IEnumerable<string> values) => values.Distinct(StringComparer.Ordinal).ToList();

    /// <summary>A recursive-descent parser for the RE2 syntax the lowering can use.</summary>
    private sealed class Parser
    {
        private readonly string _pattern;
        private readonly List<string> _characters;
        private int _position;
        private bool _caseInsensitive;

        public Parser(string pattern)
        {
            _pattern = pattern;
            _characters = pattern.EnumerateRunes().Select(r => r.ToString()).ToList();
        }

        public List<IReadOnlyList<Node>> Parse()
        {
            if (_pattern.StartsWith("(?i)", StringComparison.Ordinal))
            {
                _caseInsensitive = true;
                _position = 4;
            }
            var alternatives = Alternation();
            // Only an unbalanced `)` stops the top level early.
            if (_position < _characters.Count) throw new InvalidPattern();
            return alternatives;
        }

        private string? Peek(int offset = 0) => _position + offset < _characters.Count ? _characters[_position + offset] : null;

        private string Rest() => string.Concat(_characters.Skip(_position));

        private List<IReadOnlyList<Node>> Alternation()
        {
            var alternatives = new List<IReadOnlyList<Node>> { Sequence() };
            while (Peek() == "|")
            {
                _position++;
                alternatives.Add(Sequence());
            }
            return alternatives;
        }

        private List<Node> Sequence()
        {
            var nodes = new List<Node>();
            while (true)
            {
                var next = Peek();
                if (next is null or "|" or ")") return nodes;
                nodes.Add(Quantified(Atom()));
            }
        }

        private Node Quantified(Node atom)
        {
            var node = atom;
            while (true)
            {
                var next = Peek();
                (int Min, int? Max)? bounds = next switch
                {
                    "*" => (0, null),
                    "+" => (1, null),
                    "?" => (0, 1),
                    "{" => CountedRepeat(),
                    _ => null,
                };
                if (bounds is null) return node;
                if (next != "{") _position++;
                // RE2 rejects a repeated repetition (`a**`, "invalid nested repetition operator").
                if (node is RepeatNode) throw new InvalidPattern();
                if (node is BeginNode or EndNode) throw Unsupported(_pattern, "a repeated anchor is not read");
                // A trailing `?` makes the repetition lazy, which changes where it matches, not whether.
                if (Peek() == "?") _position++;
                node = new RepeatNode(node, bounds.Value.Min, bounds.Value.Max);
                // RE2 rejects a repetition whose nested copies exceed MaxRepeat: `(a{2}){600}`.
                if (!RepeatIsValid(node, MaxRepeat)) throw new InvalidPattern();
            }
        }

        /// <summary>
        /// <c>{n}</c>, <c>{n,}</c> or <c>{n,m}</c>; anything else starting with <c>{</c> is a literal brace in
        /// RE2, which reads a count as Go's regexp/syntax parseInt does, without a leading zero: <c>a{01}</c>
        /// is <c>a</c> then the text "{01}".
        /// </summary>
        private (int Min, int? Max)? CountedRepeat()
        {
            var match = CountedRepeatSyntax.Match(Rest());
            if (!match.Success) return null;
            // A count Go's parseInt cannot read, or beyond RE2's bound, is an invalid pattern.
            if (!int.TryParse(match.Groups[1].Value, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var min) || min > MaxRepeat) throw new InvalidPattern();
            int? max = min;
            if (match.Groups[2].Success)
            {
                if (!match.Groups[3].Success) max = null;
                else if (!int.TryParse(match.Groups[3].Value, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var m) || m > MaxRepeat || m < min) throw new InvalidPattern();
                else max = m;
            }
            _position += match.Value.EnumerateRunes().Count();
            return (min, max);
        }

        private Node Literal(string character) => new SetNode(_caseInsensitive ? FoldCase(character, _pattern) : [character]);

        private Node Atom()
        {
            var character = Peek()!;
            _position++;
            switch (character)
            {
                case "^":
                    return new BeginNode();
                case "$":
                    return new EndNode();
                case ".":
                    return new DotNode();
                case "(":
                    return Group();
                case "[":
                    return new SetNode(CharacterClass());
                case "\\":
                    return Escape();
                case "*" or "+" or "?":
                    throw new InvalidPattern(); // "missing argument to repetition operator"
                case "{":
                    {
                        // A brace that opens a valid count with nothing before it is the same error; any
                        // other brace is a literal in RE2.
                        _position--;
                        if (CountedRepeat() is not null) throw new InvalidPattern();
                        _position++;
                        return Literal(character);
                    }
                default:
                    return Literal(character);
            }
        }

        private Node Group()
        {
            if (Peek() == "?")
            {
                var rest = Rest();
                // Lookaround is not RE2 syntax: regexp.Compile rejects it, so CEL raises at evaluation.
                if (Lookaround.IsMatch(rest)) throw new InvalidPattern();
                if (rest.StartsWith("?:", StringComparison.Ordinal))
                {
                    _position += 2;
                }
                else
                {
                    throw Unsupported(_pattern, "only (?i) at the start and (?:...) groups are read");
                }
            }
            var alternatives = Alternation();
            if (Peek() != ")") throw new InvalidPattern(); // "missing closing )"
            _position++;
            return new GroupNode(alternatives);
        }

        private Node Escape()
        {
            var character = Peek() ?? throw new InvalidPattern(); // "trailing backslash"
            _position++;
            if (character == "A") return new BeginNode();
            if (character == "z") return new EndNode();
            if (PerlClasses.TryGetValue(character, out var perl)) return new SetNode(ClassMembers(perl));
            if (LiteralEscapes.TryGetValue(character, out var control)) return Literal(control);
            if (Punctuation.IsMatch(character)) return Literal(character);
            // A lone \1-\7 would be a backreference and \8, \9 are nothing: RE2 rejects both, and reads
            // \1-\7 before another octal digit as an octal escape, which is not read here.
            if (character is "8" or "9" || (character is [>= '1' and <= '7'] && Peek() is not [>= '0' and <= '7']))
            {
                throw new InvalidPattern(); // "invalid escape sequence"
            }
            throw Unsupported(_pattern, $"the escape \\{character} is not read");
        }

        /// <summary><c>[...]</c>, just after its <c>[</c>.</summary>
        private List<string> CharacterClass()
        {
            if (Peek() == "^") throw Unsupported(_pattern, "a negated class matches an unbounded set of characters");
            var members = new List<string>();
            var first = true;
            while (true)
            {
                var character = Peek() ?? throw new InvalidPattern(); // "missing closing ]"
                if (character == "]" && !first)
                {
                    _position++;
                    break;
                }
                first = false;
                if (character == "[" && Peek(1) == ":")
                {
                    var match = PosixClassSyntax.Match(Rest());
                    if (!match.Success || !PosixClasses.TryGetValue(match.Groups[1].Value, out var posix))
                    {
                        throw Unsupported(_pattern, "only ASCII POSIX classes are read");
                    }
                    members.AddRange(posix);
                    _position += match.Value.Length;
                    continue;
                }
                var start = ClassCharacter();
                if (start.Set is { } set)
                {
                    members.AddRange(set);
                    continue;
                }
                if (Peek() == "-" && Peek(1) is { } after && after != "]")
                {
                    _position++;
                    var end = ClassCharacter();
                    if (end.Set is not null) throw new InvalidPattern();
                    var (from, to) = (char.ConvertToUtf32(start.Character!, 0), char.ConvertToUtf32(end.Character!, 0));
                    if (to < from) throw new InvalidPattern();
                    if (to - from + 1 > MaxSetSize * 4) throw Unsupported(_pattern, "a class range is too wide to enumerate");
                    members.AddRange(Range(from, to));
                }
                else
                {
                    members.Add(start.Character!);
                }
            }
            return ClassMembers(members);
        }

        // Under (?i) RE2 folds every member of a class, a Perl class outside brackets included:
        // (?i)\w also matches KELVIN SIGN and LONG S.
        private List<string> ClassMembers(IEnumerable<string> members) =>
            Unique(_caseInsensitive ? members.SelectMany(m => FoldCase(m, _pattern)) : members);

        /// <summary>One class member: a character, or a Perl class's characters.</summary>
        private (string? Character, List<string>? Set) ClassCharacter()
        {
            var character = Peek()!;
            _position++;
            if (character != "\\") return (character, null);
            var escaped = Peek() ?? throw new InvalidPattern();
            _position++;
            if (PerlClasses.TryGetValue(escaped, out var perl)) return (null, perl);
            if (LiteralEscapes.TryGetValue(escaped, out var control)) return (control, null);
            if (Punctuation.IsMatch(escaped)) return (escaped, null);
            throw Unsupported(_pattern, $"the escape \\{escaped} is not read");
        }
    }

    /// <summary>
    /// Go's repeatIsValid: each repetition divides the budget by its count (its maximum, or its minimum
    /// when unbounded), and a count beyond what is left is invalid.
    /// </summary>
    private static bool RepeatIsValid(Node node, int budget) => node switch
    {
        RepeatNode { Max: 0 } => true,
        RepeatNode r => (r.Max ?? r.Min) is var count && count <= budget && RepeatIsValid(r.Inner, count == 0 ? budget : budget / count),
        GroupNode g => g.Alternatives.All(sequence => sequence.All(inner => RepeatIsValid(inner, budget))),
        _ => true,
    };

    /// <summary>Every string a node sequence matches, when that is a finite set of at most MaxLiterals.</summary>
    private static List<string>? FiniteSequence(IEnumerable<Node> nodes)
    {
        var strings = new List<string> { "" };
        foreach (var node in nodes)
        {
            var next = FiniteNode(node);
            if (next is null) return null;
            var product = new List<string>();
            foreach (var head in strings)
            {
                foreach (var tail in next)
                {
                    product.Add(head + tail);
                    if (product.Count > MaxLiterals) return null;
                }
            }
            strings = Unique(product);
        }
        return strings;
    }

    private static List<string>? FiniteNode(Node node)
    {
        switch (node)
        {
            case SetNode s:
                return [.. s.Characters];
            case GroupNode g:
                {
                    var union = new List<string>();
                    foreach (var alternative in g.Alternatives)
                    {
                        var strings = FiniteSequence(alternative);
                        if (strings is null) return null;
                        union.AddRange(strings);
                        if (union.Count > MaxLiterals) return null;
                    }
                    return Unique(union);
                }
            case RepeatNode { Max: { } max } r:
                {
                    var union = new List<string>();
                    for (var count = r.Min; count <= max; count++)
                    {
                        var strings = FiniteSequence(Enumerable.Repeat(r.Inner, count));
                        if (strings is null) return null;
                        union.AddRange(strings);
                        if (union.Count > MaxLiterals) return null;
                    }
                    return Unique(union);
                }
            default:
                return null;
        }
    }

    /// <summary>
    /// One top-level alternative, its anchors removed, or null for one that matches nothing. An end
    /// without an anchor is free: a match may start (or stop) anywhere, so a repetition there needs only
    /// its minimum number of copies, <c>a+b</c> somewhere is exactly <c>ab</c> somewhere, and <c>.*</c>
    /// there needs none.
    /// </summary>
    private static Plan? PlanAlternative(string pattern, IReadOnlyList<Node> sequence)
    {
        var nodes = sequence.ToList();
        var anchoredStart = false;
        var anchoredEnd = false;
        while (nodes.Count > 0 && nodes[0] is BeginNode)
        {
            anchoredStart = true;
            nodes.RemoveAt(0);
        }
        while (nodes.Count > 0 && nodes[^1] is EndNode)
        {
            anchoredEnd = true;
            nodes.RemoveAt(nodes.Count - 1);
        }
        if (nodes.Any(n => n is BeginNode or EndNode)) throw Unsupported(pattern, "an anchor inside the pattern is not read");
        static Node AtLeast(Node node) => node is RepeatNode r ? r with { Max = r.Min } : node;
        if (!anchoredStart)
        {
            while (nodes.Count > 0 && nodes[0] is RepeatNode { Min: 0 }) nodes.RemoveAt(0);
            if (nodes.Count > 0) nodes[0] = AtLeast(nodes[0]);
        }
        if (!anchoredEnd)
        {
            while (nodes.Count > 0 && nodes[^1] is RepeatNode { Min: 0 }) nodes.RemoveAt(nodes.Count - 1);
            if (nodes.Count > 0) nodes[^1] = AtLeast(nodes[^1]);
        }

        var literals = FiniteSequence(nodes);
        if (literals is not null)
        {
            if (literals.Count == 0) return null;
            return anchoredStart
                ? anchoredEnd ? new OneOf(literals) : new Matches("startsWith", literals)
                : anchoredEnd ? new Matches("endsWith", literals) : new Matches("contains", literals);
        }

        if (anchoredStart && anchoredEnd)
        {
            // `^X*$`, `^X+$`, `^X{n,}$` over a set or `.`.
            if (nodes is [RepeatNode { Max: null } only])
            {
                if (only.Inner is DotNode) return new NoNewline(only.Min);
                if (only.Inner is SetNode { Characters.Count: <= MaxSetSize } set) return new AllCharactersIn(set.Characters, only.Min);
            }
            // `^P.*S$`: finite prefix and suffix around one unbounded run of `.`.
            var runs = nodes.Select((node, index) => (node, index))
                .Where(p => p.node is RepeatNode { Inner: DotNode, Max: null }).Select(p => p.index).ToList();
            if (runs.Count == 1)
            {
                var run = runs[0];
                var prefixes = FiniteSequence(nodes.Take(run));
                var suffixes = FiniteSequence(nodes.Skip(run + 1));
                if (prefixes is not null && suffixes is not null)
                {
                    var pairs = prefixes.SelectMany(prefix => suffixes.Select(suffix => (prefix, suffix))).ToList();
                    if (pairs.Count == 0) return null;
                    if (pairs.Count <= MaxLiterals && pairs.All(p => !(p.prefix + p.suffix).Contains('\n')))
                    {
                        return new PrefixSuffix(pairs, ((RepeatNode)nodes[run]).Min);
                    }
                }
            }
        }
        throw Unsupported(pattern, "what it matches is not a finite set of literals");
    }

    /// <summary>
    /// One plan per top-level alternative, which the caller ORs together, or null for a pattern RE2
    /// rejects. Throws <see cref="QueryPlanRefusedException"/> for a pattern the lowering cannot read.
    /// </summary>
    public static List<Plan>? Compile(string pattern)
    {
        List<IReadOnlyList<Node>> alternatives;
        try
        {
            alternatives = new Parser(pattern).Parse();
        }
        catch (InvalidPattern)
        {
            return null;
        }
        return alternatives.Select(a => PlanAlternative(pattern, a)).Where(p => p is not null).Select(p => p!).ToList();
    }

    /// <summary>Folds a plan over a constant string, as the lowering would decide it in the store.</summary>
    public static bool Holds(Plan plan, string value)
    {
        var length = Cel.CodePointLength(value);
        switch (plan)
        {
            case OneOf e:
                return e.Literals.Contains(value, StringComparer.Ordinal);
            case Matches m:
                return m.Literals.Any(l => m.Operator switch
                {
                    "startsWith" => value.StartsWith(l, StringComparison.Ordinal),
                    "endsWith" => value.EndsWith(l, StringComparison.Ordinal),
                    _ => value.Contains(l, StringComparison.Ordinal),
                });
            case AllCharactersIn a:
                return length >= a.Min && value.EnumerateRunes().All(r => a.Characters.Contains(r.ToString(), StringComparer.Ordinal));
            case NoNewline n:
                return length >= n.Min && !value.Contains('\n');
            case PrefixSuffix p:
                return !value.Contains('\n') && p.Pairs.Any(pair =>
                    value.StartsWith(pair.Prefix, StringComparison.Ordinal) && value.EndsWith(pair.Suffix, StringComparison.Ordinal)
                    && length >= Cel.CodePointLength(pair.Prefix) + p.Min + Cel.CodePointLength(pair.Suffix));
            default:
                throw new InvalidOperationException();
        }
    }

    internal static string Describe(Plan plan)
    {
        var sb = new StringBuilder();
        sb.Append(plan.GetType().Name);
        return sb.ToString();
    }
}
