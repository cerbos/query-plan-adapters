# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""CEL's ``matches()`` on Polars' regex engine.

CEL matches with Go's ``regexp`` (RE2 syntax, Perl flags). Polars uses Rust's ``regex``
crate, which shares RE2's matching model but not its syntax: ``\\d``, ``\\w``, ``\\s`` and
``\\b`` are Unicode-aware there and ASCII in Go, ``[a&&b]`` and ``[[a]]`` are set operations
there and literals in Go, ``\\<`` is a word boundary there and a literal ``<`` in Go, and an
octal escape or a repeat count over 1000 is valid in only one of them. So a pattern is never
handed over as written. It is parsed here by Go's rules and re-emitted with every character
spelled as a code point, every class as explicit ranges, and every flag applied.

:func:`translate_regex` returns the Rust pattern, or ``None`` for a pattern Go rejects (CEL
raises at evaluation, which denies the row under both polarities). Anything this parser is
not sure it reads as Go does raises :class:`UnsupportedPlanError`.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

from cerbos_polars.errors import UnsupportedPlanError

MAX_RUNE = 0x10FFFF
MAX_REPEAT = 1000

_DIGIT = ((ord("0"), ord("9")),)
_SPACE = ((9, 10), (12, 13), (32, 32))
_WORD = (
    (ord("0"), ord("9")),
    (ord("A"), ord("Z")),
    (ord("_"), ord("_")),
    (ord("a"), ord("z")),
)

_POSIX = {
    "alnum": ((48, 57), (65, 90), (97, 122)),
    "alpha": ((65, 90), (97, 122)),
    "ascii": ((0, 127),),
    "blank": ((9, 9), (32, 32)),
    "cntrl": ((0, 31), (127, 127)),
    "digit": ((48, 57),),
    "graph": ((33, 126),),
    "lower": ((97, 122),),
    "print": ((32, 126),),
    "punct": ((33, 47), (58, 64), (91, 96), (123, 126)),
    "space": ((9, 13), (32, 32)),
    "upper": ((65, 90),),
    "word": _WORD,
    "xdigit": ((48, 57), (65, 70), (97, 102)),
}

_KELVIN = 0x212A
_LONG_S = 0x17F


def _always(_: int) -> bool:
    return True


class _GoSyntaxError(Exception):
    pass


def _unsupported(pattern: str, why: str) -> UnsupportedPlanError:
    return UnsupportedPlanError(
        f"Cannot translate the regular expression {pattern!r} for Polars: {why}"
    )


Ranges = tuple[tuple[int, int], ...]


def _normalise(ranges: list[tuple[int, int]]) -> Ranges:
    out: list[tuple[int, int]] = []
    for lo, hi in sorted(ranges):
        if out and lo <= out[-1][1] + 1:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return tuple(out)


def _negate(ranges: Ranges) -> Ranges:
    out = []
    nxt = 0
    for lo, hi in ranges:
        if lo > nxt:
            out.append((nxt, lo - 1))
        nxt = hi + 1
    if nxt <= MAX_RUNE:
        out.append((nxt, MAX_RUNE))
    return tuple(out)


def _orbit(cp: int) -> tuple[int, ...] | None:
    """Go's simple case-folding orbit of one code point, or None when it cannot be stated."""
    if cp < 128:
        c = chr(cp)
        if not c.isalpha():
            return (cp,)
        orbit = {ord(c.lower()), ord(c.upper())}
        if c in "kK":
            orbit.add(_KELVIN)
        if c in "sS":
            orbit.add(_LONG_S)
        return tuple(sorted(orbit))
    if cp == _KELVIN:
        return _orbit(ord("k"))
    if cp == _LONG_S:
        return _orbit(ord("s"))
    c = chr(cp)
    if c.upper() == c and c.lower() == c and c.casefold() == c:
        return (cp,)
    return None


def _fold(ranges: Ranges, pattern: str) -> Ranges:
    out = list(ranges)
    for lo, hi in ranges:
        for cp in range(lo, min(hi, 127) + 1):
            orbit = _orbit(cp)
            assert orbit is not None
            out.extend((o, o) for o in orbit)
        if hi >= 128:
            for special in (_KELVIN, _LONG_S):
                if lo <= special <= hi:
                    out.extend((o, o) for o in _orbit(special) or ())
            span_lo = max(lo, 128)
            if hi - span_lo > 4096:
                raise _unsupported(
                    pattern,
                    "case-insensitive matching of a large non-ASCII range needs Go's "
                    "Unicode case-folding tables",
                )
            for cp in range(span_lo, hi + 1):
                if cp in (_KELVIN, _LONG_S) or 0xD800 <= cp <= 0xDFFF:
                    continue
                if _orbit(cp) is None:
                    raise _unsupported(
                        pattern,
                        "case-insensitive matching of a non-ASCII letter needs Go's "
                        "Unicode case-folding orbit, which this adapter does not carry",
                    )
    return _normalise(out)


@dataclass
class _Flags:
    fold: bool = False
    multiline: bool = False
    dotall: bool = False


def _hex(cp: int) -> str:
    return f"\\x{{{cp:X}}}"


def _class(ranges: Ranges) -> str:
    ranges = tuple(
        r
        for lo, hi in ranges
        for r in (
            ((lo, min(hi, 0xD7FF)), (max(lo, 0xE000), hi))
            if lo <= 0xDFFF and hi >= 0xD800
            else ((lo, hi),)
        )
        if r[0] <= r[1]
    )
    if not ranges:
        return "[a&&b]"
    body = "".join(
        _hex(lo) if lo == hi else f"{_hex(lo)}-{_hex(hi)}" for lo, hi in ranges
    )
    return f"[{body}]"


class _Parser:
    def __init__(self, pattern: str) -> None:
        self.pattern = pattern
        self.src = pattern
        self.pos = 0
        self.names: set[str] = set()

    def peek(self, offset: int = 0) -> str:
        i = self.pos + offset
        return self.src[i] if i < len(self.src) else ""

    def eof(self) -> bool:
        return self.pos >= len(self.src)

    def take(self) -> str:
        c = self.src[self.pos]
        self.pos += 1
        return c

    def parse(self) -> str:
        out, closed, _ = self.alternation(_Flags(), depth=0)
        if closed:
            raise _GoSyntaxError("unexpected )")
        return out

    def alternation(
        self, flags: _Flags, depth: int
    ) -> tuple[str, bool, Callable[[int], bool]]:
        if depth > 100:
            raise _unsupported(self.pattern, "groups nest too deeply")
        branches = []
        checks: list[Callable[[int], bool]] = []
        while True:
            seq, flags, stop, valid = self.sequence(flags, depth)
            branches.append(seq)
            checks.append(valid)
            if stop == "|":
                continue
            body = "|".join(branches) if len(branches) > 1 else branches[0]
            return body, stop == ")", lambda n: all(check(n) for check in checks)

    def sequence(
        self, flags: _Flags, depth: int
    ) -> tuple[str, _Flags, str, Callable[[int], bool]]:
        flags = _Flags(flags.fold, flags.multiline, flags.dotall)
        items: list[str] = []
        repeated: list[bool] = []
        valids: list[Callable[[int], bool]] = []

        def done(stop: str) -> tuple[str, _Flags, str, Callable[[int], bool]]:
            return "".join(items), flags, stop, lambda n: all(c(n) for c in valids)

        while not self.eof():
            c = self.peek()
            if c == "|":
                self.take()
                return done("|")
            if c == ")":
                if depth == 0:
                    raise _GoSyntaxError("unexpected )")
                self.take()
                return done(")")
            if c in "*+?":
                self.take()
                op = c
                if self.peek() == "?":
                    self.take()
                    op += "?"
                self.apply_repeat(items, repeated, valids, op, None)
                continue
            if c == "{":
                rep = self.try_repeat()
                if rep is not None:
                    op, bounds = rep
                    if self.peek() == "?":
                        self.take()
                        op += "?"
                    self.apply_repeat(items, repeated, valids, op, bounds)
                    continue
                self.take()
                items.append(
                    _hex(ord("{"))
                    if not flags.fold
                    else _class(_fold(((123, 123),), self.pattern))
                )
                repeated.append(False)
                valids.append(_always)
                continue
            if c == "(":
                self.take()
                group = self.group(flags, depth)
                if group is None:
                    if repeated:
                        repeated[-1] = False
                    continue
                items.append(group[0])
                repeated.append(False)
                valids.append(group[1])
                continue
            if c == "[":
                self.take()
                items.append(_class(self.bracket(flags)))
                repeated.append(False)
                valids.append(_always)
                continue
            if c == "^":
                self.take()
                items.append("(?m:^)" if flags.multiline else r"\A")
                repeated.append(False)
                valids.append(_always)
                continue
            if c == "$":
                self.take()
                items.append("(?m:$)" if flags.multiline else r"\z")
                repeated.append(False)
                valids.append(_always)
                continue
            if c == ".":
                self.take()
                items.append(
                    _class(((0, MAX_RUNE),))
                    if flags.dotall
                    else _class(_negate(((10, 10),)))
                )
                repeated.append(False)
                valids.append(_always)
                continue
            if c == "\\" and self.src.startswith("Q", self.pos + 1):
                self.pos += 2
                end = self.src.find(r"\E", self.pos)
                text = self.src[self.pos :] if end < 0 else self.src[self.pos : end]
                self.pos = len(self.src) if end < 0 else end + 2
                for ch in text:
                    items.append(self.literal(ord(ch), flags))
                    repeated.append(False)
                    valids.append(_always)
                if repeated:
                    repeated[-1] = False
                continue
            if c == "\\":
                self.take()
                items.append(self.escape(flags))
                repeated.append(False)
                valids.append(_always)
                continue
            self.take()
            items.append(self.literal(ord(c), flags))
            repeated.append(False)
            valids.append(_always)
        if depth > 0:
            raise _GoSyntaxError("missing )")
        return done("")

    def apply_repeat(self, items, repeated, valids, op, bounds) -> None:
        if not items:
            raise _GoSyntaxError("missing argument to repetition operator")
        if repeated[-1]:
            raise _GoSyntaxError("invalid nested repetition operator")
        items[-1] = f"(?:{items[-1]}){op}"
        repeated[-1] = True
        if bounds is None:
            return
        low, high = bounds
        inner = valids[-1]

        def valid(n: int, low=low, high=high, inner=inner) -> bool:
            # Go's repeatIsValid: counted repetitions nested inside one another may make
            # at most 1000 copies in all.
            m = high
            if m == 0:
                return True
            if m < 0:
                m = low
            if m > n:
                return False
            if m > 0:
                n //= m
            return inner(n)

        valids[-1] = valid
        if (low >= 2 or high >= 2) and not valid(MAX_REPEAT):
            raise _GoSyntaxError("invalid repeat count")

    def try_repeat(self) -> tuple[str, tuple[int, int]] | None:
        m = re.match(r"\{(\d+)(,(\d*))?\}", self.src[self.pos :])
        if not m:
            return None
        if any(g and len(g) > 1 and g[0] == "0" for g in (m.group(1), m.group(3))):
            return None
        lo = int(m.group(1))
        hi = int(m.group(3)) if m.group(3) else None
        if lo > MAX_REPEAT or (hi is not None and (hi > MAX_REPEAT or hi < lo)):
            raise _GoSyntaxError("invalid repeat count")
        self.pos += m.end()
        if m.group(2) is None:
            return f"{{{lo}}}", (lo, lo)
        if hi is None:
            return f"{{{lo},}}", (lo, -1)
        return f"{{{lo},{hi}}}", (lo, hi)

    def group(
        self, flags: _Flags, depth: int
    ) -> tuple[str, Callable[[int], bool]] | None:
        if self.peek() == "?":
            if self.peek(1) == "P" and self.peek(2) == "<" or self.peek(1) == "<":
                self.pos += 3 if self.peek(1) == "P" else 2
                end = self.src.find(">", self.pos)
                if end < 0:
                    raise _GoSyntaxError("invalid named capture")
                name = self.src[self.pos : end]
                if not name or not all(
                    ch == "_" or ch.isascii() and ch.isalnum() for ch in name
                ):
                    raise _GoSyntaxError("invalid named capture")
                if name in self.names:
                    raise _GoSyntaxError("duplicate capture group name")
                self.names.add(name)
                self.pos = end + 1
                body, closed, valid = self.alternation(flags, depth + 1)
                if not closed:
                    raise _GoSyntaxError("missing )")
                return f"(?:{body})", valid
            self.take()
            new = _Flags(flags.fold, flags.multiline, flags.dotall)
            negate = False
            seen = False
            while True:
                c = self.take() if not self.eof() else ""
                if c in ("i", "m", "s", "U"):
                    seen = True
                    if c == "i":
                        new.fold = not negate
                    elif c == "m":
                        new.multiline = not negate
                    elif c == "s":
                        new.dotall = not negate
                elif c == "-":
                    if negate:
                        raise _GoSyntaxError("invalid or unsupported Perl syntax")
                    negate = True
                    seen = False
                elif c == ":":
                    if negate and not seen:
                        raise _GoSyntaxError("invalid or unsupported Perl syntax")
                    body, closed, valid = self.alternation(new, depth + 1)
                    if not closed:
                        raise _GoSyntaxError("missing )")
                    return f"(?:{body})", valid
                elif c == ")":
                    if negate and not seen:
                        raise _GoSyntaxError("invalid or unsupported Perl syntax")
                    flags.fold, flags.multiline, flags.dotall = (
                        new.fold,
                        new.multiline,
                        new.dotall,
                    )
                    return None
                else:
                    raise _GoSyntaxError("invalid or unsupported Perl syntax")
        body, closed, valid = self.alternation(flags, depth + 1)
        if not closed:
            raise _GoSyntaxError("missing )")
        return f"(?:{body})", valid

    def literal(self, cp: int, flags: _Flags) -> str:
        if 0xD800 <= cp <= 0xDFFF:
            raise _unsupported(
                self.pattern, "a surrogate code point cannot appear in a string"
            )
        if flags.fold:
            return _class(_fold(((cp, cp),), self.pattern))
        if cp < 128 and chr(cp).isalnum():
            return chr(cp)
        return _hex(cp)

    def escape(self, flags: _Flags) -> str:
        if self.eof():
            raise _GoSyntaxError("trailing backslash at end of expression")
        c = self.peek()
        if c == "A":
            self.take()
            return r"\A"
        if c == "z":
            self.take()
            return r"\z"
        if c == "b":
            self.take()
            return r"(?-u:\b)"
        if c == "B":
            self.take()
            return r"(?-u:\B)"
        perl = self.perl_class(flags)
        if perl is not None:
            return _class(perl)
        if c in "pP":
            raise _unsupported(
                self.pattern,
                "Unicode classes (\\p) depend on each engine's Unicode tables",
            )
        return self.literal(self.escape_char(), flags)

    def perl_class(self, flags: _Flags) -> Ranges | None:
        c = self.peek()
        base = {"d": _DIGIT, "s": _SPACE, "w": _WORD}.get(c.lower())
        if base is None:
            return None
        self.take()
        ranges = _normalise(list(base))
        if flags.fold:
            ranges = _fold(ranges, self.pattern)
        return _negate(ranges) if c.isupper() else ranges

    def escape_char(self) -> int:
        c = self.take()
        if c in "1234567" and not ("0" <= self.peek() <= "7"):
            raise _GoSyntaxError("invalid escape sequence")
        if c in "01234567":
            value = int(c)
            for _ in range(2):
                if "0" <= self.peek() <= "7":
                    value = value * 8 + int(self.take())
                else:
                    break
            return value
        if c == "x":
            if self.peek() == "{":
                end = self.src.find("}", self.pos)
                digits = self.src[self.pos + 1 : end] if end >= 0 else ""
                if (
                    end < 0
                    or not digits
                    or not all(d in "0123456789abcdefABCDEF" for d in digits)
                ):
                    raise _GoSyntaxError("invalid escape sequence")
                value = int(digits, 16)
                if value > MAX_RUNE:
                    raise _GoSyntaxError("invalid escape sequence")
                self.pos = end + 1
                return value
            digits = self.src[self.pos : self.pos + 2]
            if len(digits) != 2 or not all(
                d in "0123456789abcdefABCDEF" for d in digits
            ):
                raise _GoSyntaxError("invalid escape sequence")
            self.pos += 2
            return int(digits, 16)
        simple = {"a": 7, "f": 12, "n": 10, "r": 13, "t": 9, "v": 11}
        if c in simple:
            return simple[c]
        if ord(c) < 128 and not c.isalnum():
            return ord(c)
        raise _GoSyntaxError("invalid escape sequence")

    def bracket(self, flags: _Flags) -> Ranges:
        negated = False
        if self.peek() == "^":
            self.take()
            negated = True
        ranges: list[tuple[int, int]] = []
        first = True
        while True:
            if self.eof():
                raise _GoSyntaxError("missing closing ]")
            c = self.peek()
            if c == "]" and not first:
                self.take()
                break
            first = False
            if c == "[" and self.peek(1) == ":":
                end = self.src.find(":]", self.pos + 2)
                if end >= 0:
                    name = self.src[self.pos + 2 : end]
                    neg = name.startswith("^")
                    key = name[1:] if neg else name
                    if key not in _POSIX:
                        raise _GoSyntaxError("invalid character class range")
                    self.pos = end + 2
                    posix = _normalise(list(_POSIX[key]))
                    if flags.fold:
                        posix = _fold(posix, self.pattern)
                    ranges.extend(_negate(posix) if neg else posix)
                    continue
            if c == "\\":
                self.take()
                if self.eof():
                    raise _GoSyntaxError("missing closing ]")
                if self.peek() in "pP":
                    raise _unsupported(
                        self.pattern,
                        "Unicode classes (\\p) depend on each engine's Unicode tables",
                    )
                perl = self.perl_class(_Flags(flags.fold))
                if perl is not None:
                    ranges.extend(perl)
                    continue
                lo = self.escape_char()
            else:
                lo = ord(self.take())
            hi = lo
            if self.peek() == "-" and self.peek(1) not in ("]", ""):
                self.take()
                if self.peek() == "\\":
                    self.take()
                    if self.eof():
                        raise _GoSyntaxError("missing closing ]")
                    if self.peek().lower() in "dswp":
                        raise _GoSyntaxError("invalid character class range")
                    hi = self.escape_char()
                else:
                    hi = ord(self.take())
                if hi < lo:
                    raise _GoSyntaxError("invalid character class range")
            ranges.append((lo, hi))
        result = _normalise(ranges)
        if flags.fold:
            result = _fold(result, self.pattern)
        return _negate(result) if negated else result


def translate_regex(pattern: str) -> str | None:
    """The Rust pattern matching what Go's ``regexp`` matches, or None if Go rejects it."""
    try:
        return _Parser(pattern).parse()
    except _GoSyntaxError:
        return None
