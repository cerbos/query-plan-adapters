# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""CEL's ``matches()`` without a SQL regex engine.

CEL matches with RE2 (Go's ``regexp``), and no store's regex dialect is RE2: MySQL's ICU
engine lets ``$`` match before a trailing newline, PostgreSQL's ARE differs from RE2 in
classes and flags, and SQLite has no regex operator at all. So a pattern is never handed
to the store. It is parsed here, and lowered only when what it matches can be said with
the adapter's exact string predicates: equality, ``startsWith``, ``endsWith`` and
``contains`` over a finite set of literals, plus two infinite forms with an exact
reading: every character drawn from a small set (``^[ab]+$``), and a prefix and a
suffix around a run of non-newline characters (``^a.*b$``).

:func:`compile_regex` returns one :class:`RegexPlan` per top-level alternative, which
the caller ORs together; ``None`` for a pattern RE2 rejects (CEL raises at evaluation,
which denies under both polarities); and raises :class:`UnsupportedPlanError` for
anything else.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

from cerbos_sqlalchemy.errors import UnsupportedPlanError

#: The most literals one pattern may expand to before it is refused rather than enumerated.
MAX_LITERALS = 256
#: The most characters a set may hold for the every-character-in-a-set form.
MAX_SET_SIZE = 64
#: RE2's bound on the copies a counted repetition, nested ones included, may make.
MAX_REPEAT = 1000


@dataclass(frozen=True)
class Literals:
    """The string equals / starts with / ends with / contains one of ``literals``."""

    kind: str  # "equals" | "startsWith" | "endsWith" | "contains"
    literals: tuple[str, ...]


@dataclass(frozen=True)
class AllCharactersIn:
    """Every character of the string is in ``characters``, and there are at least ``min``."""

    characters: tuple[str, ...]
    min: int


@dataclass(frozen=True)
class NoNewline:
    """No character is a newline, and there are at least ``min`` of them."""

    min: int


@dataclass(frozen=True)
class PrefixSuffix:
    """For some pair, the string starts with the prefix, ends with the suffix, holds at
    least ``len(prefix) + min + len(suffix)`` characters, and contains no newline."""

    pairs: tuple[tuple[str, str], ...]
    min: int


RegexPlan = Literals | AllCharactersIn | NoNewline | PrefixSuffix


@dataclass(frozen=True)
class _Set:
    characters: tuple[str, ...]


@dataclass(frozen=True)
class _Dot:
    pass


@dataclass(frozen=True)
class _Group:
    alternatives: tuple[tuple[_Node, ...], ...]


@dataclass(frozen=True)
class _Repeat:
    node: _Node
    min: int
    max: int | None


@dataclass(frozen=True)
class _Begin:
    pass


@dataclass(frozen=True)
class _End:
    pass


_Node = _Set | _Dot | _Group | _Repeat | _Begin | _End


class _InvalidPattern(Exception):
    """RE2 rejects the pattern: CEL raises when it evaluates ``matches()``."""


def _unsupported(pattern: str, why: str) -> UnsupportedPlanError:
    return UnsupportedPlanError(
        f'Cannot translate matches("{pattern}"): {why}. The adapter lowers only '
        "patterns whose matches are a finite set of literals under anchors, every "
        "character from a small set, or a prefix and suffix around non-newline "
        "characters, because no store's regex dialect is RE2"
    )


def _range(start: str, end: str) -> tuple[str, ...]:
    return tuple(chr(code) for code in range(ord(start), ord(end) + 1))


_DIGITS = _range("0", "9")
_LOWER = _range("a", "z")
_UPPER = _range("A", "Z")
_WORD = _DIGITS + _UPPER + _LOWER + ("_",)
_SPACE = ("\t", "\n", "\f", "\r", " ")

#: RE2's ASCII POSIX classes, ``[[:name:]]``.
_POSIX_CLASSES: dict[str, tuple[str, ...]] = {
    "alnum": _DIGITS + _UPPER + _LOWER,
    "alpha": _UPPER + _LOWER,
    "blank": (" ", "\t"),
    "digit": _DIGITS,
    "lower": _LOWER,
    "space": _SPACE + ("\v",),
    "upper": _UPPER,
    "word": _WORD,
    "xdigit": _DIGITS + _range("A", "F") + _range("a", "f"),
}

#: Perl classes RE2 accepts, outside and inside brackets.
_PERL_CLASSES: dict[str, tuple[str, ...]] = {"d": _DIGITS, "s": _SPACE, "w": _WORD}

_LITERAL_ESCAPES = {"a": "\x07", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v"}

_PUNCTUATION = re.compile(r"^[!-/:-@\[-`{-~]$")
_COUNT = re.compile(r"^\{(0|[1-9]\d*)(,(0|[1-9]\d*)?)?\}")
_POSIX = re.compile(r"^\[:([a-z]+):\]")


def _fold_case(character: str, pattern: str) -> tuple[str, ...]:
    """Every character RE2's ``(?i)`` treats as the same as ``character``.

    Its case folding follows Unicode simple folding, so beyond the two ASCII cases
    ``k`` also matches KELVIN SIGN and ``s`` matches LONG S. A non-ASCII letter's orbit
    is not tabulated here, so it is refused.
    """
    if character in ("k", "K"):
        return ("k", "K", "K")
    if character in ("s", "S"):
        return ("s", "S", "ſ")
    if character in _LOWER or character in _UPPER:
        return (character.lower(), character.upper())
    if ord(character) > 0x7F and character.lower() != character.upper():
        raise _unsupported(
            pattern, f"case-insensitive matching of '{character}' is not tabulated"
        )
    return (character,)


def _unique(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


class _Parser:
    """A recursive-descent parser for the RE2 syntax the lowering can use."""

    def __init__(self, pattern: str) -> None:
        self.pattern = pattern
        self.characters = list(pattern)
        self.position = 0
        self.case_insensitive = False

    def parse(self) -> tuple[tuple[_Node, ...], ...]:
        if self.pattern.startswith("(?i)"):
            self.case_insensitive = True
            self.position = 4
        alternatives = self._alternation()
        if self.position < len(self.characters):
            # Only an unbalanced `)` stops the top level early.
            raise _InvalidPattern()
        return alternatives

    def _peek(self, offset: int = 0) -> str | None:
        index = self.position + offset
        return self.characters[index] if index < len(self.characters) else None

    def _rest(self) -> str:
        return "".join(self.characters[self.position :])

    def _alternation(self) -> tuple[tuple[_Node, ...], ...]:
        alternatives = [self._sequence()]
        while self._peek() == "|":
            self.position += 1
            alternatives.append(self._sequence())
        return tuple(alternatives)

    def _sequence(self) -> tuple[_Node, ...]:
        nodes: list[_Node] = []
        while True:
            nxt = self._peek()
            if nxt is None or nxt in ("|", ")"):
                return tuple(nodes)
            nodes.append(self._quantified(self._atom()))

    def _quantified(self, atom: _Node) -> _Node:
        node = atom
        while True:
            nxt = self._peek()
            bounds: tuple[int, int | None] | None
            if nxt == "*":
                bounds = (0, None)
            elif nxt == "+":
                bounds = (1, None)
            elif nxt == "?":
                bounds = (0, 1)
            elif nxt == "{":
                bounds = self._counted_repeat()
            else:
                bounds = None
            if bounds is None:
                return node
            if nxt != "{":
                self.position += 1
            # RE2 rejects a repeated repetition (`a**`, "invalid nested repetition").
            if isinstance(node, _Repeat):
                raise _InvalidPattern()
            if isinstance(node, (_Begin, _End)):
                raise _unsupported(self.pattern, "a repeated anchor is not read")
            # A trailing `?` makes the repetition lazy: WHERE it matches, not WHETHER.
            if self._peek() == "?":
                self.position += 1
            node = _Repeat(node, bounds[0], bounds[1])
            # RE2 rejects a repetition whose nested copies exceed MAX_REPEAT.
            if not _repeat_is_valid(node, MAX_REPEAT):
                raise _InvalidPattern()

    def _counted_repeat(self) -> tuple[int, int | None] | None:
        """``{n}``, ``{n,}`` or ``{n,m}``; anything else starting with ``{`` is a literal
        brace in RE2, which reads a count as Go's ``parseInt`` does, without a leading
        zero: ``a{01}`` is ``a`` then the text "{01}"."""
        match = _COUNT.match(self._rest())
        if not match:
            return None
        minimum = int(match.group(1))
        if match.group(2) is None:
            maximum: int | None = minimum
        elif match.group(3) is None:
            maximum = None
        else:
            maximum = int(match.group(3))
        if minimum > MAX_REPEAT or (
            maximum is not None and (maximum > MAX_REPEAT or maximum < minimum)
        ):
            raise _InvalidPattern()
        self.position += len(match.group(0))
        return minimum, maximum

    def _literal(self, character: str) -> _Node:
        if self.case_insensitive:
            return _Set(_fold_case(character, self.pattern))
        return _Set((character,))

    def _atom(self) -> _Node:
        character = self._peek()
        assert character is not None
        self.position += 1
        if character == "^":
            return _Begin()
        if character == "$":
            return _End()
        if character == ".":
            return _Dot()
        if character == "(":
            return self._group()
        if character == "[":
            return _Set(self._character_class())
        if character == "\\":
            return self._escape()
        if character in ("*", "+", "?"):
            raise _InvalidPattern()  # "missing argument to repetition operator"
        if character == "{":
            # A brace that opens a valid count with nothing before it is the same
            # error; any other brace is a literal in RE2.
            self.position -= 1
            if self._counted_repeat() is not None:
                raise _InvalidPattern()
            self.position += 1
        return self._literal(character)

    def _group(self) -> _Node:
        if self._peek() == "?":
            rest = self._rest()
            # Lookaround is not RE2 syntax: CEL raises at evaluation.
            if re.match(r"^\?(=|!|<=|<!)", rest):
                raise _InvalidPattern()
            if rest.startswith("?:"):
                self.position += 2
            else:
                raise _unsupported(
                    self.pattern, "only (?i) at the start and (?:...) groups are read"
                )
        alternatives = self._alternation()
        if self._peek() != ")":
            raise _InvalidPattern()  # "missing closing )"
        self.position += 1
        return _Group(alternatives)

    def _escape(self) -> _Node:
        character = self._peek()
        if character is None:
            raise _InvalidPattern()  # "trailing backslash"
        self.position += 1
        if character == "A":
            return _Begin()
        if character == "z":
            return _End()
        perl = _PERL_CLASSES.get(character)
        if perl is not None:
            return _Set(perl)
        control = _LITERAL_ESCAPES.get(character)
        if control is not None:
            return self._literal(control)
        if _PUNCTUATION.match(character):
            return self._literal(character)
        # A lone \1-\7 would be a backreference and \8, \9 are nothing: RE2 rejects
        # both, and reads \1-\7 before another octal digit as an octal escape, which is
        # not read here.
        following = self._peek() or ""
        if character in "89" or (
            character in "1234567" and not re.match(r"^[0-7]$", following)
        ):
            raise _InvalidPattern()  # "invalid escape sequence"
        raise _unsupported(self.pattern, f"the escape \\{character} is not read")

    def _character_class(self) -> tuple[str, ...]:
        """``[...]``, just after its ``[``."""
        if self._peek() == "^":
            raise _unsupported(
                self.pattern, "a negated class matches an unbounded set of characters"
            )
        members: list[str] = []
        first = True
        while True:
            character = self._peek()
            if character is None:
                raise _InvalidPattern()  # "missing closing ]"
            if character == "]" and not first:
                self.position += 1
                break
            first = False
            if character == "[" and self._peek(1) == ":":
                match = _POSIX.match(self._rest())
                posix = _POSIX_CLASSES.get(match.group(1)) if match else None
                if match is None or posix is None:
                    raise _unsupported(
                        self.pattern, "only ASCII POSIX classes are read"
                    )
                members.extend(posix)
                self.position += len(match.group(0))
                continue
            start = self._class_character()
            if isinstance(start, tuple):
                members.extend(start)
                continue
            if self._peek() == "-" and self._peek(1) not in ("]", None):
                self.position += 1
                end = self._class_character()
                if isinstance(end, tuple) or end < start:
                    raise _InvalidPattern()
                span = _range(start, end)
                if len(span) > MAX_SET_SIZE * 4:
                    raise _unsupported(
                        self.pattern, "a class range is too wide to enumerate"
                    )
                members.extend(span)
            else:
                members.append(start)
        if self.case_insensitive:
            folded = [c for member in members for c in _fold_case(member, self.pattern)]
            return _unique(folded)
        return _unique(members)

    def _class_character(self) -> str | tuple[str, ...]:
        """One class member: a character, or a Perl class's characters."""
        character = self._peek()
        assert character is not None
        self.position += 1
        if character != "\\":
            return character
        escaped = self._peek()
        if escaped is None:
            raise _InvalidPattern()
        self.position += 1
        perl = _PERL_CLASSES.get(escaped)
        if perl is not None:
            return perl
        control = _LITERAL_ESCAPES.get(escaped)
        if control is not None:
            return control
        if _PUNCTUATION.match(escaped):
            return escaped
        raise _unsupported(self.pattern, f"the escape \\{escaped} is not read")


def _repeat_is_valid(node: _Node, budget: int) -> bool:
    """Go's ``repeatIsValid``: each repetition divides the budget by its count (its
    maximum, or its minimum when unbounded), and a count beyond what is left is
    invalid."""
    if isinstance(node, _Repeat):
        if node.max == 0:
            return True
        count = node.min if node.max is None else node.max
        if count > budget:
            return False
        return _repeat_is_valid(node.node, budget if count == 0 else budget // count)
    if isinstance(node, _Group):
        return all(
            _repeat_is_valid(inner, budget)
            for sequence in node.alternatives
            for inner in sequence
        )
    return True


def _finite_sequence(nodes: tuple[_Node, ...]) -> tuple[str, ...] | None:
    """Every string a node sequence matches, when that is a finite set of at most
    MAX_LITERALS."""
    strings: tuple[str, ...] = ("",)
    for node in nodes:
        nxt = _finite_node(node)
        if nxt is None:
            return None
        product: list[str] = []
        for head in strings:
            for tail in nxt:
                product.append(head + tail)
                if len(product) > MAX_LITERALS:
                    return None
        strings = _unique(product)
    return strings


def _finite_node(node: _Node) -> tuple[str, ...] | None:
    if isinstance(node, _Set):
        return node.characters
    if isinstance(node, _Group):
        union: list[str] = []
        for alternative in node.alternatives:
            strings = _finite_sequence(alternative)
            if strings is None:
                return None
            union.extend(strings)
            if len(union) > MAX_LITERALS:
                return None
        return _unique(union)
    if isinstance(node, _Repeat):
        if node.max is None:
            return None
        union = []
        for count in range(node.min, node.max + 1):
            strings = _finite_sequence((node.node,) * count)
            if strings is None:
                return None
            union.extend(strings)
            if len(union) > MAX_LITERALS:
                return None
        return _unique(union)
    return None


def _at_least(node: _Node) -> _Node:
    return replace(node, max=node.min) if isinstance(node, _Repeat) else node


def _plan_alternative(pattern: str, sequence: tuple[_Node, ...]) -> RegexPlan | None:
    """One top-level alternative, its anchors removed; ``None`` when it matches nothing.

    An end without an anchor is free: a match may start (or stop) anywhere, so a
    repetition there needs only its minimum number of copies (``a+b`` somewhere is
    exactly ``ab`` somewhere) and ``.*`` there needs none.
    """
    nodes = list(sequence)
    anchored_start = anchored_end = False
    while nodes and isinstance(nodes[0], _Begin):
        anchored_start = True
        nodes.pop(0)
    while nodes and isinstance(nodes[-1], _End):
        anchored_end = True
        nodes.pop()
    if any(isinstance(node, (_Begin, _End)) for node in nodes):
        raise _unsupported(pattern, "an anchor inside the pattern is not read")
    if not anchored_start:
        while nodes and isinstance(nodes[0], _Repeat) and nodes[0].min == 0:
            nodes.pop(0)
        if nodes:
            nodes[0] = _at_least(nodes[0])
    if not anchored_end:
        while nodes and isinstance(nodes[-1], _Repeat) and nodes[-1].min == 0:
            nodes.pop()
        if nodes:
            nodes[-1] = _at_least(nodes[-1])

    literals = _finite_sequence(tuple(nodes))
    if literals is not None:
        if not literals:
            return None
        if anchored_start:
            kind = "equals" if anchored_end else "startsWith"
        else:
            kind = "endsWith" if anchored_end else "contains"
        return Literals(kind, literals)

    if anchored_start and anchored_end:
        # `^X*$`, `^X+$`, `^X{n,}$` over a set or `.`.
        if len(nodes) == 1:
            only = nodes[0]
            if isinstance(only, _Repeat) and only.max is None:
                if isinstance(only.node, _Dot):
                    return NoNewline(only.min)
                if (
                    isinstance(only.node, _Set)
                    and len(only.node.characters) <= MAX_SET_SIZE
                ):
                    return AllCharactersIn(only.node.characters, only.min)
        # `^P.*S$`: finite prefix and suffix around one unbounded run of `.`.
        runs = [
            index
            for index, node in enumerate(nodes)
            if isinstance(node, _Repeat)
            and isinstance(node.node, _Dot)
            and node.max is None
        ]
        if len(runs) == 1:
            run = runs[0]
            prefixes = _finite_sequence(tuple(nodes[:run]))
            suffixes = _finite_sequence(tuple(nodes[run + 1 :]))
            if prefixes is not None and suffixes is not None:
                pairs = tuple(
                    (prefix, suffix) for prefix in prefixes for suffix in suffixes
                )
                if not pairs:
                    return None
                if len(pairs) <= MAX_LITERALS and all(
                    "\n" not in prefix + suffix for prefix, suffix in pairs
                ):
                    run_node = nodes[run]
                    assert isinstance(run_node, _Repeat)
                    return PrefixSuffix(pairs, run_node.min)
    raise _unsupported(pattern, "what it matches is not a finite set of literals")


def compile_regex(pattern: str) -> list[RegexPlan] | None:
    """The plans whose OR is ``pattern``, or ``None`` when RE2 rejects the pattern."""
    try:
        alternatives = _Parser(pattern).parse()
    except _InvalidPattern:
        return None
    plans = []
    for alternative in alternatives:
        plan = _plan_alternative(pattern, alternative)
        if plan is not None:
            plans.append(plan)
    return plans
