"""Deterministic regex-based string generation for synthetic data.

Implements a small, self-contained subset parser instead of depending on the
private ``re`` parser modules. Supported constructs: literals, escapes
(``\\d``, ``\\w``, ``\\s`` and negations), character classes and ranges,
groups, alternation, anchors, the dot, and the ``?``, ``*``, ``+``, ``{m}``,
``{m,}``, ``{m,n}`` quantifiers that have bounded replays. Unsupported
constructs (backreferences, lookarounds, inline flags) raise a clear error.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass

from data_platform_common.errors import SyntheticDataError

DIGITS = string.digits
WORD_CHARS = string.ascii_letters + string.digits + "_"
WHITESPACE = " \t"
PRINTABLE = string.ascii_letters + string.digits + " -_.,@"
MAX_REPEAT = 8


class _Node:
    pass


@dataclass
class _Repeat(_Node):
    node: _Node
    minimum: int
    maximum: int


@dataclass
class _Literal(_Node):
    text: str


@dataclass
class _CharClass(_Node):
    characters: str
    negated: bool = False


@dataclass
class _Sequence(_Node):
    nodes: list[_Node]


@dataclass
class _Alternation(_Node):
    options: list[_Node]


def _escape_class(escape: str) -> _CharClass:
    match escape:
        case "d":
            return _CharClass(DIGITS)
        case "D":
            return _CharClass(DIGITS, negated=True)
        case "w":
            return _CharClass(WORD_CHARS)
        case "W":
            return _CharClass(WORD_CHARS, negated=True)
        case "s":
            return _CharClass(WHITESPACE)
        case "S":
            return _CharClass(WHITESPACE, negated=True)
        case _:
            return _CharClass(escape)


class _Parser:
    def __init__(self, pattern: str) -> None:
        self.pattern = pattern
        self.position = 0

    def parse(self) -> _Node:
        node = self._parse_alternation()
        if self.position != len(self.pattern):
            raise SyntheticDataError(
                f"Unsupported regex construct at index {self.position} in /{self.pattern}/"
            )
        return node

    def _peek(self) -> str | None:
        return self.pattern[self.position] if self.position < len(self.pattern) else None

    def _next(self) -> str:
        char = self.pattern[self.position]
        self.position += 1
        return char

    def _parse_alternation(self) -> _Node:
        options = [self._parse_sequence()]
        while self._peek() == "|":
            self._next()
            options.append(self._parse_sequence())
        return options[0] if len(options) == 1 else _Alternation(options)

    def _parse_sequence(self) -> _Node:
        nodes: list[_Node] = []
        while True:
            char = self._peek()
            if char is None or char in "|)":
                break
            nodes.append(self._parse_repeat())
        return _Literal("") if not nodes else _Sequence(nodes)

    def _parse_repeat(self) -> _Node:
        node = self._parse_atom()
        char = self._peek()
        if char == "?":
            self._next()
            return _Repeat(node, 0, 1)
        if char == "*":
            self._next()
            return _Repeat(node, 0, 3)
        if char == "+":
            self._next()
            return _Repeat(node, 1, 4)
        if char == "{":
            return self._parse_braces(node)
        return node

    def _parse_braces(self, node: _Node) -> _Node:
        self._next()  # consume {
        digits = ""
        while (char := self._peek()) is not None and char.isdigit():
            digits += self._next()
        if not digits:
            raise SyntheticDataError(f"Invalid quantifier in /{self.pattern}/")
        minimum = int(digits)
        if self._peek() == "}":
            self._next()
            return _Repeat(node, minimum, minimum)
        if self._peek() != ",":
            raise SyntheticDataError(f"Invalid quantifier in /{self.pattern}/")
        self._next()
        digits = ""
        while (char := self._peek()) is not None and char.isdigit():
            digits += self._next()
        if self._peek() != "}":
            raise SyntheticDataError(f"Unterminated quantifier in /{self.pattern}/")
        self._next()
        maximum = int(digits) if digits else min(minimum + MAX_REPEAT, 64)
        if maximum < minimum:
            raise SyntheticDataError(f"Quantifier range is inverted in /{self.pattern}/")
        return _Repeat(node, minimum, min(maximum, minimum + MAX_REPEAT))

    def _parse_atom(self) -> _Node:
        char = self._next()
        if char in "^$":
            return _Literal("")
        if char == "(":
            if self._peek() == "?":
                self._next()
                marker = self._next()
                if marker == ":":
                    pass
                elif marker == "P":
                    raise SyntheticDataError("Named groups are not supported for generation")
                else:
                    # Lookarounds and flags are unsupported.
                    raise SyntheticDataError(
                        f"Unsupported regex group '(?{marker}' in /{self.pattern}/"
                    )
            node = self._parse_alternation()
            if self._peek() != ")":
                raise SyntheticDataError(f"Unbalanced group in /{self.pattern}/")
            self._next()
            return node
        if char == "[":
            return self._parse_class()
        if char == ".":
            return _CharClass(PRINTABLE)
        if char == "\\":
            escape = self._next()
            return _escape_class(escape)
        if char in "*+?{":
            raise SyntheticDataError(f"Quantifier without an atom in /{self.pattern}/")
        return _Literal(char)

    def _parse_class(self) -> _Node:
        negated = self._peek() == "^"
        if negated:
            self._next()
        characters: list[str] = []
        while True:
            char = self._peek()
            if char is None:
                raise SyntheticDataError(f"Unterminated character class in /{self.pattern}/")
            if char == "]":
                self._next()
                break
            self._next()
            if char == "\\":
                escape = self._next()
                if escape in "dws":
                    characters.extend(_escape_class(escape).characters)
                elif escape in "DWS":
                    raise SyntheticDataError(
                        f"Negated escapes inside classes are not supported in /{self.pattern}/"
                    )
                else:
                    characters.append(escape)
                continue
            if self._peek() == "-" and self.pattern[self.position + 1 : self.position + 2] != "]":
                self._next()
                end = self._next()
                if ord(end) < ord(char):
                    raise SyntheticDataError(f"Invalid range {char}-{end} in /{self.pattern}/")
                characters.extend(chr(code) for code in range(ord(char), ord(end) + 1))
                continue
            characters.append(char)
        return _CharClass("".join(dict.fromkeys(characters)), negated=negated)


def _render(node: _Node, rng: random.Random) -> str:
    if isinstance(node, _Literal):
        return node.text
    if isinstance(node, _CharClass):
        chars = node.characters
        if node.negated:
            candidates = "".join(char for char in PRINTABLE if char not in chars)
            chars = candidates or PRINTABLE
        if not chars:
            raise SyntheticDataError("Empty character class cannot be generated")
        return rng.choice(chars)
    if isinstance(node, _Repeat):
        count = rng.randint(node.minimum, node.maximum)
        return "".join(_render(node.node, rng) for _ in range(count))
    if isinstance(node, _Sequence):
        return "".join(_render(child, rng) for child in node.nodes)
    if isinstance(node, _Alternation):
        return _render(rng.choice(node.options), rng)
    raise SyntheticDataError(f"Unknown regex node: {node!r}")  # pragma: no cover


def generate_from_regex(pattern: str, rng: random.Random) -> str:
    """Generate one string matching ``pattern`` using ``rng`` for determinism."""

    body = pattern
    if body.startswith("^"):
        body = body[1:]
    if body.endswith("$"):
        body = body[:-1]
    node = _Parser(body).parse()
    return _render(node, rng)
