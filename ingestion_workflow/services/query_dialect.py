"""One search query, written once and rendered for each search provider.

The syntax is documented in docs/search.md. `parse` builds a small tree; each
`to_*` renders it in one provider's dialect, and raises `QueryError` naming the
provider for anything that dialect cannot say, so no part of a query is lost.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union


class QueryError(ValueError):
    """A query that cannot be parsed, or cannot be said in a provider's dialect."""


class Field(str, Enum):
    TITLE = "title"
    ABSTRACT = "abstract"
    TITLE_ABSTRACT = "title+abstract"


@dataclass(frozen=True)
class Term:
    text: str


@dataclass(frozen=True)
class Phrase:
    text: str


@dataclass(frozen=True)
class Not:
    """Excludes its operand; only ever a later operand of an `And`."""

    operand: "Node"


@dataclass(frozen=True)
class And:
    operands: Tuple["Node", ...]


@dataclass(frozen=True)
class Or:
    operands: Tuple["Node", ...]


Node = Union[Term, Phrase, Not, And, Or]


@dataclass(frozen=True)
class Years:
    start: int
    end: Optional[int]  # None: through the latest year with hits

    @classmethod
    def parse(cls, text: str) -> "Years":
        """`2026` or `2025:2026`."""
        match = re.fullmatch(r"\s*(\d{4})\s*(?::\s*(\d{4})\s*)?", text)
        if not match:
            raise QueryError(f"a year is 2026 or a range 2025:2026, not {text!r}")
        start, end = int(match.group(1)), int(match.group(2) or match.group(1))
        if end < start:
            raise QueryError(f"the year range {text!r} ends before it starts")
        return cls(start, end)

    def __str__(self) -> str:
        if self.end is None:
            return f"{self.start} onward"
        return str(self.start) if self.start == self.end else f"{self.start}-{self.end}"


# Searched when no years are given: BOLD fMRI began in 1990.
DEFAULT_YEARS = Years(1990, None)


@dataclass(frozen=True)
class Query:
    expr: Node
    field: Field = Field.TITLE_ABSTRACT
    years: Years = DEFAULT_YEARS


OPERATORS = ("AND", "OR", "NOT")
# Operators of the provider dialects; a query using them would mean different
# things to different providers.
_FORBIDDEN = set("[]*?~|+:{}^\\")
_TOKEN = re.compile(r'\s*(?:(\()|(\))|"([^"]*)("?)|([^\s()"]+))')
_FIELD_PREFIX = re.compile(r"\s*(title|abstract)\s*:", re.IGNORECASE)


def parse(text: str, years: Years = DEFAULT_YEARS) -> Query:
    """The query `text` means, searched in `years`."""
    field = Field.TITLE_ABSTRACT
    prefix = _FIELD_PREFIX.match(text)
    if prefix:
        field = Field(prefix.group(1).lower())
        text = text[prefix.end() :]
    tokens = _tokenize(text)
    if not tokens:
        raise QueryError("the search query is empty")
    parser = _Parser(tokens)
    expr = parser.or_expr()
    if parser.peek() is not None:
        raise QueryError(f"unexpected {parser.peek()[1]!r}; is a '(' missing?")
    return Query(expr, field, years)


def _tokenize(text: str) -> List[Tuple[str, str]]:
    tokens: List[Tuple[str, str]] = []
    position = 0
    while position < len(text):
        match = _TOKEN.match(text, position)
        if match is None:  # only trailing whitespace is left
            break
        position = match.end()
        opening, closing, phrase, closed, word = match.groups()
        if opening:
            tokens.append(("(", opening))
        elif closing:
            tokens.append((")", closing))
        elif phrase is not None:
            if not closed:
                raise QueryError(f'the phrase "{phrase} has no closing quote')
            if not phrase.strip():
                raise QueryError('an empty phrase "" matches nothing')
            _check_characters(phrase)
            tokens.append(("phrase", " ".join(phrase.split())))
        elif word in OPERATORS:
            tokens.append(("op", word))
        else:
            if word.startswith("-"):
                raise QueryError(f"write NOT {word[1:]!r} rather than {word!r}")
            _check_characters(word)
            tokens.append(("term", word))
    return tokens


def _check_characters(text: str) -> None:
    bad = sorted(set(text) & _FORBIDDEN)
    if bad:
        raise QueryError(
            f"{''.join(bad)!r} in {text!r} is not part of the search syntax "
            '(terms, "phrases", AND, OR, NOT, parentheses, and a leading title: or abstract:)'
        )


class _Parser:
    """or_expr := and_expr (OR and_expr)*;  and_expr := atom ((AND | NOT) atom)*."""

    def __init__(self, tokens: List[Tuple[str, str]]) -> None:
        self.tokens = tokens
        self.position = 0

    def peek(self) -> Optional[Tuple[str, str]]:
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def take(self) -> Tuple[str, str]:
        token = self.peek()
        if token is None:
            raise QueryError("the query ends where a term was expected")
        self.position += 1
        return token

    def or_expr(self) -> Node:
        operands = [self.and_expr()]
        while self.peek() == ("op", "OR"):
            self.take()
            operands.append(self.and_expr())
        return operands[0] if len(operands) == 1 else Or(tuple(operands))

    def and_expr(self) -> Node:
        operands = [self.atom()]
        while self.peek() in (("op", "AND"), ("op", "NOT")):
            _, op = self.take()
            atom = self.atom()
            operands.append(Not(atom) if op == "NOT" else atom)
        following = self.peek()
        if following is not None and following[0] in ("term", "phrase", "("):
            raise QueryError(
                f"put AND or OR before {following[1]!r} (operators are upper case), "
                "or quote the words as one phrase"
            )
        return operands[0] if len(operands) == 1 else And(tuple(operands))

    def atom(self) -> Node:
        kind, value = self.take()
        if kind == "term":
            return Term(value)
        if kind == "phrase":
            return Phrase(value)
        if kind == "(":
            inner = self.or_expr()
            if self.peek() != (")", ")"):
                raise QueryError("a '(' is never closed")
            self.take()
            return inner
        if value == "NOT":
            raise QueryError("NOT excludes from what comes before it: write `a NOT b`")
        raise QueryError(f"{value!r} needs a term before it")


def _years(years: Years, separator: str, onward: str) -> str:
    """`years` as `start`, `start<separator>end`, or `onward` with {start} filled in."""
    if years.end is None:
        return onward.format(start=years.start, before=years.start - 1)
    return str(years.start) if years.start == years.end else f"{years.start}{separator}{years.end}"


def _grouped(node: Node, render) -> str:
    return f"({render(node)})" if isinstance(node, (And, Or)) else render(node)


def _compound(node: Union[And, Or], render, and_: str, or_: str, and_not: str) -> str:
    """`node`'s operands joined by the dialect's operators, nested groups parenthesized."""
    if isinstance(node, Or):
        return or_.join(_grouped(child, render) for child in node.operands)
    first, *rest = node.operands
    out = _grouped(first, render)
    for child in rest:
        if isinstance(child, Not):
            out += and_not + _grouped(child.operand, render)
        else:
            out += and_ + _grouped(child, render)
    return out


# -- PubMed -------------------------------------------------------------------

_PUBMED_TAGS = {Field.TITLE: "ti", Field.ABSTRACT: "ab", Field.TITLE_ABSTRACT: "tiab"}


def to_pubmed(query: Query) -> str:
    """An E-utilities `term`, limited to journal articles.

    PubMed evaluates operators left to right, so every nested group is
    parenthesized.
    """
    tag = _PUBMED_TAGS[query.field]

    def render(node: Node) -> str:
        if isinstance(node, (Term, Phrase)):
            return f'"{node.text}"[{tag}]'
        return _compound(node, render, " AND ", " OR ", " NOT ")

    parts = [_grouped(query.expr, render), '"journal article"[pt]']
    # 3000 is PubMed's own way to say "to the present", dates ahead included
    parts.append(f"{_years(query.years, ':', '{start}:3000')}[dp]")
    return " AND ".join(parts)


# -- Semantic Scholar -----------------------------------------------------------


def to_semantic_scholar(query: Query) -> Dict[str, str]:
    """`query` (and `year`) parameters for /graph/v1/paper/search/bulk."""
    if query.field is not Field.TITLE_ABSTRACT:
        raise QueryError(
            f"Semantic Scholar's bulk search matches title and abstract together; "
            f"it cannot search the {query.field.value} alone"
        )

    def render(node: Node) -> str:
        if isinstance(node, Term):
            # `-` and other punctuation inside a word are operators to the bulk search
            return node.text if node.text.isalnum() else f'"{node.text}"'
        if isinstance(node, Phrase):
            return f'"{node.text}"'
        return _compound(node, render, " + ", " | ", " + -")

    params = {"query": render(query.expr)}
    params["year"] = _years(query.years, "-", "{start}-")
    return params


def _boolean_words(node: Node, render) -> str:
    """AND / OR / NOT between operands; a term with punctuation is quoted, as a phrase is."""
    if isinstance(node, (Term, Phrase)):
        bare = isinstance(node, Term) and node.text.isalnum()
        return node.text if bare else f'"{node.text}"'
    return _compound(node, render, " AND ", " OR ", " NOT ")


# -- OpenAlex -------------------------------------------------------------------

_OPENALEX_FILTERS = {
    Field.TITLE: "title.search",
    Field.ABSTRACT: "abstract.search",
    Field.TITLE_ABSTRACT: "title_and_abstract.search",
}


def to_openalex(query: Query) -> str:
    """A /works `filter`, limited to `type:article`.

    Unfiltered, most of what OpenAlex adds over PubMed is preprints, datasets,
    dissertations and peer-review records.
    """

    def render(node: Node) -> str:
        if isinstance(node, (Term, Phrase)) and "," in node.text:
            raise QueryError(
                f"OpenAlex separates filters with commas, so {node.text!r} cannot be searched"
            )
        return _boolean_words(node, render)

    filters = [f"{_OPENALEX_FILTERS[query.field]}:({render(query.expr)})", "type:article"]
    filters.append(f"publication_year:{_years(query.years, '-', '>{before}')}")
    return ",".join(filters)


# -- ScienceDirect --------------------------------------------------------------

SCIENCEDIRECT_QUERY_LIMIT = 250  # characters the search accepts in `qs` or `title`


def to_sciencedirect(query: Query) -> Dict[str, str]:
    """Search fields of a PUT /content/search/sciencedirect body.

    ScienceDirect has no abstract field: title and abstract is searched as `qs`,
    the article's full text (references excluded).
    """
    if query.field is Field.ABSTRACT:
        raise QueryError(
            "ScienceDirect's search has no abstract field; it searches the title "
            "(title:) or the full text (the default)"
        )

    def render(node: Node) -> str:
        return _boolean_words(node, render)

    text = render(query.expr)
    if len(text) > SCIENCEDIRECT_QUERY_LIMIT:
        raise QueryError(
            f"ScienceDirect accepts at most {SCIENCEDIRECT_QUERY_LIMIT} characters of query; "
            f"this one is {len(text)}: {text}"
        )
    body = {"title" if query.field is Field.TITLE else "qs": text}
    body["date"] = _years(query.years, "-", "{start}-9999")
    return body
