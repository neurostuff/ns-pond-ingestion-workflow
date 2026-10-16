"""Search services for the ingestion workflow, and the overlap of what they find."""

from __future__ import annotations

import datetime
import itertools
import json
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import Catalog
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import ESEARCH_MAX_RESULTS, PubMedClient
from ingestion_workflow.clients.sciencedirect import ScienceDirectClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.services.query_dialect import (
    And,
    Not,
    Or,
    Phrase,
    Query,
    QueryError,
    Term,
    Years,
    to_openalex,
    to_pubmed,
    to_sciencedirect,
    to_semantic_scholar,
)


class ArticleSearchService:
    """Base class for workflow search services; `rendered` is the provider's query.

    A provider that has to split a query describes the split, one line per
    sub-query, in `plan_report`.
    """

    rendered: str

    def __init__(self, query: Query) -> None:
        self.query = query
        self.plan_report: List[str] = []

    def plan(self, echo: Callable[[str], None] = lambda line: None) -> None:
        """What must be settled before any provider searches, each line told to `echo`.

        Nothing, by default.
        """

    def search(self) -> Identifiers:  # pragma: no cover - abstract placeholder
        raise NotImplementedError


class PubMedSearchService(ArticleSearchService):
    """Journal articles PubMed matches; PMIDs only."""

    def __init__(
        self,
        query: Query,
        settings: Settings,
        *,
        client: Optional[PubMedClient] = None,
    ) -> None:
        super().__init__(query)
        self.settings = settings
        self.rendered = to_pubmed(query)
        self._client = client or self._create_client()

    def plan(self, echo: Callable[[str], None] = lambda line: None) -> None:
        """Refuse a search too large for one ESearch with hits outside the years it splits over."""
        total = self._client.count(self.rendered)
        if total < ESEARCH_MAX_RESULTS:
            return
        start = self.query.years.start
        end = self.query.years.end or self._client.latest_year(self.rendered)
        dated = self._client.count(self.rendered, mindate=start, maxdate=end)
        if dated < total:
            raise ValueError(
                f"PubMed finds {total:,} for {self.rendered!r}, more than one search "
                f"returns, and only {dated:,} are dated {start}-{end}, the years it can "
                "be split into; narrow the query or its years"
            )

    def search(self) -> Identifiers:
        # A result set too large for one ESearch is split by year over these.
        return self._client.search(
            self.rendered, start_year=self.query.years.start, end_year=self.query.years.end
        )

    def _create_client(self) -> PubMedClient:
        if not self.settings.pubmed_email:
            raise ValueError("pubmed_email must be configured to perform PubMed searches")

        return PubMedClient(
            email=self.settings.pubmed_email,
            api_key=self.settings.pubmed_api_key or None,
            tool=self.settings.pubmed_tool or "ingestion-workflow",
        )


class SemanticScholarSearchService(ArticleSearchService):
    """Papers Semantic Scholar's bulk search matches in title and abstract."""

    def __init__(self, query: Query, settings: Settings) -> None:
        super().__init__(query)
        self.params = to_semantic_scholar(query)
        self.rendered = " ".join(f"{k}={v}" for k, v in self.params.items())
        if not settings.semantic_scholar_api_key:
            raise ValueError(
                "semantic_scholar_api_key must be configured to search Semantic Scholar"
            )
        self._client = SemanticScholarClient(settings.semantic_scholar_api_key)

    def search(self) -> Identifiers:
        return self._client.search_bulk(self.params)


class OpenAlexSearchService(ArticleSearchService):
    """Journal articles OpenAlex matches."""

    def __init__(self, query: Query, settings: Settings) -> None:
        super().__init__(query)
        self.rendered = to_openalex(query)
        if not settings.openalex_api_key:
            # OpenAlex pages past the first only for a request with a key
            raise ValueError("openalex_api_key must be configured to search OpenAlex")
        self._client = OpenAlexClient(settings.openalex_email, settings.openalex_api_key)

    def search(self) -> Identifiers:
        return self._client.search(self.rendered)


# Words to split a ScienceDirect query on, as "Q AND word" and "Q NOT word". In
# full text most common words are in most articles: brain, study and data were
# in 93-97% of a 2026 neuroimaging query's hits. The first five here were in
# 56-65% of them (2026-10-15); the rarer rest suit parts already split.
SCIENCEDIRECT_PIVOTS = (
    "memory",
    "disease",
    "treatment",
    "patients",
    "human",
    "children",
    "women",
    "cohort",
    "mice",
    "anxiety",
    "longitudinal",
    "questionnaire",
    "randomized",
)
# The API ignores Boolean connectors past this many in one query, silently: a
# ninth OR added nothing to the count, and a ninth AND or NOT left it unchanged.
SCIENCEDIRECT_MAX_CONNECTORS = 8


def _connectors(node) -> int:
    if isinstance(node, (And, Or)):
        return len(node.operands) - 1 + sum(_connectors(child) for child in node.operands)
    if isinstance(node, Not):
        return _connectors(node.operand)
    return 0


def _words(node) -> Set[str]:
    if isinstance(node, (Term, Phrase)):
        return {word.lower() for word in node.text.split()}
    children = node.operands if isinstance(node, (And, Or)) else (node.operand,)
    return set().union(*(_words(child) for child in children))


def _and(query: Query, operand) -> Query:
    operands = query.expr.operands if isinstance(query.expr, And) else (query.expr,)
    return Query(And((*operands, operand)), query.field, query.years)


OR_HALVES = "into halves of its OR clauses"
# A word is taken at once when it puts this share of a query's hits on each side.
SCIENCEDIRECT_BALANCED = (0.35, 0.65)


def _this_year() -> int:
    return datetime.date.today().year


class ScienceDirectSearchService(ArticleSearchService):
    """Articles ScienceDirect matches in full text (or title), split to be paged to the end.

    The API serves a query's results only up to a fixed offset and honours a
    fixed number of Boolean connectors. `plan` splits, from counts alone, until
    every part can be paged:

    1. a query with too many connectors into two halves of its OR clauses;
    2. a query finding too many into two halves of its years (an open range
       ends at the latest year with hits), whose counts must add up to at least
       its own;
    3. else, an OR into two halves of its clauses;
    4. else into "Q AND w" and "Q NOT w" on a word of SCIENCEDIRECT_PIVOTS,
       accepted only when the two counts add up to Q's.

    A part none of these splits is refused. The union of the parts is the
    whole result.
    """

    def __init__(self, query: Query, settings: Settings) -> None:
        super().__init__(query)
        self.rendered = json.dumps(to_sciencedirect(query))
        if not settings.elsevier_api_key:
            raise ValueError("elsevier_api_key must be configured to search ScienceDirect")
        self._client = ScienceDirectClient(settings.elsevier_api_key)
        # None until `plan`; searching unplanned fails rather than finding nothing
        self._leaves: Optional[List[Tuple[Dict[str, str], int]]] = None

    def plan(self, echo: Callable[[str], None] = lambda line: None) -> None:
        self._leaves = []
        self.plan_report = []
        self._echo = echo
        self._split(self.query, None, 0)

    def search(self) -> Identifiers:
        size = self._client.PAGE_SIZE
        found: Dict[str, Identifier] = {}
        for body, total in self._leaves:
            for offset in range(0, total, size):
                for key, record in self._client.page(body, offset):
                    # the halves of an OR split find the same articles
                    found.setdefault(key, record)
        return Identifiers(list(found.values()))

    def _say(self, depth: int, body: Dict[str, str], text: str) -> None:
        line = f"{'  ' * depth}{json.dumps(body)}: {text}"
        self.plan_report.append(line)
        self._echo(line)

    def _split(self, query: Query, total: Optional[int], depth: int) -> None:
        """Plan `query`, whose count is `total` when already known, printing as it goes."""
        body = to_sciencedirect(query)
        connectors = _connectors(query.expr)
        if connectors > SCIENCEDIRECT_MAX_CONNECTORS:
            if not isinstance(query.expr, Or):
                raise QueryError(
                    f"ScienceDirect honours at most {SCIENCEDIRECT_MAX_CONNECTORS} AND, OR "
                    f"and NOT in one query, {json.dumps(body)} has {connectors}, and it is "
                    "not an OR to split; shorten it"
                )
            self._say(depth, body, f"{connectors} connectors, not counted; split {OR_HALVES}")
            self._halves(query, depth)
            return
        if total is None:
            total = self._client.count(body)
        if total <= self._client.MAX_RESULTS:
            self._say(depth, body, f"{total:,} found")
            self._leaves.append((body, total))
            return
        years = query.years
        if years.end is None:
            years = Years(years.start, self._latest_year(query))
        if years.start < years.end:
            middle = (years.start + years.end) // 2
            halves = [Years(years.start, middle), Years(middle + 1, years.end)]
            parts = [Query(query.expr, query.field, half) for half in halves]
            counts = [self._client.count(to_sciencedirect(part)) for part in parts]
            # An issue dated across two years is in both halves; one in neither is lost.
            if sum(counts) < total:
                raise QueryError(
                    f"ScienceDirect finds {total:,} for {json.dumps(body)} but only "
                    f"{counts[0]:,} + {counts[1]:,} in {halves[0]} and {halves[1]}; "
                    "the split would lose the rest"
                )
            overlap = sum(counts) - total
            self._say(
                depth,
                body,
                f"{total:,} found; split by year: {counts[0]:,} + {counts[1]:,}"
                + (f" ({overlap:,} in both)" if overlap else ""),
            )
            for part, count in zip(parts, counts):
                self._split(part, count, depth + 1)
            return
        if isinstance(query.expr, Or):
            self._say(depth, body, f"{total:,} found; split {OR_HALVES}")
            self._halves(query, depth)
            return
        pivot = self._pivot(query, total) if connectors < SCIENCEDIRECT_MAX_CONNECTORS else None
        if pivot is None:
            raise QueryError(
                f"ScienceDirect finds {total:,} for {json.dumps(body)}, more than the "
                f"{self._client.MAX_RESULTS:,} it serves, and none of its splits applies "
                "(years, OR clauses, or a word whose with and without counts add up and "
                "keep it within 250 characters); narrow it"
            )
        word, with_count = pivot
        self._say(
            depth,
            body,
            f"{total:,} found; split on {word!r}: {with_count:,} + {total - with_count:,}",
        )
        self._split(_and(query, Term(word)), with_count, depth + 1)
        self._split(_and(query, Not(Term(word))), total - with_count, depth + 1)

    def _latest_year(self, query: Query) -> int:
        """The latest year `query` has hits, and at least this year, found by bisection."""

        def any_from(year: int) -> bool:
            onward = Query(query.expr, query.field, Years(year, None))
            return self._client.count(to_sciencedirect(onward)) > 0

        latest = _this_year()
        if not any_from(latest + 1):
            return latest
        low, high = latest + 1, 9999
        while low < high:
            middle = (low + high + 1) // 2
            if any_from(middle):
                low = middle
            else:
                high = middle - 1
        return low

    def _halves(self, query: Query, depth: int) -> None:
        operands = query.expr.operands
        middle = (len(operands) + 1) // 2
        for half in (operands[:middle], operands[middle:]):
            self._split(
                Query(half[0] if len(half) == 1 else Or(half), query.field, query.years),
                None,
                depth + 1,
            )

    def _pivot(self, query: Query, total: int) -> Optional[Tuple[str, int]]:
        """A word whose with and without counts add up to `total`, as near halving it as found.

        Words are counted in turn, and the first that leaves a balanced share
        on each side is taken; otherwise the nearest of those counted.
        """
        used = _words(query.expr)
        low, high = SCIENCEDIRECT_BALANCED
        counted: List[Tuple[str, int]] = []
        for word in SCIENCEDIRECT_PIVOTS:
            if word in used:
                continue
            try:
                piece = to_sciencedirect(_and(query, Term(word)))
            except QueryError:  # the word would take the query past 250 characters
                continue
            with_count = self._client.count(piece)
            if not 0 < with_count < total:
                continue
            if low <= with_count / total <= high:
                if self._adds_up(query, word, with_count, total):
                    return word, with_count
                continue
            counted.append((word, with_count))
        for word, with_count in sorted(counted, key=lambda wc: abs(wc[1] - total / 2)):
            if self._adds_up(query, word, with_count, total):
                return word, with_count
        return None

    def _adds_up(self, query: Query, word: str, with_count: int, total: int) -> bool:
        without = self._client.count(to_sciencedirect(_and(query, Not(Term(word)))))
        return with_count + without == total


SEARCH_SERVICES = {
    "pubmed": PubMedSearchService,
    "semantic_scholar": SemanticScholarSearchService,
    "openalex": OpenAlexSearchService,
    "sciencedirect": ScienceDirectSearchService,
}


def search_services(
    query: Query,
    providers: Sequence[str],
    settings: Settings,
    echo: Callable[[str], None] = lambda line: None,
) -> Dict[str, ArticleSearchService]:
    """One service per provider, each with the query rendered in its dialect.

    Rendering and planning happen here, so a query one provider cannot express
    or split fails before any provider is searched. Each provider's query, and
    each line of its plan as it is made, is told to `echo`.
    """
    services = {name: SEARCH_SERVICES[name](query, settings) for name in providers}
    for name, service in services.items():
        echo(f"{name}: {service.rendered}")
        service.plan(lambda line: echo(f"    {line}"))
    return services


def resolve_hits(
    catalog: Catalog, searched: Dict[str, List[Identifier]]
) -> Dict[str, List[Optional[str]]]:
    """The catalog article each hit is now, in order; None for a hit with no DOI, PMID or PMCID."""
    resolved: Dict[str, List[Optional[str]]] = {}
    for provider, records in searched.items():
        refs = [catalog.resolve(record) for record in records]
        resolved[provider] = [ref.id if ref else None for ref in refs]
    return resolved


@dataclass(frozen=True)
class SearchRun:
    """One search as run: the query as given, its years, what each provider was sent and found."""

    query: str
    years: str
    rendered: Dict[str, str]
    hits: Dict[str, List[Identifier]]


def surviving(catalog: Catalog, before: Set[str]) -> Set[str]:
    """The articles that the articles in `before` are now, after any merges."""
    now = set(catalog.all_article_ids())
    return (before & now) | {catalog.current_id(gone) for gone in before - now}


@dataclass(frozen=True)
class SearchReport:
    """What each provider found, counted in catalog articles.

    Built from the hits resolved after registration and enrichment, so two
    records are one article exactly when the catalog holds them as one. An
    article is known when it is, or has absorbed, one the catalog held before.
    """

    articles: Dict[str, Set[str]]
    known: Set[str]
    without_id: Dict[str, int]

    @classmethod
    def build(cls, existing: Set[str], after: Dict[str, List[Optional[str]]]) -> "SearchReport":
        articles = {p: {a for a in ids if a is not None} for p, ids in after.items()}
        without_id = {p: sum(1 for a in ids if a is None) for p, ids in after.items()}
        return cls(articles, set().union(*articles.values()) & existing, without_id)

    @property
    def distinct(self) -> Set[str]:
        return set().union(*self.articles.values())

    def unique(self, provider: str) -> int:
        others = set().union(*(a for p, a in self.articles.items() if p != provider))
        return len(self.articles[provider] - others)

    def shared(self) -> Dict[Tuple[str, str], int]:
        return {
            (a, b): len(self.articles[a] & self.articles[b])
            for a, b in itertools.combinations(self.articles, 2)
        }
