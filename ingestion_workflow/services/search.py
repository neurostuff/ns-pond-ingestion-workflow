"""Search services for the ingestion workflow, and the overlap of what they find."""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import Catalog
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import ESEARCH_MAX_RESULTS, PUBMED_FIRST_YEAR, PubMedClient
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

    def plan(self) -> None:
        """What must be settled before any provider searches. Nothing, by default."""

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

    def plan(self) -> None:
        """Refuse a search too large for one ESearch with hits outside the years it splits over."""
        total = self._client.count(self.rendered)
        if total < ESEARCH_MAX_RESULTS:
            return
        if self.query.years:
            start, end = self.query.years.start, self.query.years.end
        else:
            start, end = PUBMED_FIRST_YEAR, self._client.latest_year(self.rendered)
        dated = self._client.count(self.rendered, mindate=start, maxdate=end)
        if dated < total:
            raise ValueError(
                f"PubMed finds {total:,} for {self.rendered!r}, more than one search "
                f"returns, and only {dated:,} are dated {start}-{end}, the years it can "
                "be split into; narrow the query or its years"
            )

    def search(self) -> Identifiers:
        # A result set too large for one ESearch is split by year over these.
        if self.query.years:
            return self._client.search(
                self.rendered, start_year=self.query.years.start, end_year=self.query.years.end
            )
        return self._client.search(self.rendered)

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
    "memory", "disease", "treatment", "patients", "human", "children", "women",
    "cohort", "mice", "anxiety", "longitudinal", "questionnaire", "randomized",
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


@dataclass
class _Slot:
    """One query of a ScienceDirect plan: paged when it has no parts."""

    body: Dict[str, str]
    total: Optional[int]  # None when the query has too many connectors to be counted
    how: str = ""
    parts: Optional[List["_Slot"]] = None

    def leaves(self) -> List["_Slot"]:
        return [leaf for part in self.parts for leaf in part.leaves()] if self.parts else [self]

    def lines(self, depth: int = 0) -> List[str]:
        found = "not counted" if self.total is None else f"{self.total:,} found"
        line = f"{'  ' * depth}{json.dumps(self.body)}: {found}"
        if self.parts and self.how == OR_HALVES:
            line += f"; split {self.how} (they overlap; their union is exact)"
        elif self.parts:
            totals = " + ".join(f"{part.total:,}" for part in self.parts)
            line += f"; split {self.how}: {totals} = {self.total:,}"
        return [line, *(below for part in self.parts or [] for below in part.lines(depth + 1))]


class ScienceDirectSearchService(ArticleSearchService):
    """Articles ScienceDirect matches in full text (or title), split to be paged to the end.

    The API serves a query's results only up to a fixed offset and honours a
    fixed number of Boolean connectors. `plan` splits, from counts alone:

    1. a query with too many connectors into two halves of its OR clauses;
    2. a query finding too many into one query per year of its range;
    3. then into "Q AND w" and "Q NOT w" on the word w of SCIENCEDIRECT_PIVOTS
       that comes closest to halving it, accepted only when the two counts add
       up to Q's;
    4. then into two halves of its OR clauses.

    Every part of every split is split again until it can be paged; a query
    none of these can split is refused. The union of the parts is the whole
    result.
    """

    def __init__(self, query: Query, settings: Settings) -> None:
        super().__init__(query)
        self.rendered = json.dumps(to_sciencedirect(query))
        if not settings.elsevier_api_key:
            raise ValueError("elsevier_api_key must be configured to search ScienceDirect")
        self._client = ScienceDirectClient(settings.elsevier_api_key)
        # None until `plan`; searching unplanned fails rather than finding nothing
        self._plan: Optional[_Slot] = None

    def plan(self) -> None:
        self._plan = self._split(self.query)
        self.plan_report = self._plan.lines()

    def search(self) -> Identifiers:
        size = self._client.PAGE_SIZE
        found: Dict[str, Identifier] = {}
        bare: List[Identifier] = []
        for leaf in self._plan.leaves():
            for offset in range(0, leaf.total, size):
                for record in self._client.page(leaf.body, offset):
                    key = record.doi or (record.other_ids or {}).get("pii")
                    if key:
                        # parts of an OR split find the same article
                        found.setdefault(key, record)
                    else:
                        bare.append(record)
        return Identifiers([*found.values(), *bare])

    def _split(self, query: Query, total: Optional[int] = None) -> _Slot:
        """`query`'s plan; `total` is its count when already known."""
        body = to_sciencedirect(query)
        connectors = _connectors(query.expr)
        if connectors > SCIENCEDIRECT_MAX_CONNECTORS:
            if not isinstance(query.expr, Or):
                raise QueryError(
                    f"ScienceDirect honours at most {SCIENCEDIRECT_MAX_CONNECTORS} AND, OR "
                    f"and NOT in one query, {json.dumps(body)} has {connectors}, and it is "
                    "not an OR to split; shorten it"
                )
            return _Slot(body, None, OR_HALVES, self._halves(query))
        if total is None:
            total = self._client.count(body)
        limit = self._client.MAX_RESULTS
        if total <= limit:
            return _Slot(body, total)
        if query.years and query.years.start < query.years.end:
            parts = [
                self._split(Query(query.expr, query.field, Years(year, year)))
                for year in range(query.years.start, query.years.end + 1)
            ]
            self._must_add_up(body, total, parts, "its years")
            return _Slot(body, total, "by year", parts)
        if connectors < SCIENCEDIRECT_MAX_CONNECTORS:
            pivot = self._pivot(query, total)
            if pivot:
                word, with_count = pivot
                parts = [
                    self._split(_and(query, Term(word)), with_count),
                    self._split(_and(query, Not(Term(word))), total - with_count),
                ]
                return _Slot(body, total, f"on {word!r}", parts)
        if isinstance(query.expr, Or):
            return _Slot(body, total, OR_HALVES, self._halves(query))
        raise QueryError(
            f"ScienceDirect finds {total:,} for {json.dumps(body)}, more than the "
            f"{limit:,} it serves, and none of its splits applies (a year range, a word "
            "whose with and without counts add up, OR clauses); narrow it"
        )

    def _halves(self, query: Query) -> List[_Slot]:
        operands = query.expr.operands
        middle = (len(operands) + 1) // 2
        halves = [operands[:middle], operands[middle:]]
        return [
            self._split(Query(half[0] if len(half) == 1 else Or(half), query.field, query.years))
            for half in halves
        ]

    def _pivot(self, query: Query, total: int) -> Optional[Tuple[str, int]]:
        """The word nearest to halving `query` whose with and without counts add up to `total`."""
        used = _words(query.expr)
        counts = [
            (word, self._client.count(to_sciencedirect(_and(query, Term(word)))))
            for word in SCIENCEDIRECT_PIVOTS
            if word not in used
        ]
        for word, with_count in sorted(counts, key=lambda wc: abs(wc[1] - total / 2)):
            if not 0 < with_count < total:
                continue
            without = self._client.count(to_sciencedirect(_and(query, Not(Term(word)))))
            if with_count + without == total:
                return word, with_count
        return None

    @staticmethod
    def _must_add_up(body: Dict[str, str], total: int, parts: List[_Slot], what: str) -> None:
        found = sum(part.total for part in parts)
        if found != total:
            raise QueryError(
                f"ScienceDirect finds {total:,} for {json.dumps(body)} but {found:,} across "
                f"{what}; the split would not be exact"
            )


SEARCH_SERVICES = {
    "pubmed": PubMedSearchService,
    "semantic_scholar": SemanticScholarSearchService,
    "openalex": OpenAlexSearchService,
    "sciencedirect": ScienceDirectSearchService,
}


def search_services(
    query: Query, providers: Sequence[str], settings: Settings
) -> Dict[str, ArticleSearchService]:
    """One service per provider, each with the query rendered in its dialect.

    Rendering and planning happen here, so a query one provider cannot express
    or split fails before any provider is searched.
    """
    services = {name: SEARCH_SERVICES[name](query, settings) for name in providers}
    for service in services.values():
        service.plan()
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
