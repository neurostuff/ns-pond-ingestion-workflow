"""Search services for the ingestion workflow, and the overlap of what they find."""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import Catalog
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.sciencedirect import (
    SCIENCEDIRECT_MAX_RESULTS,
    SCIENCEDIRECT_PAGE_SIZE,
    ScienceDirectClient,
)
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.services.query_dialect import (
    Or,
    Query,
    QueryError,
    Years,
    to_openalex,
    to_pubmed,
    to_sciencedirect,
    to_semantic_scholar,
)


class ArticleSearchService:
    """Base class for workflow search services; `rendered` is the provider's query.

    A provider that has to split a query records each sub-query it ran, with
    how many results it reported, in `parts`.
    """

    rendered: str

    def __init__(self, query: Query) -> None:
        self.query = query
        self.parts: List[Tuple[str, int]] = []

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
        # The client splits a result set too large for one ESearch by year from here.
        self.start_year = query.years.start if query.years else 1990
        self._client = client or self._create_client()

    def search(self) -> Identifiers:
        return self._client.search(self.rendered, start_year=self.start_year)

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
        self._client = OpenAlexClient(settings.openalex_email, settings.openalex_api_key)

    def search(self) -> Identifiers:
        return self._client.search(self.rendered)


class ScienceDirectSearchService(ArticleSearchService):
    """Articles ScienceDirect matches in full text (or title), split to be paged to the end.

    The API serves results only up to a fixed offset. A query finding more is
    run as its top-level OR clauses, and a clause still finding more as one
    query per year of its range; the union of the parts is the whole result.
    """

    def __init__(
        self,
        query: Query,
        settings: Settings,
        *,
        show: int = SCIENCEDIRECT_PAGE_SIZE,
        max_results: int = SCIENCEDIRECT_MAX_RESULTS,
    ) -> None:
        super().__init__(query)
        self.rendered = json.dumps(to_sciencedirect(query))
        if not settings.elsevier_api_key:
            raise ValueError("elsevier_api_key must be configured to search ScienceDirect")
        self._client = ScienceDirectClient(settings.elsevier_api_key)
        self.show = show
        self.max_results = max_results

    def search(self) -> Identifiers:
        self.parts = []
        return Identifiers(self._collect(self.query))

    def _collect(self, query: Query) -> List[Identifier]:
        body = to_sciencedirect(query)
        total, found = self._client.page(body, 0, self.show)
        if total <= self.max_results:
            for offset in range(self.show, total, self.show):
                found += self._client.page(body, offset, self.show)[1]
            self.parts.append((json.dumps(body), total))
            return found
        if isinstance(query.expr, Or):
            pieces = [Query(clause, query.field, query.years) for clause in query.expr.operands]
        elif query.years and query.years.start < query.years.end:
            pieces = [
                Query(query.expr, query.field, Years(year, year))
                for year in range(query.years.start, query.years.end + 1)
            ]
        else:
            raise QueryError(
                f"ScienceDirect finds {total:,} for {json.dumps(body)}, more than the "
                f"{self.max_results:,} it serves, and it has no OR clauses or year range "
                "left to split into; narrow it"
            )
        return [record for piece in pieces for record in self._collect(piece)]


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

    Rendering happens here, so a query one dialect cannot express fails before
    any provider is searched.
    """
    return {name: SEARCH_SERVICES[name](query, settings) for name in providers}


def resolve_hits(
    catalog: Catalog, searched: Dict[str, List[Identifier]]
) -> Dict[str, List[Optional[str]]]:
    """The catalog article each hit is now, in order; None where there is none."""
    resolved: Dict[str, List[Optional[str]]] = {}
    for provider, records in searched.items():
        refs = [catalog.resolve(record) for record in records]
        resolved[provider] = [ref.id if ref else None for ref in refs]
    return resolved


@dataclass(frozen=True)
class SearchReport:
    """What each provider found, counted in catalog articles.

    Built from the hits resolved before they were registered and again after
    registration and enrichment, so two records are one article exactly when
    the catalog holds them as one.
    """

    articles: Dict[str, Set[str]]
    known: Set[str]
    without_id: Dict[str, int]

    @classmethod
    def build(
        cls,
        before: Dict[str, List[Optional[str]]],
        after: Dict[str, List[Optional[str]]],
    ) -> "SearchReport":
        articles = {p: {a for a in ids if a is not None} for p, ids in after.items()}
        known = {
            a
            for p in after
            for was, a in zip(before[p], after[p])
            if was is not None and a is not None
        }
        without_id = {p: sum(1 for a in ids if a is None) for p, ids in after.items()}
        return cls(articles, known, without_id)

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
