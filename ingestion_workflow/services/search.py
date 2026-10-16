"""Search services for the ingestion workflow, and the overlap of what they find."""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import Catalog
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.sciencedirect import ScienceDirectClient
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

    A provider that has to split a query lists each sub-query, with how many
    results it reported, in `parts`.
    """

    rendered: str

    def __init__(self, query: Query) -> None:
        self.query = query
        self.parts: List[Tuple[str, int]] = []

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
        self._client = OpenAlexClient(settings.openalex_email, settings.openalex_api_key)

    def search(self) -> Identifiers:
        return self._client.search(self.rendered)


class ScienceDirectSearchService(ArticleSearchService):
    """Articles ScienceDirect matches in full text (or title), split to be paged to the end.

    The API serves a query's results only up to a fixed offset. `plan` runs a
    query finding more as its top-level OR clauses, and a clause still finding
    more as one query per year of its range; the union of the parts is the
    whole result.
    """

    def __init__(self, query: Query, settings: Settings) -> None:
        super().__init__(query)
        self.rendered = json.dumps(to_sciencedirect(query))
        if not settings.elsevier_api_key:
            raise ValueError("elsevier_api_key must be configured to search ScienceDirect")
        self._client = ScienceDirectClient(settings.elsevier_api_key)
        # None until `plan`; searching unplanned fails rather than finding nothing
        self._planned: Optional[List[Tuple[Dict[str, str], int, List[Identifier]]]] = None

    def plan(self) -> None:
        """Count the query, splitting it until every part can be paged to its end."""
        self._planned = self._split(self.query)
        self.parts = [(json.dumps(body), total) for body, total, _ in self._planned]

    def search(self) -> Identifiers:
        size = self._client.PAGE_SIZE
        found: Dict[str, Identifier] = {}
        bare: List[Identifier] = []
        for body, total, first in self._planned:
            records = list(first)
            for offset in range(size, total, size):
                records += self._client.page(body, offset)[1]
            for record in records:
                key = record.doi or (record.other_ids or {}).get("pii")
                if key:
                    # parts overlap: an OR's clauses find the same article
                    found.setdefault(key, record)
                else:
                    bare.append(record)
        return Identifiers([*found.values(), *bare])

    def _split(self, query: Query) -> List[Tuple[Dict[str, str], int, List[Identifier]]]:
        body = to_sciencedirect(query)
        total, first = self._client.page(body, 0)
        limit = self._client.MAX_RESULTS
        if total <= limit:
            return [(body, total, first)]
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
                f"{limit:,} it serves, and it has no OR clauses or year range left to "
                "split into; narrow it"
            )
        return [part for piece in pieces for part in self._split(piece)]


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
