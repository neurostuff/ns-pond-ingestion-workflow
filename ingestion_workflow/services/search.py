"""Search services for the ingestion workflow."""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers


class ArticleSearchService:
    """Base class for workflow search services."""

    def __init__(self, query: str) -> None:
        self.query = query

    def search(self) -> Identifiers:  # pragma: no cover - abstract placeholder
        raise NotImplementedError


class PubMedSearchService(ArticleSearchService):
    """Search wrapper that delegates to the PubMed client."""

    def __init__(
        self,
        query: str,
        settings: Settings,
        *,
        start_year: int = 1990,
        client: Optional[PubMedClient] = None,
    ) -> None:
        super().__init__(query)
        self.settings = settings
        self.start_year = start_year
        self._client = client or self._create_client()

    def search(self) -> Identifiers:
        return self._client.search(self.query, start_year=self.start_year)

    def _create_client(self) -> PubMedClient:
        if not self.settings.pubmed_email:
            raise ValueError("pubmed_email must be configured to perform PubMed searches")

        return PubMedClient(
            email=self.settings.pubmed_email,
            api_key=self.settings.pubmed_api_key or None,
            tool=self.settings.pubmed_tool or "ingestion-workflow",
        )


class OpenAlexSearchService(ArticleSearchService):
    """Journal articles OpenAlex matches in title or abstract, optionally for one year."""

    def __init__(
        self,
        query: str,
        settings: Settings,
        *,
        year: Optional[int] = None,
        client: Optional[OpenAlexClient] = None,
    ) -> None:
        super().__init__(query)
        self.year = year
        self._client = client or OpenAlexClient(settings.openalex_email, settings.openalex_api_key)

    def search(self) -> Identifiers:
        return self._client.search(self.query, year=self.year)


def collapse(
    found: Dict[str, List[Identifier]],
) -> Tuple[Dict[str, Set[str]], Dict[str, List[Identifier]]]:
    """The articles each source found, and every record of each article.

    A source's records sharing any DOI, PMID or PMCID count once, and records of
    different sources sharing one are the same article. An article is named by
    one of its ids. A record with none is its own article, named by its OpenAlex id.
    """
    parent: Dict[str, str] = {}

    def find(key: str) -> str:
        while parent.setdefault(key, key) != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def names(record: Identifier) -> List[str]:
        ids = [f"{k}:{getattr(record, k)}" for k in ("doi", "pmid", "pmcid") if getattr(record, k)]
        return ids or [f"openalex:{(record.other_ids or {}).get('openalex')}"]

    for records in found.values():
        for record in records:
            first, *rest = names(record)
            for other in rest:
                parent[find(other)] = find(first)
            find(first)
    members: Dict[str, List[Identifier]] = {}
    for records in found.values():
        for record in records:
            members.setdefault(find(names(record)[0]), []).append(record)
    return (
        {s: {find(names(r)[0]) for r in records} for s, records in found.items()},
        members,
    )
