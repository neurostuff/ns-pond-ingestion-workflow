"""Search services for the ingestion workflow."""

from __future__ import annotations

from typing import Optional

from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifiers


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
