"""Identifier gathering utilities for the ingestion workflow."""
from __future__ import annotations

import logging
from typing import List, Optional

import httpx

from ingestion_workflow.config import Settings

logger = logging.getLogger(__name__)


class PubMedGatherer:
    """Retrieve PMIDs from PubMed based on a search query."""

    BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    def __init__(self, settings: Settings):
        self.settings = settings

    def search(
        self,
        query: str,
        max_results: Optional[int] = None,
    ) -> List[str]:
        """
        Execute a PubMed query and return matched PMIDs.

        Parameters
        ----------
        query:
            Entrez-formatted search string.
        max_results:
            Optional maximum number of PMIDs to retrieve. Defaults to the
            configured `pubmed_max_results`.
        """
        limit = max_results or self.settings.pubmed_max_results
        retmax = self.settings.pubmed_retmax
        params = {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retmax": retmax,
        }
        if self.settings.pubmed_email:
            params["email"] = self.settings.pubmed_email
        if self.settings.pubmed_api_key:
            params["api_key"] = self.settings.pubmed_api_key

        pmids: List[str] = []
        retstart = 0

        with httpx.Client(timeout=30.0) as client:
            while True:
                params["retstart"] = retstart
                response = client.get(self.BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()
                result = data.get("esearchresult", {})
                ids = result.get("idlist", [])

                pmids.extend(ids)
                logger.info(
                    "Retrieved PubMed page",
                    extra={
                        "retstart": retstart,
                        "count": result.get("count"),
                        "accumulated": len(pmids),
                    },
                )

                if len(pmids) >= limit:
                    break

                if not ids:
                    break

                count = int(result.get("count", 0))
                retstart += len(ids)
                if retstart >= count:
                    break

                remaining = limit - len(pmids)
                params["retmax"] = min(retmax, remaining)
                if params["retmax"] <= 0:
                    break

        if len(pmids) > limit:
            pmids = pmids[:limit]
        return pmids


__all__ = ["PubMedGatherer"]
