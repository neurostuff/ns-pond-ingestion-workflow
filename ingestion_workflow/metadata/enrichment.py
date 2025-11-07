"""Metadata enrichment helpers backed by external APIs."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import httpx
from lxml import etree

from ingestion_workflow.config import Settings, get_settings
from ingestion_workflow.models import Identifier

logger = logging.getLogger(__name__)

SEMANTIC_SCHOLAR_ENDPOINT = "https://api.semanticscholar.org/graph/v1/paper/"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
OPENALEX_ENDPOINT = "https://api.openalex.org/works/"


class MetadataProviderError(RuntimeError):
    """Raised when a metadata provider encounters a fatal error."""


class BaseMetadataProvider:
    """Base helper for metadata clients."""

    name = "base"

    def fetch(self, identifier: Identifier) -> Optional[Dict[str, object]]:
        raise NotImplementedError

    @staticmethod
    def _first_identifier(identifier: Identifier, *fields: str) -> Optional[str]:
        for field in fields:
            value = getattr(identifier, field, None)
            if value:
                return value
        for other in identifier.other_ids:
            if other:
                return other
        return None


class SemanticScholarProvider(BaseMetadataProvider):
    name = "semantic_scholar"

    def __init__(self, api_key: Optional[str], timeout: float = 30.0) -> None:
        self.api_key = api_key
        self.timeout = timeout

    def fetch(self, identifier: Identifier) -> Optional[Dict[str, object]]:
        paper_id = None
        if identifier.doi:
            paper_id = f"DOI:{identifier.doi}"
        elif identifier.pmid:
            paper_id = f"PMID:{identifier.pmid}"
        elif identifier.other_ids:
            paper_id = identifier.other_ids[0]
        if not paper_id:
            return None
        params = {
            "fields": "title,abstract,venue,year,authors,externalIds,publicationVenue,tldr",
        }
        headers = {}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        response = httpx.get(
            f"{SEMANTIC_SCHOLAR_ENDPOINT}{paper_id}",
            params=params,
            headers=headers,
            timeout=self.timeout,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        authors = "; ".join(author.get("name", "") for author in data.get("authors", []))
        ext_ids = data.get("externalIds", {}) or {}
        return {
            "title": data.get("title"),
            "abstract": data.get("abstract") or (data.get("tldr") or {}).get("text"),
            "journal": data.get("venue") or (data.get("publicationVenue") or {}).get("name"),
            "publication_year": data.get("year"),
            "authors": authors,
            "pmid": data.get("pubmedId") or ext_ids.get("PubMed"),
            "pmcid": ext_ids.get("PubMed Central"),
            "doi": data.get("doi") or ext_ids.get("DOI"),
            "external_metadata": {"semantic_scholar": data},
        }


class PubMedProvider(BaseMetadataProvider):
    name = "pubmed"

    def __init__(
        self,
        email: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.email = email
        self.api_key = api_key
        self.timeout = timeout

    def fetch(self, identifier: Identifier) -> Optional[Dict[str, object]]:
        pmid = identifier.pmid
        if not pmid:
            return None
        params = {
            "db": "pubmed",
            "retmode": "xml",
            "id": pmid,
        }
        if self.email:
            params["email"] = self.email
        if self.api_key:
            params["api_key"] = self.api_key
        response = httpx.get(PUBMED_EFETCH_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        root = etree.fromstring(response.content)
        article_nodes = root.xpath(".//PubmedArticle")
        if not article_nodes:
            return None
        article = article_nodes[0]

        def text(xpath: str) -> Optional[str]:
            matches = article.xpath(xpath)
            if not matches:
                return None
            values = []
            for match in matches:
                if isinstance(match, etree._Element):
                    values.append(" ".join(match.itertext()).strip())
                else:
                    values.append(str(match).strip())
            values = [value for value in values if value]
            return values[0] if values else None

        title = text(".//ArticleTitle")
        abstract = text(".//Abstract")
        journal = text(".//Journal/Title")
        year = text(".//PubDate/Year") or text(".//ArticleDate/Year")
        authors = []
        for node in article.xpath(".//Author"):
            last = text(".//LastName")
            first = text(".//ForeName")
            parts = [part for part in (first, last) if part]
            if parts:
                authors.append(" ".join(parts))
        article_ids = article.xpath(".//ArticleIdList/ArticleId")
        doi = None
        pmcid = None
        for node in article_ids:
            id_type = node.attrib.get("IdType")
            value = "".join(node.itertext()).strip()
            if id_type == "doi":
                doi = value
            elif id_type == "pmc":
                pmcid = value
        metadata = {
            "title": title,
            "abstract": abstract,
            "journal": journal,
            "publication_year": int(year) if year and year.isdigit() else None,
            "authors": "; ".join(authors),
            "doi": doi,
            "pmid": pmid,
            "pmcid": pmcid,
        }
        metadata["external_metadata"] = {"pubmed": json.loads(json.dumps(metadata))}
        return metadata


class OpenAlexProvider(BaseMetadataProvider):
    name = "openalex"

    def __init__(self, email: str, timeout: float = 30.0) -> None:
        self.email = email
        self.timeout = timeout

    def fetch(self, identifier: Identifier) -> Optional[Dict[str, object]]:
        doi = identifier.doi
        if not doi:
            return None
        response = httpx.get(
            f"{OPENALEX_ENDPOINT}doi:{doi}",
            params={"mailto": self.email},
            timeout=self.timeout,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        authors = "; ".join(
            auth.get("author", {}).get("display_name", "")
            for auth in data.get("authorships", [])
        )
        ids = data.get("ids", {}) or {}
        pmid_url = ids.get("pmid")
        pmid = None
        if pmid_url:
            pmid = pmid_url.rstrip("/").split("/")[-1]
        metadata = {
            "title": data.get("display_name"),
            "abstract": _openalex_abstract(data.get("abstract_inverted_index")),
            "publication_year": data.get("publication_year"),
            "journal": (data.get("primary_location") or {}).get("source", {}).get(
                "display_name"
            ),
            "authors": authors,
            "pmid": pmid,
            "doi": doi,
            "external_metadata": {"openalex": data},
        }
        return metadata


def _openalex_abstract(indexed: Optional[dict]) -> Optional[str]:
    if not indexed:
        return None
    words = []
    for word, positions in indexed.items():
        for position in positions:
            words.append((position, word))
    if not words:
        return None
    ordered = [word for _, word in sorted(words)]
    return " ".join(ordered)


class MetadataEnricher:
    """Fetch supplemental metadata from external providers."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self._providers: dict[str, BaseMetadataProvider] = {}

    def enrich(self, identifier: Identifier, metadata: dict) -> dict:
        merged = {**metadata}
        for provider_name in self.settings.metadata_provider_order:
            provider = self._get_provider(provider_name)
            if not provider:
                continue
            try:
                result = provider.fetch(identifier)
            except (httpx.HTTPError, MetadataProviderError) as exc:
                logger.debug(
                    "Metadata provider failed",
                    extra={"provider": provider_name, "error": str(exc)},
                )
                continue
            if not result:
                continue
            merged = self._merge_metadata(merged, result, provider_name)
        return merged

    def _merge_metadata(self, base: dict, new_data: dict, provider: str) -> dict:
        updated = {**base}
        for key, value in new_data.items():
            if key == "external_metadata":
                existing = updated.setdefault("external_metadata", {})
                provider_blob = value.get(provider) if isinstance(value, dict) else value
                if provider_blob:
                    existing[provider] = provider_blob
                continue
            if value in (None, "", []):
                continue
            current = updated.get(key)
            if not current:
                updated[key] = value
        return updated

    def _get_provider(self, name: str) -> Optional[BaseMetadataProvider]:
        if name in self._providers:
            return self._providers[name]
        provider: Optional[BaseMetadataProvider] = None
        if name == "semantic_scholar":
            provider = SemanticScholarProvider(self.settings.semantic_scholar_api_key)
        elif name == "pubmed":
            provider = PubMedProvider(
                email=self.settings.pubmed_email,
                api_key=self.settings.pubmed_api_key,
            )
        elif name == "openalex":
            provider = OpenAlexProvider(email=self.settings.openalex_email)
        if provider:
            self._providers[name] = provider
        return provider
