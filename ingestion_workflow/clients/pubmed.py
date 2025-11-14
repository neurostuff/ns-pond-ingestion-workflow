"""Helpers for interacting with the PubMed ID converter and search APIs."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests
import xmltodict
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.models.metadata import ArticleMetadata, Author

IDCONV_BATCH_SIZE = 200
PUBMED_REQUEST_LIMIT = 3  # requests per second (polite throttle)
_MIN_REQUEST_INTERVAL = 1 / PUBMED_REQUEST_LIMIT
ESEARCH_MAX_RESULTS = 10_000
ESEARCH_CHUNK_SIZE = 1_000


class PubMedClient:
    """Client for the PubMed/PMC API."""

    ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    BASE_URL = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
    _SUPPORTED_ID_TYPES = {"pmid", "pmcid", "doi"}

    def __init__(self, email: str, api_key: str = None, tool: str = "ingestion-workflow") -> None:
        self.email = email
        self.api_key = api_key
        self.tool = tool
        self._session = requests.Session()
        self._last_request = 0.0

    def get_ids(self, id_type: str, identifiers: Identifiers) -> Identifiers:
        """Fetch additional identifiers for the provided collection."""
        self.validate_ids(id_type, identifiers)
        return self.get_ids_by_type(id_type, identifiers)

    def search(
        self,
        query: str,
        *,
        start_year: int = 1990,
    ) -> Identifiers:
        """Search PubMed and return identifiers for matching PMIDs."""

        query = query.strip()
        if not query:
            return Identifiers()

        pmids, total_count = self._collect_esearch_ids(query)

        if total_count >= ESEARCH_MAX_RESULTS:
            pmid_set: set[str] = set()
            current_year = self._current_year()
            for year in range(start_year, current_year + 1):
                yearly_pmids, _ = self._collect_esearch_ids(
                    query,
                    mindate=year,
                    maxdate=year,
                )
                pmid_set.update(yearly_pmids)
            pmids = sorted(pmid_set)

        identifiers = Identifiers([Identifier(pmid=pmid) for pmid in pmids])
        identifiers.set_index("pmid")
        return identifiers

    def validate_ids(self, id_type: str, identifiers: Identifiers) -> None:
        """Ensure the identifiers support the requested lookup type."""
        if id_type not in self._SUPPORTED_ID_TYPES:
            raise ValueError("PubMed ID lookups support only 'pmid', 'pmcid', or 'doi'.")

        if any(getattr(identifier, id_type) is None for identifier in identifiers):
            raise ValueError(f"All identifiers must provide a {id_type.upper()} for lookup.")

    def get_ids_by_type(
        self,
        id_type: str,
        identifiers: Identifiers,
    ) -> Identifiers:
        """Query the ID Converter for the chosen identifier type."""
        values = self._collect_values(id_type, identifiers)
        if not values:
            return identifiers

        batches = [
            values[index : index + IDCONV_BATCH_SIZE]
            for index in range(0, len(values), IDCONV_BATCH_SIZE)
        ]

        for batch in batches:
            params = self._build_params(id_type, batch)
            payload = self._request_idconv(params)
            records = payload.get("records", [])
            self._apply_records(id_type, identifiers, records)

        return identifiers

    def _collect_values(
        self,
        id_type: str,
        identifiers: Identifiers,
    ) -> list[str]:
        return [
            str(getattr(identifier, id_type)).strip()
            for identifier in identifiers
            if getattr(identifier, id_type)
        ]

    def _build_params(
        self,
        id_type: str,
        batch: Iterable[str],
    ) -> List[Tuple[str, str]]:
        params: List[Tuple[str, str]] = [
            ("email", self.email),
            ("format", "json"),
            ("ids", ",".join(batch)),
        ]

        if id_type in self._SUPPORTED_ID_TYPES:
            params.append(("idtype", id_type))

        params.append(("tool", self.tool))

        if self.api_key:
            params.append(("api_key", self.api_key))

        return params

    def _apply_records(
        self,
        id_type: str,
        identifiers: Identifiers,
        records: Iterable[Dict[str, object]],
    ) -> None:
        for record in records or []:
            if not isinstance(record, dict):
                continue
            if record.get("status") == "error" or record.get("error"):
                continue

            requested_id = record.get("requested-id")
            if not requested_id:
                continue

            identifier = identifiers.lookup(str(requested_id), key=id_type)
            if identifier is None and id_type == "pmcid":
                lookup_value = str(requested_id)
                if not lookup_value.upper().startswith("PMC"):
                    lookup_value = f"PMC{lookup_value}"
                identifier = identifiers.lookup(lookup_value, key="pmcid")

            if identifier is None:
                continue

            pmid = record.get("pmid")
            if pmid and identifier.pmid is None:
                identifier.pmid = str(pmid)

            pmcid = record.get("pmcid")
            if pmcid and identifier.pmcid is None:
                identifier.pmcid = str(pmcid)

            doi = record.get("doi")
            if doi and identifier.doi is None:
                identifier.doi = str(doi)

            self._store_extra_metadata(identifier, record)

    @staticmethod
    def _store_extra_metadata(identifier, record: Dict[str, object]) -> None:
        extra_fields = ("mid", "aiid", "version", "release-date")

        for field in extra_fields:
            value = record.get(field)
            if not value:
                continue

            if identifier.other_ids is None:
                identifier.other_ids = {}

            identifier.other_ids.setdefault(field, str(value))

    def _rate_limit_sleep(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request = time.monotonic()

    def _collect_esearch_ids(
        self,
        query: str,
        *,
        mindate: Optional[int] = None,
        maxdate: Optional[int] = None,
        retmax: int = ESEARCH_CHUNK_SIZE,
    ) -> Tuple[List[str], int]:
        pmids: List[str] = []
        retstart = 0
        total_count = 0

        while True:
            payload = self._esearch(
                query,
                retstart=retstart,
                retmax=retmax,
                mindate=mindate,
                maxdate=maxdate,
            )

            esearch_result = payload.get("esearchresult", {})
            count_value = esearch_result.get("count", 0)
            try:
                total_count = int(count_value)
            except (TypeError, ValueError):
                total_count = 0

            id_list = esearch_result.get("idlist", []) or []
            pmids.extend(str(pmid) for pmid in id_list)
            retstart += len(id_list)

            if retstart >= total_count or not id_list:
                break

        return pmids, total_count

    def _esearch(
        self,
        query: str,
        *,
        retstart: int,
        retmax: int,
        mindate: Optional[int] = None,
        maxdate: Optional[int] = None,
    ) -> Dict[str, object]:
        params: List[Tuple[str, str]] = [
            ("db", "pubmed"),
            ("term", query),
            ("retmode", "json"),
            ("retstart", str(retstart)),
            ("retmax", str(retmax)),
            ("email", self.email),
            ("tool", self.tool),
        ]

        if self.api_key:
            params.append(("api_key", self.api_key))

        if mindate is not None:
            params.append(("mindate", str(mindate)))
        if maxdate is not None:
            params.append(("maxdate", str(maxdate)))
        if mindate is not None or maxdate is not None:
            params.append(("datetype", "pdat"))

        return self._request_json(self.ESEARCH_URL, params)

    def _current_year(self) -> int:
        return datetime.utcnow().year

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=8),
    )
    def _request_idconv(
        self,
        params: Sequence[Tuple[str, str]],
    ) -> Dict[str, object]:
        return self._request_json(self.BASE_URL, params)

    def _request_json(
        self,
        url: str,
        params: Mapping[str, str] | Sequence[Tuple[str, str]],
    ) -> Dict[str, object]:
        self._rate_limit_sleep()
        response = self._session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def _request_xml(
        self,
        url: str,
        params: Mapping[str, str] | Sequence[Tuple[str, str]],
    ) -> Dict[str, Any]:
        self._rate_limit_sleep()
        response = self._session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return xmltodict.parse(response.text)

    def get_metadata(self, identifiers: List[Identifier]) -> Dict[str, ArticleMetadata]:
        """
        Fetch article metadata for batch of identifiers using efetch.

        Parameters
        ----------
        identifiers : list of Identifier
            Identifiers with PMIDs to fetch metadata for

        Returns
        -------
        dict
            Mapping from slug to ArticleMetadata
        """
        if not identifiers:
            return {}

        # Filter identifiers with PMIDs
        pmid_map: Dict[str, Identifier] = {}
        for identifier in identifiers:
            if identifier.pmid:
                pmid_map[identifier.pmid] = identifier

        if not pmid_map:
            return {}

        # Batch requests
        results: Dict[str, ArticleMetadata] = {}
        pmids = list(pmid_map.keys())
        batch_size = IDCONV_BATCH_SIZE

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i : i + batch_size]
            try:
                response_data = self._request_efetch(batch)
                self._process_efetch_response(response_data, pmid_map, results)
            except Exception:
                # Continue with other batches on failure
                continue

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=8),
    )
    def _request_efetch(self, pmids: List[str]) -> Dict[str, Any]:
        """Request article metadata from PubMed efetch endpoint."""
        params: List[Tuple[str, str]] = [
            ("db", "pubmed"),
            ("id", ",".join(pmids)),
            ("retmode", "xml"),
            ("email", self.email),
            ("tool", self.tool),
        ]
        if self.api_key:
            params.append(("api_key", self.api_key))

        return self._request_xml(self.EFETCH_URL, params)

    def _process_efetch_response(
        self,
        response_data: Dict[str, Any],
        pmid_map: Dict[str, Identifier],
        results: Dict[str, ArticleMetadata],
    ) -> None:
        article_set = response_data.get("PubmedArticleSet", {})
        articles = article_set.get("PubmedArticle")
        if not articles:
            return
        if not isinstance(articles, list):
            articles = [articles]

        for article in articles:
            if not isinstance(article, dict):
                continue
            pmid = self._extract_pmid(article)
            if not pmid:
                continue
            identifier = pmid_map.get(pmid)
            if not identifier:
                continue
            metadata = self._build_article_metadata(article)
            if metadata:
                results[identifier.slug] = metadata

    @staticmethod
    def _extract_pmid(article: Mapping[str, Any]) -> Optional[str]:
        citation = article.get("MedlineCitation")
        if isinstance(citation, dict):
            pmid = citation.get("PMID")
            if isinstance(pmid, dict):
                pmid = pmid.get("#text")
            if pmid:
                return str(pmid)
        return None

    def _build_article_metadata(self, article: Mapping[str, Any]) -> Optional[ArticleMetadata]:
        citation = article.get("MedlineCitation")
        if not isinstance(citation, dict):
            return None
        article_data = citation.get("Article")
        if not isinstance(article_data, dict):
            return None

        title = self._text_from(article_data.get("ArticleTitle")) or ""
        abstract = self._parse_abstract(article_data.get("Abstract"))
        authors = self._parse_authors(article_data.get("AuthorList"))
        journal = self._text_from(
            article_data.get("Journal", {}).get("Title") if isinstance(article_data.get("Journal"), dict) else None
        )
        publication_year = self._extract_publication_year(article_data, citation)
        keywords = self._parse_keywords(citation)

        return ArticleMetadata(
            title=title,
            authors=authors,
            abstract=abstract,
            journal=journal,
            publication_year=publication_year,
            keywords=keywords,
            source="pubmed",
            raw_metadata={"pubmed": article},
        )

    def _parse_authors(self, author_list: Any) -> List[Author]:
        if not author_list:
            return []
        if isinstance(author_list, dict):
            entries = author_list.get("Author", author_list)
        else:
            entries = author_list
        authors: List[Author] = []
        for entry in self._ensure_list(entries):
            if not isinstance(entry, Mapping):
                continue
            if entry.get("CollectiveName"):
                name = self._text_from(entry.get("CollectiveName"))
            else:
                fore = entry.get("ForeName") or entry.get("Initials")
                last = entry.get("LastName")
                name = " ".join(part for part in [fore, last] if part)
            if not name:
                continue

            affiliation = None
            affiliation_info = entry.get("AffiliationInfo")
            if affiliation_info:
                info_entry = self._ensure_list(affiliation_info)[0]
                if isinstance(info_entry, Mapping):
                    affiliation = info_entry.get("Affiliation")
                elif isinstance(info_entry, str):
                    affiliation = info_entry

            authors.append(Author(name=name, affiliation=affiliation))
        return authors

    def _parse_abstract(self, abstract_section: Any) -> Optional[str]:
        if not abstract_section or not isinstance(abstract_section, Mapping):
            return None
        texts = abstract_section.get("AbstractText")
        parts: List[str] = []
        for item in self._ensure_list(texts):
            label = None
            text_value = ""
            if isinstance(item, Mapping):
                label = item.get("@Label")
                text_value = self._text_from(item.get("#text") or item)
            else:
                text_value = self._text_from(item)
            if text_value:
                if label:
                    parts.append(f"{label}: {text_value}")
                else:
                    parts.append(text_value)
        joined = "\n\n".join(part.strip() for part in parts if part.strip())
        return joined or None

    def _parse_keywords(self, citation: Mapping[str, Any]) -> List[str]:
        keyword_section = citation.get("KeywordList")
        if not keyword_section:
            return []
        keywords: List[str] = []
        for entry in self._ensure_list(keyword_section):
            if isinstance(entry, Mapping):
                keyword_values = entry.get("Keyword", entry)
            else:
                keyword_values = entry
            for keyword in self._ensure_list(keyword_values):
                text = self._text_from(keyword)
                if text and text not in keywords:
                    keywords.append(text)
        return keywords

    def _extract_publication_year(
        self,
        article_data: Mapping[str, Any],
        citation: Mapping[str, Any],
    ) -> Optional[int]:
        journal = article_data.get("Journal")
        if isinstance(journal, dict):
            issue = journal.get("JournalIssue")
            if isinstance(issue, dict):
                candidate = self._year_from_pubdate(issue.get("PubDate"))
                if candidate:
                    return candidate
        candidate = self._year_from_pubdate(article_data.get("ArticleDate"))
        if candidate:
            return candidate
        candidate = self._year_from_pubdate(citation.get("DateCompleted"))
        return candidate

    def _year_from_pubdate(self, data: Any) -> Optional[int]:
        if not data:
            return None
        for entry in self._ensure_list(data):
            if isinstance(entry, Mapping):
                year = entry.get("Year") or entry.get("#text")
                if year:
                    try:
                        return int(str(year)[:4])
                    except (TypeError, ValueError):
                        pass
                medline_date = entry.get("MedlineDate")
                if medline_date:
                    year_match = self._extract_year_from_string(medline_date)
                    if year_match:
                        return year_match
            elif isinstance(entry, str):
                year_match = self._extract_year_from_string(entry)
                if year_match:
                    return year_match
        return None

    @staticmethod
    def _extract_year_from_string(value: str) -> Optional[int]:
        import re

        match = re.search(r"(19|20)\d{2}", value or "")
        if match:
            return int(match.group(0))
        return None

    @staticmethod
    def _ensure_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    @staticmethod
    def _text_from(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, Mapping):
            if "#text" in value:
                return str(value["#text"])
            return " ".join(
                part
                for key, part in value.items()
                if not key.startswith("@") and isinstance(part, str)
            )
        return str(value)
