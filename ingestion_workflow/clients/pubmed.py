"""Helpers for interacting with the PubMed ID converter and search APIs."""

from __future__ import annotations

from datetime import datetime
import time
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion_workflow.models import Identifier, Identifiers

IDCONV_BATCH_SIZE = 200
PUBMED_REQUEST_LIMIT = 3  # requests per second (polite throttle)
_MIN_REQUEST_INTERVAL = 1 / PUBMED_REQUEST_LIMIT
ESEARCH_MAX_RESULTS = 10_000
ESEARCH_CHUNK_SIZE = 1_000


class PubMedClient:
    """Client for the PubMed/PMC API."""

    ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    BASE_URL = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
    _SUPPORTED_ID_TYPES = {"pmid", "pmcid", "doi"}

    def __init__(
        self, email: str, api_key: str = None, tool: str = "ingestion-workflow"
    ) -> None:
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
            raise ValueError(
                "PubMed ID lookups support only 'pmid', 'pmcid', or 'doi'."
            )

        if any(
            getattr(identifier, id_type) is None for identifier in identifiers
        ):
            raise ValueError(
                f"All identifiers must provide a {id_type.upper()} for lookup."
            )

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
            values[index:index + IDCONV_BATCH_SIZE]
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
