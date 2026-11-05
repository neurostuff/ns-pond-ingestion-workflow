"""re3data's public API (https://www.re3data.org/api/doc): research data repositories.

The repositories listed under neuroscience subjects or found by an imaging query, each read
from its full record.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Dict, Iterator, List

import requests

from ingestion_workflow.utils.http import Pacer, retry_transient

BASE = "https://www.re3data.org/api"
#: The listings searched, as (parameter, value).
LISTINGS = (
    ("subjects[]", "206 Neurosciences"),
    ("subjects[]", "20606 Cognitive Neuroscience and Neuroimaging"),
    ("query", "neuroimaging"),
    ("query", "MRI"),
    ("query", "magnetic resonance"),
    ("query", "PET"),
    ("query", "positron emission"),
    ("query", "brain imaging"),
)
NS = {"r3d": "http://www.re3data.org/schema/2-2"}
REQUEST_INTERVAL = 0.2


class Re3dataClient:
    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers["User-Agent"] = (
            "ns-pond-ingestion-workflow (https://github.com/neurostuff/ns-pond-ingestion-workflow)")
        self._pace = Pacer()

    @retry_transient(4, 16)
    def _get(self, url: str, params=None) -> bytes:
        self._pace.wait(REQUEST_INTERVAL)
        response = self._session.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.content

    def repository_ids(self) -> List[str]:
        ids = set()
        for parameter, value in LISTINGS:
            listing = ET.fromstring(self._get(f"{BASE}/beta/repositories", {parameter: value}))
            ids.update(repo.findtext("id") for repo in listing.iter("repository"))
        return sorted(ids - {None})

    def repository(self, re3data_id: str) -> Dict[str, List[str]]:
        """{tag: [texts]} of the record's repositoryName, additionalName, repositoryURL,
        subject, keyword and description."""
        repo = ET.fromstring(self._get(f"{BASE}/v1/repository/{re3data_id}")).find(
            "r3d:repository", NS)
        if repo is None:
            return {}
        tags = ("repositoryName", "additionalName", "repositoryURL", "subject", "keyword",
                "description")
        return {tag: [re.sub(r"\s+", " ", e.text).strip()
                      for e in repo.findall(f"r3d:{tag}", NS) if e.text and e.text.strip()]
                for tag in tags}

    def repositories(self) -> Iterator[tuple]:
        """(re3data id, record) of each repository listed."""
        for re3data_id in self.repository_ids():
            yield re3data_id, self.repository(re3data_id)
