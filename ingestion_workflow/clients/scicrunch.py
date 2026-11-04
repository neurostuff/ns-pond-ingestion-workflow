"""SciCrunch's registry (https://scicrunch.org/resources): the data resources, portals and
consortia whose record mentions MRI or PET. Needs an API key, sent as a header so that no
error message or log line holding the URL holds the key."""

from __future__ import annotations

from typing import Iterator

import requests

from ingestion_workflow.utils.http import Pacer, retry_transient

URL = "https://scicrunch.org/api/1/elastic/RIN_Tool_pr/_search"
TYPES = (
    "database", "data set", "data repository", "image repository", "image collection",
    "project portal", "organization portal", "consortium", "biobank", "topical portal",
    "data or information resource",
)
IMAGING = ('MRI OR fMRI OR PET OR "magnetic resonance" OR neuroimaging OR "positron emission" '
           'OR "brain imaging" OR connectome OR DTI OR "diffusion tensor"')
FIELDS = ("item.identifier", "item.name", "item.synonyms", "item.types", "item.keywords",
          "item.description", "item.abbreviations")
PAGE = 500


class SciCrunchClient:
    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError("the SciCrunch registry needs scicrunch_api_key")
        self._session = requests.Session()
        self._session.headers.update({
            "apikey": api_key,
            "User-Agent": "ns-pond-ingestion-workflow "
                          "(https://github.com/neurostuff/ns-pond-ingestion-workflow)"})
        self._pace = Pacer()

    @retry_transient(4, 16)
    def _search(self, start: int) -> dict:
        self._pace.wait(1)
        body = {
            "size": PAGE, "from": start, "sort": [{"item.identifier.aggregate": "asc"}],
            "_source": list(FIELDS),
            "query": {"bool": {"must": [
                {"query_string": {"query": IMAGING, "lenient": True,
                                  "fields": ["item.name", "item.description",
                                             "item.keywords.keyword", "item.synonyms.name"]}},
                {"terms": {"item.types.name.aggregate": list(TYPES)}},
            ]}},
        }
        response = self._session.post(URL, json=body, timeout=120)
        response.raise_for_status()
        return response.json()["hits"]

    def resources(self) -> Iterator[dict]:
        """Each matching resource's `item`."""
        start, total = 0, None
        while total is None or start < total:
            hits = self._search(start)
            total = hits["total"] if isinstance(hits["total"], int) else hits["total"]["value"]
            for hit in hits["hits"]:
                yield hit["_source"]["item"]
            start += PAGE
