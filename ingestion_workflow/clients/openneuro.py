"""OpenNeuro's public GraphQL API: which modalities a dataset's snapshot holds.

OpenNeuro hosts EEG, MEG and iEEG datasets as well as MRI and PET ones. A snapshot's
summary names its modalities, and BIDS stores MRI and PET images as NIfTI (`.nii`,
`.nii.gz`), so one of those two among them is a NIfTI file in the dataset; EEG, MEG and
iEEG recordings are not NIfTI. One request per dataset: a snapshot's full file list can
run to hundreds of thousands of entries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import requests

from ingestion_workflow.utils.http import Pacer, retry_transient, wait_retry_after

OPENNEURO_REQUEST_LIMIT = 5
_MAX_RETRY_AFTER = 120
#: The BIDS modalities whose images are NIfTI files.
NIFTI_MODALITIES = ("mri", "pet")

_SUMMARY = "tag summary { modalities secondaryModalities primaryModality }"
_LATEST = "query($id: ID!) { dataset(id: $id) { id latestSnapshot { %s } } }" % _SUMMARY
_TAGGED = ("query($id: ID!, $tag: String!) { snapshot(datasetId: $id, tag: $tag) { %s } }"
           % _SUMMARY)


@dataclass
class Snapshot:
    """What OpenNeuro says of one dataset's snapshot."""

    has_nifti: Optional[bool]
    reason: Optional[str] = None
    tag: Optional[str] = None
    modalities: List[str] = field(default_factory=list)

    def seen(self) -> dict:
        return {"tag": self.tag, "modalities": self.modalities}


class OpenNeuroClient:
    URL = "https://openneuro.org/crn/graphql"

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "ns-pond-ingestion-workflow "
                          "(https://github.com/neurostuff/ns-pond-ingestion-workflow)"})
        self._pace = Pacer()

    @retry_transient(4, 16, wait=wait_retry_after(_MAX_RETRY_AFTER, 16))
    def _query(self, query: str, variables: dict) -> dict:
        self._pace.wait(1 / OPENNEURO_REQUEST_LIMIT)
        response = self._session.post(self.URL, json={"query": query, "variables": variables},
                                      timeout=30)
        response.raise_for_status()
        return response.json()

    def snapshot(self, dataset: str, tag: Optional[str] = None) -> Snapshot:
        """The modalities of the dataset's snapshot `tag`, or of its latest. A dataset
        OpenNeuro does not show (missing, private, no snapshot) says nothing: its answer
        says why. Raises when OpenNeuro is down or busy."""
        if tag:
            answer = self._query(_TAGGED, {"id": dataset, "tag": tag})
            found = (answer.get("data") or {}).get("snapshot")
        else:
            answer = self._query(_LATEST, {"id": dataset})
            found = ((answer.get("data") or {}).get("dataset") or {}).get("latestSnapshot")
            if found is None and not answer.get("errors"):
                return Snapshot(None, "no public snapshot on OpenNeuro")
        if found is None:
            errors = "; ".join(e.get("message", "") for e in answer.get("errors") or [])
            return Snapshot(None, f"OpenNeuro: {errors or 'no such snapshot'}")
        summary = found.get("summary")
        if not summary:
            return Snapshot(None, "OpenNeuro holds no summary of the snapshot", found.get("tag"))
        modalities = sorted({m.lower() for m in summary.get("modalities") or [] if m})
        if not modalities:
            return Snapshot(None, "OpenNeuro's summary names no modality", found.get("tag"))
        return Snapshot(any(m in NIFTI_MODALITIES for m in modalities), None, found.get("tag"),
                        modalities)
