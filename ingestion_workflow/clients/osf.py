"""OSF's public API (https://developer.osf.io): whether a project holds NIfTI files.

A guid names a project, a component, a registration or a single file. A project's files
are listed provider by provider (osfstorage and any add-on), folder by folder and page by
page, with its components after it, until a `.nii` or `.nii.gz` turns up or
`MAX_REQUESTS` is spent.
"""

from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests

from ingestion_workflow.utils.http import Pacer, retry_transient, transient, wait_retry_after

#: A personal access token, read from the environment only, as the neurostore token is.
#: Anonymous requests are throttled per hour; a token's limit is per day.
TOKEN_ENV = "OSF_TOKEN"
OSF_REQUEST_LIMIT = 2
#: The most requests one guid's listing may take: 100 entries a page.
MAX_REQUESTS = 60
PAGE_SIZE = 100
#: The longest a Retry-After is waited out before the request counts as failed.
_MAX_RETRY_AFTER = 120
NIFTI = (".nii", ".nii.gz")
#: How many NIfTI paths the answer keeps as what was seen.
_KEPT = 5


@dataclass
class Listing:
    """What OSF showed of one guid."""

    #: True: a NIfTI file was seen. False: every file was listed and none is one.
    #: None: nothing can be said (`reason`).
    has_nifti: Optional[bool]
    reason: Optional[str] = None
    #: The referent's type: nodes, registrations, files, ...
    kind: Optional[str] = None
    requests: int = 0
    files: int = 0
    nifti: List[str] = field(default_factory=list)
    #: The listings that failed: {href: error}. A transient one is asked again later.
    failed: Dict[str, str] = field(default_factory=dict)
    transient: bool = False

    def seen(self) -> dict:
        return {"kind": self.kind, "requests": self.requests, "files": self.files,
                "nifti": self.nifti, **({"failed": self.failed} if self.failed else {})}


def _is_nifti(name: str) -> bool:
    return name.lower().endswith(NIFTI)


class _Refused(Exception):
    """OSF answered, and the answer is that the record cannot be read."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status


class OsfClient:
    BASE_URL = "https://api.osf.io/v2/"

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/vnd.api+json",
            "User-Agent": "ns-pond-ingestion-workflow "
                          "(https://github.com/neurostuff/ns-pond-ingestion-workflow)"})
        token = (os.environ.get(TOKEN_ENV) or "").strip()
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        self._pace = Pacer()

    @retry_transient(4, 16, wait=wait_retry_after(_MAX_RETRY_AFTER, 16))
    def _get(self, url: str, params: Optional[dict] = None) -> dict:
        self._pace.wait(1 / OSF_REQUEST_LIMIT)
        response = self._session.get(url, params=params, timeout=30)
        if response.status_code in (401, 403, 404, 410):
            raise _Refused(response.status_code, _detail(response))
        response.raise_for_status()
        return response.json()

    def listing(self, guid: str, view_only: Optional[str] = None) -> Listing:
        """What OSF shows of `guid`. A guid OSF refuses (private, gone) is no failure: its
        listing says why it says nothing. Raises when OSF is down or busy for the guid
        itself; a listing under it that fails is noted in `failed`."""
        base = {"view_only": view_only} if view_only else {}
        out = Listing(has_nifti=None)
        try:
            referent = self._get(f"{self.BASE_URL}guids/{guid}/", base)["data"]
        except _Refused as exc:
            out.reason = _refusal(exc)
            return out
        finally:
            out.requests += 1
        out.kind = referent.get("type")
        if out.kind == "files":
            name = (referent.get("attributes") or {}).get("name") or ""
            out.files = 1
            out.has_nifti = _is_nifti(name)
            out.nifti = [name] if out.has_nifti else []
            return out
        relations = referent.get("relationships") or {}
        if "files" not in relations:
            out.reason = f"an OSF {out.kind} holds no files"
            return out
        queue = deque(filter(None, (_related(relations, "files"),
                                    _related(relations, "children"))))
        while queue and not out.nifti:
            url = queue.popleft()
            while url and not out.nifti:
                if out.requests >= MAX_REQUESTS:
                    out.reason = f"listing stopped after {MAX_REQUESTS} requests"
                    return out
                try:
                    page = self._get(url, {**base, "page[size]": PAGE_SIZE})
                except Exception as exc:  # noqa: BLE001 - one listing; the rest still answer
                    out.failed[url] = _refusal(exc) if isinstance(exc, _Refused) else str(exc)
                    out.transient = out.transient or transient(exc)
                    break
                finally:
                    out.requests += 1
                for item in page.get("data") or []:
                    self._take(item, out, queue)
                url = (page.get("links") or {}).get("next")
        if out.nifti:
            out.has_nifti = True
        elif out.failed:
            out.reason = f"{len(out.failed)} listing(s) failed"
        else:
            out.has_nifti = False
        return out

    @staticmethod
    def _take(item: dict, out: Listing, queue: deque) -> None:
        attributes = item.get("attributes") or {}
        relations = item.get("relationships") or {}
        if item.get("type") in ("nodes", "registrations"):  # a component
            queue.extend(filter(None, (_related(relations, "files"),
                                       _related(relations, "children"))))
        elif attributes.get("kind") == "file":
            out.files += 1
            if _is_nifti(attributes.get("name") or ""):
                if len(out.nifti) < _KEPT:
                    out.nifti.append(attributes.get("materialized_path") or attributes["name"])
        elif attributes.get("kind") == "folder" or item.get("type") == "files":
            # a folder, or a storage provider at the top of the files listing
            if href := _related(relations, "files"):
                queue.append(href)


def _related(relations: dict, name: str) -> Optional[str]:
    return (((relations.get(name) or {}).get("links") or {}).get("related") or {}).get("href")


def _detail(response) -> str:
    try:
        errors = response.json().get("errors") or []
        return "; ".join(e.get("detail", "") for e in errors) or response.reason
    except ValueError:
        return response.reason or str(response.status_code)


def _refusal(exc: _Refused) -> str:
    what = {401: "private", 403: "private", 404: "not found", 410: "deleted"}[exc.status]
    return f"{what} on OSF ({exc.status}: {exc})"
