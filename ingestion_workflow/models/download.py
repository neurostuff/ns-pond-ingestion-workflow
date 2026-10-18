"""Download pipeline data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Mapping, Optional

from .ids import Identifier


class FileType(str, Enum):
    """Enumeration of file types encountered during download/extraction."""

    PDF = "pdf"
    XML = "xml"
    HTML = "html"
    TEXT = "text"
    CSV = "csv"
    JSON = "json"
    BINARY = "binary"


class DownloadSource(str, Enum):
    """Enumeration of download sources."""

    PUBGET = "pubget"
    ELSEVIER = "elsevier"
    ACE = "ace"
    PDF = "pdf"
    PMC = "pmc"
    EUROPEPMC = "europepmc"
    WILEY = "wiley"
    SPRINGER = "springer"


@dataclass
class DownloadedFile:
    """Metadata about an individual downloaded file."""

    file_path: Path
    file_type: FileType
    content_type: str
    source: DownloadSource
    downloaded_at: datetime = field(default_factory=datetime.utcnow)
    md5_hash: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "file_path": str(self.file_path),
            "file_type": self.file_type.value,
            "content_type": self.content_type,
            "source": self.source.value,
            "downloaded_at": self.downloaded_at.isoformat(),
            "md5_hash": self.md5_hash,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "DownloadedFile":
        return cls(
            file_path=Path(str(payload["file_path"])),
            file_type=FileType(str(payload["file_type"])),
            content_type=str(payload["content_type"]),
            source=DownloadSource(str(payload["source"])),
            downloaded_at=datetime.fromisoformat(str(payload["downloaded_at"])),
            md5_hash=payload.get("md5_hash") or None,
        )


# study_schema's CopyRoute, CopyVersion and CopyAccess (study_schema b314eb3), spelled here
# until the pin reaches them: that pin also brings the parse changes the parse stage makes.
class CopyRoute(str, Enum):
    """Where a copy of a paper is hosted."""

    PMC = "pmc"
    PUBLISHER = "publisher"
    REPOSITORY = "repository"
    PREPRINT_SERVER = "preprint_server"
    UNRECORDED = "unrecorded"


class CopyVersion(str, Enum):
    """Which version of the paper a copy is, in the NISO Journal Article Versions terms."""

    VOR = "vor"
    AM = "am"
    SUBMITTED = "submitted"


class CopyAccess(str, Enum):
    """How we obtained a copy we hold."""

    PUBLIC = "public"
    #: Under terms that permit text mining only: a publisher's API with our key, or a
    #: PMC author manuscript offered for text mining.
    TDM_GRANT = "tdm_grant"
    UNRECORDED = "unrecorded"


@dataclass(frozen=True)
class DownloadCopy:
    """The facts about a held copy that are fixed when its bytes are fetched: study_schema's
    HeldCopy less its licence, which the metadata stage decides."""

    route: CopyRoute
    access: CopyAccess
    version: Optional[CopyVersion] = None
    url: Optional[str] = None
    #: The server's name in `utils.preprints`, for a copy on a preprint server.
    preprint_server: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        out = {"route": self.route.value, "access": self.access.value}
        if self.version is not None:
            out["version"] = self.version.value
        if self.url:
            out["url"] = self.url
        if self.preprint_server is not None:
            out["preprint_server"] = self.preprint_server
        return out

    @classmethod
    def from_dict(cls, payload: Mapping[str, str]) -> "DownloadCopy":
        return cls(
            route=CopyRoute(payload["route"]),
            access=CopyAccess(payload["access"]),
            version=CopyVersion(payload["version"]) if payload.get("version") else None,
            url=payload.get("url") or None,
            preprint_server=payload.get("preprint_server") or None,
        )


@dataclass
class DownloadResult:
    """Result of attempting to download an article."""

    identifier: Identifier
    source: DownloadSource
    success: bool
    files: List[DownloadedFile] = field(default_factory=list)
    error_message: Optional[str] = None
    #: Rejected files moved out of a shared cache: original path -> where it went.
    moved_aside: Dict[str, str] = field(default_factory=dict)
    #: Each URL tried or passed over for this article, in order, with its outcome.
    url_attempts: List[Dict[str, object]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        out = {
            "identifier": self.identifier.__dict__.copy(),
            "source": self.source.value,
            "success": self.success,
            "files": [file.to_dict() for file in self.files],
            "error_message": self.error_message,
        }
        if self.moved_aside:
            out["moved_aside"] = dict(self.moved_aside)
        if self.url_attempts:
            out["url_attempts"] = [dict(attempt) for attempt in self.url_attempts]
        return out

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "DownloadResult":
        identifier_data = payload.get("identifier", {})
        identifier = Identifier(**identifier_data)  # type: ignore[arg-type]
        files_payload = payload.get("files", [])
        files = [DownloadedFile.from_dict(item) for item in files_payload]  # type: ignore[arg-type]
        return cls(
            identifier=identifier,
            source=DownloadSource(str(payload["source"])),
            success=bool(payload["success"]),
            files=files,
            error_message=payload.get("error_message") or None,
            moved_aside=dict(payload.get("moved_aside") or {}),  # type: ignore[arg-type]
            url_attempts=[dict(a) for a in payload.get("url_attempts") or []],  # type: ignore[union-attr]
        )


__all__ = [
    "CopyAccess",
    "CopyRoute",
    "CopyVersion",
    "DownloadCopy",
    "DownloadResult",
    "DownloadSource",
    "DownloadedFile",
    "FileType",
]
