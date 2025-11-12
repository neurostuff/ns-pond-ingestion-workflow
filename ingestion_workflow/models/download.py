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
            downloaded_at=datetime.fromisoformat(
                str(payload["downloaded_at"])
            ),
            md5_hash=payload.get("md5_hash") or None,
        )


@dataclass
class DownloadResult:
    """Result of attempting to download an article."""

    identifier: Identifier
    source: DownloadSource
    success: bool
    files: List[DownloadedFile] = field(default_factory=list)
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "identifier": self.identifier.__dict__.copy(),
            "source": self.source.value,
            "success": self.success,
            "files": [file.to_dict() for file in self.files],
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "DownloadResult":
        identifier_data = payload.get("identifier", {})
        identifier = Identifier(**identifier_data)  # type: ignore[arg-type]
        files_payload = payload.get("files", [])
        files = [
            DownloadedFile.from_dict(item) for item in files_payload
        ]  # type: ignore[arg-type]
        return cls(
            identifier=identifier,
            source=DownloadSource(str(payload["source"])),
            success=bool(payload["success"]),
            files=files,
            error_message=payload.get("error_message") or None,
        )


__all__ = [
    "DownloadResult",
    "DownloadSource",
    "DownloadedFile",
    "FileType",
]
