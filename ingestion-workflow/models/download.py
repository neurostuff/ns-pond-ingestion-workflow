"""Data models for downloaded and extracted content."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

import json


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
    """
    Metadata about a downloaded file.

    Tracks where the file came from, what it contains, and where
    it's stored in the cache.
    """

    file_path: Path
    file_type: FileType
    content_type: str
    source: DownloadSource
    downloaded_at: datetime = field(default_factory=datetime.now)
    md5_hash: Optional[str] = None


@dataclass
class DownloadResult:
    """
    Result of attempting to download a article.

    Contains all successfully downloaded files plus metadata about
    the download attempt.
    """

    hash_id: str
    source: DownloadSource
    success: bool
    files: List[DownloadedFile] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class DownloadIndex:
    """
    Index of all downloads in the cache.

    Maintains a mapping of hash_id -> download results to enable
    cache-only mode and avoid redundant downloads.
    """

    downloads: Dict[str, DownloadResult] = field(default_factory=dict)
    index_path: Optional[Path] = None

    def add_download(self, result: DownloadResult) -> None:
        """
        Add a download result to the index.

        Parameters
        ----------
        result : DownloadResult
            Download result to add
        """
        self.downloads[result.hash_id] = result

    def get_download(self, hash_id: str) -> Optional[DownloadResult]:
        """
        Get download result for a article.

        Parameters
        ----------
        hash_id : str
            Hash ID of the article

        Returns
        -------
        DownloadResult or None
            Download result if found, None otherwise
        """
        return self.downloads.get(hash_id)

    def has_download(self, hash_id: str) -> bool:
        """
        Check if a article has been downloaded.

        Parameters
        ----------
        hash_id : str
            Hash ID of the article

        Returns
        -------
        bool
            True if article has been downloaded
        """
        return hash_id in self.downloads

    def save(self) -> None:
        """Save the index to disk."""
        if self.index_path is None:
            raise ValueError("index_path must be set before saving")

        # Convert dataclass to dict, excluding index_path
        from dataclasses import asdict

        payload = asdict(self)
        del payload["index_path"]

        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(
            json.dumps(payload, indent=2, default=str),
            encoding="utf-8",
        )

    @classmethod
    def load(cls, index_path: Path) -> DownloadIndex:
        """
        Load download index from disk.

        Parameters
        ----------
        index_path : Path
            Path to index file

        Returns
        -------
        DownloadIndex
            Loaded index
        """
        if not index_path.exists():
            return cls(downloads={}, index_path=index_path)

        raw = index_path.read_text(encoding="utf-8")
        data = json.loads(raw)

        # Convert nested dicts back to dataclasses
        downloads = {}
        for hash_id, result_data in data.get("downloads", {}).items():
            files = [
                DownloadedFile(
                    file_path=Path(f["file_path"]),
                    file_type=FileType(f["file_type"]),
                    content_type=f["content_type"],
                    source=DownloadSource(f["source"]),
                    downloaded_at=datetime.fromisoformat(f["downloaded_at"]),
                    md5_hash=f.get("md5_hash"),
                )
                for f in result_data.get("files", [])
            ]
            downloads[hash_id] = DownloadResult(
                hash_id=result_data["hash_id"],
                source=DownloadSource(result_data["source"]),
                success=result_data["success"],
                files=files,
                error_message=result_data.get("error_message"),
            )

        return cls(downloads=downloads, index_path=index_path)
