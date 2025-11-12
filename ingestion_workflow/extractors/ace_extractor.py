"""Download and Extract tables from articles using ACE."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Optional

from ace.config import update_config
from ace.scrape import Scraper

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    Identifier,
    Identifiers,
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    ExtractionResult,
    FileType,
)

logger = logging.getLogger(__name__)


class ACEExtractor(BaseExtractor):
    """Extractor that uses ACE to download and extract tables from articles."""

    _HTML_CONTENT_TYPE = "text/html"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        download_mode: str = "browser",
    ) -> None:
        self.settings = settings or load_settings()
        self.settings.ensure_directories()

        self._cache_root = self._resolve_cache_root()

        update_config(SAVE_ORIGINAL_HTML=True)
        update_config(DOWNLOAD_ROOT=str(self._cache_root))

        self._scraper = Scraper(
            str(self._cache_root),
            api_key=self.settings.pubmed_api_key,
        )
        self._download_mode = download_mode

    def download(self, identifiers: Identifiers) -> list[DownloadResult]:
        if not identifiers:
            return []

        results: list[DownloadResult] = []
        for identifier in identifiers.identifiers:
            results.append(self._download_single(identifier))
        return results

    def extract(
        self, download_results: list[DownloadResult]
    ) -> list[ExtractionResult]:
        """Extract tables from downloaded articles using ACE."""
        raise NotImplementedError(
            "ACEExtractor extract method not implemented."
        )

    def _download_single(self, identifier: Identifier) -> DownloadResult:
        pmid_value = identifier.pmid
        if not pmid_value:
            message = "ACE download requires a PMID."
            logger.warning(message)
            return self._failure(identifier, message)

        pmid = str(pmid_value).strip()

        journal = "IngestionWorkflow"

        try:
            file_path, valid = self._scraper.process_article(
                pmid,
                journal,
                delay=None,
                mode=self._download_mode,
                overwrite=self.settings.force_redownload,
                prefer_pmc_source=False,
            )
        except Exception as exc:  # pragma: no cover - surfaced to caller
            logger.exception("ACE scraper failed for PMID %s", pmid)
            return self._failure(identifier, f"ACE scrape failed: {exc}")

        resolved_path = self._resolve_file_path(file_path, journal, pmid)

        if valid is False:
            message = f"ACE validation failed for PMID {pmid}."
            logger.warning(message)
            return self._failure(identifier, message)

        if resolved_path is None:
            message = "ACE scrape did not produce HTML content."
            logger.warning("%s PMID=%s", message, pmid)
            return self._failure(identifier, message)

        downloaded_file = self._build_downloaded_file(resolved_path)
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.ACE,
            success=True,
            files=[downloaded_file],
            error_message=None,
        )

    def _resolve_file_path(
        self,
        file_path: Path | str | None,
        journal: str,
        pmid: str,
    ) -> Optional[Path]:
        candidate = Path(file_path) if file_path else None
        if candidate and candidate.exists():
            return candidate

        journal_dir = self._cache_root / "html" / journal
        fallback = journal_dir / f"{pmid}.html"
        if fallback.exists():
            return fallback

        matches = list(self._cache_root.glob(f"html/**/{pmid}.html"))
        return matches[0] if matches else None

    def _build_downloaded_file(self, file_path: Path) -> DownloadedFile:
        payload = file_path.read_bytes()
        md5_hash = hashlib.md5(payload).hexdigest()
        return DownloadedFile(
            file_path=file_path,
            file_type=FileType.HTML,
            content_type=self._HTML_CONTENT_TYPE,
            source=DownloadSource.ACE,
            md5_hash=md5_hash,
        )

    def _failure(self, identifier: Identifier, message: str) -> DownloadResult:
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.ACE,
            success=False,
            files=[],
            error_message=message,
        )

    def _resolve_cache_root(self) -> Path:
        base = (
            self.settings.ace_cache_root
            or self.settings.get_cache_dir("ace")
        )
        base.mkdir(parents=True, exist_ok=True)
        (base / "html").mkdir(parents=True, exist_ok=True)
        return base
