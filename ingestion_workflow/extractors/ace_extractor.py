"""Download and Extract tables from articles using ACE."""

from __future__ import annotations

import hashlib
import logging
import threading
import re
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from pathlib import Path
from typing import Any, Callable, Optional

from ace.config import update_config
from ace.scrape import Scraper
from ace.sources import SourceManager
from ace import extract as ace_extract

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
    ExtractedContent,
    ExtractedTable,
    Coordinate,
    CoordinateSpace,
)
from ingestion_workflow.patches import apply_ace_patch
from ingestion_workflow.utils.progress import emit_progress


apply_ace_patch()


logger = logging.getLogger(__name__)

_HTML_INVALID_MARKERS: list[tuple[str, str]] = [
    ("<title>new tab</title>", "HTML payload captured a browser new-tab page."),
    ("api rate limit exceeded", "HTML payload contains an API rate limit error."),
    ("captcha", "HTML payload appears to be a CAPTCHA challenge."),
    ("access to this page has been denied", "HTML payload was an access-denied page."),
]
_MIN_HTML_LENGTH = 500


def _safe_hash_stem(hash_id: str | None) -> str:
    """Sanitize the hash stem used for ACE extraction directories."""
    candidate = hash_id or ""
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", candidate).strip("-")
    if sanitized:
        return sanitized.lower()
    digest = hashlib.sha256(candidate.encode("utf-8"))
    return digest.hexdigest()[:16]


def _sanitize_table_id(candidate: Optional[str], index: int) -> str:
    fallback = f"table-{index}"
    if not candidate:
        return fallback
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", candidate).strip("-")
    return sanitized.lower() or fallback


def _coordinate_space_from_guess(guess: Optional[str]) -> CoordinateSpace:
    if not guess:
        return CoordinateSpace.OTHER
    normalized = str(guess).strip().upper()
    if normalized == "MNI":
        return CoordinateSpace.MNI
    if normalized in {"TAL", "TALAIRACH"}:
        return CoordinateSpace.TALAIRACH
    if normalized == "UNKNOWN":
        return CoordinateSpace.OTHER
    return CoordinateSpace.OTHER


def _resolve_table_space(table: Any, article: Any) -> CoordinateSpace:
    parts = [
        getattr(table, "caption", None),
        getattr(table, "label", None),
        getattr(table, "notes", None),
    ]
    metadata_text = " ".join(part for part in parts if part)
    guess = ace_extract.guess_space(metadata_text)
    if guess == "UNKNOWN":
        guess = getattr(article, "space", None)
    return _coordinate_space_from_guess(guess)


def _coordinate_from_activation(activation: Any, space: CoordinateSpace) -> Optional[Coordinate]:
    if activation is None:
        return None
    coords = (
        getattr(activation, "x", None),
        getattr(activation, "y", None),
        getattr(activation, "z", None),
    )
    if any(value is None for value in coords):
        return None
    try:
        x_val = float(activation.x)
        y_val = float(activation.y)
        z_val = float(activation.z)
    except (TypeError, ValueError):
        return None

    statistic_value = None
    raw_stat = getattr(activation, "statistic", None)
    if raw_stat not in (None, ""):
        try:
            statistic_value = float(raw_stat)
        except (TypeError, ValueError):
            statistic_value = None

    cluster_size = None
    raw_size = getattr(activation, "size", None)
    if raw_size not in (None, ""):
        try:
            cluster_size = int(float(str(raw_size)))
        except (TypeError, ValueError):
            cluster_size = None

    return Coordinate(
        x=x_val,
        y=y_val,
        z=z_val,
        space=space,
        statistic_value=statistic_value,
        cluster_size=cluster_size,
    )


def _select_html_file(
    download_result: DownloadResult,
) -> Optional[DownloadedFile]:
    for downloaded in download_result.files:
        if downloaded.file_type is FileType.HTML:
            return downloaded
    return None


def _validate_downloaded_html(file_path: Path) -> tuple[bool, Optional[str]]:
    """Validate downloaded HTML content to catch placeholders or errors."""
    try:
        html_text = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        html_text = file_path.read_text(errors="ignore")

    normalized = html_text.strip()
    if not normalized:
        return False, "HTML payload was empty."

    lowered = normalized.lower()
    if "<html" not in lowered:
        return False, "HTML payload is missing an <html> tag."

    for marker, reason in _HTML_INVALID_MARKERS:
        if marker in lowered:
            return False, reason

    if len(normalized) < _MIN_HTML_LENGTH:
        return False, (f"HTML payload is unexpectedly small ({len(normalized)} characters).")

    return True, None


def _build_failure_extraction(download_result: DownloadResult, message: str) -> ExtractedContent:
    return ExtractedContent(
        hash_id=download_result.identifier.hash_id,
        source=DownloadSource.ACE,
        identifier=download_result.identifier,
        full_text_path=None,
        tables=[],
        has_coordinates=False,
        error_message=message,
    )


def _translate_ace_table(
    table: Any,
    article: Any,
    tables_dir: Path,
    table_index: int,
) -> ExtractedTable:
    table_id = _sanitize_table_id(
        getattr(table, "number", None),
        table_index + 1,
    )
    raw_filename = tables_dir / f"{table_id}.html"
    raw_html = getattr(table, "input_html", None) or ""
    raw_filename.parent.mkdir(parents=True, exist_ok=True)
    if raw_html:
        raw_filename.write_text(raw_html, encoding="utf-8")
    else:
        raw_filename.write_text(
            "<!-- ACE did not retain raw table HTML -->",
            encoding="utf-8",
        )

    space = _resolve_table_space(table, article)
    coordinates = [
        coord
        for coord in (
            _coordinate_from_activation(activation, space)
            for activation in getattr(table, "activations", [])
        )
        if coord is not None
    ]

    metadata = {
        "label": getattr(table, "label", None),
        "notes": getattr(table, "notes", None),
        "position": getattr(table, "position", None),
        "n_activations": getattr(table, "n_activations", None),
        "n_columns": getattr(table, "n_columns", None),
    }

    table_number = None
    raw_number = getattr(table, "number", None)
    if raw_number is not None:
        try:
            table_number = int(str(raw_number))
        except ValueError:
            table_number = None

    return ExtractedTable(
        table_id=table_id,
        raw_content_path=raw_filename,
        table_number=table_number,
        caption=getattr(table, "caption", "") or "",
        footer=getattr(table, "notes", "") or "",
        metadata={k: v for k, v in metadata.items() if v is not None},
        coordinates=coordinates,
        space=space,
    )


def _extract_ace_article(
    download_result: DownloadResult,
    extraction_root: Path,
) -> ExtractedContent:
    update_config(SAVE_ORIGINAL_HTML=True)
    html_file = _select_html_file(download_result)
    if html_file is None:
        raise ValueError("ACE extraction requires an HTML payload.")

    hash_id = download_result.identifier.hash_id
    article_dir = extraction_root / _safe_hash_stem(hash_id)
    tables_dir = article_dir / "tables"
    source_tables_dir = article_dir / "downloaded_tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    source_tables_dir.mkdir(parents=True, exist_ok=True)

    html_text = html_file.file_path.read_text(encoding="utf-8")
    manager = SourceManager(table_dir=str(source_tables_dir))
    source = manager.identify_source(html_text)
    if source is None:
        raise ValueError("ACE could not identify an article source.")

    article = source.parse_article(
        html_text,
        pmid=download_result.identifier.pmid,
        metadata_dir=None,
    )
    if not article:
        raise ValueError("ACE failed to parse the article content.")

    article_text = getattr(article, "text", "") or ""
    full_text_path = article_dir / "article.txt"
    full_text_path.write_text(article_text, encoding="utf-8")

    extracted_tables = [
        _translate_ace_table(table, article, tables_dir, index)
        for index, table in enumerate(getattr(article, "tables", []))
    ]

    has_coordinates = any(table.coordinates for table in extracted_tables)

    return ExtractedContent(
        hash_id=hash_id,
        source=DownloadSource.ACE,
        identifier=download_result.identifier,
        full_text_path=full_text_path,
        tables=extracted_tables,
        has_coordinates=has_coordinates,
        error_message=None,
    )


def _run_ace_extraction_task(
    download_result: DownloadResult, extraction_root: Path | str
) -> ExtractedContent:
    root_path = Path(extraction_root)
    try:
        return _extract_ace_article(download_result, root_path)
    except Exception as exc:  # pragma: no cover - worker failure logging
        logger.exception("ACE extraction failed for %s", download_result.identifier.hash_id)
        return _build_failure_extraction(download_result, str(exc))


class ACEExtractor(BaseExtractor):
    """Extractor that uses ACE to download and extract tables from articles."""

    _SUPPORTED_IDS = {"pmid"}
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
        self._extraction_root = self._resolve_extraction_root()

        update_config(SAVE_ORIGINAL_HTML=True)
        self._download_mode = download_mode

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> list[DownloadResult]:
        if not identifiers:
            return []

        worker_count = self.settings.ace_max_workers
        if worker_count <= 0:
            worker_count = self.settings.max_workers
        worker_count = max(1, worker_count)

        identifiers_list = identifiers.identifiers

        if worker_count == 1 or len(identifiers_list) <= 1:
            scraper = self._build_scraper()
            results = []
            for identifier in identifiers_list:
                result = self._download_single(identifier, scraper=scraper)
                results.append(result)
                emit_progress(progress_hook)
            return results

        ordered_results: list[Optional[DownloadResult]] = [None] * len(identifiers_list)

        thread_local = threading.local()

        def _run(identifier: Identifier) -> DownloadResult:
            scraper = getattr(thread_local, "scraper", None)
            if scraper is None:
                scraper = self._build_scraper()
                thread_local.scraper = scraper
            return self._download_single(identifier, scraper=scraper)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_run, identifier): index
                for index, identifier in enumerate(identifiers_list)
            }
            for future in as_completed(future_map):
                index = future_map[future]
                identifier = identifiers_list[index]
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.exception(
                        "ACE download raised exception for PMID %s",
                        identifier.pmid,
                    )
                    result = self._failure(
                        identifier,
                        f"ACE download raised an exception: {exc}",
                    )
                ordered_results[index] = result
                emit_progress(progress_hook)

        results: list[DownloadResult] = []
        for index, identifier in enumerate(identifiers_list):
            result = ordered_results[index]
            if result is None:
                result = self._failure(
                    identifier,
                    "ACE download did not return a result.",
                )
            results.append(result)

        return results

    def extract(
        self,
        download_results: list[DownloadResult],
        progress_hook: Callable[[int], None] | None = None,
    ) -> list[ExtractionResult]:
        """Extract tables from downloaded articles using ACE."""
        if not download_results:
            return []

        extraction_root = self._extraction_root
        extraction_root.mkdir(parents=True, exist_ok=True)

        for download_result in download_results:
            if not download_result.success:
                raise ValueError(
                    "ACEExtractor.extract received an unsuccessful download "
                    "result; download validation should occur before "
                    "extraction."
                )

        ordered_results: list[Optional[ExtractionResult]] = [None] * len(download_results)

        worker_count = max(1, self.settings.max_workers)

        if worker_count == 1 or len(download_results) == 1:
            for index, download_result in enumerate(download_results):
                ordered_results[index] = _run_ace_extraction_task(
                    download_result,
                    extraction_root,
                )
                emit_progress(progress_hook)
        else:
            root_str = str(extraction_root)
            with ProcessPoolExecutor(max_workers=worker_count) as executor:
                future_map = {
                    executor.submit(
                        _run_ace_extraction_task,
                        download_result,
                        root_str,
                    ): index
                    for index, download_result in enumerate(download_results)
                }
                for future in as_completed(future_map):
                    index = future_map[future]
                    download_result = download_results[index]
                    try:
                        ordered_results[index] = future.result()
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.exception(
                            "ACE extraction raised exception for %s",
                            download_result.identifier.hash_id,
                        )
                        ordered_results[index] = _build_failure_extraction(
                            download_result,
                            f"ACE extraction raised an exception: {exc}",
                        )
                    finally:
                        emit_progress(progress_hook)

        final_results: list[ExtractionResult] = []
        for index, download_result in enumerate(download_results):
            result = ordered_results[index]
            if result is None:
                result = _build_failure_extraction(
                    download_result,
                    "ACE extraction did not produce a result.",
                )
            final_results.append(result)

        return final_results

    def _download_single(
        self,
        identifier: Identifier,
        *,
        scraper: Scraper | None = None,
    ) -> DownloadResult:
        pmid_value = identifier.pmid
        if not pmid_value:
            message = "ACE download requires a PMID."
            logger.warning(message)
            return self._failure(identifier, message)

        pmid = str(pmid_value).strip()

        active_scraper = scraper or self._build_scraper()

        journal = "IngestionWorkflow"

        try:
            file_path, valid = active_scraper.process_article(
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

        html_valid, invalid_reason = _validate_downloaded_html(resolved_path)
        if not html_valid:
            try:
                resolved_path.unlink()
            except OSError as exc:  # pragma: no cover - best-effort cleanup
                logger.debug(
                    "Failed to delete invalid ACE HTML payload %s: %s",
                    resolved_path,
                    exc,
                )
            reason = invalid_reason or "HTML payload failed validation."
            message = f"ACE scrape produced invalid HTML: {reason}"
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

    def _build_scraper(self) -> Scraper:
        return Scraper(
            str(self._cache_root),
            api_key=self.settings.pubmed_api_key,
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
        base = self.settings.ace_cache_root or self.settings.get_cache_dir("ace")
        base.mkdir(parents=True, exist_ok=True)
        (base / "html").mkdir(parents=True, exist_ok=True)
        return base

    def _resolve_extraction_root(self) -> Path:
        base = self._cache_root / "extracted"
        base.mkdir(parents=True, exist_ok=True)
        return base
