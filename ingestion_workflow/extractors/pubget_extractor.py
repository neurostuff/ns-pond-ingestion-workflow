"""Download and extract tables from articles using Pubget."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from lxml import etree

from pubget._articles import extract_articles
from pubget._coordinates import _extract_coordinates_from_table
from pubget._coordinate_space import _neurosynth_guess_space
from pubget._download import download_pmcids
from pubget._typing import ExitCode
from pubget._utils import (
    get_pmcid_from_article_dir,
    get_table_info_files_from_article_dir,
    load_stylesheet,
    read_article_table,
)

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.extractors.utils import (
    build_downloaded_file,
    build_failure_extraction,
    coordinate_from_row,
    coordinate_space_from_guess,
    parse_table_number,
    safe_hash_stem,
    sanitize_table_id,
)
from ingestion_workflow.models import (
    Coordinate,
    CoordinateSpace,
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    ExtractionResult,
    ExtractedContent,
    ExtractedTable,
    FileType,
    Identifier,
    Identifiers,
)


logger = logging.getLogger(__name__)


class PubgetExtractor(BaseExtractor):
    """Extractor that uses Pubget for article and table downloads."""

    _SUPPORTED_IDS = {"pmcid"}

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()
        self.settings.ensure_directories()
        self._extraction_root = self._resolve_extraction_root()

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> list[DownloadResult]:
        """Download articles using Pubget."""
        if not identifiers:
            return []

        pmcid_map: Dict[str, List[int]] = {}
        results_by_index: Dict[int, DownloadResult] = {}

        for index, identifier in enumerate(identifiers.identifiers):
            normalized = self._normalize_pmcid(identifier.pmcid)
            assert normalized is not None, "Identifiers must include PMCIDs"
            pmcid_map.setdefault(normalized, []).append(index)

        if not pmcid_map:
            failure_message = "No valid PMCIDs were provided for Pubget download."
            return self._ordered_results(
                identifiers,
                results_by_index,
                lambda identifier, msg=failure_message: self._build_failure(identifier, msg),
            )

        data_dir = self._resolve_data_dir()
        pmcids_to_fetch = [int(pmcid) for pmcid in sorted(pmcid_map)]

        try:
            articlesets_dir, download_code = download_pmcids(
                pmcids_to_fetch,
                data_dir=data_dir,
                api_key=self.settings.pubmed_api_key,
                retmax=self.settings.pubmed_batch_size,
            )
        except Exception as exc:  # pragma: no cover - surfaced to caller
            failure_message = f"Pubget download failed: {exc}"
            for indices in pmcid_map.values():
                for idx in indices:
                    identifier = identifiers.identifiers[idx]
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        failure_message,
                    )
            return self._ordered_results(
                identifiers,
                results_by_index,
                lambda identifier, msg=failure_message: self._build_failure(identifier, msg),
                progress_hook=progress_hook,
            )

        if download_code == ExitCode.ERROR:
            failure_message = "Pubget reported an error while downloading PMCIDs."
            for indices in pmcid_map.values():
                for idx in indices:
                    identifier = identifiers.identifiers[idx]
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        failure_message,
                    )
            return self._ordered_results(
                identifiers,
                results_by_index,
                lambda identifier, msg=failure_message: self._build_failure(identifier, msg),
                progress_hook=progress_hook,
            )

        n_jobs = max(1, self.settings.max_workers)
        try:
            articles_dir, extract_code = extract_articles(
                articlesets_dir,
                n_jobs=n_jobs,
            )
        except Exception as exc:  # pragma: no cover - surfaced to caller
            failure_message = f"Pubget extraction failed: {exc}"
            for indices in pmcid_map.values():
                for idx in indices:
                    identifier = identifiers.identifiers[idx]
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        failure_message,
                    )
            return self._ordered_results(
                identifiers,
                results_by_index,
                lambda identifier, msg=failure_message: self._build_failure(identifier, msg),
                progress_hook=progress_hook,
            )

        if extract_code == ExitCode.ERROR:
            failure_message = "Pubget failed to extract downloaded articles."
            for indices in pmcid_map.values():
                for idx in indices:
                    identifier = identifiers.identifiers[idx]
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        failure_message,
                    )
            return self._ordered_results(
                identifiers,
                results_by_index,
                lambda identifier, msg=failure_message: self._build_failure(identifier, msg),
                progress_hook=progress_hook,
            )

        warning_messages: List[str] = []
        if download_code == ExitCode.INCOMPLETE:
            warning_messages.append(
                "Pubget reported an incomplete download; some PMCIDs may be missing."
            )
        if extract_code == ExitCode.INCOMPLETE:
            warning_messages.append(
                "Pubget reported incomplete article extraction; outputs may be partial."
            )
        combined_warning = " ".join(warning_messages) if warning_messages else None

        article_index = self._index_articles(articles_dir)
        for pmcid, indices in pmcid_map.items():
            article_dir = article_index.get(pmcid)
            for idx in indices:
                identifier = identifiers.identifiers[idx]
                if article_dir is None:
                    message = combined_warning or (
                        "Pubget did not produce output for the requested PMCID."
                    )
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        message,
                    )
                    continue
                results_by_index[idx] = self._build_success(
                    identifier,
                    article_dir,
                    combined_warning,
                )

        default_message = "Pubget did not return content for this identifier."
        return self._ordered_results(
            identifiers,
            results_by_index,
            lambda identifier, msg=default_message: self._build_failure(identifier, msg),
            progress_hook=progress_hook,
        )

    def extract(
        self,
        download_results: list[DownloadResult],
        progress_hook: Callable[[int], None] | None = None,
    ) -> list[ExtractionResult]:
        """Extract tables from downloaded articles using Pubget."""
        return self._run_extraction_pipeline(
            download_results,
            extraction_root=self._extraction_root,
            worker=_run_pubget_extraction_task,
            worker_count=self.settings.max_workers,
            source_name="Pubget",
            failure_message="Pubget extraction did not produce a result.",
            failure_builder=lambda download_result, message: build_failure_extraction(
                download_result,
                DownloadSource.PUBGET,
                message,
            ),
            progress_hook=progress_hook,
        )

    def _resolve_extraction_root(self) -> Path:
        base = self.settings.get_cache_dir("extract")
        root = base / "pubget"
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _resolve_data_dir(self) -> Path:
        if self.settings.pubget_cache_root is not None:
            self.settings.pubget_cache_root.mkdir(parents=True, exist_ok=True)
            return self.settings.pubget_cache_root
        return self.settings.get_cache_dir("pubget")

    def _normalize_pmcid(self, pmcid: str | None) -> str | None:
        if not pmcid:
            return None
        value = pmcid.strip().upper()
        if value.startswith("PMC"):
            value = value[3:]
        value = value.strip()
        if not value.isdigit():
            return None
        # Remove leading zeros for consistent lookups.
        return str(int(value))

    def _index_articles(self, articles_dir: Path) -> Dict[str, Path]:
        index: Dict[str, Path] = {}
        if not articles_dir.exists():
            return index
        for bucket in articles_dir.iterdir():
            if not bucket.is_dir():
                continue
            for article_dir in bucket.glob("pmcid_*"):
                try:
                    pmcid_value = str(get_pmcid_from_article_dir(article_dir))
                except Exception:  # pragma: no cover - defensive guard
                    continue
                index[pmcid_value] = article_dir
        return index

    def _build_success(
        self,
        identifier: Identifier,
        article_dir: Path,
        combined_warning: str | None,
    ) -> DownloadResult:
        files: List[DownloadedFile] = []
        missing: List[str] = []
        seen_paths: set[Path] = set()

        article_xml = article_dir.joinpath("article.xml")
        if article_xml.is_file():
            files.append(
                build_downloaded_file(
                    article_xml,
                    FileType.XML,
                    source=DownloadSource.PUBGET,
                )
            )
            seen_paths.add(article_xml)
        else:
            missing.append("article.xml")

        tables_xml = article_dir.joinpath("tables", "tables.xml")
        if tables_xml.is_file():
            files.append(
                build_downloaded_file(
                    tables_xml,
                    FileType.XML,
                    source=DownloadSource.PUBGET,
                )
            )
            seen_paths.add(tables_xml)
        else:
            missing.append("tables/tables.xml")

        tables_dir = article_dir.joinpath("tables")
        if tables_dir.is_dir():
            info_paths = get_table_info_files_from_article_dir(article_dir)
            for info_path in info_paths:
                if info_path in seen_paths:
                    continue
                if not info_path.is_file():
                    missing.append(str(info_path.relative_to(article_dir)))
                    continue
                files.append(
                    build_downloaded_file(
                        info_path,
                        FileType.JSON,
                        source=DownloadSource.PUBGET,
                    )
                )
                seen_paths.add(info_path)
                try:
                    table_info = json.loads(info_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    missing.append(f"Invalid JSON: {info_path.name}")
                    continue
                data_name = table_info.get("table_data_file")
                if not data_name:
                    missing.append(f"Missing table_data_file in {info_path.name}")
                    continue
                data_path = info_path.with_name(str(data_name))
                if not data_path.is_file():
                    missing.append(str(data_path.relative_to(article_dir)))
                    continue
                if data_path in seen_paths:
                    continue
                files.append(
                    build_downloaded_file(
                        data_path,
                        FileType.CSV,
                        source=DownloadSource.PUBGET,
                    )
                )
                seen_paths.add(data_path)
        elif "tables/tables.xml" not in missing:
            missing.append("tables directory missing")

        success = not missing
        error_message = None
        if missing:
            error_message = f"Pubget output missing expected files: {', '.join(missing)}."
        if combined_warning:
            error_message = (
                f"{combined_warning} {error_message}".strip()
                if error_message
                else combined_warning
            )

        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.PUBGET,
            success=success,
            files=files,
            error_message=error_message,
        )

    def _build_failure(self, identifier: Identifier, message: str) -> DownloadResult:
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.PUBGET,
            success=False,
            files=[],
            error_message=message,
        )


def _run_pubget_extraction_task(
    download_result: DownloadResult,
    extraction_root: Path | str,
) -> ExtractedContent:
    root_path = Path(extraction_root)
    try:
        return _extract_pubget_article(download_result, root_path)
    except Exception as exc:  # pragma: no cover - worker safeguard
        logger.exception(
            "Pubget extraction failed for %s",
            download_result.identifier.slug,
        )
        return build_failure_extraction(download_result, DownloadSource.PUBGET, str(exc))


def _extract_pubget_article(
    download_result: DownloadResult,
    extraction_root: Path,
) -> ExtractedContent:
    article_file = _select_article_file(download_result)
    if article_file is None:
        raise ValueError("Pubget extraction requires article.xml content.")

    tables_file = _select_tables_file(download_result)
    if tables_file is None:
        raise ValueError("Pubget extraction requires tables.xml content.")

    article_input_dir = article_file.file_path.parent
    slug = download_result.identifier.slug
    output_dir = extraction_root / safe_hash_stem(slug)
    tables_output_dir = output_dir / "tables"
    tables_output_dir.mkdir(parents=True, exist_ok=True)

    article_tree = etree.parse(str(article_file.file_path))
    stylesheet = load_stylesheet("text_extraction.xsl")

    try:
        transformed = stylesheet(article_tree)
        text_parts: List[str] = []
        for field_name in ("title", "keywords", "abstract", "body"):
            elem = transformed.find(field_name)
            if elem is not None and elem.text:
                part = elem.text.strip()
                if part:
                    text_parts.append(part)
        full_text = "\n\n".join(text_parts)
    except Exception as exc:
        logger.warning(
            "Failed to transform article text for %s: %s",
            slug,
            exc,
        )
        full_text = " ".join(article_tree.xpath(".//text()"))

    output_dir.mkdir(parents=True, exist_ok=True)
    full_text_path = output_dir / "article.txt"
    full_text_path.write_text(full_text, encoding="utf-8")

    article_text = " ".join(article_tree.xpath(".//text()"))
    article_space = coordinate_space_from_guess(_neurosynth_guess_space(article_text))

    tables_tree = etree.parse(str(tables_file.file_path))
    element_by_id, element_by_label = _build_table_lookup(tables_tree)

    info_files = get_table_info_files_from_article_dir(article_input_dir)
    if not info_files:
        message = "Pubget tables metadata not found; returning empty extraction."
        logger.info("%s: %s", slug, message)
        return ExtractedContent(
            slug=slug,
            source=DownloadSource.PUBGET,
            identifier=download_result.identifier,
            full_text_path=full_text_path,
            tables=[],
            has_coordinates=False,
            error_message=message,
        )

    extracted_tables: List[ExtractedTable] = []
    table_sources: Dict[str, Dict[str, str]] = {}
    failure_reasons: List[str] = []

    for index, info_path in enumerate(info_files):
        try:
            table_info, table_frame = read_article_table(info_path)
        except Exception as exc:
            reason = f"{info_path.name}: {exc}"
            failure_reasons.append(reason)
            logger.warning(
                "Failed to load table metadata for %s: %s",
                slug,
                reason,
            )
            continue

        element = _resolve_table_element(
            table_info,
            element_by_id,
            element_by_label,
        )
        if element is None:
            reason = f"Missing table XML for {table_info.get('table_id') or info_path.name}"
            failure_reasons.append(reason)
            logger.warning("%s: %s", slug, reason)
            continue

        original_wrapper = element.find("original-table")
        if original_wrapper is None:
            reason = (
                f"original-table node not found for {table_info.get('table_id') or info_path.name}"
            )
            failure_reasons.append(reason)
            logger.warning("%s: %s", slug, reason)
            continue

        data_name = table_info.get("table_data_file")
        if not data_name:
            reason = f"table_data_file missing for {info_path.name}"
            failure_reasons.append(reason)
            logger.warning("%s: %s", slug, reason)
            continue
        data_path = info_path.with_name(str(data_name))

        sanitized_id = sanitize_table_id(
            table_info.get("table_id"),
            table_info.get("table_label"),
            index,
        )

        raw_table_path = tables_output_dir / f"{sanitized_id}.xml"
        raw_table_xml = etree.tostring(
            original_wrapper,
            encoding="unicode",
            pretty_print=True,
        )
        raw_table_path.write_text(raw_table_xml, encoding="utf-8")

        try:
            coordinates_frame = _extract_coordinates_from_table(table_frame)
        except Exception as exc:
            reason = f"Coordinate parsing failed for {sanitized_id}: {exc}"
            failure_reasons.append(reason)
            logger.warning("%s: %s", slug, reason)
            coordinates_frame = None

        coordinates: List[Coordinate] = []
        coordinate_csv_path = tables_output_dir.joinpath(f"{sanitized_id}_coordinates.csv")
        if coordinates_frame is not None:
            coordinates_frame.to_csv(coordinate_csv_path, index=False)
            if not coordinates_frame.empty:
                for _, row in coordinates_frame.iterrows():
                    coord = coordinate_from_row(row, article_space)
                    if coord is not None:
                        coordinates.append(coord)
        else:
            coordinate_csv_path.write_text("x,y,z\n", encoding="utf-8")

        table_sources[sanitized_id] = {
            "info_path": str(info_path),
            "data_path": str(data_path),
            "coordinates_path": str(coordinate_csv_path),
        }

        table_number = parse_table_number(table_info.get("table_label"))
        caption = table_info.get("table_caption") or ""
        footer = table_info.get("table_foot") or ""
        metadata = {
            "table_id": table_info.get("table_id"),
            "table_label": table_info.get("table_label"),
            "info_path": str(info_path),
            "data_path": str(data_path),
            "coordinates_path": str(coordinate_csv_path),
        }

        extracted_tables.append(
            ExtractedTable(
                table_id=sanitized_id,
                raw_content_path=raw_table_path,
                table_number=table_number,
                caption=caption,
                footer=footer,
                metadata={k: v for k, v in metadata.items() if v},
                coordinates=coordinates,
                space=article_space,
            )
        )

    if not extracted_tables:
        message = "Pubget failed to extract any tables."
        if failure_reasons:
            message = f"{message} Reasons: {' | '.join(failure_reasons)}"
        logger.error("%s: %s", slug, message)
        return build_failure_extraction(
            download_result,
            DownloadSource.PUBGET,
            message,
            full_text_path,
        )

    sources_path = tables_output_dir / "table_sources.json"
    sources_path.write_text(
        json.dumps(table_sources, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    error_message = None
    if failure_reasons:
        error_message = "Pubget skipped tables: " + " | ".join(failure_reasons)

    has_coordinates = any(table.coordinates for table in extracted_tables)

    return ExtractedContent(
        slug=slug,
        source=DownloadSource.PUBGET,
        identifier=download_result.identifier,
        full_text_path=full_text_path,
        tables=extracted_tables,
        has_coordinates=has_coordinates,
        error_message=error_message,
    )


def _build_table_lookup(
    tables_tree: etree._ElementTree,
) -> tuple[Dict[str, etree._Element], Dict[str, etree._Element]]:
    by_id: Dict[str, etree._Element] = {}
    by_label: Dict[str, etree._Element] = {}
    for table in tables_tree.findall(".//extracted-table"):
        table_id = table.findtext("table-id")
        label = table.findtext("table-label")
        if table_id and table_id not in by_id:
            by_id[table_id] = table
        if label and label not in by_label:
            by_label[label] = table
    return by_id, by_label


def _resolve_table_element(
    table_info: Dict[str, Any],
    by_id: Dict[str, etree._Element],
    by_label: Dict[str, etree._Element],
) -> etree._Element | None:
    table_id = table_info.get("table_id")
    if table_id and table_id in by_id:
        return by_id[table_id]
    table_label = table_info.get("table_label")
    if table_label and table_label in by_label:
        return by_label[table_label]
    return None


def _select_article_file(
    download_result: DownloadResult,
) -> Optional[DownloadedFile]:
    for downloaded in download_result.files:
        if downloaded.file_type is FileType.XML and downloaded.file_path.name == "article.xml":
            return downloaded
    return None


def _select_tables_file(
    download_result: DownloadResult,
) -> Optional[DownloadedFile]:
    for downloaded in download_result.files:
        if downloaded.file_type is FileType.XML and downloaded.file_path.name == "tables.xml":
            return downloaded
    return None
