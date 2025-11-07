"""Extractor that wraps the ACE pipeline to parse local HTML articles."""
from __future__ import annotations

import importlib
import json
import logging
import re
import shutil
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from ingestion_workflow.extractors.base import DownloadError, Extractor
from ingestion_workflow.models import DownloadArtifact, DownloadResult, Identifier

logger = logging.getLogger(__name__)

_FILENAME_SANITIZER = re.compile(r"[^0-9A-Za-z_.-]+")


def _sanitize_name(value: str | None, default: str) -> str:
    if not value:
        return default
    cleaned = _FILENAME_SANITIZER.sub("_", value).strip("_")
    return cleaned or default


class ACEExtractor(Extractor):
    """Extractor that reuses ACE's HTML parsers to recover coordinate tables."""

    name = "ace"

    def __init__(self, context):
        super().__init__(context)
        self._source_manager_cls: Any | None = None
        self._scraper_cls: Any | None = None
        self._pubmed_metadata_fn: Optional[Callable[..., Any]] = None
        self._scraper_instance: Any | None = None
        self.settings = context.settings

    def supports(self, identifier: Identifier) -> bool:
        return bool(identifier.normalized().pmid)

    def download(self, identifiers: Sequence[Identifier]) -> List[DownloadResult]:
        results: List[DownloadResult] = []
        total_supported = 0
        download_failures: List[str] = []
        missing_coordinates: List[str] = []
        coordinates_found_count = 0
        for identifier in identifiers:
            if not self.supports(identifier):
                continue
            total_supported += 1
            try:
                result = self._download_single(identifier)
                results.append(result)
                if result.extra_metadata.get("coordinates_found"):
                    coordinates_found_count += 1
                else:
                    missing_coordinates.append(self._identifier_label(identifier))
            except DownloadError as exc:
                logger.debug(
                    "ACE download failed",
                    extra={"hash_id": identifier.hash_id, "error": str(exc)},
                )
                download_failures.append(self._identifier_label(identifier))
        if total_supported:
            logger.info(
                "ACE extractor summary: downloaded %d/%d (failures=%d); coordinates found=%d, missing=%d",
                len(results),
                total_supported,
                len(download_failures),
                coordinates_found_count,
                len(missing_coordinates),
            )
            if download_failures:
                logger.info(
                    "ACE download failure examples: %s",
                    download_failures[:5],
                )
            if missing_coordinates:
                logger.info(
                    "ACE no-coordinate examples: %s",
                    missing_coordinates[:5],
                )
        return results

    def _download_single(self, identifier: Identifier) -> DownloadResult:
        normalized = identifier.normalized()
        pmid = normalized.pmid
        if not pmid:
            raise DownloadError("ACE extractor requires a PMID to locate HTML articles.")

        html_path = self._resolve_html_path(pmid)
        storage_paths = self.context.storage.paths_for(identifier)
        source_dir = storage_paths.source_for(self.name)
        processed_dir = storage_paths.processed_for(self.name)
        source_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)

        source_html_path = source_dir / f"{pmid}.html"
        shutil.copy2(html_path, source_html_path)
        html_text = source_html_path.read_text(encoding="utf-8")

        tables_source_dir = source_dir / "tables"
        tables_source_dir.mkdir(parents=True, exist_ok=True)

        manager = self._create_source_manager()
        source = manager.identify_source(html_text)
        if source is None:
            raise DownloadError(f"ACE could not identify a source parser for PMID {pmid}.")

        metadata_dir = self._metadata_directory()
        article = source.parse_article(html_text, pmid=pmid, metadata_dir=str(metadata_dir))
        if not article:
            raise DownloadError("ACE failed to parse article content.")

        collected_tables = self._collect_tables(article)
        coordinates_df = self._build_coordinates_dataframe(collected_tables, pmid)
        coordinates_found = not coordinates_df.empty
        if coordinates_found:
            coordinate_table_ids: set[str] = {
                str(value) for value in coordinates_df["table_id"].dropna().astype(str)
            }
        else:
            coordinate_table_ids = set()
        tables_df = self._build_tables_dataframe(collected_tables, pmid)
        manifest = self._persist_table_html(collected_tables, tables_source_dir)

        coordinates_path = processed_dir / "coordinates.csv"
        tables_path = processed_dir / "tables.csv"
        text_path = processed_dir / "text.txt"
        metadata_path = processed_dir / "metadata.json"
        manifest_path = source_dir / "tables_manifest.json"

        coordinates_df.to_csv(coordinates_path, index=False)
        tables_df.to_csv(tables_path, index=False)
        text_content = getattr(article, "text", None) or html_text
        text_path.write_text(text_content, encoding="utf-8")

        metadata_payload = self._build_metadata_payload(article, source, pmid)
        metadata_path.write_text(json.dumps(metadata_payload, indent=2), encoding="utf-8")
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        artifacts = [
            DownloadArtifact(
                path=source_html_path,
                kind="article_html",
                media_type="text/html",
            ),
            DownloadArtifact(
                path=coordinates_path,
                kind="coordinates_csv",
                media_type="text/csv",
            ),
            DownloadArtifact(
                path=tables_path,
                kind="tables_csv",
                media_type="text/csv",
            ),
            DownloadArtifact(
                path=text_path,
                kind="article_text",
                media_type="text/plain",
            ),
            DownloadArtifact(
                path=metadata_path,
                kind="metadata_json",
                media_type="application/json",
            ),
        ]

        extra_metadata = {"ace_source": type(source).__name__, "coordinates_found": coordinates_found}
        return DownloadResult(
            identifier=identifier,
            source=self.name,
            artifacts=artifacts,
            open_access=False,
            extra_metadata=extra_metadata,
        )

    def _resolve_html_path(self, pmid: str) -> Path:
        cached = self._find_cached_html(pmid)
        if cached:
            return cached
        logger.debug("ACE cache miss; attempting remote download", extra={"pmid": pmid})
        downloaded = self._download_article_html(pmid)
        if downloaded.exists():
            return downloaded
        raise DownloadError(f"Unable to locate or download HTML for PMID {pmid}.")

    def _find_cached_html(self, pmid: str) -> Optional[Path]:
        html_root = self.settings.ace_html_root
        if not html_root.exists():
            return None
        matches = list(html_root.rglob(f"{pmid}.html"))
        if matches:
            if len(matches) > 1:
                logger.info(
                    "Multiple HTML files matched PMID; using first result.",
                    extra={"pmid": pmid, "choices": [str(m) for m in matches[:3]]},
                )
            return matches[0]
        return None

    def _create_source_manager(self):
        source_manager_cls = self._get_source_manager_cls()
        return source_manager_cls(  # type: ignore[operator]
            table_dir=None,
            use_readability=self.settings.ace_use_readability,
        )

    def _download_article_html(self, pmid: str) -> Path:
        metadata_dir = self._metadata_directory()
        metadata_fn = self._get_pubmed_metadata_fn()
        metadata = metadata_fn(
            pmid,
            store=str(metadata_dir),
            save=True,
            api_key=self.settings.pubmed_api_key,
        )
        if not metadata or "journal" not in metadata or not metadata["journal"]:
            raise DownloadError(f"Unable to determine journal for PMID {pmid}.")
        journal = metadata["journal"]
        scraper = self._scraper()
        store = self.settings.ace_html_root
        store.mkdir(parents=True, exist_ok=True)
        filename, valid = scraper.process_article(
            pmid,
            journal,
            mode=self.settings.ace_download_mode,
            overwrite=False,
            prefer_pmc_source=self.settings.ace_prefer_pmc_source,
        )
        if not filename or not valid:
            raise DownloadError(f"ACE scraper failed to download article for PMID {pmid}.")
        return Path(filename)

    def _scraper(self):
        if self._scraper_instance is None:
            scraper_cls = self._get_scraper_cls()
            self._scraper_instance = scraper_cls(
                store=str(self.settings.ace_html_root),
                api_key=self.settings.pubmed_api_key,
            )
        return self._scraper_instance

    def _get_source_manager_cls(self):
        if self._source_manager_cls is None:
            sources_mod = importlib.import_module("ace.sources")
            config_mod = importlib.import_module("ace.config")
            if hasattr(config_mod, "update_config"):
                config_mod.update_config(SAVE_ORIGINAL_HTML=self.settings.ace_save_table_html)
            self._source_manager_cls = getattr(sources_mod, "SourceManager", None)
            if not self._source_manager_cls:
                raise DownloadError("ACE SourceManager could not be located.")
        return self._source_manager_cls

    def _get_scraper_cls(self):
        if self._scraper_cls is None:
            scrape_mod = importlib.import_module("ace.scrape")
            self._scraper_cls = getattr(scrape_mod, "Scraper", None)
            if not self._scraper_cls:
                raise DownloadError("ACE Scraper class could not be located.")
        return self._scraper_cls

    def _get_pubmed_metadata_fn(self):
        if self._pubmed_metadata_fn is None:
            scrape_mod = importlib.import_module("ace.scrape")
            self._pubmed_metadata_fn = getattr(scrape_mod, "get_pubmed_metadata", None)
            if not self._pubmed_metadata_fn:
                raise DownloadError("ACE get_pubmed_metadata function missing.")
        return self._pubmed_metadata_fn

    def _metadata_directory(self) -> Path:
        if self.settings.ace_metadata_root is not None:
            metadata_root = self.settings.ace_metadata_root
        else:
            candidate = self.settings.ace_html_root / "metadata_cache"
            if candidate.exists():
                metadata_root = candidate
            else:
                metadata_root = self.context.storage.cache_root / "ace_metadata"
        metadata_root.mkdir(parents=True, exist_ok=True)
        return metadata_root

    @staticmethod
    def _identifier_label(identifier: Identifier) -> str:
        normalized = identifier.normalized()
        return (
            normalized.pmid
            or normalized.doi
            or normalized.pmcid
            or (normalized.other_ids[0] if normalized.other_ids else None)
            or identifier.hash_id
        )

    def _collect_tables(self, article: Any) -> List[Tuple[str, Any]]:
        tables = getattr(article, "tables", []) or []
        collected: List[Tuple[str, Any]] = []
        for idx, table in enumerate(tables):
            table_id = self._table_identifier(table, idx)
            collected.append((table_id, table))
        return collected

    def _table_identifier(self, table: Any, index: int) -> str:
        candidate = getattr(table, "number", None)
        if candidate:
            return _sanitize_name(str(candidate), f"table_{index+1:03d}")
        label = getattr(table, "label", None)
        if label:
            return _sanitize_name(str(label), f"table_{index+1:03d}")
        return f"table_{index+1:03d}"

    def _build_tables_dataframe(self, tables: Sequence[Tuple[str, Any]], pmid: str) -> pd.DataFrame:
        records = []
        for table_id, table in tables:
            records.append(
                {
                    "pmid": pmid,
                    "table_id": table_id,
                    "table_label": getattr(table, "label", None),
                    "table_caption": getattr(table, "caption", None),
                    "table_notes": getattr(table, "notes", None),
                    "n_activations": len(getattr(table, "activations", []) or []),
                    "n_columns": getattr(table, "n_columns", None),
                    "position": getattr(table, "position", None),
                }
            )
        return pd.DataFrame(records)

    def _build_coordinates_dataframe(self, tables: Sequence[Tuple[str, Any]], pmid: str) -> pd.DataFrame:
        rows: List[dict[str, Any]] = []
        for table_id, table in tables:
            activations = getattr(table, "activations", []) or []
            for activation in activations:
                groups = getattr(activation, "groups", None)
                if isinstance(groups, str):
                    groups_serialized = groups
                elif isinstance(groups, Iterable):
                    groups_serialized = ";".join(str(item) for item in groups)
                else:
                    groups_serialized = ""
                rows.append(
                    {
                        "pmid": pmid,
                        "table_id": table_id,
                        "table_label": getattr(table, "label", None),
                        "x": getattr(activation, "x", None),
                        "y": getattr(activation, "y", None),
                        "z": getattr(activation, "z", None),
                        "region": getattr(activation, "region", None),
                        "hemisphere": getattr(activation, "hemisphere", None),
                        "brodmann_area": getattr(activation, "ba", None),
                        "cluster_size": getattr(activation, "size", None),
                        "statistic": getattr(activation, "statistic", None),
                        "p_value": getattr(activation, "p_value", None),
                        "groups": groups_serialized,
                    }
                )
        columns = [
            "pmid",
            "table_id",
            "table_label",
            "x",
            "y",
            "z",
            "region",
            "hemisphere",
            "brodmann_area",
            "cluster_size",
            "statistic",
            "p_value",
            "groups",
        ]
        if rows:
            return pd.DataFrame(rows, columns=columns)
        return pd.DataFrame(columns=columns)

    def _persist_table_html(
        self,
        tables: Sequence[Tuple[str, Any]],
        destination: Path,
    ) -> List[dict[str, Any]]:
        manifest: List[dict[str, Any]] = []
        for table_id, table in tables:
            entry = {
                "table_id": table_id,
                "number": getattr(table, "number", None),
                "label": getattr(table, "label", None),
                "caption": getattr(table, "caption", None),
                "position": getattr(table, "position", None),
                "n_activations": len(getattr(table, "activations", []) or []),
            }
            html = getattr(table, "input_html", None)
            if html:
                filename = f"{_sanitize_name(table_id, table_id)}.html"
                table_path = destination / filename
                table_path.write_text(html, encoding="utf-8")
                entry["html_file"] = table_path.name
            manifest.append(entry)
        return manifest

    def _build_metadata_payload(self, article: Any, source: Any, pmid: str) -> dict[str, Any]:
        payload = {
            "pmid": pmid,
            "title": getattr(article, "title", None),
            "journal": getattr(article, "journal", None),
            "year": getattr(article, "year", None),
            "doi": getattr(article, "doi", None),
            "authors": getattr(article, "authors", None),
            "abstract": getattr(article, "abstract", None),
            "ace_source": type(source).__name__,
        }
        pubmed_metadata = getattr(article, "pubmed_metadata", None)
        if pubmed_metadata:
            # Ensure the metadata is JSON serializable.
            payload["pubmed_metadata"] = json.loads(json.dumps(pubmed_metadata))
        return payload
