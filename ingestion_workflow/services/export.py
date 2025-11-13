"""Export utilities for writing bundle artifacts to disk."""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Iterable, Sequence

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    ArticleExtractionBundle,
    CreateAnalysesResult,
)
from ingestion_workflow.services.create_analyses import sanitize_table_id


logger = logging.getLogger(__name__)


class ExportService:
    """Persist bundle artifacts to the configured export directory."""

    def __init__(self, settings: Settings, *, overwrite: bool = False) -> None:
        self.settings = settings
        self.overwrite = overwrite

    def export(
        self,
        bundle: ArticleExtractionBundle,
        analyses: Sequence[CreateAnalysesResult] | None = None,
    ) -> None:
        identifier = bundle.article_data.identifier
        if identifier is None:
            logger.debug(
                "Skipping export for bundle %s because identifier is missing",
                bundle.article_data.hash_id,
            )
            return

        hash_id = identifier.hash_id
        root = self.settings.data_root / "export" / hash_id
        root.mkdir(parents=True, exist_ok=True)

        self._write_identifiers(root, identifier.to_dict())
        self._write_source(bundle, root)
        processed_dir = root / "processed" / bundle.article_data.source.value
        processed_dir.mkdir(parents=True, exist_ok=True)
        self._write_metadata(bundle, processed_dir)
        self._write_tables(bundle, processed_dir)

        if analyses:
            self._write_analyses(processed_dir, bundle, analyses)

    # ------------------------------------------------------------------ helpers
    def _write_identifiers(self, root: Path, payload: dict) -> None:
        dest = root / "identifiers.json"
        self._write_json(dest, payload)

    def _write_source(self, bundle: ArticleExtractionBundle, root: Path) -> None:
        source_dir = root / "source" / bundle.article_data.source.value
        source_dir.mkdir(parents=True, exist_ok=True)

        full_text_path = bundle.article_data.full_text_path
        if full_text_path and full_text_path.exists():
            destination = source_dir / full_text_path.name
            self._copy_file(full_text_path, destination)

    def _write_metadata(self, bundle: ArticleExtractionBundle, processed_dir: Path) -> None:
        metadata_path = processed_dir / "article_metadata.json"
        self._write_json(metadata_path, bundle.article_metadata.to_dict())

        content_path = processed_dir / "article_data.json"
        self._write_json(content_path, bundle.article_data.to_dict())

    def _write_tables(self, bundle: ArticleExtractionBundle, processed_dir: Path) -> None:
        tables_dir = processed_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        tables_payload = []
        for index, table in enumerate(bundle.article_data.tables):
            sanitized = sanitize_table_id(table.table_id, index)
            tables_payload.append(table.to_dict())
            dest_file = tables_dir / f"{sanitized}{Path(table.raw_content_path).suffix or '.html'}"
            if table.raw_content_path.exists():
                self._copy_file(table.raw_content_path, dest_file)
        tables_path = processed_dir / "tables.json"
        self._write_json(tables_path, tables_payload)

    def _write_analyses(
        self,
        processed_dir: Path,
        bundle: ArticleExtractionBundle,
        analyses: Sequence[CreateAnalysesResult],
    ) -> None:
        filtered = [
            result for result in analyses if result.article_hash == bundle.article_data.hash_id
        ]
        if not filtered:
            return

        analyses_dir = processed_dir / "analyses"
        analyses_dir.mkdir(parents=True, exist_ok=True)
        used_names: set[str] = set()

        for index, result in enumerate(filtered):
            base_name = result.table_id or result.sanitized_table_id or f"table_{index:02d}"
            file_stem = sanitize_table_id(base_name, index)
            if file_stem in used_names:
                file_stem = f"{file_stem}_{index}"
            used_names.add(file_stem)
            dest = analyses_dir / f"{file_stem}.jsonl"
            if dest.exists() and not self.overwrite:
                continue
            payload = result.analysis_collection.to_dict()
            dest.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    def _write_json(self, path: Path, payload: object) -> None:
        if path.exists() and not self.overwrite:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _copy_file(self, source: Path, destination: Path) -> None:
        if destination.exists() and not self.overwrite:
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(source, destination)
        except FileNotFoundError:
            logger.debug("Source file missing during export: %s", source)


__all__ = ["ExportService"]
