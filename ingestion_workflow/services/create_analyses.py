"""Service responsible for creating analyses from extracted tables."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Callable, Dict, List, Optional

from ingestion_workflow.clients import CoordinateParsingClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleExtractionBundle,
    Coordinate,
    CoordinateSpace,
    ExtractedTable,
)
from ingestion_workflow.models.coordinate_parsing import (
    CoordinatePoint,
    ParseAnalysesOutput,
)
from ingestion_workflow.utils.progress import emit_progress

logger = logging.getLogger(__name__)


def sanitize_table_id(table_id: str | None, index: int) -> str:
    """Sanitize table identifiers for filesystem-safe usage."""
    if table_id:
        normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", table_id).strip("-")
        if normalized:
            return normalized.lower()
    return f"table-{index + 1}"


class CreateAnalysesService:
    """Create AnalysisCollection objects from extracted tables."""

    def __init__(
        self,
        settings: Settings,
        *,
        extractor_name: Optional[str] = None,
    ) -> None:
        self.settings = settings
        self.extractor_name = extractor_name
        self.client = CoordinateParsingClient(settings)

    def run(
        self,
        bundle: ArticleExtractionBundle,
        progress_hook: Callable[[int], None] | None = None,
    ) -> Dict[str, AnalysisCollection]:
        """Create analyses for every table in the bundle."""
        if not bundle.article_data.tables:
            return {}

        results: Dict[str, AnalysisCollection] = {}
        article_hash = bundle.article_data.hash_id
        identifier = bundle.article_data.identifier

        for index, table in enumerate(bundle.article_data.tables):
            if not table.contains_coordinates and not table.coordinates:
                logger.debug(
                    "Skipping table %s for article %s (no coordinates detected).",
                    table.table_id,
                    bundle.article_data.hash_id,
                )
                continue
            sanitized_table_id = sanitize_table_id(table.table_id, index)
            table_key = table.table_id or sanitized_table_id

            try:
                table_text = self._read_table_content(table)
            except FileNotFoundError as exc:
                logger.warning(
                    "Skipping table %s for article %s: %s",
                    table_key,
                    article_hash,
                    exc,
                )
                continue
            prompt = self._build_prompt(bundle, table, table_text, table_key)
            parsed_output = self.client.parse_analyses(prompt)
            if not parsed_output.analyses:
                logger.warning(
                    "LLM returned no analyses for article %s table %s",
                    article_hash,
                    table_key,
                )

            collection = self._build_collection(
                parsed_output,
                table,
                identifier,
                sanitized_table_id,
                table_key,
                article_hash,
            )
            results[table_key] = collection
            emit_progress(progress_hook)

        return results

    def _build_collection(
        self,
        parsed_output: ParseAnalysesOutput,
        table: ExtractedTable,
        identifier,
        sanitized_table_id: str,
        table_key: str,
        article_hash: str,
    ) -> AnalysisCollection:
        table_space = table.space or CoordinateSpace.OTHER
        collection = AnalysisCollection(
            hash_id=f"{article_hash}::{sanitized_table_id}",
            coordinate_space=table_space,
            identifier=identifier,
        )
        for idx, parsed in enumerate(parsed_output.analyses, start=1):
            coordinates = self._convert_points(
                parsed.points,
                table_space,
            )
            analysis_name = parsed.name or f"{table_key} analysis {idx}"
            analysis = Analysis(
                name=analysis_name,
                description=parsed.description,
                coordinates=coordinates,
                table_id=table_key,
                table_number=table.table_number,
                table_caption=table.caption or "",
                table_footer=table.footer or "",
                metadata={
                    "table_metadata": dict(table.metadata),
                    "sanitized_table_id": sanitized_table_id,
                },
            )
            collection.add_analysis(analysis)
        return collection

    def _convert_points(
        self,
        points: List[CoordinatePoint],
        default_space: CoordinateSpace,
    ) -> List[Coordinate]:
        coordinates: List[Coordinate] = []
        for point in points:
            space = self._coerce_space(point.space, default_space)
            statistic_value = None
            statistic_type = None
            if point.values:
                primary_value = point.values[0]
                statistic_type = primary_value.kind
                try:
                    statistic_value = (
                        float(primary_value.value) if primary_value.value is not None else None
                    )
                except (TypeError, ValueError):
                    statistic_value = None
            coordinates.append(
                Coordinate(
                    x=point.coordinates[0],
                    y=point.coordinates[1],
                    z=point.coordinates[2],
                    space=space,
                    statistic_value=statistic_value,
                    statistic_type=statistic_type,
                )
            )
        return coordinates

    def _coerce_space(
        self, space_label: Optional[str], fallback: CoordinateSpace
    ) -> CoordinateSpace:
        if not space_label:
            return fallback
        normalized = str(space_label).strip().upper()
        if normalized == "MNI":
            return CoordinateSpace.MNI
        if normalized in {"TAL", "TALAIRACH"}:
            return CoordinateSpace.TALAIRACH
        return fallback

    def _build_prompt(
        self,
        bundle: ArticleExtractionBundle,
        table: ExtractedTable,
        table_text: str,
        table_key: str,
    ) -> str:
        article_title = bundle.article_metadata.title
        article_abstract = bundle.article_metadata.abstract or ""
        table_metadata = json.dumps(table.metadata, indent=2, sort_keys=True)
        prompt = f"""
You are a neuroimaging data curation assistant.

You will receive the raw HTML or XML contents for an activation table extracted
from a published article. The goal is to identify analyses/contrasts and their
associated coordinate triplets.

Article Title: {article_title}
Article Abstract: {article_abstract}
Table ID: {table_key}
Table Number: {table.table_number}
Table Caption: {table.caption}
Table Footer: {table.footer}
Table Metadata: {table_metadata}

Instructions:
- Only treat numeric triplets drawn from the X/Y/Z columns as coordinates.
- Provide the coordinate space when possible (MNI or TAL). If unknown, leave null.
- Group coordinates under analyses/contrasts explicitly referenced in the table text.
- Return JSON strictly matching the parse_analyses schema.
- Do not invent analysis names.

Raw Table Content:
{table_text}
"""
        return prompt.strip()

    def _read_table_content(self, table: ExtractedTable) -> str:
        path = Path(table.raw_content_path)
        if not path.exists():
            raise FileNotFoundError(f"Table raw content missing: {path}")
        try:
            return path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return path.read_text(encoding="utf-8", errors="ignore")


__all__ = ["CreateAnalysesService", "sanitize_table_id"]
