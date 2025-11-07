"""Default processing pipeline that builds ProcessedPaper records from downloads."""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd

from ingestion_workflow.models import (
    CoordinateSet,
    Identifier,
    MetadataRecord,
    ProcessedPaper,
    TableArtifact,
)
from ingestion_workflow.storage import StorageManager

from .base import Processor, ProcessorError
from ingestion_workflow.metadata.enrichment import MetadataEnricher

logger = logging.getLogger(__name__)


class DefaultProcessor(Processor):
    """Process downloaded artifacts into structured ProcessedPaper records."""

    name = "default"

    def __init__(self, storage: StorageManager):
        self.storage = storage
        self._enricher = MetadataEnricher(storage.settings)

    def process(self, identifier: Identifier, source: str) -> ProcessedPaper:
        paths = self.storage.paths_for(identifier)
        processed_dir = paths.processed_for(source)
        source_dir = paths.source_for(source)

        metadata = self._load_metadata(processed_dir / "metadata.json")
        metadata = self._enrich_metadata(identifier, metadata)
        coordinates_path = processed_dir / "coordinates.csv"
        coordinate_sets, coordinate_table_ids = self._load_coordinate_sets(coordinates_path)
        tables = self._load_tables(source_dir / "tables", coordinate_table_ids)
        full_text_path = self._resolve_full_text_path(source_dir)

        return ProcessedPaper(
            identifier=identifier,
            source=source,
            full_text_path=full_text_path,
            metadata=metadata,
            tables=tables,
            coordinates=coordinate_sets,
        )

    # ------------------------------------------------------------------
    def _load_metadata(self, metadata_path: Path) -> MetadataRecord:
        if not metadata_path.exists():
            raise ProcessorError(f"Metadata file missing: {metadata_path}")
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        title = payload.get("title") or "Untitled"
        authors_raw = payload.get("authors") or ""
        authors = [author.strip() for author in re.split(r";|,", authors_raw) if author.strip()]
        keywords_raw = payload.get("keywords") or ""
        keywords = [kw.strip() for kw in re.split(r"\n|;|,", keywords_raw) if kw.strip()]
        journal = payload.get("journal")
        year = payload.get("publication_year") or payload.get("year")
        if isinstance(year, str) and year.isdigit():
            year = int(year)
        abstract = payload.get("abstract")
        standard_keys = {
            "title",
            "authors",
            "keywords",
            "journal",
            "publication_year",
            "year",
            "abstract",
        }
        external_metadata = {k: v for k, v in payload.items() if k not in standard_keys}
        return MetadataRecord(
            title=title,
            authors=authors,
            journal=journal,
            year=year,
            abstract=abstract,
            keywords=keywords,
            external_metadata=external_metadata,
        )

    def _enrich_metadata(self, identifier: Identifier, metadata: MetadataRecord) -> MetadataRecord:
        metadata_payload = {
            "title": metadata.title,
            "authors": "; ".join(metadata.authors),
            "journal": metadata.journal,
            "publication_year": metadata.year,
            "abstract": metadata.abstract,
            "keywords": "; ".join(metadata.keywords),
            "external_metadata": metadata.external_metadata,
            "doi": identifier.doi,
            "pmid": identifier.pmid,
            "pmcid": identifier.pmcid,
        }
        enriched = self._enricher.enrich(identifier, metadata_payload)
        return MetadataRecord(
            title=enriched.get("title") or metadata.title,
            authors=self._split_authors(enriched.get("authors")) or metadata.authors,
            journal=enriched.get("journal") or metadata.journal,
            year=enriched.get("publication_year") or metadata.year,
            abstract=enriched.get("abstract") or metadata.abstract,
            keywords=self._split_keywords(enriched.get("keywords")) or metadata.keywords,
            external_metadata=enriched.get("external_metadata", metadata.external_metadata),
        )

    @staticmethod
    def _split_authors(value: Optional[str]) -> List[str]:
        if not value:
            return []
        return [author.strip() for author in re.split(r";|,", value) if author.strip()]

    @staticmethod
    def _split_keywords(value: Optional[str]) -> List[str]:
        if not value:
            return []
        return [kw.strip() for kw in re.split(r"\n|;|,", value) if kw.strip()]

    def _load_coordinate_sets(self, coordinates_path: Path) -> Tuple[List[CoordinateSet], Set[str]]:
        if not coordinates_path.exists():
            raise ProcessorError(f"Coordinates file missing: {coordinates_path}")
        df = pd.read_csv(coordinates_path)
        if df.empty:
            return [], set()
        table_ids: Set[str] = set()
        if "table_id" in df.columns:
            table_ids = {str(value) for value in df["table_id"].dropna().astype(str)}
        coordinate_sets = [
            CoordinateSet(
                table_name=table_id or "table",
                coordinates_csv=coordinates_path,
                extraction_metadata={"table_id": table_id},
            )
            for table_id in sorted(table_ids) if table_id
        ]
        if not coordinate_sets:
            coordinate_sets.append(
                CoordinateSet(
                    table_name="table",
                    coordinates_csv=coordinates_path,
                    extraction_metadata={},
                )
            )
        return coordinate_sets, table_ids

    def _load_tables(
        self,
        tables_dir: Path,
        coordinate_table_ids: Set[str],
    ) -> List[TableArtifact]:
        if not tables_dir.exists():
            logger.info("Tables directory missing", extra={"path": str(tables_dir)})
            return []
        artifacts: List[TableArtifact] = []
        for info_path in sorted(tables_dir.glob("table_*_info.json")):
            fallback_id = info_path.stem.replace("_info", "")
            metadata = json.loads(info_path.read_text(encoding="utf-8"))
            source_table_id = metadata.get("table_id") or fallback_id
            data_file = metadata.get("table_data_file") or f"{fallback_id}.csv"
            raw_csv = tables_dir / data_file
            artifacts.append(
                TableArtifact(
                    name=metadata.get("table_label") or metadata.get("label") or source_table_id,
                    raw_path=raw_csv if raw_csv.exists() else info_path,
                    normalized_csv_path=raw_csv if raw_csv.exists() else info_path,
                    metadata_path=info_path,
                    is_coordinate_table=source_table_id in coordinate_table_ids,
                    source_table_id=source_table_id,
                )
            )
        return artifacts

    def _resolve_full_text_path(self, source_dir: Path) -> Path:
        article_xml = source_dir / "article.xml"
        if article_xml.exists():
            return article_xml
        xml_files = sorted(source_dir.glob("*.xml"))
        if xml_files:
            return xml_files[0]
        raise ProcessorError("Full text XML not found in source directory")
