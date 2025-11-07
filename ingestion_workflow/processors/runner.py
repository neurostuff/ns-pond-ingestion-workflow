"""Helpers for running processing stages over manifest entries."""
from __future__ import annotations

from typing import List

from ingestion_workflow.logging_utils import get_logger
from ingestion_workflow.models import Identifier, ProcessedPaper, RunManifest
from ingestion_workflow.storage import StorageManager

from .base import ProcessorError
from .default_processor import DefaultProcessor

logger = get_logger(__name__)


def run_processing(manifest: RunManifest, storage: StorageManager) -> List[ProcessedPaper]:
    """Process all downloaded entries recorded in the manifest."""

    processor = DefaultProcessor(storage)
    processed: List[ProcessedPaper] = []
    for payload in manifest.items.values():
        identifier_payload = payload.get("identifier")
        download_entry = payload.get("download")
        if not identifier_payload or not download_entry:
            continue
        identifier = Identifier(
            pmid=identifier_payload.get("pmid"),
            doi=identifier_payload.get("doi"),
            pmcid=identifier_payload.get("pmcid"),
            other_ids=tuple(identifier_payload.get("other_ids", [])),
        )
        source = download_entry.get("source")
        if not source:
            continue
        try:
            paper = processor.process(identifier, source)
        except ProcessorError as exc:
            logger.warning(
                "Processing failed",
                extra={"hash_id": identifier.hash_id, "source": source, "error": str(exc)},
            )
            continue
        manifest.register_processing(
            identifier,
            {
                "source": source,
                "table_count": len(paper.tables),
                "coordinate_tables": paper.coordinate_table_ids(),
                "title": paper.metadata.title,
            },
        )
        processed.append(paper)
    return processed
