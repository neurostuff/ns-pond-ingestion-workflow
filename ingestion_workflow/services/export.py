"""Export utilities for writing bundle artifacts to disk."""

from __future__ import annotations

import logging
from typing import Sequence

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    ArticleDirectory,
    ArticleExtractionBundle,
    CreateAnalysesResult,
)


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
                bundle.article_data.slug,
            )
            return

        article_dir = ArticleDirectory.from_bundle(bundle, analyses or [])
        export_root = self.settings.data_root / "export"
        export_root.mkdir(parents=True, exist_ok=True)
        article_dir.save(export_root, overwrite=self.overwrite)


__all__ = ["ExportService"]
