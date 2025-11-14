"""Export utilities for writing bundle artifacts to disk."""

from __future__ import annotations

import logging
import shutil
from typing import Sequence

from ingestion_workflow.config import Settings
from ingestion_workflow.models import ArticleExtractionBundle, CreateAnalysesResult
from ingestion_workflow.models.export_schema import build_article_export


logger = logging.getLogger(__name__)


class ExportService:
    """Persist bundle artifacts to the configured export directory."""

    def __init__(self, settings: Settings, *, overwrite: bool = True) -> None:
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

        export_root = self.settings.data_root / "export"
        export_root.mkdir(parents=True, exist_ok=True)

        slug, export_bundle = build_article_export(
            bundle,
            analyses or (),
            settings=self.settings,
        )
        article_path = export_root / slug
        if article_path.exists():
            if not self.overwrite:
                logger.debug("Skipping export for %s; path already exists and overwrite disabled", slug)
                return
            shutil.rmtree(article_path)

        export_bundle.write(article_path, overwrite=self.overwrite)


__all__ = ["ExportService"]
