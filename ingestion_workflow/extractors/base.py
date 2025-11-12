from typing import List
from ingestion_workflow.models import (
    DownloadResult,
    ExtractionResult,
    Identifiers,
)


class BaseExtractor:
    """Shared interface for extractor implementations."""

    def download(self, identifiers: Identifiers) -> List[DownloadResult]:
        raise NotImplementedError("Subclasses must implement this method.")

    def extract(self, download_result: List[DownloadResult]) -> List[ExtractionResult]:
        raise NotImplementedError("Subclasses must implement this method.")
