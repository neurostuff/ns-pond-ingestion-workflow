"""Download and Extract tables from articles using ACE."""

from ingestion_workflow.extractors.base import BaseExtractor

from ingestion_workflow.models import Identifiers, DownloadResult, ExtractionResult


class ACEExtractor(BaseExtractor):
    """Extractor that uses ACE to download and extract tables from articles."""

    def download(self, identifiers):
        """Download articles using ACE."""
        raise NotImplementedError("ACEExtractor download method not implemented.")

    def extract(self, download_result):
        """Extract tables from downloaded articles using ACE."""
        raise NotImplementedError("ACEExtractor extract method not implemented.")
