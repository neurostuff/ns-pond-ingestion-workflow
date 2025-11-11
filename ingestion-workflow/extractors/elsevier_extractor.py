"""Download and Extract tables from articles using Elsevier."""

from ingestion_workflow.extractors.base import BaseExtractor

from ingestion_workflow.models import Identifiers, DownloadResult, ExtractionResult


class ElsevierExtractor(BaseExtractor):
    """Extractor that uses Elsevier to download and extract tables from articles."""

    def download(self, identifiers):
        """Download articles using Elsevier."""
        raise NotImplementedError("ElsevierExtractor download method not implemented.")

    def extract(self, download_result):
        """Extract tables from downloaded articles using Elsevier."""
        raise NotImplementedError("ElsevierExtractor extract method not implemented.")
