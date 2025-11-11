from ingestion_workflow.models import (
    Identifiers, DownloadResult, ExtractionResult
) 

class BaseExtractor:
    def download(self, identifiers: Identifiers) -> Identifiers:
        raise NotImplementedError("Subclasses must implement this method.")
    
    def extract()
