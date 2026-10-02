import pytest

from ingestion_workflow.extractors import ace_extractor


@pytest.fixture(autouse=True)
def readability_available(monkeypatch):
    """ACE refuses to extract without a working readabilipy; tests stub ACE."""
    monkeypatch.setattr(ace_extractor, "_READABILITY_OK", True)
