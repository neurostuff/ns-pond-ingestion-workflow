import pytest

from ingestion_workflow.extractors import ace_extractor


@pytest.fixture(autouse=True)
def readability_available(monkeypatch):
    """ACE refuses to extract without a working readabilipy; tests stub ACE."""
    monkeypatch.setattr(ace_extractor, "_READABILITY_OK", True)


@pytest.fixture(autouse=True)
def node_check_skipped(request, monkeypatch):
    """Extractor tests do not need a real node; the node tests call it directly."""
    if "node" not in request.node.name:
        monkeypatch.setattr(ace_extractor, "prepare_node", lambda settings: None)
