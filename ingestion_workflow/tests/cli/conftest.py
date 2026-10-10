import pytest

from ingestion_workflow.extractors import ace_extractor


@pytest.fixture(autouse=True)
def node_check_skipped(monkeypatch):
    """`ingest run` checks node when ACE is enabled; these tests do not need one."""
    monkeypatch.setattr(ace_extractor, "prepare_node", lambda settings: None)
