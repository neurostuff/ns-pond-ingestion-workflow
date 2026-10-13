import pytest
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.extractors import ace_extractor
from ingestion_workflow.tests.oa_hosts import OaHosts


@pytest.fixture(autouse=True)
def readability_available(monkeypatch):
    """ACE refuses to extract without a working readabilipy; tests stub ACE."""
    monkeypatch.setattr(ace_extractor, "_READABILITY_OK", True)


@pytest.fixture(autouse=True)
def node_check_skipped(request, monkeypatch):
    """Extractor tests do not need a real node; the node tests call it directly."""
    if "node" not in request.node.name:
        monkeypatch.setattr(ace_extractor, "prepare_node", lambda settings: None)


@pytest.fixture
def stub(monkeypatch):
    """OpenAlex, Semantic Scholar and the file hosts, served locally."""
    server = OaHosts()
    monkeypatch.setattr(OpenAlexClient, "BASE_URL", server.url)
    monkeypatch.setattr(SemanticScholarClient, "BASE_URL", server.url + "/graph/v1/paper/batch")
    yield server
    server.close()
