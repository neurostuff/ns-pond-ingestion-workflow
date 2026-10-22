import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.catalog.evidence import Evidence
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.doi_registry import DoiRegistry
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.tests import cassette_secrets


@pytest.fixture()
def pubmed():
    with PubMedClient(email=cassette_secrets.REPLAY_EMAIL) as client:
        yield client


@pytest.fixture()
def evidence(pubmed):
    """NCBI and the registries, asked with the placeholder email and recorded."""
    registry = DoiRegistry(CrossrefClient())
    yield Evidence(pubmed, registry)
    registry.close()


@pytest.fixture()
def checked(tmp_path, evidence):
    """A catalog that checks what it registers against the evidence."""
    with Catalog.open(tmp_path / "checked", evidence=evidence) as cat:
        yield cat
