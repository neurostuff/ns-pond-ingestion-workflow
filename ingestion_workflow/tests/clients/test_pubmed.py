import pytest
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.models.ids import Identifier, Identifiers


@pytest.mark.vcr()
def test_pubmed_client_enriches_from_doi(manifest_identifiers):
    original = manifest_identifiers.identifiers[0]
    identifiers = Identifiers(
        [
            Identifier(
                neurostore=original.neurostore,
                doi=original.doi,
            )
        ]
    )

    client = PubMedClient(email="test@example.com", tool="ingestion-workflow-tests")
    client.get_ids("doi", identifiers)

    enriched = identifiers[0]
    assert enriched.pmid == original.pmid
    assert enriched.pmcid == original.pmcid
    assert enriched.doi == original.doi
