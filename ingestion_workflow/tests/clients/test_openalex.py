import pytest
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.models.ids import Identifier, Identifiers


@pytest.mark.vcr()
def test_openalex_client_enriches_pmids(manifest_identifiers):
    original = manifest_identifiers[0]
    identifiers = Identifiers(
        [
            Identifier(
                neurostore=original.neurostore,
                pmid=original.pmid,
                doi=original.doi,
                pmcid=original.pmcid,
            )
        ]
    )

    client = OpenAlexClient(email="test@example.com")
    client.get_ids("pmid", identifiers)

    enriched = identifiers[0]
    assert enriched.other_ids is not None
    expected_openalex = "https://openalex.org/W2954579665"
    assert enriched.other_ids.get("openalex") == expected_openalex
