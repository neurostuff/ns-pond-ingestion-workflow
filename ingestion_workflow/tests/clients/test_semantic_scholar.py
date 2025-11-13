import pytest

from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.models.ids import Identifier, Identifiers


@pytest.mark.vcr()
def test_semantic_scholar_client_enriches_from_doi(manifest_identifiers, semantic_scholar_api_key):
    original = manifest_identifiers.identifiers[0]
    identifiers = Identifiers(
        [
            Identifier(
                neurostore=original.neurostore,
                doi=original.doi,
            )
        ]
    )

    client = SemanticScholarClient(api_key=semantic_scholar_api_key)
    client.get_ids("doi", identifiers)

    enriched = identifiers[0]
    assert enriched.pmid == original.pmid
    assert enriched.doi == original.doi
    assert enriched.other_ids is not None
    assert enriched.other_ids.get("semantic_scholar_paper_id")
    assert enriched.other_ids.get("semantic_scholar_corpus_id")
