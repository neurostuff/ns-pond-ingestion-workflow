import os

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


#: The title of the pubget fixture article, PMC9056519.
TITLE = (
    "Preliminary findings demonstrating latent effects of early adolescent marijuana use "
    "onset on cortical architecture"
)


@pytest.mark.vcr()
def test_a_malformed_pmcid_drops_only_its_own_paper(caplog):
    """Recorded from the paper batch endpoint, then edited: the second paper's
    `PubMedCentral` was changed from 3407125 to 3407125x."""
    identifiers = Identifiers(
        [Identifier(doi="10.1002/hbm.24703"), Identifier(doi="10.1371/journal.pone.0041873")]
    )
    # the key header is dropped from the cassette; any value replays
    SemanticScholarClient(os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay")).get_ids(
        "doi", identifiers
    )
    sound, malformed = identifiers.identifiers
    assert (malformed.pmid, malformed.pmcid) == (None, None)
    assert sound.pmid == "31268615"
    assert "not a PMCID: '3407125x'; its ids not used" in caplog.text


@pytest.mark.vcr()
def test_a_title_match_with_a_malformed_pmcid_is_not_returned(caplog):
    """Recorded from the title match endpoint, then edited: the paper's `PubMedCentral`
    was changed from 4691364 to 4691364x."""
    client = SemanticScholarClient(os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"))
    assert client.search_title(TITLE) == []
    assert "not a PMCID: '4691364x'; its ids not used" in caplog.text


@pytest.mark.vcr()
def test_a_bulk_search_passes_over_a_paper_with_a_malformed_pmcid(caplog):
    """Recorded from the bulk search, then edited: the one paper found had its
    `PubMedCentral` changed from 4691364 to 4691364x, and the two 429 answers before it
    were removed."""
    client = SemanticScholarClient(os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"))
    assert client.search_bulk({"query": f'"{TITLE}"'}).identifiers == []
    assert "not a PMCID: '4691364x'; its ids not used" in caplog.text
