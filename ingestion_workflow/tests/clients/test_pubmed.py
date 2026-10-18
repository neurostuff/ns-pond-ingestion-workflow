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


#: Two articles with a DOI, PMID and PMCID each.
TWO_DOIS = ("10.1002/hbm.24703", "10.1371/journal.pone.0041873")


@pytest.mark.vcr()
def test_a_malformed_pmcid_drops_only_its_own_record(caplog):
    """Recorded from the id converter, then edited: the first record's pmcid was changed
    from PMC6865431 to PMC6865431.x."""
    identifiers = Identifiers([Identifier(doi=doi) for doi in TWO_DOIS])
    PubMedClient(email="", tool="ingestion-workflow-tests").get_ids(
        "doi", identifiers
    )
    malformed, sound = identifiers.identifiers
    assert (malformed.pmid, malformed.pmcid) == (None, None)
    assert (sound.pmid, sound.pmcid) == ("22848644", "PMC3407125")
    assert "not a PMCID: 'PMC6865431.x'" in caplog.text
