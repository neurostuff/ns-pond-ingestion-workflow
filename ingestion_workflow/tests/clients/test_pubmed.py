import httpx
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


def _requested_query(email, **client_args):
    """The query string of the request the client builds, caught by a transport that
    answers an empty id-converter reply."""
    seen = []

    def answer(request):
        seen.append(request.url.params)
        return httpx.Response(200, json={"records": []})

    client = PubMedClient(email=email, **client_args)
    client._client = httpx.Client(transport=httpx.MockTransport(answer))
    client.get_ids("doi", Identifiers([Identifier(doi=TWO_DOIS[0])]))
    return seen[0]


def test_a_configured_email_tool_and_key_are_sent():
    query = _requested_query("someone@example.com", api_key="placeholder-key")
    assert query["email"] == "someone@example.com"
    assert query["tool"] == "ingestion-workflow"
    assert query["api_key"] == "placeholder-key"


@pytest.mark.parametrize("unset", ["", None])
def test_an_unset_email_sends_no_email_parameter(unset):
    query = _requested_query(unset, tool="")
    assert "email" not in query
    assert "tool" not in query
    assert "api_key" not in query
