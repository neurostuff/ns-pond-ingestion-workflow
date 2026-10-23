import re

import pytest
from vcr.filters import replace_query_parameters
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


@pytest.mark.vcr()
def test_a_configured_email_and_tool_are_sent():
    identifiers = Identifiers([Identifier(doi=TWO_DOIS[1])])
    PubMedClient(email="someone@example.com", tool="ingestion-workflow-tests").get_ids(
        "doi", identifiers
    )
    assert identifiers.identifiers[0].pmid == "22848644"


def _keep_only_the_empty_name(key, value, request):
    return value if value == "EMPTY" else None


def _show_empty_parameters(request):
    """VCR's query matcher and filters drop a parameter with no value, so a bare `email=`
    would match a request without one; naming it keeps it in what is matched. The key is
    dropped here too, since a query filter would reparse the query first."""
    request.uri = re.sub(r"([?&](?:email|tool|api_key)=)(?=&|$)", r"\1EMPTY", request.uri)
    return replace_query_parameters(request, [("api_key", _keep_only_the_empty_name)])


# No query filter, for the reason above; the shared config's email placeholder is not
# applied either, so the request is matched as sent.
as_sent = pytest.mark.vcr(filter_query_parameters=[], before_record_request=_show_empty_parameters)


@as_sent
def test_an_unset_email_is_not_sent_as_an_empty_parameter():
    """Recorded with no email parameter: a request carrying a bare email= does not
    match it."""
    identifiers = Identifiers([Identifier(doi=TWO_DOIS[1])])
    PubMedClient(email="", tool="ingestion-workflow-tests").get_ids("doi", identifiers)
    assert identifiers.identifiers[0].pmid == "22848644"


@as_sent
def test_an_unset_api_key_is_not_sent_as_an_empty_parameter():
    identifiers = Identifiers([Identifier(doi=TWO_DOIS[1])])
    PubMedClient(email="someone@example.com", tool="ingestion-workflow-tests").get_ids(
        "doi", identifiers
    )
    assert identifiers.identifiers[0].pmid == "22848644"
