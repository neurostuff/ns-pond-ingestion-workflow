"""OpenAlex lookups by DOI, against recorded answers.

OpenAlex returns each work's *current* DOI as a resolver URL, so a lookup
keyed by the DOI that was asked has to survive brackets in the DOI and a
journal that has moved the article.
"""

import pytest
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.utils.doi import normalize_doi

SICI = "10.1002/(SICI)1097-0193(1998)6:3<160::AID-HBM5>3.0.CO;2-1"
ELSEVIER = "10.1016/S0924-9338(02)00676-4"
PLAIN = "10.1038/nature14539"
#: Wiley's old DOI for an article OpenAlex now lists under Cambridge's 10.1017/s0012162204001306.
MOVED = "10.1111/j.1469-8749.2004.tb00996.x"
UNKNOWN = "10.1000/no-such-work-zzz"


@pytest.fixture
def client():
    return OpenAlexClient(email="test@example.com")


@pytest.mark.parametrize(
    "value, bare",
    [
        ("https://doi.org/" + SICI, SICI.lower()),
        ("https://doi.org/" + ELSEVIER, ELSEVIER.lower()),
        ("https://doi.org/(odd)/<x>", "https://doi.org/(odd)/<x>"),
        ("http://dx.doi.org/" + ELSEVIER, ELSEVIER.lower()),
        ("HTTPS://DOI.ORG/10.1000/X", "10.1000/x"),
        ("https://doi.org/" + PLAIN, PLAIN),
        ("doi:" + SICI, SICI.lower()),
        ("DOI:10.1000/x", "10.1000/x"),
        (SICI, SICI.lower()),
        (PLAIN, PLAIN),
        ("  " + PLAIN + " ", PLAIN),
    ],
)
def test_normalize_doi_keeps_the_whole_suffix(value, bare):
    assert normalize_doi(value) == bare


@pytest.mark.parametrize("value", [None, "", "  ", "doi:"])
def test_normalize_doi_of_nothing_is_none(value):
    assert normalize_doi(value) is None


@pytest.mark.vcr
def test_retractions_of_bracketed_dois_are_keyed_by_the_bare_doi(client):
    found = client.get_retractions(["https://doi.org/" + SICI, ELSEVIER, PLAIN])
    assert found == {SICI.lower(): False, ELSEVIER.lower(): False, PLAIN: False}


@pytest.mark.vcr
def test_a_moved_doi_is_keyed_by_the_doi_that_was_asked(client):
    found = client.get_retractions([MOVED, PLAIN])
    assert found == {MOVED.lower(): False, PLAIN: False}


@pytest.mark.vcr
def test_a_doi_openalex_does_not_know_is_absent(client):
    assert client.get_retractions([UNKNOWN, PLAIN]) == {PLAIN: False}


@pytest.mark.vcr
def test_cited_works_of_a_moved_doi_are_keyed_by_the_doi_that_was_asked(client):
    found = client.get_cited_works([MOVED, SICI])
    assert set(found) == {MOVED.lower(), SICI.lower()}
    assert found[MOVED.lower()]["year"] == 2004


@pytest.mark.vcr
def test_metadata_of_a_moved_doi_is_keyed_by_the_identifier_asked_for(client):
    moved = Identifier(doi=MOVED)
    sici = Identifier(doi="https://doi.org/" + SICI)
    found = client.get_metadata([moved, sici])
    assert set(found) == {moved.slug, sici.slug}
