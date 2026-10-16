import os

import pytest
import requests
from ingestion_workflow.extractors.elsevier_extractor import (
    ELSEVIER_DOI_PREFIXES,
    ElsevierExtractor,
)
from ingestion_workflow.models import Identifier

CROSSREF_ELSEVIER = "https://api.crossref.org/members/78"


@pytest.fixture
def extractor(tmp_path):
    from ingestion_workflow.config import Settings

    return ElsevierExtractor(Settings(cache_root=tmp_path / "cache", data_root=tmp_path / "data"))


@pytest.mark.parametrize(
    "doi",
    [
        "10.1016/j.neuroimage.2019.01.001",
        # Imprints (Academic Press, Churchill Livingstone) with prefixes of their own.
        "10.1006/nimg.1999.0483",
        "10.1053/j.gastro.2009.01.001",
    ],
)
def test_elsevier_is_asked_for_its_own_and_its_imprints_dois(extractor, doi):
    assert extractor.can_address(Identifier(doi=doi))


#: Prefixes of other publishers' DOIs that Elsevier served in the run of 2026-10-16.
SERVED_IN_THE_RUN = [
    "10.5004", "10.1007", "10.1124", "10.1017", "10.14283", "10.1111", "10.1182",
    "10.3967", "10.26599", "10.1093", "10.24920", "10.3168", "10.24875", "10.58888",
    "10.15232",
]


@pytest.mark.parametrize("prefix", SERVED_IN_THE_RUN)
def test_a_prefix_elsevier_served_in_the_run_is_asked(extractor, prefix):
    assert extractor.can_address(Identifier(doi=f"{prefix}/x.1"))


def test_the_list_is_crossrefs_thirty_two_and_the_fifteen_served(extractor):
    assert len(ELSEVIER_DOI_PREFIXES) == 32 + len(SERVED_IN_THE_RUN) == 47


@pytest.mark.parametrize(
    "doi",
    [
        "10.1145/3290605.3300233",
        "10.5281/zenodo.1234567",
        "10.1021/acs.jpcb.0c01234",
        # Served in past years, and asked in the run without one success.
        "10.1101/2023.06.02.543242",
        "10.1186/s12888-020-02800-w",
    ],
)
def test_elsevier_is_not_asked_for_other_publishers_dois(extractor, doi):
    assert not extractor.can_address(Identifier(doi=doi))


def test_a_pubmed_id_does_not_make_another_publishers_doi_worth_asking(extractor):
    assert not extractor.can_address(Identifier(doi="10.1145/3290605.3300233", pmid="31234567"))


def test_a_doi_of_a_served_prefix_is_asked_with_or_without_a_pubmed_id(extractor):
    assert extractor.can_address(Identifier(doi="10.1007/s00429-019-01900-1", pmid="31234567"))
    assert extractor.can_address(Identifier(doi="10.1007/s00429-019-01900-1"))


def test_an_article_with_only_a_pubmed_id_is_asked(extractor):
    assert extractor.can_address(Identifier(pmid="41075610"))


def test_an_article_with_only_a_pmcid_is_not_asked(extractor):
    assert not extractor.can_address(Identifier(pmcid="PMC1234567"))


@pytest.mark.vcr(filter_query_parameters=["mailto"])
def test_every_prefix_crossref_lists_for_elsevier_is_asked():
    """The list in code is a copy of Crossref's for Elsevier BV (member 78), plus
    the prefixes it served in the run; the cassette is the copy it was taken from."""
    mailto = os.environ.get("EMAIL")
    response = requests.get(
        CROSSREF_ELSEVIER, params={"mailto": mailto} if mailto else {}, timeout=30
    )
    response.raise_for_status()
    message = response.json()["message"]

    assert message["primary-name"] == "Elsevier BV"
    assert set(message["prefixes"]) <= ELSEVIER_DOI_PREFIXES
