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
        # An upper-case prefix is the same prefix.
        "10.1016/J.CORTEX.2020.01.001",
    ],
)
def test_elsevier_is_asked_for_its_own_and_its_imprints_dois(extractor, doi):
    assert extractor.can_address(Identifier(doi=doi))


@pytest.mark.parametrize(
    "doi",
    ["10.1145/3290605.3300233", "10.5281/zenodo.1234567", "10.1021/acs.jpcb.0c01234"],
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
    prefixes Elsevier has served; the cassette is the copy it was taken from."""
    mailto = os.environ.get("EMAIL")
    response = requests.get(
        CROSSREF_ELSEVIER, params={"mailto": mailto} if mailto else {}, timeout=30
    )
    response.raise_for_status()
    message = response.json()["message"]

    assert message["primary-name"] == "Elsevier BV"
    assert set(message["prefixes"]) <= ELSEVIER_DOI_PREFIXES
