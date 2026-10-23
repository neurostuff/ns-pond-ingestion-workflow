"""The Crossref lookup files each article's other ids into the preprint and alternative
buckets, checking them against the catalog and the registries first.

Every case is a real work; NCBI's and Crossref's answers are recorded, the requests
carrying the placeholder email.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.catalog.evidence import Evidence
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.doi_registry import DoiRegistry
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.services.id_lookup import CrossrefIDLookupService
from ingestion_workflow.services.metadata import MetadataService
from ingestion_workflow.tests import cassette_secrets
from ingestion_workflow.utils.preprints import server_of_filed_preprint_doi

EMAIL = cassette_secrets.REPLAY_EMAIL


@pytest.fixture()
def settings(tmp_path):
    return Settings(cache_root=tmp_path / "cache", data_root=tmp_path / "data",
                    catalog_root=tmp_path / "catalog", pubmed_email=EMAIL)


@pytest.fixture()
def catalog(tmp_path):
    with PubMedClient(email=EMAIL) as pubmed:
        registry = DoiRegistry(CrossrefClient(EMAIL))
        with Catalog.open(tmp_path / "checked", evidence=Evidence(pubmed, registry)) as cat:
            yield cat
        registry.close()


@pytest.fixture()
def service(settings, catalog):
    return CrossrefIDLookupService(settings, catalog)


def _looked_up(service, identifier: Identifier) -> Identifier:
    service.find_identifiers(Identifiers([identifier]))
    return identifier


# -- eLife: a published paper, and a reviewed preprint that is only a preprint ------------

@pytest.mark.vcr()
def test_a_published_elife_paper_keeps_its_doi_and_files_its_preprint_and_versions(service):
    found = _looked_up(service, Identifier(doi="10.7554/eLife.106070"))

    assert found.doi == "10.7554/elife.106070"
    assert found.preprint_dois == ("10.1101/2025.02.05.636581",)
    assert found.alternative_dois == ("10.7554/elife.106070.1", "10.7554/elife.106070.2",
                                      "10.7554/elife.106070.3")
    assert service.omitted == []


@pytest.mark.vcr()
def test_an_elife_version_doi_finds_the_base_doi_the_version_does_not_name(service):
    # Crossref's base record says is-same-as the .3 version; the version's does not say so back.
    found = _looked_up(service, Identifier(doi="10.7554/elife.106070.3"))

    assert found.doi == "10.7554/elife.106070.3"
    assert "10.7554/elife.106070" in found.alternative_dois
    assert found.preprint_dois == ("10.1101/2025.02.05.636581",)


@pytest.mark.vcr()
def test_an_elife_reviewed_preprint_is_a_preprint_by_its_crossref_type(service):
    found = _looked_up(service, Identifier(doi="10.7554/elife.112197"))

    assert found.doi is None
    assert found.publication_status == "preprint"
    assert "10.7554/elife.112197" in found.preprint_dois
    assert server_of_filed_preprint_doi("10.7554/elife.112197") == "elife"
    # the other preprint server's copy, and the reviewed version's own DOI
    assert "10.31234/osf.io/edaxh_v3" in found.preprint_dois
    assert found.alternative_dois == ("10.7554/elife.112197.1",)


# -- a relation one side deposited ---------------------------------------------------------

@pytest.mark.vcr()
def test_a_preprint_finds_its_paper_the_paper_names_and_it_does_not(service):
    # Schizophr Bull's record says has-preprint; bioRxiv's says nothing.
    found = _looked_up(service, Identifier(doi="10.1101/2020.05.06.058792"))

    assert found.doi == "10.1093/schbul/sbaa075"
    assert found.preprint_dois == ("10.1101/2020.05.06.058792",)
    assert found.publication_status == "published"


@pytest.mark.vcr()
def test_a_paper_finds_its_preprint_by_the_spelling_pubmed_gives(service):
    # bioRxiv's record says is-preprint-of "10.1162/IMAG.a.1156", which Crossref matches by
    # case; the paper's record says nothing back, and the catalog's spelling is lower case.
    found = _looked_up(service, Identifier(pmid="41799680", pmcid="PMC12964139",
                                           doi="10.1162/imag.a.1156"))

    assert found.doi == "10.1162/imag.a.1156"
    assert found.preprint_dois == ("10.1101/2024.05.08.593260",)


# -- PubMed ----------------------------------------------------------------------------------

@pytest.mark.vcr()
def test_a_preprint_typed_pubmed_record_is_filed_as_the_preprint_s(service):
    found = _looked_up(service, Identifier(pmid="38013996", pmcid="PMC10680589",
                                           doi="10.1101/2023.11.12.566592"))

    assert found.pmid is None and found.pmcid is None
    assert found.preprint_pmids == ("38013996",)
    assert found.preprint_pmcids == ("PMC10680589",)
    assert "10.1101/2023.11.12.566592" in found.preprint_dois


@pytest.mark.vcr()
def test_the_metadata_stage_s_stored_pubmed_record_is_used_instead_of_asking_ncbi(
        service, settings):
    identifier = Identifier(pmid="38013996", pmcid="PMC10680589", doi="10.1101/2023.11.12.566592")
    # The stage's own cache of PubMed's record; the recording holds its efetch and no esummary.
    MetadataService(settings)._get_pubmed_metadata_cached([identifier])

    found = _looked_up(service, identifier)

    assert found.preprint_pmids == ("38013996",)


@pytest.mark.vcr()
def test_a_journal_record_s_doi_is_the_article_s_doi(service):
    found = _looked_up(service, Identifier(pmid="11335702"))

    assert found.doi == "10.1093/brain/124.5.1003"


@pytest.mark.vcr()
def test_a_pubmed_doi_the_registry_gives_another_paper_is_not_filed(service):
    # PubMed gives the ocular lymphoma review 10.14670/HH-25-013, which doi.org gives a
    # parotid study.
    found = _looked_up(service, Identifier(pmid="41221578"))

    assert found.doi is None
    assert [(kind, value) for kind, value, _, _ in service.omitted] == [
        ("doi", "10.14670/hh-25-013")]
    assert "does not give that paper" in service.omitted[0][3]


# -- collisions ---------------------------------------------------------------------------------

@pytest.mark.vcr()
def test_an_id_another_article_holds_is_left_there_and_recorded(service, catalog):
    preprint = catalog.register(Identifier(doi="10.1101/2024.05.08.593260")).id

    found = _looked_up(service, Identifier(pmid="41799680", pmcid="PMC12964139",
                                           doi="10.1162/imag.a.1156"))

    assert found.preprint_dois == ()
    assert [(kind, value, why) for kind, value, _, why in service.omitted] == [
        ("doi", "10.1101/2024.05.08.593260", f"{preprint} holds it")]
    assert catalog.refused == []


@pytest.mark.vcr()
def test_a_second_doi_of_one_work_is_filed_as_an_alternative(service):
    # bioRxiv's preprint is-preprint-of the .3 version; the article holds eLife's base DOI.
    found = _looked_up(service, Identifier(doi="10.7554/elife.92563",
                                           preprint_dois=["10.1101/2023.09.27.559792"]))

    assert found.doi == "10.7554/elife.92563"
    assert "10.7554/elife.92563.3" in found.alternative_dois
    assert ("doi", "10.7554/elife.92563.3",
            "a second journal DOI of 10.7554/elife.92563's work") in service.resolved
    assert service.omitted == []


@pytest.mark.vcr()
def test_a_preprint_of_another_part_is_not_a_second_doi_of_the_work(service):
    # Part 1 and Part 2 of one series, each with a bioRxiv preprint of its own.
    part_1 = "10.1016/j.actbio.2021.09.033"
    part_2 = "10.1016/j.actbio.2021.11.031"
    found = _looked_up(service, Identifier(doi=part_1,
                                           preprint_dois=["10.1101/2021.06.04.447120"]))

    assert found.doi == part_1
    assert part_2 not in found.alternative_dois
    assert [(kind, value) for kind, value, _, _ in service.omitted] == [("doi", part_2)]
    assert "do not give it and the article's" in service.omitted[0][3]


# -- Crossmark updates ------------------------------------------------------------------------

@pytest.mark.vcr()
def test_a_new_version_names_the_versions_it_replaces(service):
    found = _looked_up(service, Identifier(doi="10.12688/f1000research.141553.3"))

    assert {"10.12688/f1000research.141553.1",
            "10.12688/f1000research.141553.2"} <= set(found.alternative_dois)


@pytest.mark.vcr()
def test_a_correction_is_a_notice_of_another_work_and_is_recorded(service):
    notice = "10.1074/jbc.aac119.012166"
    found = _looked_up(service, Identifier(doi=notice))

    assert "10.1074/jbc.ra119.010298" not in found.alternative_dois
    assert ("doi", "10.1074/jbc.ra119.010298") in [(k, v) for k, v, _, _ in service.omitted]
