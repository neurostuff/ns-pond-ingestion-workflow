"""The Crossref lookup files each article's other ids into the preprint and alternative
buckets, checking them against the catalog and the registries first.

Every case is a real work; NCBI's and Crossref's answers are recorded, the requests
carrying the placeholder email.
"""

from __future__ import annotations

import pytest
import requests
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.catalog.evidence import Evidence
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.doi_registry import DoiRegistry
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.services.id_lookup import CrossrefIDLookupService
from ingestion_workflow.services.metadata import PROVIDER_CACHE_DIRS, MetadataService
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
def test_a_published_elife_paper_keeps_its_doi_and_files_its_preprint_and_versions(
        service, catalog):
    found = _looked_up(service, Identifier(doi="10.7554/eLife.106070"))

    assert found.doi == "10.7554/elife.106070"
    assert found.preprint_dois == ("10.1101/2025.02.05.636581",)
    assert found.alternative_dois == ("10.7554/elife.106070.1", "10.7554/elife.106070.2",
                                      "10.7554/elife.106070.3")
    assert catalog.refused == []


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
    # the other preprint server's copy, and the reviewed version's own DOI: preprints, as
    # Crossref types them, and eLife's
    assert "10.31234/osf.io/edaxh_v3" in found.preprint_dois
    # and, asked in turn, that copy's earlier versions
    assert {"10.31234/osf.io/edaxh_v1", "10.31234/osf.io/edaxh_v2"} <= set(found.preprint_dois)
    assert "10.7554/elife.112197.1" in found.preprint_dois
    assert found.alternative_dois == ()
    assert server_of_filed_preprint_doi("10.7554/elife.112197.1") == "elife"


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
def test_a_pubmed_doi_the_registry_gives_another_paper_is_not_filed(service, catalog):
    # PubMed gives the ocular lymphoma review 10.14670/HH-25-013, which doi.org gives a
    # parotid study.
    found = _looked_up(service, Identifier(pmid="41221578"))

    assert found.doi is None
    assert [(kind, value) for kind, value, _, _ in catalog.refused] == [
        ("doi", "10.14670/hh-25-013")]
    assert "does not give that paper" in catalog.refused[0][3]


# -- collisions ---------------------------------------------------------------------------------

@pytest.mark.vcr()
def test_an_id_another_article_holds_is_left_there_and_recorded(service, catalog):
    preprint = catalog.register(Identifier(doi="10.1101/2024.05.08.593260")).id

    found = _looked_up(service, Identifier(pmid="41799680", pmcid="PMC12964139",
                                           doi="10.1162/imag.a.1156"))

    assert found.preprint_dois == ()
    assert [(kind, value, why) for kind, value, _, why in catalog.refused] == [
        ("doi", "10.1101/2024.05.08.593260", f"{preprint} holds it")]


@pytest.mark.vcr()
def test_a_preprint_names_the_version_of_the_doi_the_article_holds(service, catalog):
    # bioRxiv's preprint is-preprint-of the .3 version; the article holds eLife's base DOI.
    found = _looked_up(service, Identifier(doi="10.7554/elife.92563",
                                           preprint_dois=["10.1101/2023.09.27.559792"]))

    assert found.doi == "10.7554/elife.92563"
    assert "10.7554/elife.92563.3" in found.alternative_dois
    assert catalog.refused == []


@pytest.mark.vcr()
def test_a_preprint_of_another_part_is_not_a_second_doi_of_the_work(service, catalog):
    # Part 1 and Part 2 of one series, each with a bioRxiv preprint of its own.
    part_1 = "10.1016/j.actbio.2021.09.033"
    part_2 = "10.1016/j.actbio.2021.11.031"
    found = _looked_up(service, Identifier(doi=part_1,
                                           preprint_dois=["10.1101/2021.06.04.447120"]))

    assert found.doi == part_1
    assert part_2 not in found.alternative_dois
    assert [(kind, value) for kind, value, _, _ in catalog.refused] == [("doi", part_2)]
    assert "do not give it and the article's" in catalog.refused[0][3]


# -- Crossmark updates ------------------------------------------------------------------------

@pytest.mark.vcr()
def test_a_new_version_names_the_versions_it_replaces(service):
    found = _looked_up(service, Identifier(doi="10.12688/f1000research.141553.3"))

    assert {"10.12688/f1000research.141553.1",
            "10.12688/f1000research.141553.2"} <= set(found.alternative_dois)


@pytest.mark.vcr()
def test_a_correction_is_a_notice_of_another_work_and_is_recorded(service, catalog):
    notice = "10.1074/jbc.aac119.012166"
    found = _looked_up(service, Identifier(doi=notice))

    assert "10.1074/jbc.ra119.010298" not in found.alternative_dois
    assert ("doi", "10.1074/jbc.ra119.010298") in [(k, v) for k, v, _, _ in catalog.refused]


@pytest.mark.vcr()
def test_a_retraction_s_update_of_its_own_work_is_not_another_work(service, catalog):
    found = _looked_up(service, Identifier(doi="10.1016/j.nbd.2012.05.020"))

    assert found.doi == "10.1016/j.nbd.2012.05.020"
    assert found.alternative_dois == ()
    assert catalog.refused == []


# -- one DOI of the work, or another -------------------------------------------------------------

@pytest.mark.vcr()
def test_a_doi_the_registries_give_the_same_work_is_an_alternative(service, catalog):
    # the Journal of Criminal Justice Research's record has-version the Criminology
    # Preprint Server's copy under another prefix, typed a journal article
    found = _looked_up(service, Identifier(doi="10.1186/s40352-024-00291-8"))

    assert found.alternative_dois == ("10.21428/cb6ab371.153bd084",)
    assert ("doi", "10.21428/cb6ab371.153bd084",
            "a second DOI of 10.1186/s40352-024-00291-8's work") in service.resolved
    assert catalog.refused == []


@pytest.mark.vcr()
def test_a_related_doi_the_registries_give_another_work_is_not_filed(service, catalog):
    # a book-review column's record has-version the review of a book
    found = _looked_up(service, Identifier(doi="10.1524/srsr.1992.15.1.43"))

    assert found.alternative_dois == ()
    assert [(kind, value) for kind, value, _, _ in catalog.refused] == [
        ("doi", "10.20378/irb-56286")]
    assert "do not give it and the article's" in catalog.refused[0][3]


@pytest.mark.vcr()
def test_a_preprint_server_s_copy_and_a_journal_typed_copy_are_filed_apart(service):
    found = _looked_up(service, Identifier(doi="10.1186/s40163-021-00151-y"))

    assert found.doi == "10.1186/s40163-021-00151-y"
    assert found.preprint_dois == ("10.31235/osf.io/3jfwu",)
    assert found.alternative_dois == ("10.21428/cb6ab371.bedeec7e",)


@pytest.mark.vcr()
def test_posted_content_that_is_not_a_preprint_is_not_filed_as_one(service):
    found = _looked_up(service, Identifier(doi="10.32920/30757796"))

    assert found.doi == "10.32920/30757796"
    assert found.preprint_dois == ()


# -- PubMed against the article the catalog holds ------------------------------------------------

@pytest.mark.vcr()
def test_a_refiled_pmid_is_held_by_the_catalog_as_the_preprint_s(service, catalog):
    article = catalog.register(Identifier(pmid="38013996", pmcid="PMC10680589")).id

    _looked_up(service, catalog.ref(article).identifier)

    assert catalog.aliases(article, "pmid") == []
    assert catalog.aliases(article, "preprint_pmid") == ["38013996"]
    assert catalog.aliases(article, "pmcid") == []
    assert catalog.aliases(article, "preprint_pmcid") == ["PMC10680589"]
    assert catalog.aliases(article, "preprint_doi") == ["10.1101/2023.11.12.566592"]
    # and the paper Crossref relates the preprint to is its version of record
    assert catalog.aliases(article, "doi") == ["10.1126/sciadv.adn4202"]
    assert catalog.refused == []


@pytest.mark.vcr()
def test_a_doi_crossref_types_a_preprint_is_held_by_the_catalog_as_the_preprint_s(
        service, catalog):
    # an eLife reviewed preprint with no version of record, held as `doi` by a catalog that
    # could not tell it from a published eLife paper
    article = catalog.register(Identifier(doi="10.7554/elife.112197")).id

    _looked_up(service, catalog.ref(article).identifier)

    assert catalog.aliases(article, "doi") == []
    assert {"10.7554/elife.112197", "10.7554/elife.112197.1"} <= set(
        catalog.aliases(article, "preprint_doi"))
    assert catalog.aliases(article, "alternative_doi") == []
    assert catalog.identifier(article).publication_status == "preprint"
    assert catalog.refused == []


@pytest.mark.vcr()
def test_a_pmcid_that_is_not_the_preprint_s_stays_the_article_s(service):
    found = _looked_up(service, Identifier(pmid="38013996", pmcid="PMC12964139"))

    assert found.preprint_pmids == ("38013996",)
    assert found.pmcid == "PMC12964139"
    assert found.preprint_pmcids == ()


@pytest.mark.vcr()
def test_a_stored_record_of_another_pmid_is_not_taken_for_this_one(service, settings):
    # the stage's cache of the article's slug holds Brain's 11335702, not the preprint
    other = Identifier(pmid="11335702")
    stored = MetadataService(settings)._get_pubmed_metadata_cached([other])
    cache = settings.get_cache_dir("metadata") / PROVIDER_CACHE_DIRS["pubmed"]
    (cache / "38013996.json").write_text((cache / "11335702.json").read_text(),
                                         encoding="utf-8")
    assert stored

    found = _looked_up(service, Identifier(pmid="38013996"))

    assert found.preprint_pmids == ("38013996",)
    assert found.doi != "10.1093/brain/124.5.1003"


@pytest.mark.vcr()
def test_a_stored_record_gives_the_spelling_of_a_doi_the_other_side_is_asked_by(
        service, settings):
    # the preprint's record names the paper "10.1162/IMAG.a.1156"; the catalog's DOI is lower
    # case, and the spelling is only in PubMed's record, which the stage stored and NCBI is not
    # asked for
    identifier = Identifier(pmid="41799680", pmcid="PMC12964139", doi="10.1162/imag.a.1156")
    MetadataService(settings)._get_pubmed_metadata_cached([identifier])

    found = _looked_up(service, identifier)

    assert found.preprint_dois == ("10.1101/2024.05.08.593260",)


@pytest.mark.vcr()
def test_pubmed_s_doi_is_not_filed_over_the_one_the_article_holds(service, catalog):
    # 20885123: mEDRA's DOI, which the article holds, then Springer's, which PubMed gives
    found = _looked_up(service, Identifier(pmid="20885123", doi="10.3275/7271"))

    assert found.doi == "10.3275/7271"
    assert found.alternative_dois == ()
    [(kind, value, _, why)] = catalog.refused
    assert (kind, value) == ("doi", "10.1007/bf03337758")
    assert "not the article's 10.3275/7271" in why and "pmid-doi" in why


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_another_article_holds_is_dropped(service, catalog):
    holder = catalog.register(Identifier(doi="10.1016/j.biopsych.2011.09.005")).id

    found = _looked_up(service, Identifier(pmid="22015111"))

    assert found.pmid is None
    assert [(kind, value, why) for kind, value, _, why in catalog.refused] == [
        ("pmid", "22015111",
         f"PubMed gives it 10.1016/j.biopsych.2011.09.005, which {holder} holds")]


@pytest.mark.vcr()
def test_a_pubmed_doi_the_registry_cannot_read_is_not_taken_for_the_paper_s(service, catalog):
    # doi.org sends 10.7507/1001-5515.201511032 to an agency over plain HTTP
    found = _looked_up(service, Identifier(pmid="29745568"))

    assert found.doi is None
    [(kind, value, _, why)] = catalog.refused
    assert (kind, value) == ("doi", "10.7507/1001-5515.201511032")
    assert "cannot say whether that is its paper" in why


@pytest.mark.vcr()
def test_a_pubmed_doi_the_article_holds_is_not_asked_of_the_registry(service):
    # a registry request for 10.1162/imag.a.1156 would have no recording to play
    found = _looked_up(service, Identifier(pmid="41799680", doi="10.1162/imag.a.1156"))

    assert found.doi == "10.1162/imag.a.1156"
    assert found.preprint_dois == ("10.1101/2024.05.08.593260",)


# -- a batch -------------------------------------------------------------------------------------

@pytest.mark.vcr()
def test_an_id_two_articles_of_one_batch_are_given_goes_to_neither(service, catalog):
    paper = Identifier(pmid="41799680", doi="10.1162/imag.a.1156")
    preprint = Identifier(doi="10.1101/2024.05.08.593260")

    service.find_identifiers(Identifiers([paper, preprint]))

    assert paper.preprint_dois == () and preprint.doi is None
    assert sorted(value for _, value, _, _ in catalog.refused) == [
        "10.1101/2024.05.08.593260", "10.1162/imag.a.1156"]
    assert all("in this batch, holds it" in why for _, _, _, why in catalog.refused)


@pytest.mark.vcr()
def test_an_article_one_failing_request_leaves_does_not_stop_the_batch(
        service, catalog, monkeypatch):
    work = CrossrefClient.work

    def fail_for_one(client, doi):
        if doi == "10.1162/imag.a.1156":
            raise requests.ConnectionError("Crossref is unreachable")
        return work(client, doi)
    monkeypatch.setattr(CrossrefClient, "work", fail_for_one)
    unreachable = Identifier(pmid="41799680", doi="10.1162/imag.a.1156")
    reachable = Identifier(doi="10.7554/eLife.106070")

    service.find_identifiers(Identifiers([unreachable, reachable]))

    assert unreachable.preprint_dois == ()
    assert reachable.preprint_dois == ("10.1101/2025.02.05.636581",)
    assert not catalog.was_looked_up(catalog.resolve(unreachable).id, "crossref")
    assert catalog.was_looked_up(catalog.resolve(reachable).id, "crossref")


@pytest.mark.vcr()
def test_an_article_crossref_was_asked_of_is_not_asked_again(service, catalog, monkeypatch):
    sent = []
    send = requests.Session.send
    monkeypatch.setattr(requests.Session, "send",
                        lambda self, request, **kw: sent.append(request.url) or send(
                            self, request, **kw))
    article = catalog.register(Identifier(doi="10.7554/eLife.106070")).id
    _looked_up(service, catalog.ref(article).identifier)
    asked = len(sent)

    # a catalog opened again has no answers in hand, only what it was told
    with PubMedClient(email=EMAIL) as pubmed:
        registry = DoiRegistry(CrossrefClient(EMAIL))
        with Catalog.open(catalog.root, evidence=Evidence(pubmed, registry)) as again:
            found = _looked_up(CrossrefIDLookupService(service.settings, again),
                               again.ref(article).identifier)
        registry.close()

    assert asked > 0 and len(sent) == asked
    assert found.preprint_dois == ("10.1101/2025.02.05.636581",)
