"""Registration refuses ids that would put a second paper on an article, and reports them.

The papers are from the production catalog's conflicts: og3a4u5wsy26 held a Biol
Psychiatry paper (PMID 22015111) and a Neurobiol Learn Mem one (PMID 21704176).
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.tests import cassette_secrets

BIOPSYCH = ("22015111", "10.1016/j.biopsych.2011.09.005")
NLM = ("21704176", "10.1016/j.nlm.2011.06.006")


@pytest.fixture()
def catalog(tmp_path):
    with Catalog.open(tmp_path / "cat") as cat:
        yield cat


@pytest.fixture()
def pubmed():
    with PubMedClient(email=cassette_secrets.REPLAY_EMAIL) as client:
        yield client


@pytest.fixture()
def checked(tmp_path, pubmed):
    with Catalog.open(tmp_path / "cat", pubmed_dois=pubmed.dois) as cat:
        yield cat


# -- one journal DOI per article ----------------------------------------------------

def test_a_second_journal_doi_is_reported_not_added(catalog):
    a = catalog.register(Identifier(pmid=BIOPSYCH[0], doi=BIOPSYCH[1])).id

    ref = catalog.register(Identifier(pmid=BIOPSYCH[0], doi=NLM[1]))

    assert ref.id == a
    assert catalog.aliases(a, "doi") == [BIOPSYCH[1]]
    assert catalog.conflicts == 1
    assert catalog.resolve(Identifier(doi=NLM[1])) is None


def test_a_preprint_doi_joins_the_journal_doi(catalog):
    a = catalog.register(Identifier(pmid="37961286", doi="10.1016/j.neuroimage.2023.120450")).id
    catalog.register(Identifier(pmid="37961286", doi="10.1101/2023.10.21.563317"))
    assert sorted(catalog.aliases(a, "doi")) == ["10.1016/j.neuroimage.2023.120450",
                                                 "10.1101/2023.10.21.563317"]
    assert catalog.conflicts == 0


def test_the_same_journal_doi_again_is_no_conflict(catalog):
    a = catalog.register(Identifier(doi=BIOPSYCH[1])).id
    assert catalog.register(Identifier(doi=BIOPSYCH[1].upper(), pmid=BIOPSYCH[0])).id == a
    assert catalog.conflicts == 0


def test_two_articles_with_different_journal_dois_are_not_merged(catalog):
    a = catalog.register(Identifier(doi=BIOPSYCH[1])).id
    b = catalog.register(Identifier(pmcid="PMC3189437", doi=NLM[1])).id

    ref = catalog.register(Identifier(pmcid="PMC3189437", doi=BIOPSYCH[1]))

    assert ref.id == b
    assert catalog.live(a) and catalog.live(b)
    assert catalog.aliases(b, "doi") == [NLM[1]]
    assert catalog.conflicts == 2  # the merge refused, the DOI left with its holder


# -- a PMID is the article its PubMed DOI names --------------------------------------

@pytest.mark.vcr()
def test_pubmed_gives_each_pmids_doi_and_leaves_out_what_it_lacks(pubmed):
    # 40359377 was on rllqpyia5alz; PubMed has no record of it.
    assert pubmed.dois([BIOPSYCH[0], NLM[0], "40359377"]) == {
        BIOPSYCH[0]: BIOPSYCH[1], NLM[0]: NLM[1]}


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_is_another_articles_is_reported_not_added(checked):
    nlm = checked.register(Identifier(doi=NLM[1])).id
    biopsych = checked.register(Identifier(doi=BIOPSYCH[1])).id

    # a base study pairing the Biol Psychiatry DOI with the other paper's PMID
    ref = checked.register(Identifier(doi=BIOPSYCH[1], pmid=NLM[0]))

    assert ref.id == biopsych
    assert checked.aliases(biopsych, "pmid") == []
    assert checked.aliases(nlm, "pmid") == []
    assert checked.conflicts == 1


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_is_the_records_own_is_added(checked):
    biopsych = checked.register(Identifier(doi=BIOPSYCH[1])).id
    assert checked.register(Identifier(doi=BIOPSYCH[1], pmid=BIOPSYCH[0])).id == biopsych
    assert checked.aliases(biopsych, "pmid") == [BIOPSYCH[0]]
    assert checked.conflicts == 0


@pytest.mark.vcr()
def test_a_pmid_is_not_put_on_a_new_article_beside_an_unknown_doi(checked):
    nlm = checked.register(Identifier(doi=NLM[1])).id

    ref = checked.register(Identifier(doi="10.1016/j.biopsych.2021.02.423", pmid=NLM[0]))

    assert ref.id != nlm
    assert checked.identifier(ref.id).pmid is None
    assert checked.conflicts == 1


def test_a_pmid_alone_or_already_held_is_not_asked_about(checked):
    # No cassette: a request to PubMed fails the test under --block-network.
    a = checked.register(Identifier(pmid=NLM[0])).id
    assert checked.register(Identifier(pmid=NLM[0], doi=NLM[1])).id == a
    assert checked.conflicts == 0
