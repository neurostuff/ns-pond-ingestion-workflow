"""Registration refuses ids that would put a second paper on an article, and records it.

The papers are from the production catalog's conflicts: og3a4u5wsy26 held a Biol
Psychiatry paper (PMID 22015111) and a Neurobiol Learn Mem one (PMID 21704176).
NCBI and doi.org answers are recorded; requests carry the placeholder email.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.clients import pubmed as pubmed_module
from ingestion_workflow.models.ids import Identifier

BIOPSYCH = ("22015111", "10.1016/j.biopsych.2011.09.005")
NLM = ("21704176", "10.1016/j.nlm.2011.06.006")


@pytest.fixture()
def catalog(tmp_path):
    with Catalog.open(tmp_path / "cat") as cat:
        yield cat


def _refusals(catalog):
    return [tuple(r) for r in catalog._conn.execute(
        "SELECT kind, value, article_id FROM refusals ORDER BY refused_at, kind, value")]


# -- one journal DOI per article ----------------------------------------------------

def test_a_second_journal_doi_is_refused_and_recorded(catalog):
    a = catalog.register(Identifier(pmid=BIOPSYCH[0], doi=BIOPSYCH[1])).id

    ref = catalog.register(Identifier(pmid=BIOPSYCH[0], doi=NLM[1]))

    assert ref.id == a
    assert catalog.aliases(a, "doi") == [BIOPSYCH[1]]
    assert catalog.resolve(Identifier(doi=NLM[1])) is None
    assert _refusals(catalog) == [("doi", NLM[1], a)]
    assert [r[:3] for r in catalog.refused] == [("doi", NLM[1], a)]


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


def test_two_base_studies_are_not_merged(catalog):
    # bqb4eprbjxw7 was made of two articles, each one of neurostore's base studies.
    a = catalog.register(
        Identifier(neurostore="8YJSSriJx23a", doi="10.1016/j.pnpbp.2026.111696")).id
    b = catalog.register(Identifier(neurostore="AkYZauDLLAS5", pmid="41962621")).id
    catalog.register(Identifier(doi="10.1016/j.pnpbp.2026.111696", pmid="41962621"))
    assert catalog.live(a) and catalog.live(b)


@pytest.mark.vcr()
def test_a_publisher_s_new_doi_for_the_paper_is_an_alternate(checked):
    # ki7akud6c347: the journal's earlier mEDRA DOI, then Springer's for the same paper.
    a = checked.register(Identifier(pmid="20885123", doi="10.3275/7271")).id

    checked.register(Identifier(pmid="20885123", doi="10.1007/bf03337758"))

    assert checked.aliases(a, "doi") == ["10.3275/7271"]
    assert checked.aliases(a, "alternate_doi") == ["10.1007/bf03337758"]
    assert checked.resolve(Identifier(doi="10.1007/bf03337758")).id == a
    assert checked.refused == []


@pytest.mark.vcr()
def test_another_paper_s_doi_is_refused_with_evidence_too(checked):
    a = checked.register(Identifier(pmid=BIOPSYCH[0], doi=BIOPSYCH[1])).id
    checked.register(Identifier(pmid=BIOPSYCH[0], doi=NLM[1]))
    assert checked.aliases(a, "alternate_doi") == []
    assert _refusals(checked) == [("doi", NLM[1], a)]


# -- a PMID is the article its PubMed DOI names ---------------------------------------

@pytest.mark.vcr()
def test_pubmed_summaries_give_the_doi_title_and_types(pubmed):
    # 40359377 was on rllqpyia5alz; PubMed has no record of it. 10320212 gives no DOI.
    found = pubmed.summaries([BIOPSYCH[0], "10320212", "40359377", "10912589"])
    assert set(found) == {BIOPSYCH[0], "10320212", "10912589"}
    assert found[BIOPSYCH[0]].doi == BIOPSYCH[1]
    assert found[BIOPSYCH[0]].title.startswith("Fronto-striato-cerebellar dysregulation")
    assert found["10320212"].doi is None
    assert "Corrected and Republished Article" in found["10320212"].pubtypes
    # held with its XML entities escaped
    assert found["10912589"].doi == "10.1002/1097-0193(200007)10:3<99::AID-HBM10>3.0.CO;2-Q"


@pytest.mark.vcr()
@pytest.mark.parametrize("pmid, supplement", [
    ("21811190", True),  # issue "1 Suppl Operative"
    ("31149901", True),  # issue "S1"
    ("38085977", True),  # issue "4S"
    ("11506650", True),  # volume "12 Suppl 2"
    ("11373141", True),  # issue "1 Pt 2", pages "S110-7"
    ("22015111", False),
])
def test_pubmed_summaries_say_whether_the_issue_is_a_supplement(pubmed, pmid, supplement):
    assert pubmed.summaries([pmid])[pmid].supplement is supplement


@pytest.mark.vcr()
def test_pubmed_summaries_ask_each_pmid_once(pubmed):
    assert set(pubmed.summaries([BIOPSYCH[0], BIOPSYCH[0], "", NLM[0]])) == {BIOPSYCH[0], NLM[0]}


@pytest.mark.vcr()
def test_pubmed_summaries_are_asked_in_batches(pubmed, monkeypatch):
    monkeypatch.setattr(pubmed_module, "IDCONV_BATCH_SIZE", 2)
    found = pubmed.summaries([BIOPSYCH[0], NLM[0], "10320212"])
    assert set(found) == {BIOPSYCH[0], NLM[0], "10320212"}


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_is_another_article_s_is_refused(checked):
    nlm = checked.register(Identifier(doi=NLM[1])).id
    biopsych = checked.register(Identifier(doi=BIOPSYCH[1])).id

    # a base study pairing the Biol Psychiatry DOI with the other paper's PMID
    ref = checked.register(Identifier(doi=BIOPSYCH[1], pmid=NLM[0]))

    assert ref.id == biopsych
    assert checked.aliases(biopsych, "pmid") == []
    assert checked.aliases(nlm, "pmid") == []
    assert _refusals(checked) == [("pmid", NLM[0], None)]


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_is_the_record_s_own_is_added(checked):
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


@pytest.mark.vcr()
def test_a_pubmed_doi_the_registry_gives_another_paper_is_no_evidence(checked):
    # PubMed gives 10.14670/HH-25-013 to the ocular lymphoma review 41221578 and to a
    # parotid tumour study; doi.org gives it the parotid study, and -014 the review.
    parotid = checked.register(Identifier(pmid="41178409", doi="10.14670/hh-25-013")).id

    review = checked.register(Identifier(pmid="41221578", doi="10.14670/hh-25-014")).id

    assert review != parotid
    assert checked.aliases(review, "pmid") == ["41221578"]
    assert checked.refused == []


def test_a_pmid_alone_or_already_held_is_not_asked_about(checked):
    # No cassette: a request to PubMed fails the test under --block-network.
    a = checked.register(Identifier(pmid=NLM[0])).id
    assert checked.register(Identifier(pmid=NLM[0], doi=NLM[1])).id == a
    assert checked.conflicts == 0


@pytest.mark.vcr()
def test_pubmed_summaries_give_the_record_s_pmc_copy(pubmed):
    assert pubmed.summaries(["17134483"])["17134483"].pmcid == "PMC1769396"


def test_a_refusal_made_again_is_on_record_once(catalog):
    a = catalog.register(Identifier(pmid=BIOPSYCH[0], doi=BIOPSYCH[1])).id
    for _ in range(2):
        catalog.register(Identifier(pmid=BIOPSYCH[0], doi=NLM[1]))
    assert _refusals(catalog) == [("doi", NLM[1], a)]


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_the_registry_cannot_read_is_refused_with_why(checked):
    # PubMed gives 29745568 10.7507/1001-5515.201511032, which doi.org sends to chinadoi,
    # which has no record of it: nothing says whether it is the holder's paper.
    holder = checked.register(Identifier(doi="10.7507/1001-5515.201511032")).id
    checked.register(Identifier(pmid="29745568", neurostore="3Rob7cK8y6gY"))
    [(kind, value, _, why)] = checked.refused
    assert (kind, value) == ("pmid", "29745568")
    assert f"which {holder} holds, and the registry cannot say" in why
    assert checked.holder("pmid", "29745568") is None


@pytest.mark.vcr()
def test_a_second_journal_doi_the_registry_cannot_read_is_refused_with_why(checked):
    a = checked.register(Identifier(pmid=BIOPSYCH[0], doi=BIOPSYCH[1])).id
    checked.register(Identifier(pmid=BIOPSYCH[0], doi="10.7507/1001-5515.201511032"))
    [(kind, value, article, why)] = checked.refused
    assert (kind, value, article) == ("doi", "10.7507/1001-5515.201511032", a)
    assert "the registry cannot say whether it is the same work" in why


def test_a_refusal_is_on_record_once_for_each_reason_with_its_latest_time(catalog):
    import time
    a = catalog.register(Identifier(pmid=BIOPSYCH[0], doi=BIOPSYCH[1])).id
    b = catalog.register(Identifier(pmid=NLM[0], doi="10.1016/j.biopsych.2021.02.423")).id
    catalog.register(Identifier(pmid=BIOPSYCH[0], doi=NLM[1]))
    first = catalog._conn.execute("SELECT refused_at FROM refusals").fetchone()[0]
    time.sleep(1.1)
    catalog.register(Identifier(pmid=BIOPSYCH[0], doi=NLM[1]))
    catalog.register(Identifier(pmid=NLM[0], doi=NLM[1]))
    assert sorted(_refusals(catalog)) == sorted([("doi", NLM[1], a), ("doi", NLM[1], b)])
    assert catalog._conn.execute(
        "SELECT refused_at FROM refusals WHERE article_id=?", (a,)).fetchone()[0] > first
    assert len(catalog.refused) == 2
