"""The catalog's invariants, on the cases the production catalog's repair met.

PubMed and doi.org answers are recorded; the requests carry the placeholder email.
"""

from __future__ import annotations

import pytest
import requests
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.catalog.invariants import suspects, violations
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.services.local_pdfs import doi_registered
from ingestion_workflow.tests import cassette_secrets

BIOPSYCH = ("22015111", "10.1016/j.biopsych.2011.09.005")
NLM = ("21704176", "10.1016/j.nlm.2011.06.006")


@pytest.fixture()
def pubmed():
    with PubMedClient(email=cassette_secrets.REPLAY_EMAIL) as client:
        yield client


@pytest.fixture()
def catalog(tmp_path, pubmed):
    with Catalog.open(tmp_path / "cat", pubmed_dois=pubmed.dois) as cat:
        yield cat


@pytest.fixture()
def registered():
    with requests.Session() as session:
        yield doi_registered(session)


def _hold(catalog, article_id, kind, value):
    """An alias written as the catalog took them before registration refused a second paper's."""
    catalog._conn.execute("INSERT INTO aliases VALUES (?, ?, ?)", (kind, value, article_id))


def _rules(found):
    return sorted((v.article_id, v.rule) for v in found)


def test_two_journal_dois_pmids_or_pmcids_are_violations_and_suspects(catalog, registered):
    a = catalog.register(Identifier(doi=BIOPSYCH[1])).id
    _hold(catalog, a, "doi", NLM[1])
    b = catalog.register(Identifier(pmcid="PMC3189437")).id
    _hold(catalog, b, "pmcid", "PMC5382267")
    c = catalog.register(Identifier(doi="10.1101/2023.10.21.563317")).id
    _hold(catalog, c, "doi", "10.1016/j.neuroimage.2023.120450")  # a preprint beside its paper

    assert _rules(violations(catalog, [a, b, c], registered)) == sorted(
        [(a, "journal-dois"), (b, "pmcids")])
    assert suspects(catalog) == {a, b, c}


@pytest.mark.vcr()
def test_two_pubmed_records_of_one_paper_on_one_article(catalog, registered):
    # 7s73slufuvhk: PubMed indexes 10.1155/2017/2634389 twice.
    a = catalog.register(Identifier(doi="10.1155/2017/2634389", pmid="29065582")).id
    _hold(catalog, a, "pmid", "29076331")
    found = violations(catalog, [a], registered)
    assert [(v.rule, v.detail) for v in found] == [("pmids", "holds 29065582, 29076331")]


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_another_article_holds(catalog, registered):
    nlm = catalog.register(Identifier(doi=NLM[1])).id
    # a PMID-only article: it holds no DOI to disagree with, yet the PMID is nlm's paper
    lone = catalog.register(Identifier(pmid=NLM[0])).id

    found = violations(catalog, [nlm, lone], registered)

    assert _rules(found) == [(lone, "pmid-doi")]
    assert found[0].detail == f"pmid {NLM[0]} is {NLM[1]} in PubMed, which {nlm} holds"


@pytest.mark.vcr()
def test_a_pmid_beside_another_papers_doi(catalog, registered):
    a = catalog.register(Identifier(doi="10.1016/j.jse.2011.01.043", pmid=BIOPSYCH[0])).id
    assert _rules(violations(catalog, [a], registered)) == [(a, "pmid-doi")]


@pytest.mark.vcr()
def test_a_pubmed_doi_no_registry_knows_is_not_held_against_the_article(catalog, registered):
    # PubMed 23840428 gives 10.137/journal.pone.0066241, a typo of the paper's DOI.
    a = catalog.register(Identifier(doi="10.1371/journal.pone.0066241", pmid="23840428")).id
    assert violations(catalog, [a], registered) == []


@pytest.mark.vcr()
def test_pubmed_dois_are_unescaped(pubmed):
    assert pubmed.dois(["10912589"]) == {
        "10912589": "10.1002/1097-0193(200007)10:3<99::AID-HBM10>3.0.CO;2-Q"}


@pytest.mark.vcr()
def test_the_pmid_of_the_articles_own_doi_is_no_violation(catalog, registered):
    a = catalog.register(Identifier(doi=BIOPSYCH[1], pmid=BIOPSYCH[0])).id
    assert violations(catalog, [a], registered) == []


@pytest.mark.vcr()
def test_a_preprint_s_pmid_is_held_to_its_pubmed_doi(catalog, registered):
    # vrr7x2ygtjdt holds its eLife paper's PMID and, as its preprint's, 37808793.
    a = catalog.register(Identifier(doi="10.7554/elife.92563", pmid="38345852")).id
    _hold(catalog, a, "preprint_pmid", "37808793")

    found = violations(catalog, [a], registered)
    assert [(v.rule, v.detail) for v in found] == [("pmid-doi", (
        "pmid 37808793 is 10.1101/2023.09.27.559792 in PubMed, not 10.7554/elife.92563"))]

    _hold(catalog, a, "doi", "10.1101/2023.09.27.559792")
    assert violations(catalog, [a], registered) == []


def _download(catalog, article_id, source, tmp_path, name, text):
    path = tmp_path / source / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    catalog.record([Outcome(article_id, "download", source, Status.OK,
                            payload={"files": [{"file_path": str(path)}]})])


def test_a_download_whose_xml_is_another_articles_paper(catalog, registered, tmp_path):
    # mwni3etc4i7l held an Elsevier copy of 54xpppoekzpj's paper.
    a = catalog.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
    b = catalog.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015")).id
    _download(catalog, a, "elsevier", tmp_path / "a", "content.xml",
              "<full-text-retrieval-response><prism:doi>10.1016/j.neuroimage.2004.05.015</prism:doi>")
    _download(catalog, b, "pubget", tmp_path / "b", "pmcid_1234567/article.xml",
              '<article-id pub-id-type="doi">10.1016/J.NEUROIMAGE.2004.05.015</article-id>')

    found = violations(catalog, [a, b], registered)

    assert _rules(found) == [(a, "download")]
    assert found[0].detail == (f"download/elsevier names doi 10.1016/j.neuroimage.2004.05.015, "
                               f"which {b} holds")


def test_a_download_naming_one_of_the_articles_own_ids_is_its_own(catalog, registered, tmp_path):
    # 2dd6hgs4ymiv's PMC copy sits under its own PMCID; its XML names the preprint's DOI.
    a = catalog.register(Identifier(doi="10.1016/j.ebiom.2026.106400", pmcid="PMC13370491")).id
    catalog.register(Identifier(doi="10.64898/2026.01.29.26344990"))
    _download(catalog, a, "pmc", tmp_path, "pmcid_13370491/article.xml",
              '<article-id pub-id-type="doi">10.64898/2026.01.29.26344990</article-id>')
    assert violations(catalog, [a], registered) == []


@pytest.mark.vcr()
def test_an_ace_file_names_its_paper_by_pmid(catalog, registered, tmp_path):
    # ml7z5xc7cvgo and su74e7qsacoi each held the other's ACE page.
    a = catalog.register(Identifier(pmid="15658005")).id
    b = catalog.register(Identifier(pmid="15465288")).id
    _download(catalog, a, "ace", tmp_path / "a", "15465288.html", "<html/>")
    _download(catalog, b, "ace", tmp_path / "b", "15465288.html", "<html/>")
    assert _rules(violations(catalog, [a, b], registered)) == [(a, "download")]


def test_violations_need_pubmed(tmp_path, registered):
    with Catalog.open(tmp_path / "cat") as catalog:
        with pytest.raises(ValueError, match="pubmed_dois"):
            violations(catalog, [], registered)
