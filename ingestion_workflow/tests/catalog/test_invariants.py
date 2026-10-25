"""The catalog's invariants, on the cases the production catalog's repair met.

NCBI, doi.org and Crossref answers are recorded; requests carry the placeholder email.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Outcome, Status
from ingestion_workflow.catalog.invariants import (
    index_downloads,
    neighbourhood,
    suspects,
    violations,
)
from ingestion_workflow.models.ids import Identifier

BIOPSYCH = ("22015111", "10.1016/j.biopsych.2011.09.005")
NLM = ("21704176", "10.1016/j.nlm.2011.06.006")


def _hold(catalog, article_id, kind, value):
    """An alias written as the catalog took them before registration refused a second paper's."""
    catalog._conn.execute("INSERT INTO aliases VALUES (?, ?, ?)", (kind, value, article_id))


def _rules(found):
    return sorted((v.article_id, v.rule) for v in found)


# -- one of a kind -----------------------------------------------------------------------

@pytest.mark.vcr()
def test_two_of_a_kind_are_violations_and_suspects(catalog, evidence):
    a = catalog.register(Identifier(doi=BIOPSYCH[1]), evidence).id
    _hold(catalog, a, "doi", NLM[1])
    b = catalog.register(Identifier(pmcid="PMC3189437"), evidence).id
    _hold(catalog, b, "pmcid", "PMC5382267")
    c = catalog.register(Identifier(neurostore="8YJSSriJx23a"), evidence).id
    _hold(catalog, c, "neurostore", "AkYZauDLLAS5")
    d = catalog.register(Identifier(doi="10.7554/elife.92563"), evidence).id
    _hold(catalog, d, "preprint_pmid", "37808793")
    _hold(catalog, d, "preprint_pmid", "37808794")

    one_each = {"journal-dois", "pmids", "pmcids", "preprint-pmids", "base-studies"}
    found = [v for v in violations(catalog, [a, b, c, d], evidence) if v.rule in one_each]
    # a paper indexed on two preprint servers has a PubMed record of each
    assert _rules(found) == sorted(
        [(a, "journal-dois"), (b, "pmcids"), (c, "base-studies")])
    assert suspects(catalog) == {a, b, c}


@pytest.mark.vcr()
def test_what_is_not_a_second_of_a_kind(catalog, evidence):
    # a preprint beside its paper; an alternate DOI; one DOI in two spellings
    a = catalog.register(Identifier(doi="10.1101/2023.10.21.563317"), evidence).id
    _hold(catalog, a, "doi", "10.1016/j.neuroimage.2023.120450")
    b = catalog.register(Identifier(doi="10.3275/7271"), evidence).id
    _hold(catalog, b, "alternative_doi", "10.1007/bf03337758")
    c = catalog.register(Identifier(doi="10.1100/tsw.2011.147"), evidence).id
    _hold(catalog, c, "doi", "10.1100/TSW.2011.147")
    assert violations(catalog, [a, b, c], evidence) == []
    assert suspects(catalog) == {c}


def test_merged_articles_are_not_checked_or_suspected(catalog, evidence):
    a = catalog.register(Identifier(doi=BIOPSYCH[1]), evidence).id
    b = catalog.register(Identifier(pmcid="PMC3189437"), evidence).id
    survivor = catalog.merge([a, b])
    retired = b if survivor == a else a
    _hold(catalog, retired, "doi", NLM[1])
    _hold(catalog, retired, "doi", "10.1016/j.jse.2011.01.043")
    assert violations(catalog, [retired], evidence) == []
    assert suspects(catalog) == set()


# -- PMIDs and their PubMed DOIs ------------------------------------------------------------

@pytest.mark.vcr()
def test_two_pubmed_records_of_one_paper_on_one_article(catalog, evidence):
    # 7s73slufuvhk: PubMed indexes 10.1155/2017/2634389 twice.
    a = catalog.register(Identifier(doi="10.1155/2017/2634389", pmid="29065582"), evidence).id
    _hold(catalog, a, "pmid", "29076331")
    found = violations(catalog, [a], evidence)
    assert [(v.rule, v.detail) for v in found] == [("pmids", "holds 29065582, 29076331")]


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_another_article_holds(catalog, evidence):
    nlm = catalog.register(Identifier(doi=NLM[1]), evidence).id
    # a PMID-only article: it holds no DOI to disagree with, yet the PMID is nlm's paper
    lone = catalog.register(Identifier(pmid=NLM[0]), evidence).id

    found = violations(catalog, [nlm, lone], evidence)

    assert _rules(found) == [(lone, "pmid-doi")]
    assert found[0].detail == f"pmid {NLM[0]} is {NLM[1]} in PubMed, which {nlm} holds"


@pytest.mark.vcr()
def test_a_pmid_beside_another_paper_s_doi(catalog, evidence):
    a = catalog.register(Identifier(doi="10.1016/j.jse.2011.01.043", pmid=BIOPSYCH[0]),
            evidence).id
    assert _rules(violations(catalog, [a], evidence)) == [(a, "pmid-doi")]


@pytest.mark.vcr()
def test_the_pmid_of_the_article_s_own_doi_is_no_violation(catalog, evidence):
    a = catalog.register(Identifier(doi=BIOPSYCH[1], pmid=BIOPSYCH[0]), evidence).id
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_pubmed_doi_no_registry_knows_is_no_evidence(catalog, evidence):
    # PubMed 23840428 gives 10.137/journal.pone.0066241, a typo of the paper's DOI.
    a = catalog.register(Identifier(doi="10.1371/journal.pone.0066241", pmid="23840428"),
            evidence).id
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_pubmed_doi_the_registry_gives_another_paper_is_no_evidence(catalog, evidence):
    # 2imk3jt7rl7a: PubMed gives 41221578 (an ocular lymphoma review) 10.14670/HH-25-013,
    # which doi.org gives a parotid tumour study; the article's -014 is the review's.
    a = catalog.register(Identifier(doi="10.14670/hh-25-014", pmid="41221578"), evidence).id
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_preprint_s_pmid_is_held_to_its_pubmed_doi(catalog, evidence):
    # vrr7x2ygtjdt holds its eLife paper's PMID and, as its preprint's, 37808793.
    a = catalog.register(Identifier(doi="10.7554/elife.92563", pmid="38345852"), evidence).id
    _hold(catalog, a, "preprint_pmid", "37808793")

    assert [(v.rule, v.detail) for v in violations(catalog, [a], evidence)] == [("pmid-doi", (
        "pmid 37808793 is 10.1101/2023.09.27.559792 in PubMed, not 10.7554/elife.92563"))]

    _hold(catalog, a, "preprint_doi", "10.1101/2023.09.27.559792")
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_pmid_its_own_pmc_record_agrees_with_is_not_another_edition_s(catalog, evidence):
    # nwlokmfhhxwb, Rev Psiquiatr Salud Ment's English edition: PubMed gives its record the
    # Spanish edition's DOI, which pbig65ccs2nv holds; its PMC record gives the English DOI.
    english = catalog.register(Identifier(pmid="32962948", pmcid="PMC10068027",
                                          doi="10.1016/j.rpsmen.2020.07.005"), evidence).id
    catalog.register(
        Identifier(pmid="38620670", pmcid="PMC7456305", doi="10.1016/j.rpsm.2020.07.003"),
                evidence)
    assert [v for v in violations(catalog, [english], evidence) if v.rule == "pmid-doi"] == []


# -- PMCIDs and their PMC records ------------------------------------------------------------

@pytest.mark.vcr()
def test_a_pmcid_whose_pmc_record_is_another_pmid(catalog, evidence):
    # lg6selcsnr7f: PMC5582689 is PubMed 28290265 (Psychol Med); the article holds the
    # craft beer paper's PMID and DOI beside it.
    a = catalog.register(Identifier(pmid="28290323", doi="10.1016/j.foodres.2016.11.043"),
            evidence).id
    _hold(catalog, a, "pmcid", "PMC5582689")
    found = violations(catalog, [a], evidence)
    assert [(v.rule, v.detail) for v in found] == [
        ("pmcid", "PMC5582689 is PubMed 28290265 in PMC, which no article holds"),
        ("pmcid",
         "PMC5582689 is 10.1017/s0033291717000265 in PMC, not 10.1016/j.foodres.2016.11.043")]


@pytest.mark.vcr()
def test_a_pmcid_whose_pmc_record_is_another_doi(catalog, evidence):
    # x5xhvjd45lqm: PMC4043787 is a JCMR meeting abstract, beside a Magn Reson Med paper.
    a = catalog.register(Identifier(doi="10.1002/mrm.25078"), evidence).id
    _hold(catalog, a, "pmcid", "PMC4043787")
    found = violations(catalog, [a], evidence)
    assert [v.rule for v in found] == ["pmcid"]
    assert found[0].detail.startswith("PMC4043787 is 10.1186/1532-429x-16-s1-o7 in PMC, not ")


@pytest.mark.vcr()
def test_a_pmcid_agreeing_with_the_article_is_no_violation(catalog, evidence):
    # 2hauw7ec2exh once repaired: its PMCID, PMID and DOI are one PubMed record's.
    a = catalog.register(Identifier(pmid="17134483", doi="10.1186/1747-5333-1-13",
                                    pmcid="PMC1769396"), evidence).id
    assert violations(catalog, [a], evidence) == []


# -- a preprint and its version of record -----------------------------------------------------

@pytest.mark.vcr()
def test_a_preprint_and_its_version_of_record_on_two_articles(catalog, evidence):
    # btk5tlwqap5k held bioRxiv's preprint of vrr7x2ygtjdt's eLife paper.
    paper = catalog.register(Identifier(doi="10.7554/elife.92563"), evidence).id
    preprint = catalog.register(Identifier(doi="10.1101/2023.09.27.559792"), evidence).id
    found = violations(catalog, [preprint], evidence)
    assert [(v.article_id, v.rule) for v in found] == [(preprint, "two-versions")]
    assert paper in found[0].detail


# -- the articles a change can reach -----------------------------------------------------------

@pytest.mark.vcr()
def test_an_article_holding_the_pmid_of_a_doi_is_its_holder_s_neighbour(catalog, evidence):
    # The reviewer's probe: a plan adds d to A, and B holds the PMID PubMed gives d.
    a = catalog.register(Identifier(doi=NLM[1]), evidence).id
    b = catalog.register(Identifier(pmid=NLM[0]), evidence).id
    assert neighbourhood(catalog, [a], evidence) == {a, b}


@pytest.mark.parametrize("held, neighbour", [
    # holding one id: a PMID beside another article's preprint PMID of the same value
    ([("pmid", "37808793")], [("preprint_pmid", "37808793"), ("doi", "10.7554/elife.92563")]),
    # PubMed's DOI for a PMID
    ([("pmid", NLM[0])], [("doi", NLM[1])]),
    # PubMed's PMC copy of a PMID
    ([("pmid", "17134483")], [("pmcid", "PMC1769396")]),
    # a PMC record's PMID, and its DOI
    ([("pmcid", "PMC1769396")], [("pmid", "17134483")]),
    ([("pmcid", "PMC5582689")], [("doi", "10.1017/s0033291717000265")]),
    # a preprint's version of record, by Crossref
    ([("doi", "10.1101/2023.09.27.559792")], [("doi", "10.7554/elife.92563")]),
    # a preprint's PubMed record and its paper's, by title alone: btk5tlwqap5k and vrr7x2ygtjdt
    ([("pmid", "37808793")], [("pmid", "38345852")]),
], ids=["shared-id", "pubmed-doi", "pubmed-pmc", "pmc-pmid", "pmc-doi", "crossref-version",
        "pubmed-title"])
@pytest.mark.vcr()
def test_each_link_reaches_the_neighbour(catalog, held, neighbour, evidence):
    a = catalog.register(Identifier(neurostore="3Rob7cK8y6gY"), evidence).id
    b = catalog.register(Identifier(neurostore="26SP7hw2qb8u"), evidence).id
    for kind, value in held:
        _hold(catalog, a, kind, value)
    for kind, value in neighbour:
        _hold(catalog, b, kind, value)
    assert neighbourhood(catalog, [a], evidence) == {a, b}


@pytest.mark.vcr()
def test_the_neighbourhood_check_is_the_full_check_restricted_to_it(catalog, tmp_path, evidence):
    # A catalog of the repair's cases, some consistent, some not.
    ids = [
        catalog.register(Identifier(doi=NLM[1]), evidence).id,
        catalog.register(Identifier(pmid=NLM[0]),
                evidence).id,                    # NLM's PMID, alone
        catalog.register(Identifier(doi=BIOPSYCH[1], pmid=BIOPSYCH[0]), evidence).id,
        catalog.register(Identifier(doi="10.7554/elife.92563", pmid="38345852"), evidence).id,
        catalog.register(Identifier(doi="10.1101/2023.09.27.559792"),
                evidence).id,  # its preprint, apart
        catalog.register(Identifier(pmid="17134483", doi="10.1186/1747-5333-1-13",
                                    pmcid="PMC1769396"), evidence).id,
        catalog.register(Identifier(doi="10.1016/j.jse.2011.01.043", pmid="22015112"),
                evidence).id,
    ]
    full = violations(catalog, ids, evidence)
    for start in ids:
        around = neighbourhood(catalog, [start], evidence)
        assert violations(catalog, around, evidence) == [v for v in full if v.article_id in around]
    assert {v.rule for v in full} >= {"pmid-doi", "two-versions"}


# -- downloads --------------------------------------------------------------------------------

def _download(catalog, article_id, source, root, name, text, status=Status.OK):
    path = root / source / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    catalog.record([Outcome(article_id, "download", source, status,
                            payload={"files": [{"file_path": str(path)}]})])
    return path


def test_a_download_whose_xml_is_another_article_s_paper(catalog, tmp_path, evidence):
    # mwni3etc4i7l held an Elsevier copy of 54xpppoekzpj's paper.
    a = catalog.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007"), evidence).id
    b = catalog.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015"), evidence).id
    _download(catalog, a, "elsevier", tmp_path / "a", "content.xml",
              "<full-text-retrieval-response>"
              "<prism:doi>10.1016/j.neuroimage.2004.05.015</prism:doi>")
    _download(catalog, b, "pubget", tmp_path / "b", "pmcid_1234567/article.xml",
              '<article-id pub-id-type="doi">10.1016/J.NEUROIMAGE.2004.05.015</article-id>')

    found = violations(catalog, [a, b], evidence)

    assert _rules(found) == [(a, "download")]
    assert found[0].detail == (f"download/elsevier names doi 10.1016/j.neuroimage.2004.05.015, "
                               f"which {b} holds")


def test_a_jats_file_naming_another_article_s_doi(catalog, tmp_path, evidence):
    a = catalog.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007"), evidence).id
    b = catalog.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015"), evidence).id
    _download(catalog, a, "pmc", tmp_path, "pmcid_1234567/article.xml",
              '<article-id pub-id-type="doi">10.1016/j.neuroimage.2004.05.015</article-id>')
    assert [v.detail for v in violations(catalog, [a], evidence)] == [
        f"download/pmc names pmcid PMC1234567, doi 10.1016/j.neuroimage.2004.05.015, "
        f"which {b} holds"]


@pytest.mark.vcr()
def test_a_download_naming_one_of_the_article_s_own_ids_is_its_own(catalog, tmp_path, evidence):
    # 2dd6hgs4ymiv's PMC copy sits under its own PMCID; its XML names the preprint's DOI.
    a = catalog.register(Identifier(doi="10.1016/j.ebiom.2026.106400", pmcid="PMC13370491"),
            evidence).id
    catalog.register(Identifier(doi="10.64898/2026.01.29.26344990"), evidence)
    _download(catalog, a, "pmc", tmp_path, "pmcid_13370491/article.xml",
              '<article-id pub-id-type="doi">10.64898/2026.01.29.26344990</article-id>')
    assert [v for v in violations(catalog, [a], evidence) if v.rule.startswith("download")] == []


def test_only_ace_names_its_paper_by_file_name(catalog, tmp_path, evidence):
    a = catalog.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007"), evidence).id
    catalog.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015", pmcid="PMC100"), evidence)
    _download(catalog, a, "pdf", tmp_path, "15465288.html", "<html/>")
    assert violations(catalog, [a], evidence) == []


def test_a_failed_download_names_nothing(catalog, tmp_path, evidence):
    a = catalog.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007"), evidence).id
    catalog.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015"), evidence)
    _download(catalog, a, "elsevier", tmp_path, "content.xml",
              "<prism:doi>10.1016/j.neuroimage.2004.05.015</prism:doi>", status=Status.FAILED)
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_an_ace_file_names_its_paper_by_pmid(catalog, tmp_path, evidence):
    # ml7z5xc7cvgo and su74e7qsacoi each held the other's ACE page.
    a = catalog.register(Identifier(pmid="15658005"), evidence).id
    b = catalog.register(Identifier(pmid="15465288"), evidence).id
    _download(catalog, a, "ace", tmp_path / "a", "15465288.html", "<html/>")
    _download(catalog, b, "ace", tmp_path / "b", "15465288.html", "<html/>")
    assert _rules(violations(catalog, [a, b], evidence)) == [(a, "download")]


def test_a_download_whose_record_or_file_is_gone_is_a_violation(catalog, tmp_path, evidence):
    a = catalog.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007"), evidence).id
    path = _download(catalog, a, "elsevier", tmp_path, "content.xml", "<prism:doi>x</prism:doi>")
    path.unlink()
    b = catalog.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015"), evidence).id
    _download(catalog, b, "pubget", tmp_path, "pmcid_1/article.xml", "<article/>")
    blob = catalog.artifact(b, "download", "pubget").blob
    for found in catalog.root.glob(f"blobs/**/{blob}*"):
        found.unlink()

    found = sorted((v.article_id, v.rule, v.detail.split(":")[0])
                   for v in violations(catalog, [a, b], evidence))
    assert found == sorted([(a, "download-unreadable", "download/elsevier"),
                            (b, "download-unreadable", "download/pubget")])


@pytest.mark.vcr()
def test_a_journal_record_s_doi_is_the_article_s_doi_not_an_alternate(catalog, evidence):
    # ki7akud6c347: the journal's earlier mEDRA DOI, and PubMed's Springer DOI
    a = catalog.register(Identifier(doi="10.3275/7271", pmid="20885123"), evidence).id
    _hold(catalog, a, "alternative_doi", "10.1007/bf03337758")
    assert _rules(violations(catalog, [a], evidence)) == [(a, "vor-doi")]
    catalog._conn.execute("UPDATE aliases SET kind=CASE kind WHEN 'doi' THEN 'alternative_doi' "
                          "ELSE 'doi' END WHERE article_id=? AND kind IN ('doi', 'alternative_doi')",
                          (a,))
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_pmc_record_without_a_pmid_names_its_paper_only_by_a_registered_doi(catalog, evidence):
    # PMC10411397, a European Psychiatry abstract, gives 10.1192/j.eurpsy.2023.1163, which
    # no registry has; the article's own DOI is another such abstract's.
    a = catalog.register(Identifier(pmcid="PMC10411397", doi="10.1192/j.eurpsy.2023.662"),
            evidence).id
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_preprint_s_pubmed_record_and_its_paper_on_two_articles(catalog, evidence):
    # btk5tlwqap5k held only 37808793, the Preprint-typed record of vrr7x2ygtjdt's paper.
    paper = catalog.register(Identifier(doi="10.7554/elife.92563"), evidence).id
    preprint = catalog.register(Identifier(pmid="37808793"), evidence).id
    found = [v for v in violations(catalog, [preprint], evidence) if v.rule == "two-versions"]
    assert [v.article_id for v in found] == [preprint] and paper in found[0].detail


@pytest.mark.vcr()
def test_a_meeting_abstract_s_pmc_record_beside_the_paper_breaks_the_pmcid_rule(catalog, evidence):
    # x5xhvjd45lqm: the MRM paper's PMID and DOI, and PMC4043787, the JCMR supplement's
    # abstract (no PMID in PMC), whose DOI the plan had made an alternate.
    a = catalog.register(Identifier(pmid="24532480", doi="10.1002/mrm.25133"), evidence).id
    _hold(catalog, a, "pmcid", "PMC4043787")
    _hold(catalog, a, "alternative_doi", "10.1186/1532-429x-16-s1-o7")
    found = violations(catalog, [a], evidence)
    assert (a, "pmcid") in _rules(found) and (a, "alternate-doi") in _rules(found)
    assert any("only as an alternate DOI" in v.detail for v in found if v.rule == "pmcid")


@pytest.mark.vcr()
def test_an_erratum_kept_as_an_alternate_doi_is_another_work(catalog, evidence):
    # rg5bc6o7gs7m: the paper, and "Erratum to 'A quantitative comparison...'"
    a = catalog.register(Identifier(pmid="11595367", doi="10.1016/s0730-725x(01)00418-0"),
            evidence).id
    _hold(catalog, a, "alternative_doi", "10.1016/s0730-725x(03)00069-9")
    assert _rules(violations(catalog, [a], evidence)) == [(a, "alternate-doi")]


@pytest.mark.vcr()
def test_a_preprint_s_pubmed_record_with_its_paper_s_title_is_its_other_version(catalog, evidence):
    # wlvovj2lmdff, the bioRxiv preprint's PubMed record, and xxv5qzt4dg7u, the Sci Rep
    # paper: "Neural effects of TMS trains on the human prefrontal cortex."
    preprint = catalog.register(Identifier(pmid="36778457", doi="10.1101/2023.01.30.526374"),
            evidence).id
    paper = catalog.register(Identifier(pmid="38123591", doi="10.1038/s41598-023-49250-7"),
            evidence).id
    found = [v for v in violations(catalog, [preprint], evidence) if v.rule == "two-versions"]
    assert any(f"PubMed 38123591's work, which {paper} holds" in v.detail for v in found)
    assert preprint in neighbourhood(catalog, [paper], evidence)


def test_a_scope_over_downloads_not_indexed_is_refused(catalog, tmp_path, evidence):
    a = catalog.register(Identifier(neurostore="3Rob7cK8y6gY"), evidence).id
    catalog.record([Outcome(a, "download", "ace", Status.OK,
                            payload={"files": [{"file_path": "/ace/22015111.html"}]})])
    with pytest.raises(ValueError, match="1 downloads are not indexed"):
        neighbourhood(catalog, [a], evidence)
    assert index_downloads(catalog) == 1
    assert neighbourhood(catalog, [a], evidence) == {a}


def test_an_unreadable_download_is_indexed_as_naming_nothing(catalog, tmp_path, evidence):
    a = catalog.register(Identifier(neurostore="3Rob7cK8y6gY"), evidence).id
    gone = tmp_path / "elsevier" / "content.xml"
    catalog.record([Outcome(a, "download", "elsevier", Status.OK,
                            payload={"files": [{"file_path": str(gone)}]})])
    assert index_downloads(catalog) == 1
    assert neighbourhood(catalog, [a], evidence) == {a}
    assert [v.rule for v in violations(catalog, [a], evidence)] == ["download-unreadable"]


@pytest.mark.vcr()
def test_a_pmc_record_of_the_article_s_pubmed_record_may_give_its_other_doi(catalog, evidence):
    # aucqqxrbcno2: PubMed gives PNAS's issue DOI, PMC its early edition's
    a = catalog.register(Identifier(pmid="10725372", pmcid="PMC16281",
                                    doi="10.1073/pnas.97.7.3573"), evidence).id
    _hold(catalog, a, "alternative_doi", "10.1073/pnas.050583797")
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_preprint_and_its_paper_each_with_a_base_study_are_accepted_not_violations(
        catalog, evidence):
    # bdarfou5chmg (Int J Obes, base study vY5mktCMTn9P) and nj52vmozfa3k (Research Square,
    # JAJmM4dhUPuZ): apart until neurostore can make one base study a version of the other.
    # The paper's PMC download is the preprint's copy.
    paper = catalog.register(Identifier(pmid="42230985", pmcid="PMC13387087",
                                        doi="10.1038/s41366-026-02114-2",
                                        neurostore="vY5mktCMTn9P"), evidence).id
    preprint = catalog.register(Identifier(pmid="41674811", pmcid="PMC12889832",
                                           doi="10.21203/rs.3.rs-8694593/v1",
                                           neurostore="JAJmM4dhUPuZ"), evidence).id
    catalog.record([Outcome(paper, "download", "pubget", Status.OK, payload={"files": [
        {"file_path": "/pubget/articles/pmcid_12889832/tables.xml"}]})])
    found = violations(catalog, [paper, preprint], evidence)
    assert sorted((v.article_id, v.rule) for v in found) == sorted(
        [(paper, "download")] + [(preprint, "two-versions")] * sum(
            v.rule == "two-versions" for v in found))
    assert found and all(v.accepted for v in found)
    catalog._conn.execute("DELETE FROM aliases WHERE kind='neurostore' AND article_id=?", (paper,))
    assert not any(v.accepted for v in violations(catalog, [paper, preprint], evidence))


@pytest.mark.vcr()
def test_an_article_holding_only_its_pmid_asks_no_registry_of_pubmed_s_doi(catalog, evidence):
    # 7i57itl5rdoi: PubMed gives 16059980 a DOI whose agency serves a bad certificate; no
    # article holds it, and the article holds no DOI, so no rule has anything to judge
    a = catalog.register(Identifier(pmid="16059980"), evidence).id
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_download_between_two_based_articles_that_are_not_versions_is_a_violation(
        catalog, tmp_path, evidence):
    # mwni3etc4i7l's Elsevier copy is 54xpppoekzpj's paper; each article holds a base study
    holder = catalog.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007",
                                         neurostore="3Rob7cK8y6gY"), evidence).id
    owner = catalog.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015",
                                        neurostore="AkYZauDLLAS5"), evidence).id
    xml = tmp_path / "elsevier" / "content.xml"
    xml.parent.mkdir()
    xml.write_text("<prism:doi>10.1016/j.neuroimage.2004.05.015</prism:doi>")
    catalog.record([Outcome(holder, "download", "elsevier", Status.OK,
                            payload={"files": [{"file_path": str(xml)}]})])
    [found] = [v for v in violations(catalog, [holder], evidence) if v.rule == "download"]
    assert owner in found.detail and not found.accepted


@pytest.mark.vcr()
def test_each_download_is_judged_though_another_is_unreadable(catalog, evidence):
    # an article's ACE copy has no record; its pubget copy is lg6selcsnr7f's PMC copy
    a = catalog.register(Identifier(neurostore="3Rob7cK8y6gY"), evidence).id
    b = catalog.register(Identifier(pmcid="PMC5582689", pmid="28290265"), evidence).id
    catalog.record([Outcome(a, "download", "ace", Status.OK),
                    Outcome(a, "download", "pubget", Status.OK, payload={"files": [
                        {"file_path": "/pubget/articles/pmcid_5582689/tables.xml"}]})])
    assert sorted(v.rule for v in violations(catalog, [a],
            evidence)) == ["download", "download-unreadable"]
    assert b in next(v.detail for v in violations(catalog, [a], evidence) if v.rule == "download")


@pytest.mark.vcr()
def test_a_registry_that_cannot_be_read_judges_nothing(catalog, evidence):
    # mxp7s3fbegzb: PubMed's DOI, and the alias spelling no registry describes
    a = catalog.register(Identifier(doi="10.1176/jnp.2009.21.3.307"), evidence).id
    _hold(catalog, a, "alternative_doi", "10.1176/appi.neuropsych.21.3.307")
    assert [v.rule for v in violations(catalog, [a], evidence)] == ["evidence-unreadable"]


@pytest.mark.vcr()
def test_an_unregistered_alternate_spelling_is_no_other_work(catalog, evidence):
    a = catalog.register(Identifier(pmid=BIOPSYCH[0], doi=BIOPSYCH[1]), evidence).id
    _hold(catalog, a, "alternative_doi", "10.137/journal.pone.0066241")
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_pmid_its_own_pmc_record_agrees_with_may_hold_pmc_s_doi_not_pubmed_s(catalog, evidence):
    # aucqqxrbcno2: PubMed gives PNAS's issue DOI; the article holds its PMC record's early
    # edition DOI
    a = catalog.register(
        Identifier(pmid="10725372", pmcid="PMC16281", doi="10.1073/pnas.050583797"), evidence).id
    assert violations(catalog, [a], evidence) == []


@pytest.mark.vcr()
def test_a_preprint_record_s_doi_held_as_an_alternate_is_no_version_of_record_s(catalog, evidence):
    # 37808793 is bioRxiv's record of vrr7x2ygtjdt's eLife paper
    a = catalog.register(Identifier(pmid="37808793", doi="10.7554/elife.92563"), evidence).id
    _hold(catalog, a, "alternative_doi", "10.1101/2023.09.27.559792")
    assert "vor-doi" not in [v.rule for v in violations(catalog, [a], evidence)]


@pytest.mark.vcr()
def test_a_preprint_pmid_s_doi_held_as_an_alternate_is_no_version_of_record_s(catalog, evidence):
    a = catalog.register(Identifier(pmid="38345852", doi="10.7554/elife.92563"), evidence).id
    _hold(catalog, a, "preprint_pmid", "37808793")
    _hold(catalog, a, "alternative_doi", "10.1101/2023.09.27.559792")
    assert "vor-doi" not in [v.rule for v in violations(catalog, [a], evidence)]


@pytest.mark.vcr()
def test_several_preprint_pmcids_are_no_violation(catalog, evidence):
    a = catalog.register(Identifier(doi="10.7554/elife.92563"), evidence).id
    _hold(catalog, a, "preprint_pmcid", "PMC10634801")
    _hold(catalog, a, "preprint_pmcid", "PMC10634864")
    assert "preprint-pmcids" not in [v.rule for v in violations(catalog, [a], evidence)]
    assert a not in suspects(catalog)


@pytest.mark.vcr()
def test_a_download_naming_an_id_held_as_a_second_kind_links_its_article(catalog, evidence):
    # the other article holds the PMID as its preprint's
    a = catalog.register(Identifier(doi="10.7554/elife.92563"), evidence).id
    _hold(catalog, a, "preprint_pmid", "37808793")
    x = catalog.register(Identifier(neurostore="3Rob7cK8y6gY"), evidence).id
    catalog.record([Outcome(x, "download", "ace", Status.OK,
                            payload={"files": [{"file_path": "/ace/37808793.html"}]})])
    index_downloads(catalog)
    assert x in neighbourhood(catalog, [a], evidence)


@pytest.mark.vcr()
def test_a_paper_s_record_held_as_a_preprint_s_with_its_doi_an_alternate_is_no_vor_violation(
        catalog, evidence):
    # only the article's own PMID's DOI must be its DOI
    a = catalog.register(Identifier(doi=NLM[1]), evidence).id
    _hold(catalog, a, "preprint_pmid", BIOPSYCH[0])
    _hold(catalog, a, "alternative_doi", BIOPSYCH[1])
    assert "vor-doi" not in [v.rule for v in violations(catalog, [a], evidence)]


@pytest.mark.vcr()
def test_a_preprint_record_held_as_the_preprint_s_finds_its_paper_by_title(catalog, evidence):
    # wlvovj2lmdff's bioRxiv record, held only as a preprint's; xxv5qzt4dg7u holds Sci Rep's
    preprint = catalog.register(Identifier(neurostore="3Rob7cK8y6gY"), evidence).id
    _hold(catalog, preprint, "preprint_pmid", "36778457")
    paper = catalog.register(Identifier(pmid="38123591", doi="10.1038/s41598-023-49250-7"),
            evidence).id
    found = [v for v in violations(catalog, [preprint], evidence) if v.rule == "two-versions"]
    assert any(f"PubMed 38123591's work, which {paper} holds" in v.detail for v in found)
