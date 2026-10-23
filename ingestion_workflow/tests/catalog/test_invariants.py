"""The catalog's invariants, on the cases the production catalog's repair met.

NCBI, doi.org and Crossref answers are recorded; requests carry the placeholder email.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
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
def test_two_of_a_kind_are_violations_and_suspects(checked):
    a = checked.register(Identifier(doi=BIOPSYCH[1])).id
    _hold(checked, a, "doi", NLM[1])
    b = checked.register(Identifier(pmcid="PMC3189437")).id
    _hold(checked, b, "pmcid", "PMC5382267")
    c = checked.register(Identifier(neurostore="8YJSSriJx23a")).id
    _hold(checked, c, "neurostore", "AkYZauDLLAS5")
    d = checked.register(Identifier(doi="10.7554/elife.92563")).id
    _hold(checked, d, "preprint_pmid", "37808793")
    _hold(checked, d, "preprint_pmid", "37808794")

    one_each = {"journal-dois", "pmids", "pmcids", "preprint-pmids", "base-studies"}
    found = [v for v in violations(checked, [a, b, c, d]) if v.rule in one_each]
    assert _rules(found) == sorted(
        [(a, "journal-dois"), (b, "pmcids"), (c, "base-studies"), (d, "preprint-pmids")])
    assert suspects(checked) == {a, b, c, d}


@pytest.mark.vcr()
def test_what_is_not_a_second_of_a_kind(checked):
    # a preprint beside its paper; an alternate DOI; one DOI in two spellings
    a = checked.register(Identifier(doi="10.1101/2023.10.21.563317")).id
    _hold(checked, a, "doi", "10.1016/j.neuroimage.2023.120450")
    b = checked.register(Identifier(doi="10.3275/7271")).id
    _hold(checked, b, "alternative_doi", "10.1007/bf03337758")
    c = checked.register(Identifier(doi="10.1100/tsw.2011.147")).id
    _hold(checked, c, "doi", "10.1100/TSW.2011.147")
    assert violations(checked, [a, b, c]) == []
    assert suspects(checked) == {c}


def test_merged_articles_are_not_checked_or_suspected(checked):
    a = checked.register(Identifier(doi=BIOPSYCH[1])).id
    b = checked.register(Identifier(pmcid="PMC3189437")).id
    survivor = checked.merge([a, b])
    retired = b if survivor == a else a
    _hold(checked, retired, "doi", NLM[1])
    _hold(checked, retired, "doi", "10.1016/j.jse.2011.01.043")
    assert violations(checked, [retired]) == []
    assert suspects(checked) == set()


def test_violations_need_evidence(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        with pytest.raises(ValueError, match="evidence"):
            violations(catalog, [])


# -- PMIDs and their PubMed DOIs ------------------------------------------------------------

@pytest.mark.vcr()
def test_two_pubmed_records_of_one_paper_on_one_article(checked):
    # 7s73slufuvhk: PubMed indexes 10.1155/2017/2634389 twice.
    a = checked.register(Identifier(doi="10.1155/2017/2634389", pmid="29065582")).id
    _hold(checked, a, "pmid", "29076331")
    found = violations(checked, [a])
    assert [(v.rule, v.detail) for v in found] == [("pmids", "holds 29065582, 29076331")]


@pytest.mark.vcr()
def test_a_pmid_whose_pubmed_doi_another_article_holds(checked):
    nlm = checked.register(Identifier(doi=NLM[1])).id
    # a PMID-only article: it holds no DOI to disagree with, yet the PMID is nlm's paper
    lone = checked.register(Identifier(pmid=NLM[0])).id

    found = violations(checked, [nlm, lone])

    assert _rules(found) == [(lone, "pmid-doi")]
    assert found[0].detail == f"pmid {NLM[0]} is {NLM[1]} in PubMed, which {nlm} holds"


@pytest.mark.vcr()
def test_a_pmid_beside_another_paper_s_doi(checked):
    a = checked.register(Identifier(doi="10.1016/j.jse.2011.01.043", pmid=BIOPSYCH[0])).id
    assert _rules(violations(checked, [a])) == [(a, "pmid-doi")]


@pytest.mark.vcr()
def test_the_pmid_of_the_article_s_own_doi_is_no_violation(checked):
    a = checked.register(Identifier(doi=BIOPSYCH[1], pmid=BIOPSYCH[0])).id
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_pubmed_doi_no_registry_knows_is_no_evidence(checked):
    # PubMed 23840428 gives 10.137/journal.pone.0066241, a typo of the paper's DOI.
    a = checked.register(Identifier(doi="10.1371/journal.pone.0066241", pmid="23840428")).id
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_pubmed_doi_the_registry_gives_another_paper_is_no_evidence(checked):
    # 2imk3jt7rl7a: PubMed gives 41221578 (an ocular lymphoma review) 10.14670/HH-25-013,
    # which doi.org gives a parotid tumour study; the article's -014 is the review's.
    a = checked.register(Identifier(doi="10.14670/hh-25-014", pmid="41221578")).id
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_preprint_s_pmid_is_held_to_its_pubmed_doi(checked):
    # vrr7x2ygtjdt holds its eLife paper's PMID and, as its preprint's, 37808793.
    a = checked.register(Identifier(doi="10.7554/elife.92563", pmid="38345852")).id
    _hold(checked, a, "preprint_pmid", "37808793")

    assert [(v.rule, v.detail) for v in violations(checked, [a])] == [("pmid-doi", (
        "pmid 37808793 is 10.1101/2023.09.27.559792 in PubMed, not 10.7554/elife.92563"))]

    _hold(checked, a, "preprint_doi", "10.1101/2023.09.27.559792")
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_pmid_its_own_pmc_record_agrees_with_is_not_another_edition_s(checked):
    # nwlokmfhhxwb, Rev Psiquiatr Salud Ment's English edition: PubMed gives its record the
    # Spanish edition's DOI, which pbig65ccs2nv holds; its PMC record gives the English DOI.
    english = checked.register(Identifier(pmid="32962948", pmcid="PMC10068027",
                                          doi="10.1016/j.rpsmen.2020.07.005")).id
    checked.register(
        Identifier(pmid="38620670", pmcid="PMC7456305", doi="10.1016/j.rpsm.2020.07.003"))
    assert [v for v in violations(checked, [english]) if v.rule == "pmid-doi"] == []


# -- PMCIDs and their PMC records ------------------------------------------------------------

@pytest.mark.vcr()
def test_a_pmcid_whose_pmc_record_is_another_pmid(checked):
    # lg6selcsnr7f: PMC5582689 is PubMed 28290265 (Psychol Med); the article holds the
    # craft beer paper's PMID and DOI beside it.
    a = checked.register(Identifier(pmid="28290323", doi="10.1016/j.foodres.2016.11.043")).id
    _hold(checked, a, "pmcid", "PMC5582689")
    found = violations(checked, [a])
    assert [(v.rule, v.detail) for v in found] == [
        ("pmcid", "PMC5582689 is PubMed 28290265 in PMC, which no article holds"),
        ("pmcid",
         "PMC5582689 is 10.1017/s0033291717000265 in PMC, not 10.1016/j.foodres.2016.11.043")]


@pytest.mark.vcr()
def test_a_pmcid_whose_pmc_record_is_another_doi(checked):
    # x5xhvjd45lqm: PMC4043787 is a JCMR meeting abstract, beside a Magn Reson Med paper.
    a = checked.register(Identifier(doi="10.1002/mrm.25078")).id
    _hold(checked, a, "pmcid", "PMC4043787")
    found = violations(checked, [a])
    assert [v.rule for v in found] == ["pmcid"]
    assert found[0].detail.startswith("PMC4043787 is 10.1186/1532-429x-16-s1-o7 in PMC, not ")


@pytest.mark.vcr()
def test_a_pmcid_agreeing_with_the_article_is_no_violation(checked):
    # 2hauw7ec2exh once repaired: its PMCID, PMID and DOI are one PubMed record's.
    a = checked.register(Identifier(pmid="17134483", doi="10.1186/1747-5333-1-13",
                                    pmcid="PMC1769396")).id
    assert violations(checked, [a]) == []


# -- a preprint and its version of record -----------------------------------------------------

@pytest.mark.vcr()
def test_a_preprint_and_its_version_of_record_on_two_articles(checked):
    # btk5tlwqap5k held bioRxiv's preprint of vrr7x2ygtjdt's eLife paper.
    paper = checked.register(Identifier(doi="10.7554/elife.92563")).id
    preprint = checked.register(Identifier(doi="10.1101/2023.09.27.559792")).id
    found = violations(checked, [preprint])
    assert [(v.article_id, v.rule) for v in found] == [(preprint, "two-versions")]
    assert paper in found[0].detail


# -- the articles a change can reach -----------------------------------------------------------

@pytest.mark.vcr()
def test_an_article_holding_the_pmid_of_a_doi_is_its_holder_s_neighbour(checked):
    # The reviewer's probe: a plan adds d to A, and B holds the PMID PubMed gives d.
    a = checked.register(Identifier(doi=NLM[1])).id
    b = checked.register(Identifier(pmid=NLM[0])).id
    assert neighbourhood(checked, [a]) == {a, b}


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
def test_each_link_reaches_the_neighbour(checked, held, neighbour):
    a = checked.register(Identifier(neurostore="3Rob7cK8y6gY")).id
    b = checked.register(Identifier(neurostore="26SP7hw2qb8u")).id
    for kind, value in held:
        _hold(checked, a, kind, value)
    for kind, value in neighbour:
        _hold(checked, b, kind, value)
    assert neighbourhood(checked, [a]) == {a, b}


@pytest.mark.vcr()
def test_the_neighbourhood_check_is_the_full_check_restricted_to_it(checked, tmp_path):
    # A catalog of the repair's cases, some consistent, some not.
    ids = [
        checked.register(Identifier(doi=NLM[1])).id,
        checked.register(Identifier(pmid=NLM[0])).id,                    # NLM's PMID, alone
        checked.register(Identifier(doi=BIOPSYCH[1], pmid=BIOPSYCH[0])).id,
        checked.register(Identifier(doi="10.7554/elife.92563", pmid="38345852")).id,
        checked.register(Identifier(doi="10.1101/2023.09.27.559792")).id,  # its preprint, apart
        checked.register(Identifier(pmid="17134483", doi="10.1186/1747-5333-1-13",
                                    pmcid="PMC1769396")).id,
        checked.register(Identifier(doi="10.1016/j.jse.2011.01.043", pmid="22015112")).id,
    ]
    full = violations(checked, ids)
    for start in ids:
        around = neighbourhood(checked, [start])
        assert violations(checked, around) == [v for v in full if v.article_id in around]
    assert {v.rule for v in full} >= {"pmid-doi", "two-versions"}


# -- downloads --------------------------------------------------------------------------------

def _download(catalog, article_id, source, root, name, text, status=Status.OK):
    path = root / source / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    catalog.record([Outcome(article_id, "download", source, status,
                            payload={"files": [{"file_path": str(path)}]})])
    return path


def test_a_download_whose_xml_is_another_article_s_paper(checked, tmp_path):
    # mwni3etc4i7l held an Elsevier copy of 54xpppoekzpj's paper.
    a = checked.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
    b = checked.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015")).id
    _download(checked, a, "elsevier", tmp_path / "a", "content.xml",
              "<full-text-retrieval-response>"
              "<prism:doi>10.1016/j.neuroimage.2004.05.015</prism:doi>")
    _download(checked, b, "pubget", tmp_path / "b", "pmcid_1234567/article.xml",
              '<article-id pub-id-type="doi">10.1016/J.NEUROIMAGE.2004.05.015</article-id>')

    found = violations(checked, [a, b])

    assert _rules(found) == [(a, "download")]
    assert found[0].detail == (f"download/elsevier names doi 10.1016/j.neuroimage.2004.05.015, "
                               f"which {b} holds")


def test_a_jats_file_naming_another_article_s_doi(checked, tmp_path):
    a = checked.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
    b = checked.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015")).id
    _download(checked, a, "pmc", tmp_path, "pmcid_1234567/article.xml",
              '<article-id pub-id-type="doi">10.1016/j.neuroimage.2004.05.015</article-id>')
    assert [v.detail for v in violations(checked, [a])] == [
        f"download/pmc names pmcid PMC1234567, doi 10.1016/j.neuroimage.2004.05.015, "
        f"which {b} holds"]


@pytest.mark.vcr()
def test_a_download_naming_one_of_the_article_s_own_ids_is_its_own(checked, tmp_path):
    # 2dd6hgs4ymiv's PMC copy sits under its own PMCID; its XML names the preprint's DOI.
    a = checked.register(Identifier(doi="10.1016/j.ebiom.2026.106400", pmcid="PMC13370491")).id
    checked.register(Identifier(doi="10.64898/2026.01.29.26344990"))
    _download(checked, a, "pmc", tmp_path, "pmcid_13370491/article.xml",
              '<article-id pub-id-type="doi">10.64898/2026.01.29.26344990</article-id>')
    assert [v for v in violations(checked, [a]) if v.rule.startswith("download")] == []


def test_only_ace_names_its_paper_by_file_name(checked, tmp_path):
    a = checked.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
    checked.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015", pmcid="PMC100"))
    _download(checked, a, "pdf", tmp_path, "15465288.html", "<html/>")
    assert violations(checked, [a]) == []


def test_a_failed_download_names_nothing(checked, tmp_path):
    a = checked.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
    checked.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015"))
    _download(checked, a, "elsevier", tmp_path, "content.xml",
              "<prism:doi>10.1016/j.neuroimage.2004.05.015</prism:doi>", status=Status.FAILED)
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_an_ace_file_names_its_paper_by_pmid(checked, tmp_path):
    # ml7z5xc7cvgo and su74e7qsacoi each held the other's ACE page.
    a = checked.register(Identifier(pmid="15658005")).id
    b = checked.register(Identifier(pmid="15465288")).id
    _download(checked, a, "ace", tmp_path / "a", "15465288.html", "<html/>")
    _download(checked, b, "ace", tmp_path / "b", "15465288.html", "<html/>")
    assert _rules(violations(checked, [a, b])) == [(a, "download")]


def test_a_download_whose_record_or_file_is_gone_is_a_violation(checked, tmp_path):
    a = checked.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
    path = _download(checked, a, "elsevier", tmp_path, "content.xml", "<prism:doi>x</prism:doi>")
    path.unlink()
    b = checked.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015")).id
    _download(checked, b, "pubget", tmp_path, "pmcid_1/article.xml", "<article/>")
    blob = checked.artifact(b, "download", "pubget").blob
    for found in checked.root.glob(f"blobs/**/{blob}*"):
        found.unlink()

    found = sorted((v.article_id, v.rule, v.detail.split(":")[0])
                   for v in violations(checked, [a, b]))
    assert found == sorted([(a, "download-unreadable", "download/elsevier"),
                            (b, "download-unreadable", "download/pubget")])


@pytest.mark.vcr()
def test_a_journal_record_s_doi_is_the_article_s_doi_not_an_alternate(checked):
    # ki7akud6c347: the journal's earlier mEDRA DOI, and PubMed's Springer DOI
    a = checked.register(Identifier(doi="10.3275/7271", pmid="20885123")).id
    _hold(checked, a, "alternative_doi", "10.1007/bf03337758")
    assert _rules(violations(checked, [a])) == [(a, "vor-doi")]
    checked._conn.execute("UPDATE aliases SET kind=CASE kind WHEN 'doi' THEN 'alternative_doi' "
                          "ELSE 'doi' END WHERE article_id=? AND kind IN ('doi', 'alternative_doi')",
                          (a,))
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_pmc_record_without_a_pmid_names_its_paper_only_by_a_registered_doi(checked):
    # PMC10411397, a European Psychiatry abstract, gives 10.1192/j.eurpsy.2023.1163, which
    # no registry has; the article's own DOI is another such abstract's.
    a = checked.register(Identifier(pmcid="PMC10411397", doi="10.1192/j.eurpsy.2023.662")).id
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_preprint_s_pubmed_record_and_its_paper_on_two_articles(checked):
    # btk5tlwqap5k held only 37808793, the Preprint-typed record of vrr7x2ygtjdt's paper.
    paper = checked.register(Identifier(doi="10.7554/elife.92563")).id
    preprint = checked.register(Identifier(pmid="37808793")).id
    found = [v for v in violations(checked, [preprint]) if v.rule == "two-versions"]
    assert [v.article_id for v in found] == [preprint] and paper in found[0].detail


@pytest.mark.vcr()
def test_a_meeting_abstract_s_pmc_record_beside_the_paper_breaks_the_pmcid_rule(checked):
    # x5xhvjd45lqm: the MRM paper's PMID and DOI, and PMC4043787, the JCMR supplement's
    # abstract (no PMID in PMC), whose DOI the plan had made an alternate.
    a = checked.register(Identifier(pmid="24532480", doi="10.1002/mrm.25133")).id
    _hold(checked, a, "pmcid", "PMC4043787")
    _hold(checked, a, "alternative_doi", "10.1186/1532-429x-16-s1-o7")
    found = violations(checked, [a])
    assert (a, "pmcid") in _rules(found) and (a, "alternate-doi") in _rules(found)
    assert any("only as an alternate DOI" in v.detail for v in found if v.rule == "pmcid")


@pytest.mark.vcr()
def test_an_erratum_kept_as_an_alternate_doi_is_another_work(checked):
    # rg5bc6o7gs7m: the paper, and "Erratum to 'A quantitative comparison...'"
    a = checked.register(Identifier(pmid="11595367", doi="10.1016/s0730-725x(01)00418-0")).id
    _hold(checked, a, "alternative_doi", "10.1016/s0730-725x(03)00069-9")
    assert _rules(violations(checked, [a])) == [(a, "alternate-doi")]


@pytest.mark.vcr()
def test_a_preprint_s_pubmed_record_with_its_paper_s_title_is_its_other_version(checked):
    # wlvovj2lmdff, the bioRxiv preprint's PubMed record, and xxv5qzt4dg7u, the Sci Rep
    # paper: "Neural effects of TMS trains on the human prefrontal cortex."
    preprint = checked.register(Identifier(pmid="36778457", doi="10.1101/2023.01.30.526374")).id
    paper = checked.register(Identifier(pmid="38123591", doi="10.1038/s41598-023-49250-7")).id
    found = [v for v in violations(checked, [preprint]) if v.rule == "two-versions"]
    assert any(f"PubMed 38123591's work, which {paper} holds" in v.detail for v in found)
    assert preprint in neighbourhood(checked, [paper])


def test_a_scope_over_downloads_not_indexed_is_refused(checked, tmp_path):
    a = checked.register(Identifier(neurostore="3Rob7cK8y6gY")).id
    checked.record([Outcome(a, "download", "ace", Status.OK,
                            payload={"files": [{"file_path": "/ace/22015111.html"}]})])
    with pytest.raises(ValueError, match="1 downloads are not indexed"):
        neighbourhood(checked, [a])
    assert index_downloads(checked) == 1
    assert neighbourhood(checked, [a]) == {a}


def test_an_unreadable_download_is_indexed_as_naming_nothing(checked, tmp_path):
    a = checked.register(Identifier(neurostore="3Rob7cK8y6gY")).id
    gone = tmp_path / "elsevier" / "content.xml"
    checked.record([Outcome(a, "download", "elsevier", Status.OK,
                            payload={"files": [{"file_path": str(gone)}]})])
    assert index_downloads(checked) == 1
    assert neighbourhood(checked, [a]) == {a}
    assert [v.rule for v in violations(checked, [a])] == ["download-unreadable"]


@pytest.mark.vcr()
def test_a_pmc_record_of_the_article_s_pubmed_record_may_give_its_other_doi(checked):
    # aucqqxrbcno2: PubMed gives PNAS's issue DOI, PMC its early edition's
    a = checked.register(Identifier(pmid="10725372", pmcid="PMC16281",
                                    doi="10.1073/pnas.97.7.3573")).id
    _hold(checked, a, "alternative_doi", "10.1073/pnas.050583797")
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_preprint_and_its_paper_each_with_a_base_study_are_accepted_not_violations(checked):
    # bdarfou5chmg (Int J Obes, base study vY5mktCMTn9P) and nj52vmozfa3k (Research Square,
    # JAJmM4dhUPuZ): apart until neurostore can make one base study a version of the other.
    # The paper's PMC download is the preprint's copy.
    paper = checked.register(Identifier(pmid="42230985", pmcid="PMC13387087",
                                        doi="10.1038/s41366-026-02114-2",
                                        neurostore="vY5mktCMTn9P")).id
    preprint = checked.register(Identifier(pmid="41674811", pmcid="PMC12889832",
                                           doi="10.21203/rs.3.rs-8694593/v1",
                                           neurostore="JAJmM4dhUPuZ")).id
    checked.record([Outcome(paper, "download", "pubget", Status.OK, payload={"files": [
        {"file_path": "/pubget/articles/pmcid_12889832/tables.xml"}]})])
    found = violations(checked, [paper, preprint])
    assert sorted((v.article_id, v.rule) for v in found) == sorted(
        [(paper, "download")] + [(preprint, "two-versions")] * sum(
            v.rule == "two-versions" for v in found))
    assert found and all(v.accepted for v in found)
    checked._conn.execute("DELETE FROM aliases WHERE kind='neurostore' AND article_id=?", (paper,))
    assert not any(v.accepted for v in violations(checked, [paper, preprint]))


@pytest.mark.vcr()
def test_an_article_holding_only_its_pmid_asks_no_registry_of_pubmed_s_doi(checked):
    # 7i57itl5rdoi: PubMed gives 16059980 a DOI whose agency serves a bad certificate; no
    # article holds it, and the article holds no DOI, so no rule has anything to judge
    a = checked.register(Identifier(pmid="16059980")).id
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_download_between_two_based_articles_that_are_not_versions_is_a_violation(
        checked, tmp_path):
    # mwni3etc4i7l's Elsevier copy is 54xpppoekzpj's paper; each article holds a base study
    holder = checked.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007",
                                         neurostore="3Rob7cK8y6gY")).id
    owner = checked.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015",
                                        neurostore="AkYZauDLLAS5")).id
    xml = tmp_path / "elsevier" / "content.xml"
    xml.parent.mkdir()
    xml.write_text("<prism:doi>10.1016/j.neuroimage.2004.05.015</prism:doi>")
    checked.record([Outcome(holder, "download", "elsevier", Status.OK,
                            payload={"files": [{"file_path": str(xml)}]})])
    [found] = [v for v in violations(checked, [holder]) if v.rule == "download"]
    assert owner in found.detail and not found.accepted


@pytest.mark.vcr()
def test_each_download_is_judged_though_another_is_unreadable(checked):
    # an article's ACE copy has no record; its pubget copy is lg6selcsnr7f's PMC copy
    a = checked.register(Identifier(neurostore="3Rob7cK8y6gY")).id
    b = checked.register(Identifier(pmcid="PMC5582689", pmid="28290265")).id
    checked.record([Outcome(a, "download", "ace", Status.OK),
                    Outcome(a, "download", "pubget", Status.OK, payload={"files": [
                        {"file_path": "/pubget/articles/pmcid_5582689/tables.xml"}]})])
    assert sorted(v.rule for v in violations(checked, [a])) == ["download", "download-unreadable"]
    assert b in next(v.detail for v in violations(checked, [a]) if v.rule == "download")


@pytest.mark.vcr()
def test_a_registry_that_cannot_be_read_judges_nothing(checked):
    # mxp7s3fbegzb: PubMed's DOI, and the alias spelling no registry describes
    a = checked.register(Identifier(doi="10.1176/jnp.2009.21.3.307")).id
    _hold(checked, a, "alternative_doi", "10.1176/appi.neuropsych.21.3.307")
    assert [v.rule for v in violations(checked, [a])] == ["evidence-unreadable"]


@pytest.mark.vcr()
def test_an_unregistered_alternate_spelling_is_no_other_work(checked):
    a = checked.register(Identifier(pmid=BIOPSYCH[0], doi=BIOPSYCH[1])).id
    _hold(checked, a, "alternative_doi", "10.137/journal.pone.0066241")
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_pmid_its_own_pmc_record_agrees_with_may_hold_pmc_s_doi_not_pubmed_s(checked):
    # aucqqxrbcno2: PubMed gives PNAS's issue DOI; the article holds its PMC record's early
    # edition DOI
    a = checked.register(
        Identifier(pmid="10725372", pmcid="PMC16281", doi="10.1073/pnas.050583797")).id
    assert violations(checked, [a]) == []


@pytest.mark.vcr()
def test_a_preprint_record_s_doi_held_as_an_alternate_is_no_version_of_record_s(checked):
    # 37808793 is bioRxiv's record of vrr7x2ygtjdt's eLife paper
    a = checked.register(Identifier(pmid="37808793", doi="10.7554/elife.92563")).id
    _hold(checked, a, "alternative_doi", "10.1101/2023.09.27.559792")
    assert "vor-doi" not in [v.rule for v in violations(checked, [a])]


@pytest.mark.vcr()
def test_a_preprint_pmid_s_doi_held_as_an_alternate_is_no_version_of_record_s(checked):
    a = checked.register(Identifier(pmid="38345852", doi="10.7554/elife.92563")).id
    _hold(checked, a, "preprint_pmid", "37808793")
    _hold(checked, a, "alternative_doi", "10.1101/2023.09.27.559792")
    assert "vor-doi" not in [v.rule for v in violations(checked, [a])]


@pytest.mark.vcr()
def test_two_preprint_pmcids_are_a_violation(checked):
    a = checked.register(Identifier(doi="10.7554/elife.92563")).id
    _hold(checked, a, "preprint_pmcid", "PMC10634801")
    _hold(checked, a, "preprint_pmcid", "PMC10634864")
    assert (a, "preprint-pmcids") in _rules(violations(checked, [a]))


@pytest.mark.vcr()
def test_a_download_naming_an_id_held_as_a_second_kind_links_its_article(checked):
    # the other article holds the PMID as its preprint's
    a = checked.register(Identifier(doi="10.7554/elife.92563")).id
    _hold(checked, a, "preprint_pmid", "37808793")
    x = checked.register(Identifier(neurostore="3Rob7cK8y6gY")).id
    checked.record([Outcome(x, "download", "ace", Status.OK,
                            payload={"files": [{"file_path": "/ace/37808793.html"}]})])
    index_downloads(checked)
    assert x in neighbourhood(checked, [a])


@pytest.mark.vcr()
def test_a_paper_s_record_held_as_a_preprint_s_with_its_doi_an_alternate_is_no_vor_violation(
        checked):
    # only the article's own PMID's DOI must be its DOI
    a = checked.register(Identifier(doi=NLM[1])).id
    _hold(checked, a, "preprint_pmid", BIOPSYCH[0])
    _hold(checked, a, "alternative_doi", BIOPSYCH[1])
    assert "vor-doi" not in [v.rule for v in violations(checked, [a])]


@pytest.mark.vcr()
def test_a_preprint_record_held_as_the_preprint_s_finds_its_paper_by_title(checked):
    # wlvovj2lmdff's bioRxiv record, held only as a preprint's; xxv5qzt4dg7u holds Sci Rep's
    preprint = checked.register(Identifier(neurostore="3Rob7cK8y6gY")).id
    _hold(checked, preprint, "preprint_pmid", "36778457")
    paper = checked.register(Identifier(pmid="38123591", doi="10.1038/s41598-023-49250-7")).id
    found = [v for v in violations(checked, [preprint]) if v.rule == "two-versions"]
    assert any(f"PubMed 38123591's work, which {paper} holds" in v.detail for v in found)
