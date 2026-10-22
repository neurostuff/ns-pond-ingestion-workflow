"""The identity evidence: when two records are one work, and what the registries say of a DOI.

The records are PubMed's and the registries' own, recorded with the placeholder email.
"""

from __future__ import annotations

import httpx
import pytest
from ingestion_workflow.catalog.evidence import same_work
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.doi_registry import DoiRegistry, RegisteredWork, Unreadable
from ingestion_workflow.clients.pubmed import PubMedSummary, _retryable

#: Pairs of PubMed records that are two works.
TWO_PUBMED_WORKS = [
    # a 2007 review titled after the 2001 paper, by its first author
    ("17719799", "11209064"),
    # a debate's two sides: "...for thrombolysis: no." and "...: yes."
    ("19498182", "19498183"),
    # "A neural signature of the current self" and "...of the unique hues"
    ("19015110", "28186142"),
    # a reply and the paper it answers
    ("17907799", "16261197"),
    # a retraction notice and the retracted paper
    ("16790551", "9482945"),
]

#: Pairs of DOIs the registries give two works, though PubMed's titles are alike.
TWO_REGISTERED_WORKS = [
    # an erratum and the paper it corrects
    ("10.1016/s0730-725x(03)00069-9", "10.1016/s0730-725x(01)00418-0"),
    # a book chapter and the Nature Reviews primer of its title
    ("10.1016/b978-0-443-21710-4.00015-x", "10.1038/s43586-021-00047-w"),
    # a JCMR supplement's abstract and the MRM paper
    ("10.1186/1532-429x-16-s1-o7", "10.1002/mrm.25133"),
]


#: PubMed's record of 11209064, and what the registry says of its DOI, 10.1073/pnas.98.2.676.
RAICHLE = PubMedSummary(doi="10.1073/pnas.98.2.676", title="A default mode of brain function.",
                        pubtypes=("Journal Article",), first_author="Raichle ME", year=2001,
                        issns=("0027-8424", "1091-6490"))
REGISTERED = RegisteredWork(title="A default mode of brain function", first_author="Raichle",
                            year=2001, type="article-journal", issns=("0027-8424", "1091-6490"))


def _as(**changed):
    return RegisteredWork(**{**REGISTERED.__dict__, **changed})


@pytest.mark.parametrize("other, same", [
    (REGISTERED, True),
    # one thing apart, each enough to make two works
    (_as(title="Erratum: A default mode of brain function"), False),
    (_as(first_author="Gusnard"), False),
    (_as(year=2004), False),
    (_as(year=2002), True),
    (_as(issns=("1053-8119",)), False),
    (_as(issns=()), True),
    (_as(type="chapter"), False),
    # a main title of fewer than five words is not enough
    (_as(title="A default mode: a brief history of an evolving idea in brain science"), False),
    (_as(title="A default mode of brain function: a brief history"), True),
    # the family name inside a registry's whole name
    (_as(first_author="Marcus E. Raichle"), True),
])
def test_one_part_of_a_record_apart_is_another_work(other, same):
    assert same_work(RAICHLE, other) is same


def test_a_main_title_needs_five_words_to_stand_for_the_whole():
    assert not same_work(_as(title="Default mode"), _as(title="Default mode: a brief history"))
    assert same_work(_as(title="A default mode of brain"),
                     _as(title="A default mode of brain: a history"))


def test_alike_letters_are_one_title_only_when_the_words_are_spellings_of_each_other():
    tumour = _as(title="Tumour odour analysis")
    assert same_work(tumour, _as(title="Tumor odour analyses"))
    # alike letter by letter, but below the likeness asked of a title
    assert not same_work(tumour, _as(title="Tumors odor analyses"))
    # alike letter by letter, but another word
    debate = "MR mismatch is useful for patient selection for thrombolysis: "
    assert not same_work(_as(title=debate + "no"), _as(title=debate + "yes"))
    # a notice of the same paper, by the same author, the same year, is the same notice
    notice = PubMedSummary(**{**RAICHLE.__dict__, "pubtypes": ("Published Erratum",)})
    assert same_work(notice, notice) and not same_work(notice, RAICHLE)


@pytest.mark.vcr()
@pytest.mark.parametrize("first, second", TWO_PUBMED_WORKS)
def test_alike_pubmed_records_of_two_works_are_two_works(pubmed, first, second):
    records = pubmed.summaries([first, second])
    assert not same_work(records[first], records[second])
    assert not same_work(records[first], records[second], versions=True)


@pytest.mark.vcr()
@pytest.mark.parametrize("first, second", TWO_REGISTERED_WORKS)
def test_alike_registered_works_are_two_works(evidence, first, second):
    assert not evidence.same_dois(first, second)


@pytest.mark.vcr()
@pytest.mark.parametrize("pmid, doi", [
    # PubMed's and Crossref's titles differ in case and the full stop
    ("22015111", "10.1016/j.biopsych.2011.09.005"),
    # the journal's earlier mEDRA DOI and PubMed's Springer one
    ("20885123", "10.3275/7271"),
    ("20885123", "10.1007/bf03337758"),
])
def test_a_doi_registered_to_a_pubmed_record_s_work_is_its_paper(pubmed, evidence, pmid, doi):
    assert evidence.same_paper(doi, pubmed.summaries([pmid])[pmid])


@pytest.mark.vcr()
@pytest.mark.parametrize("preprint, paper", [
    # the Preprint-typed record of vrr7x2ygtjdt's eLife paper
    ("37808793", "38345852"),
    # bioRxiv's record by Dranovsky, the paper's by Kirshenbaum: one title, the authors reordered
    ("36778445", "37479778"),
])
def test_a_preprint_and_its_paper_are_versions_of_one_work(pubmed, preprint, paper):
    records = pubmed.summaries([preprint, paper])
    assert same_work(records[preprint], records[paper], versions=True)
    assert not same_work(records[preprint], records[paper])


@pytest.mark.vcr()
def test_the_registry_tells_an_unregistered_doi_from_one_it_cannot_read():
    registry = DoiRegistry(CrossrefClient())
    assert registry.work("10.137/journal.pone.0066241") is None
    assert registry.work("10.14670/hh-25-014").title.startswith("Ocular adnexal lymphomas")
    # doi.org sends it to chinadoi, which answers 404
    with pytest.raises(Unreadable, match="10.7507/1001-5515.201511032"):
        registry.work("10.7507/1001-5515.201511032")


def test_an_agency_serving_another_host_s_certificate_cannot_be_read(monkeypatch):
    # doi.org sends some DOIs to data.chinadoi.cn, whose certificate names another host.
    request = httpx.Request("GET", "https://data.chinadoi.cn/x")
    bad = httpx.ConnectError(
        "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, "
        "certificate is not valid for 'data.chinadoi.cn'.", request=request)
    registry = DoiRegistry(CrossrefClient())
    asked = []

    def refuse(doi):
        asked.append(doi)
        raise bad

    client = type("Client", (), {"get": lambda self, url: refuse(url)})()
    monkeypatch.setattr(registry, "_client", client)
    with pytest.raises(Unreadable, match="bad certificate"):
        registry.work("10.3969/j.issn.1671-7104.2017.02.005")
    assert len(asked) == 1


@pytest.mark.parametrize("response, retried", [(429, True), (500, True), (503, True), (404, False),
                                               (400, False)])
def test_ncbi_is_asked_again_after_a_refusal_to_come_back_later(response, retried):
    request = httpx.Request("GET", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi")
    error = httpx.HTTPStatusError(
        "x", request=request, response=httpx.Response(response, request=request))
    assert _retryable(error) is retried
    assert _retryable(httpx.ConnectError("refused", request=request)) is True


def test_a_preprint_by_another_first_author_is_its_paper_s_only_word_for_word():
    preprint = PubMedSummary(**{**RAICHLE.__dict__, "pubtypes": ("Preprint",),
                                "first_author": "Gusnard DA",
                                "title": "Colour behaviour in the default mode of brain function"})
    paper = _as(title="Color behavior in the default mode of brain function")
    assert not same_work(preprint, paper, versions=True)
    assert same_work(preprint, _as(title="Colour behaviour in the default mode of brain function"),
                     versions=True)
    assert same_work(PubMedSummary(**{**preprint.__dict__, "first_author": "Raichle ME"}), paper,
                     versions=True)
