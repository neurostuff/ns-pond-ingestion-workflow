"""The identity evidence: when two records are one work, and what the registries say of a DOI.

The records are PubMed's and the registries' own, recorded with the placeholder email.
"""

from __future__ import annotations

import httpx
import pytest
from ingestion_workflow.catalog.evidence import same_work
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
    # "...in Homosexual Men" and "...in Homosexual Women"
    ("28145518", "29330483"),
    # tau PET with T807 and with T808
    ("23234879", "23948934"),
    # Part I and Part II of one series
    ("33907337", "33935294"),
    ("20851196", "21497658"),
    ("10385582", "10860796"),
    # a review's protocol and the review
    ("35025895", "36156089"),
    # "Reply: X" and X; 'On "X"' and X
    ("24993959", "25398234"),
    ("39155793", "39803924"),
    ("30415448", "31267308"),
]

@pytest.mark.vcr()
def test_an_early_release_with_one_word_fewer_in_its_title_is_the_paper(evidence):
    # PLoS Comput Biol's early online release: "...trapping in the ribosome" (2005), and the
    # paper: "...trapping in the ribosome tunnel" (2006)
    assert evidence.same_dois("10.1371/journal.pcbi.0020098", "10.1371/journal.pcbi.0020098.eor")


@pytest.mark.vcr()
def test_an_osf_dataset_is_not_its_paper(pubmed, evidence):
    # 7yzditxbiw6f: "Dataset: Prediction Errors..." typed a journal article, and PubMed 34911768
    record = pubmed.summaries(["34911768"])["34911768"]
    assert not evidence.same_paper("10.17605/osf.io/xb7sq", record)


@pytest.mark.vcr()
def test_versions_are_read_from_both_sides_of_crossref_s_relation(evidence):
    # bioRxiv's preprint is-preprint-of the PLoS paper; the paper's record does not say so back
    assert "10.1101/2022.04.28.489908" in evidence.versions("10.1371/journal.pcbi.1013995")


@pytest.mark.vcr()
def test_the_other_side_of_a_relation_is_found_by_the_doi_as_pubmed_spells_it(evidence):
    # bioRxiv's 10.1101/2025.04.29.651293 is-preprint-of 10.1523/JNEUROSCI.0839-25.2025, in
    # that case; the J Neurosci paper's record (PubMed 41330636) does not say so back
    evidence.summaries(["41330636"])
    assert "10.1101/2025.04.29.651293" in evidence.versions("10.1523/jneurosci.0839-25.2025")


@pytest.mark.vcr()
def test_the_other_side_of_a_relation_is_found_by_the_doi_as_pmc_spells_it(evidence):
    # bioRxiv's 10.1101/2025.06.13.659501 is-preprint-of 10.1162/IMAG.a.1007, the spelling of
    # PMC12612100's record; the Imaging Neuroscience paper's own record relates nothing
    evidence.pmc(["PMC12612100"])
    assert "10.1101/2025.06.13.659501" in evidence.versions("10.1162/imag.a.1007")


@pytest.mark.vcr()
def test_a_reviewed_version_s_concept_doi_is_a_version_only_when_the_registry_says_so(evidence):
    # eLife's reviewed version 10.7554/elife.92563.N stands for 10.7554/elife.92563; JOV's
    # 10.1167/jov.24.6.11 is no version of a "10.1167/jov.24.6"
    assert "10.7554/elife.92563" in evidence.versions("10.1101/2023.09.27.559792")
    assert "10.1167/jov.24.6" not in evidence.versions("10.1101/2024.01.11.575288")


def test_a_short_title_gaining_a_word_is_another_title():
    assert not same_work(_as(title="Neural correlates of auditory attention"),
                         _as(title="Neural correlates of auditory attention shifts"))


#: Pairs of DOIs the registries give two works, though PubMed's titles are alike.
TWO_REGISTERED_WORKS = [
    # an erratum and the paper it corrects
    ("10.1016/s0730-725x(03)00069-9", "10.1016/s0730-725x(01)00418-0"),
    # a book chapter and the Nature Reviews primer of its title
    ("10.1016/b978-0-443-21710-4.00015-x", "10.1038/s43586-021-00047-w"),
    # a JCMR supplement's abstract and the MRM paper
    ("10.1186/1532-429x-16-s1-o7", "10.1002/mrm.25133"),
    # a Corrigendum, its title ending in the word, and the paper
    ("10.1107/s1744309113029321", "10.1107/s1744309113016709"),
    # HBM's abstract in a NeuroImage supplement and the NeuroImage paper
    ("10.1016/s1053-8119(00)91391-0", "10.1006/nimg.2000.0687"),
    # OARSI abstracts in Osteoarthritis Cartilage's supplement and the papers
    ("10.1016/j.joca.2022.02.507", "10.1016/j.joca.2022.06.004"),
    ("10.1016/j.joca.2023.01.269", "10.1016/j.joca.2023.07.007"),
    # an IEEE conference paper and the MRI paper
    ("10.1109/bhi.2012.6211720", "10.1016/j.mri.2012.05.009"),
]


#: PubMed's record of 11209064, and what the registry says of its DOI, 10.1073/pnas.98.2.676.
RAICHLE = PubMedSummary(doi="10.1073/pnas.98.2.676", title="A default mode of brain function.",
                        pubtypes=("Journal Article",), first_author="Raichle ME", year=2001,
                        issns=("0027-8424", "1091-6490"),
                        journal="Proceedings of the National Academy of Sciences of the United "
                                "States of America")
REGISTERED = RegisteredWork(title="A default mode of brain function", first_author="Raichle",
                            year=2001, type="article-journal", issns=("0027-8424", "1091-6490"),
                            journal="Proceedings of the National Academy of Sciences")


def _as(**changed):
    return RegisteredWork(**{**REGISTERED.__dict__, **changed})


@pytest.mark.parametrize("other, same", [
    (REGISTERED, True),
    # one thing apart, each enough to make two works
    (_as(title="Erratum: A default mode of brain function"), False),
    (_as(first_author="Gusnard"), False),
    (_as(year=2004), False),
    (_as(year=2003), False),
    (_as(year=2002), True),
    (_as(issns=("1053-8119",), journal="NeuroImage"), False),
    (_as(issns=("1053-8119",), journal=None), False),
    (_as(supplement=True), False),
    (_as(type="proceedings-article"), False),
    # one journal by name, by its electronic ISSN alone
    (_as(issns=("1091-6491",)), True),
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


@pytest.mark.parametrize("author, same", [("Marcus E. Raichle", True), ("Gusnard", False)])
def test_a_notice_is_its_notice_by_the_same_author(author, same):
    # one reply, PubMed's record and the registry's, whose author is the whole name
    reply = "Reply: A default mode of brain function"
    assert same_work(_pm(reply), _as(title=reply, first_author=author)) is same


@pytest.mark.parametrize("other", [_as(supplement=True), _as(type="proceedings-article")])
def test_a_supplement_or_proceedings_record_of_the_pubmed_record_s_own_doi_is_its_paper(other):
    assert same_work(RAICHLE, RegisteredWork(**{**other.__dict__, "doi": RAICHLE.doi}))


def _pm(title, doi=RAICHLE.doi):
    return PubMedSummary(**{**RAICHLE.__dict__, "title": title, "doi": doi})


def test_a_main_title_needs_five_words_to_stand_for_the_whole():
    assert not same_work(_pm("Default mode"), _as(title="Default mode: a brief history"))
    assert same_work(_pm("A default mode of brain"),
                     _as(title="A default mode of brain: a history"))
    # two PubMed records of two DOIs: two papers of one series
    assert not same_work(_pm("A default mode of brain"),
                         _pm("A default mode of brain: a history", doi="10.1073/pnas.98.2.677"))


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
    # Crossref gives Brain's electronic ISSN, PubMed its print one
    ("11335702", "10.1093/brain/124.5.1003"),
    # "...5-HT1A autoreceptors" at Crossref, "...5-HT(1A) autoreceptors." in PubMed
    ("39396089", "10.1038/s41386-024-01999-9"),
    # the journal's earlier mEDRA DOI and PubMed's Springer one
    ("20885123", "10.3275/7271"),
    ("20885123", "10.1007/bf03337758"),
])
def test_a_doi_registered_to_a_pubmed_record_s_work_is_its_paper(pubmed, evidence, pmid, doi):
    assert evidence.same_paper(doi, pubmed.summaries([pmid])[pmid])


@pytest.mark.vcr()
@pytest.mark.parametrize("pmid", [
    # "Jack CR Jr", "Byrd TF 4th", "Murty DVPS" (wjoo3yyhy5io's paper), "Schaper FLWVJ", and
    # "Grubb RL Jr" beside the registry's "Grubb, Jr"
    "22409939", "37486632", "32276055", "37399040", "9757852",
    # the registry's "Ulu?" for "Uluğ", "Soon" for "Soong", "Gomez M.", and PubMed's
    # "Rajesh-Khanna D-S" and "Frederick B deB"
    "10373028", "9124801", "10580535", "41055359", "9888405",
    # a supplement issue to PubMed, not to Crossref
    "15228754", "18813171", "34628088",
    # IEEE EMBC and SMC papers: Journal Articles to PubMed, proceedings-articles to Crossref
    "17271546", "40039850", "37333993",
    # the series' part in PubMed's title alone: "...mood disorders. I. Comparison..."
    "2294857", "3492982", "8527465",
    # "sigma" and "sigma1", "p17gag" and "pl7gag", "encoding" and "coding", "18Fluoro"
    "12885582", "1458053", "37070082", "9760998",
    # a reply: 2009 in PubMed, 2008 at Crossref
    "19382289",
])
def test_a_pubmed_record_and_its_own_doi_s_record_are_one_paper(pubmed, evidence, pmid):
    record = pubmed.summaries([pmid])[pmid]
    assert evidence.same_paper(record.doi, record)


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
    registry = DoiRegistry()
    assert registry.work("10.137/journal.pone.0066241") is None
    assert registry.work("10.14670/hh-25-014").title.startswith("Ocular adnexal lymphomas")
    # doi.org sends it to chinadoi over plain HTTP: not followed
    with pytest.raises(Unreadable, match="10.7507/1001-5515.201511032: .* over plain HTTP"):
        registry.work("10.7507/1001-5515.201511032")


def test_an_agency_serving_another_host_s_certificate_cannot_be_read(monkeypatch):
    # doi.org sends some DOIs to data.chinadoi.cn, whose certificate names another host.
    request = httpx.Request("GET", "https://data.chinadoi.cn/x")
    bad = httpx.ConnectError(
        "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, "
        "certificate is not valid for 'data.chinadoi.cn'.", request=request)
    registry = DoiRegistry()
    asked = []

    def refuse(doi):
        asked.append(doi)
        raise bad

    client = type("Client", (), {"get": lambda self, url, **_: refuse(url)})()
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


@pytest.mark.parametrize("first, second, same", [
    # an 8-word title may gain a word, a 7-word one may not, nor may one gain two
    ("Neural coding of reward in the human striatum",
     "Neural coding of reward in the human ventral striatum", True),
    ("Neural coding of reward in human striatum",
     "Neural coding of reward in human ventral striatum", False),
    ("Neural coding of reward in the human striatum",
     "Neural coding of monetary reward in the human ventral striatum", False),
    # a four-word main title is not enough, a five-word one is
    ("Default mode network connectivity",
     "Default mode network connectivity: a brief history", False),
    ("Default mode network connectivity changes",
     "Default mode network connectivity changes: a history", True),
    # alike letters, but one word in place of another of half its letters
    ("Neural correlates of the default mode network in older adults with depression",
     "Neural correlates of the default mode network in older adults with deprivation", False),
    # a series' years, and a journal's editors' best
    ("Endogenous opiates and behavior: 2020", "Endogenous opiates and behavior: 2021", False),
    ("Editors' Best of 2021", "Editors' Best of 2022", False),
    # one record a notice by its title alone
    ("Reprint of: Default mode network connectivity changes",
     "Default mode network connectivity changes", False),
    ("Dataset: Prediction errors in the default mode network",
     "Prediction errors in the default mode network", False),
    ("Reprint of: Neural coding of reward in the human ventral striatum",
     "Neural coding of reward in the human ventral striatum", False),
    # a one-letter British prefix is a spelling
    ("Oestrogen receptors in the human brain", "Estrogen receptors in the human brain", True),
    # a main title, then a series' part
    ("Early time points perfusion imaging", "Early time points perfusion imaging: Part II", False),
])
def test_where_one_title_ends_and_another_begins(first, second, same):
    assert same_work(_pm(first), _as(title=second)) is same


def test_initials_in_any_script_are_not_the_family_name():
    # PubMed 37249071's first author is "Özdemir İ"; Crossref's, of 10.1002/mrm.29706, "Özdemir"
    pubmed = PubMedSummary(**{**RAICHLE.__dict__, "first_author": "Özdemir İ"})
    assert same_work(pubmed, _as(first_author="Özdemir"))


def test_initials_in_any_script_are_dropped_before_a_registry_s_whole_name():
    pubmed = PubMedSummary(**{**RAICHLE.__dict__, "first_author": "Özdemir İ"})
    assert same_work(pubmed, _as(first_author="İlkay Özdemir"))


def test_titles_of_the_same_letters_are_one_title_across_two_dois():
    # "5-HT1A" and "5-HT(1A)", the registry's record of a DOI the PubMed record does not give
    assert same_work(_pm("Agonist binding at 5-HT1A autoreceptors in the raphe"),
                     _as(title="Agonist binding at 5-HT(1A) autoreceptors in the raphe"))


def test_a_two_letter_family_name_ending_another_is_another_author():
    assert not same_work(_as(first_author="Ng"), _as(first_author="Wang"))
    assert same_work(_as(first_author="Raichle"), _as(first_author="Marcus E. Raichle"))


@pytest.mark.vcr()
def test_two_pubmed_records_of_one_doi_are_one_paper_though_one_has_a_subtitle(pubmed):
    # 5qxpg3twr524's 37401785 and w5rawsklx347's 35770780, both 10.1684/epd.2022.1433
    records = pubmed.summaries(["35770780", "37401785"])
    assert same_work(records["35770780"], records["37401785"])


def _played(vcr):
    return [vcr.requests[i].uri for i in sorted(vcr.play_counts)]


@pytest.mark.vcr()
def test_crossref_dois_are_read_in_one_query_and_another_agency_s_singly(evidence, vcr):
    # Crossref's list query leaves out 10.1254/jjp.52.504, which doi.org sends on to
    # Elsevier's 10.1016/S0021-5198(19)40046-2: it is asked of doi.org alone
    crossref = ["10.1016/j.biopsych.2011.09.005", "10.1016/j.nlm.2011.06.006"]
    evidence.ask_registry([*crossref, "10.1254/jjp.52.504"])

    works = {doi: evidence.work(doi) for doi in [*crossref, "10.1254/jjp.52.504"]}

    assert all(works.values())
    played = _played(vcr)
    assert sum("api.crossref.org/works?" in uri for uri in played) == 1
    asked = [uri for uri in played if uri.startswith("https://doi.org/")]
    assert asked[0] == "https://doi.org/10.1254/jjp.52.504"
    assert not [uri for uri in asked if "biopsych" in uri or "nlm" in uri]


@pytest.mark.vcr()
def test_an_alias_doi_s_own_record_names_its_pubmed_paper(evidence, vcr):
    # doi.org follows Crossref's alias 10.1016/0165-3806(96)00035-1 to
    # 10.1016/s0165-3806(96)80004-6; the list query reads the alias's own record, which
    # names the same paper (PubMed 8836571)
    doi = "10.1016/0165-3806(96)00035-1"
    summary = evidence.summaries(["8836571"])["8836571"]
    evidence.ask_registry([doi])

    assert evidence.same_paper(doi, summary)
    assert not [uri for uri in _played(vcr) if uri.startswith("https://doi.org/")]
