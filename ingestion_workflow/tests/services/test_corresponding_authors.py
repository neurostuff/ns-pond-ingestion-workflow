"""Corresponding authors: what each source marks, and which source speaks for the paper.

The JATS, Elsevier and publisher-page files are real, cut down to their author markup;
each is named for a way the papers on beast pair (or fail to pair) a name with an email.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml
from ingestion_workflow.clients.openalex import _metadata_from
from ingestion_workflow.services import corresponding_authors as ca
from lxml import etree
from study_schema.models.paper_parse import CorrespondingAuthor

DATA = Path(__file__).parents[1] / "data"
FIXTURES = DATA / "corresponding_authors"


def _jats(name):
    return [(a.name, a.email) for a in ca.from_jats(etree.parse(str(FIXTURES / name)).getroot())]


@pytest.mark.parametrize(
    "fixture, expected",
    [
        # Each marked author's own <email>.
        ("jats_own_email_pmc11016347.xml",
         [("Xiaodong Pan", "pxd77316@163.com"), ("Yanping Wang", "yp1014wang@163.com")]),
        # @corresp="yes" with no xref, and a note each: a note is theirs by the name it
        # prints, not by coming first.
        ("jats_two_notes_no_xref_pmc7093201.xml",
         [("Ding Ding", "dingding@huashan.org.cn"),
          ("Pei-Jun Wang", "tongjipjwang@vip.sina.com")]),
        # One note for two, each email tagged with initials: "x@ncu.edu.cn (L.X.)".
        ("jats_initials_after_email_pmc9738657.xml",
         [("Liang Xie", "ndefy14067@ncu.edu.cn"), ("Wei Wu", "ndefy02019@ncu.edu.cn")]),
        # Nobody marked; Frontiers' "Correspondence: Nina Levar levar.nina@...".
        ("jats_unmarked_note_pmc5406467.xml", [("Nina Levar", "levar.nina@gmail.com")]),
        # Five marked, "Zhuo or Wang or Tu, e-mail: a or b or c": only the address that
        # holds a surname is paired, and the other two are kept without a name.
        ("jats_names_then_emails_pmc11079637.xml",
         [("Xiaodong Lin", None), ("Gongying Li", None), ("Lina Wang", None),
          ("Chuanjun Zhuo", "chuanjunzhuotjmh@ieee.org"), ("Wenzhen Tu", None),
          (None, "adyysk2010@163.com"), (None, "twz89870018@163.com")]),
    ],
)
def test_a_jats_copy_pairs_each_marked_author_with_their_email(fixture, expected):
    assert _jats(fixture) == expected


def test_a_name_with_no_latin_letters_is_never_found_in_a_note():
    # Folded to ASCII it is empty, and an empty surname is in every text.
    root = etree.fromstring(
        "<article><front><article-meta><contrib-group>"
        "<contrib contrib-type='author'>"
        "<name><surname>王</surname><given-names>伟</given-names></name></contrib>"
        "</contrib-group><author-notes><corresp id='c1'>Correspondence: a@b.org</corresp>"
        "</author-notes></article-meta></front></article>")
    assert [(a.name, a.email) for a in ca.from_jats(root)] == [(None, "a@b.org")]


def test_an_elsevier_copy_names_its_corresponding_authors_without_emails():
    found = ca.from_elsevier(FIXTURES / "elsevier_10.1016_j.cub.2009.03.065.xml")
    assert [(a.name, a.email, a.stated_by) for a in found] == [
        ("Martin Tegenthoff", None, "elsevier"), ("Hubert R. Dinse", None, "elsevier")]



@pytest.mark.parametrize(
    "fixture, expected",
    [
        # ScienceDirect's current layout: the ce: markup as JSON, e-address beside the
        # cross-ref to the correspondence.
        ("sciencedirect_json_pmid25514517.html", [("Rachael Seidler", "rseidler@umich.edu")]),
        # Its older one: Maguire has no "Corresponding author" link, only the mail link
        # labelled "E-mail the corresponding author".
        ("sciencedirect_list_pmid16584892.html",
         [("Hugo J. Spiers", "h.spiers@fil.ion.ucl.ac.uk"),
          ("Eleanor A. Maguire", "e.maguire@fil.ion.ucl.ac.uk")]),
        # citation_author_email follows its author; the journal's mailto is not read.
        ("citation_meta_pmid21173266.html", [("Elizabeth A. Phelps", "liz.phelps@nyu.edu")]),
        # Springer and Nature: "Correspondence to <a href=mailto:...>Name</a>".
        ("springer_nature_list_pmid30511111.html", [("Huafu Chen", "chenhf@uestc.edu.cn")]),
        ("springer_author_items_pmid26935551.html",
         [("Roland Beisteiner", "roland.beisteiner@meduniwien.ac.at")]),
        ("springer_contacts_pmid21484518.html",
         [("Kevin A. Pelphrey", "kevin.pelphrey@yale.edu")]),
        # Wiley's accordion: each author's own address, not the first in the shared note.
        ("wiley_accordion_pmid37051692.html",
         [("Bin Hu", "bh@bit.edu.cn"), ("Haiyan Wu", "haiyanwu@um.edu.mo")]),
        # Atypon repeats Chiu's address on King-Casas: it stays with the name it spells.
        ("atypon_properties_pmid33257568.html",
         [("Pearl H. Chiu", "chiup@vtc.vt.edu"), ("Brooks King-Casas", None)]),
        # An OUP note shared by two cards: "Foxe or Molholm ... foxe@ or molholm@".
        ("oup_shared_note_pmid15342438.html", [("Sophie Molholm", "molholm@nki.rfmh.org")]),
        # Taylor & Francis prints Hanlon's address in Canterberry's entry.
        ("tandf_entry_of_another_pmid27441590.html", [("Colleen A. Hanlon", "hanlon@musc.edu")]),
        # Notes read as text: two names each with "at" an address; a note set line by line
        # in <i>; an address broken by <wbr/>.
        ("jneurosci_note_pmid36823039.html",
         [("Vanessa Scholz", "scholz_v@ukw.de"), ("Lorenz Deserno", "deserno_l@ukw.de")]),
        ("liebert_note_lines_pmid25495216.html", [("Dong Soo Lee", "dsl@snu.ac.kr")]),
        ("ovid_note_wbr_pmid24236764.html", [("Lars Nyberg", "lars.nyberg@diagrad.umu.se")]),
    ],
)
def test_a_publisher_page_pairs_each_corresponding_author_with_their_email(fixture, expected):
    found = ca.from_page((FIXTURES / fixture).read_bytes())
    assert [(a.name, a.email) for a in found] == expected
    assert {a.stated_by for a in found} == {"publisher_page"}


@pytest.mark.parametrize(
    "fixture, expected",
    [
        # A line break inside the first address; "or" before the second note.
        ("pdf_wiley_line_break.txt",
         [("Christian Gerloff", "christian.gerloff@uni-tuebingen.de"),
          ("Christoph Braun", "christoph.braun@uni-tuebingen.de")]),
        # "Corresponding authors: Yi-Zhuang Zou, Shu-Ping Tan. E-mail: a, b"
        ("pdf_names_listed.txt",
         [("Yi-Zhuang Zou", "yzouy@263.net"), ("Shu-Ping Tan", "shupingtan@126.com")]),
        ("pdf_names_joined_by_and.txt",
         [("Feliberto de la Cruz", "feliberto.cruz@uni-jena.de"),
          ("Karl-Jürgen Bär", "karl-juergen.baer@med.uni-jena.de")]),
        # "E-mails: a; b" are one person's.
        ("pdf_two_addresses.txt",
         [("Michael Datko", "mdatko@ucsd.edu"), ("Michael Datko", "mikedatko@gmail.com")]),
        ("pdf_name_ends_at_full_stop.txt", [("Jesus Pujol", "21404jpn@comb.cat")]),
        ("pdf_name_runs_into_department.txt", [("Annalena Venneri", "a.venneri@sheffield.ac.uk")]),
        # Springer prints no cue: "R. R. Wetherill (*)" and "* Christine Lochner cl2@...".
        ("pdf_springer_marked.txt", [("R. R. Wetherill", "rweth@mail.med.upenn.edu")]),
        ("pdf_springer_starred.txt", [("Christine Lochner", "cl2@sun.ac.za")]),
        # "To whom correspondence should be addressed. E-mail: ..." names no one.
        ("pdf_no_name.txt", [(None, "s.love@psy.gla.ac.uk")]),
        # A repository's take-down address is no note.
        ("pdf_not_an_author_page.txt", []),
    ],
)
def test_a_pdf_s_text_pairs_each_corresponding_author_with_their_email(fixture, expected):
    found = ca.from_pdf_text(FIXTURES / fixture)
    assert [(a.name, a.email) for a in found] == expected
    assert {a.stated_by for a in found} <= {"pdf"}


def test_a_citation_author_written_surname_first_is_turned_round():
    page = (b"<html><head><meta name='citation_author' content='Hu, Jianping'>"
            b"<meta name='citation_author_email' content='hu@example.org'></head></html>")
    assert [(a.name, a.email) for a in ca.from_page(page)] == [("Jianping Hu", "hu@example.org")]

def test_a_held_copy_is_read_from_its_full_text_and_no_other_file(tmp_path):
    jats = tmp_path / "article.xml"
    shutil.copy(FIXTURES / "jats_unmarked_note_pmc5406467.xml", jats)
    elsevier = tmp_path / "content.xml"
    shutil.copy(FIXTURES / "elsevier_10.1016_j.cub.2009.03.065.xml", elsevier)
    page = tmp_path / "page.html"
    page.write_text("<p>Correspondence: someone@example.org</p>")

    assert [a.stated_by for a in ca.from_held("pubget", [jats, page])] == ["jats"]
    assert [a.stated_by for a in ca.from_held("elsevier", [elsevier])] == ["elsevier"] * 2
    assert ca.from_held("pdf", [page]) == []
    # An ACE download is its page: read when it marks someone, never fetched for it.
    shutil.copy(FIXTURES / "citation_meta_pmid21173266.html", page)
    assert [a.stated_by for a in ca.from_held("ace", [page])] == ["publisher_page"]
    assert ca.from_held("ace", []) == []
    # A PDF copy is read from the text its extraction gave, and without it not at all.
    text = tmp_path / "text.txt"
    shutil.copy(FIXTURES / "pdf_springer_starred.txt", text)
    for source in ("pdf", "wiley"):
        assert [a.email for a in ca.from_held(source, [tmp_path / "x.pdf"], text)] == [
            "cl2@sun.ac.za"]
        assert ca.from_held(source, [tmp_path / "x.pdf"]) == []


def test_pubmed_names_the_author_whose_affiliation_prints_an_email():
    cached = json.loads((DATA / "metadata_cache" / "pubmed" /
                         "25568119-10-1523-jneurosci-1313-14-2015-pmc4287146.json").read_text())
    found = ca.from_pubmed(cached["raw_metadata"]["pubmed"])
    assert [(a.name, a.email, a.stated_by) for a in found] == [
        ("Maital Neta", "maitalneta@unl.edu", "pubmed")]


def test_openalex_names_the_authorship_it_marks_corresponding():
    cassette = yaml.safe_load((Path(__file__).parents[1] / "cassettes" /
                               "test_a_malformed_pmcid_costs_no_metadata_or_citation.yaml")
                              .read_text())
    work = next(json.loads(i["response"]["body"]["string"])["results"][0]
                for i in cassette["interactions"]
                if "is_corresponding" in i["response"]["body"]["string"])
    found = _metadata_from(work).corresponding_authors
    assert [(a.name, a.email, a.stated_by) for a in found] == [
        ("Michael Marxen", None, "openalex")]


def _author(name=None, email=None, by="jats"):
    return CorrespondingAuthor(name=name, email=email, stated_by=by)


def test_the_first_source_with_an_email_speaks_for_the_paper():
    elsevier = [_author("Hubert R. Dinse", by="elsevier")]
    pubmed = [_author("Hubert R Dinse", "dinse@example.org", by="pubmed")]
    openalex = [_author("Hubert R. Dinse", by="openalex")]
    assert ca.decide([elsevier, pubmed, openalex], []) == pubmed
    # Without an email anywhere, the first to name anyone.
    assert ca.decide([[], elsevier, openalex], []) == elsevier
    assert ca.decide([[], []], []) == []


def test_a_name_is_spelt_as_the_author_list_spells_it():
    found = ca.decide([[_author("H Dinse", "dinse@example.org"), _author("M Tegenthoff")]],
                      ["Martin Tegenthoff", "Hubert R. Dinse", "Burkhard Pleger"])
    assert [a.name for a in found] == ["Hubert R. Dinse", "Martin Tegenthoff"]
    # Two authors who share a surname and initial leave the source's spelling alone.
    found = ca.decide([[_author("J Smith")]], ["John Smith", "Jane Smith"])
    assert [a.name for a in found] == ["J Smith"]
