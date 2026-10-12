"""ACE text that readability cut short of the article, built again from stored pages.

Each fixture is a stored page from the ns-pond catalog, named by its article id. The
extraction stored for each had a small fraction of the article: the Karger page gave 55
words (readability kept the citation dialog), the Pediatrics page 473 (the table
placeholders led readability to a small block). Needs node >= 20.19 for readabilipy.
"""

import gzip
from pathlib import Path

import pytest
from ingestion_workflow.extractors import ace_extractor as ace
from ingestion_workflow.extractors.figure_captions import HEADING
from ingestion_workflow.models import (
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    FileType,
    Identifier,
)
from ingestion_workflow.patches.ace_patch import set_skip_remote_tables

PAGES = Path(__file__).resolve().parents[1] / "data" / "download_pages"


def real(article_id: str) -> str:
    return gzip.decompress((PAGES / f"{article_id}.html.gz").read_bytes()).decode("utf-8")


@pytest.fixture(autouse=True, scope="module")
def _readability_runs():
    # Without it ACE falls back to its own cleaner and these tests fail on what that
    # gives; this says why instead: readabilipy needs node >= 20.19 on PATH.
    ace._require_readability()


@pytest.fixture(autouse=True)
def _local_tables_only():
    set_skip_remote_tables(True)
    yield
    set_skip_remote_tables(False)


def built(article_id, pmid, tmp_path):
    return ace.article_text_and_captions(real(article_id), pmid, tmp_path)


def test_a_page_whose_readability_text_is_a_dialog_is_read_without_readability(tmp_path):
    out = built("af3skhwhkgkm", "30699438", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_READABILITY
    assert out.text_source["prose_words"][ace.READABILITY] == 0
    assert "readability kept 0 of the page's" in out.text_source["reason"]
    assert len(out.text.split()) > 5000
    for heading in ("Introduction", "Results", "Discussion"):
        assert f"\n{heading}\n" in f"\n{out.text}\n"


def test_table_placeholders_that_mislead_readability_are_left_out(tmp_path):
    page = real("gdsqtwkjfl24")
    first = ace.SourceManager(table_dir=str(tmp_path / "first"))
    source = first.identify_source(page) or first.default_source
    tables = source.parse_article(page, pmid="12456913", metadata_dir=None,
                                  skip_metadata=True, keep_tables=True).tables
    out = built("gdsqtwkjfl24", "12456913", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_TABLE_PLACEHOLDERS
    assert out.text_source["prose_words"][ace.READABILITY] == 0
    assert len(out.text.split()) > 8000
    # The tables are the first parse's, and every table on the page is in the text.
    assert len(out.article.tables) == len(tables) == 1
    soup = ace.BeautifulSoup(source.decode_html_entities(page), "lxml")
    rendered = [ace.table_text(t) for t in soup.find_all("table") if not t.find("table")]
    assert len({r for r in rendered if r}) == 3
    assert all(out.text.count(r) == 1 for r in rendered if r)


def readability_only(article_id, pmid, tmp_path, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(ace, "SHORT_TEXT_SHARE", 0)
        return ace.article_text_and_captions(real(article_id), pmid, tmp_path / "gate-off").text


def test_a_letter_keeps_readability_s_text_unrebuilt(tmp_path, monkeypatch):
    out = built("ipxutfjxf2p7", "41196626", tmp_path)
    assert out.text_source is None
    assert out.text == readability_only("ipxutfjxf2p7", "41196626", tmp_path, monkeypatch)


def test_a_full_article_readability_read_well_is_left_as_it_was(tmp_path, monkeypatch):
    """readability keeps 3,253 of the page's 3,415 words of running prose."""
    out = built("onv6piwppvwo", "21407123", tmp_path)
    assert out.text_source is None
    assert out.text == readability_only("onv6piwppvwo", "21407123", tmp_path, monkeypatch)


def test_a_page_without_body_sections_is_not_rebuilt(tmp_path):
    """Epilepsia: an abstract of 35 words, which readability keeps, beside a password
    dialog and a copyright notice, which the page's other 119 words of prose are."""
    out = built("fi4fhyepaqsc", "9579947", tmp_path)
    assert out.text_source is None


def test_caption_spans_are_those_of_the_rebuilt_text(tmp_path):
    caption = "Figure 1. Insula activity during feedback in the trained group."
    page = real("af3skhwhkgkm").replace(
        "</body>", f'<figure id="F1"><figcaption>{caption}</figcaption></figure></body>', 1)
    out = ace.article_text_and_captions(page, "30699438", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_READABILITY
    [found] = out.figure_captions
    start, end = found["span"]
    assert out.text.index(HEADING) < start
    assert out.text[start:end] == caption


def test_the_extraction_records_how_its_text_was_made(tmp_path):
    page = tmp_path / "page.html"
    page.write_text(real("af3skhwhkgkm"), encoding="utf-8")
    download = DownloadResult(
        identifier=Identifier(pmid="30699438"),
        source=DownloadSource.ACE,
        success=True,
        files=[DownloadedFile(file_path=page, file_type=FileType.HTML,
                              content_type="text/html", source=DownloadSource.ACE)],
    )
    content = ace._extract_ace_article(download, tmp_path / "out")
    assert content.full_text_path.read_text(encoding="utf-8").count(" ") > 5000
    assert content.text_source["method"] == ace.WITHOUT_READABILITY
    again = ExtractedContent.from_dict(content.to_dict())
    assert again.text_source == content.text_source


def test_the_extract_summary_says_which_way_and_why(tmp_path):
    from ingestion_workflow.catalog import ArticleRef
    from ingestion_workflow.config import Settings
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages import ExtractStage

    text = tmp_path / "article.txt"
    text.write_text("text", encoding="utf-8")
    source = {"method": ace.WITHOUT_READABILITY, "reason": "readability kept 0 of 4,460",
              "page_prose_words": 4460, "prose_words": {}}
    content = ExtractedContent(slug="s", source=DownloadSource.ACE, full_text_path=text,
                               text_source=source)
    stage = ExtractStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c"))
    work = Work(ref=ArticleRef(id="a1", identifier=Identifier(pmid="1")), source="ace",
                fingerprint="fp")
    summary = stage._outcome(work, "ace", content).summary
    assert summary["text_from"] == "without_readability (readability kept 0 of 4,460)"
    plain = ExtractedContent(slug="s", source=DownloadSource.ACE, full_text_path=text)
    assert "text_from" not in stage._outcome(work, "ace", plain).summary
    assert "text_source" not in plain.to_dict()


def test_a_rebuild_holds_each_paragraph_and_caption_once(tmp_path):
    """J Neurosurg: each paragraph is on the page again in a figure viewer and a tab.
    Rebuilt with every copy, the text had 12 passage hits for its 4 coordinates."""
    import collections

    from ingestion_workflow.services.prose_passages import passages

    out = built("i7faoldakter", "21529188", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_READABILITY
    assert len(out.text.split()) > 5000
    paragraphs = [p for p in out.text.split("\n\n") if len(p.split()) >= 20]
    assert [p for p, n in collections.Counter(paragraphs).items() if n > 1] == []
    hits = [hit for found in passages(out.text) for hit in found.hits]
    assert len(hits) == len({(h.x, h.y, h.z) for h in hits}) == 4


def test_a_rebuild_leaves_out_menus_and_repeated_tables(tmp_path):
    table = ("<table><tr><td>Region</td><td>x</td><td>y</td><td>z</td></tr>"
             "<tr><td>Left insula</td><td>-38</td><td>12</td><td>4</td></tr></table>")
    furniture = (
        "<nav><p>Journals Close Journals Menu Browse all journals by subject</p></nav>"
        "<p>Quick links</p><p>Two volunteers later withdrew consent.</p>"
        '<div role="navigation"><p>Table of contents alerts for this journal by subject</p></div>'
        "<footer><p>About us, contact, the terms of use and the site's privacy charter"
        "</p></footer>"
    )
    page = real("af3skhwhkgkm").replace(
        "</body>", f"{furniture}<div>{table}</div><div class='popup'>{table}</div></body>", 1)
    out = ace.article_text_and_captions(page, "30699438", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_READABILITY
    for line in ("Browse all journals", "Quick links", "Table of contents alerts",
                 "privacy charter"):
        assert line not in out.text
    assert "Two volunteers later withdrew consent." in out.text  # short, but not a label
    assert out.text.count("Left insula\t-38\t12\t4") == 1


def test_a_rebuild_leaves_out_sign_in_forms_alerts_and_consent_dialogs(tmp_path):
    """Karger's sign-in form and alerts widget; JAMA's OneTrust consent dialog, which
    readability's text of that page kept."""
    karger = built("af3skhwhkgkm", "30699438", tmp_path)
    for line in ("Individual Login", "Institutional Login", "Email alerts"):
        assert line not in karger.text
    jama = built("44yiqcp3u3na", "18838634", tmp_path)
    assert jama.text_source["method"] == ace.WITHOUT_READABILITY
    for line in ("Manage Consent Preferences", "Strictly Necessary Cookies", "Cookie List"):
        assert line not in jama.text


def test_a_page_wrapped_in_a_cookie_banner_s_name_is_still_read(tmp_path):
    """Karger and ARVO pages name their <body> "gdpr-visible"; a wrapper of that name
    holding the title is read too."""
    import re

    page = real("af3skhwhkgkm")
    assert re.search(r'<body class="[^"]*gdpr-visible', page)
    wrapped = re.sub(r"(<body\b[^>]*>)", r'\1<div class="gdpr-wrapper">', page, count=1)
    wrapped = wrapped.replace("</body>", "</div></body>", 1)
    # ARVO's pages have no <h1>
    untitled = re.sub(r"<(/?)h1\b", r"<\1h2", page)
    for html in (wrapped, untitled):
        out = ace.article_text_and_captions(html, "30699438", tmp_path)
        assert out.text_source["method"] == ace.WITHOUT_READABILITY
        assert len(out.text.split()) > 5000


@pytest.mark.parametrize("article_id, pmid", [
    ("c3jumvpmalif", "41115132"),  # JoVE: readability kept 41% of the page's prose
    ("7g33qvhphokl", "24523549"),  # J Neurosci
    ("xjibolva7npz", "25762673"),  # J Neurosci
])
def test_a_text_with_under_half_the_page_s_prose_is_rebuilt(
        tmp_path, monkeypatch, article_id, pmid):
    """Readability kept 41-45% of these pages' running prose, and a rebuild gives more."""
    from ingestion_workflow.extractors.figure_captions import take_html_figures
    from ingestion_workflow.services.download_check import page_prose, prose_words

    out = built(article_id, pmid, tmp_path)
    page = page_prose(take_html_figures(real(article_id))[0])
    kept = prose_words(readability_only(article_id, pmid, tmp_path, monkeypatch))
    assert 0.4 * page.words <= kept < 0.5 * page.words
    assert out.text_source["method"] != ace.READABILITY
    assert out.text_source["prose_words"][out.text_source["method"]] > kept


def test_refresh_text_records_how_the_rebuilt_text_was_made(tmp_path):
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.models.ids import Identifier as Id
    from ingestion_workflow.services import text_refresh as R

    page = tmp_path / "page.html"
    page.write_text(real("af3skhwhkgkm"), encoding="utf-8")
    text = tmp_path / "article.txt"
    text.write_text("Download citation file:", encoding="utf-8")
    stale = {"method": "readability", "reason": "an older rebuild", "page_prose_words": 1,
             "prose_words": {}}
    with Catalog.open(tmp_path / "k") as catalog:
        ref_id = catalog.register(Id(pmid="30699438")).id
        catalog.record([Outcome(
            article_id=ref_id, stage="extract", source="ace", fingerprint="ex-1",
            payload={"full_text_path": str(text), "text_source": stale},
            summary={"text_sha256": R.sha256("Download citation file:"), "text_from": "old"})])
        result = R.rebuild(R.Job(ref_id, "ace", str(text), str(page), "30699438"), write=True)
        assert result.status == "rewritten"
        R.carry_all(catalog, [result], write=True)
        extraction = catalog.artifacts([ref_id], "extract")[ref_id]["ace"]
        recorded = catalog.payload(extraction)["text_source"]
        assert recorded["method"] == ace.WITHOUT_READABILITY
        assert extraction.summary["text_from"].startswith("without_readability (readability kept")


def test_refresh_text_drops_a_text_source_that_no_longer_holds(tmp_path):
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.models.ids import Identifier as Id
    from ingestion_workflow.services import text_refresh as R

    page = tmp_path / "page.html"
    page.write_text(real("ipxutfjxf2p7"), encoding="utf-8")
    text = tmp_path / "article.txt"
    text.write_text("an older rendering", encoding="utf-8")
    stale = {"method": ace.WITHOUT_READABILITY, "reason": "an older rebuild",
             "page_prose_words": 1, "prose_words": {}}
    with Catalog.open(tmp_path / "k") as catalog:
        ref_id = catalog.register(Id(pmid="41196626")).id
        catalog.record([Outcome(
            article_id=ref_id, stage="extract", source="ace", fingerprint="ex-1",
            payload={"full_text_path": str(text), "text_source": stale},
            summary={"text_sha256": R.sha256("an older rendering"), "text_from": "old"})])
        result = R.rebuild(R.Job(ref_id, "ace", str(text), str(page), "41196626"), write=True)
        assert result.status == "rewritten" and result.text_source is None
        R.carry_all(catalog, [result], write=True)
        extraction = catalog.artifacts([ref_id], "extract")[ref_id]["ace"]
        assert "text_source" not in catalog.payload(extraction)
        assert "text_from" not in extraction.summary


OLD_TEXT = ("Activation peaked at (-38, 12, 4) in the left insula. The peak of "
            "deactivation was in the angular gyrus.")


@pytest.mark.parametrize("new, stale", [
    # a body paragraph the rebuild recovered
    (OLD_TEXT + "\n\nThe peak of deactivation was at (40, -62, 30) in the angular gyrus.",
     True),
    # a bare triplet inserted into a kept sentence, whose "peak" makes it a coordinate
    (OLD_TEXT[:-1] + " at (40, -62, 30).", True),
    (OLD_TEXT + "\n\nDeactivation was found in the right angular gyrus as well.", False),
])
def test_passages_of_a_text_that_gained_a_coordinate_are_read_again(
        tmp_path, monkeypatch, new, stale):
    """A remap carries the old passages, but only a read finds the coordinates of a body
    the rebuild recovered."""
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.models.ids import Identifier as Id
    from ingestion_workflow.services import text_refresh as R

    old = OLD_TEXT
    text = tmp_path / "article.txt"
    text.write_text(old, encoding="utf-8")
    page = tmp_path / "page.html"
    page.write_text("<html></html>", encoding="utf-8")
    source = {"method": ace.WITHOUT_READABILITY, "reason": "r", "page_prose_words": 1,
              "prose_words": {}}
    monkeypatch.setattr(R, "build", lambda *a: (new, [], source))
    sha = R.sha256(old)
    with Catalog.open(tmp_path / "k") as catalog:
        ref_id = catalog.register(Id(pmid="1")).id
        catalog.record([
            Outcome(article_id=ref_id, stage="extract", source="ace", fingerprint="ex-1",
                    payload={"full_text_path": str(text)}, summary={"text_sha256": sha}),
            Outcome(article_id=ref_id, stage="passages", source="", fingerprint="pa-1",
                    summary={}, payload={
                        "text_from": "extract", "full_text_path": str(text),
                        "text_sha256": sha, "passages": [{
                            "span": [0, 53], "before": None, "after": None,
                            "heading": None, "hits": [{"x": -38, "y": 12, "z": 4,
                                                        "span": [21, 33]}]}]}),
        ])
        result = R.rebuild(R.Job(ref_id, "ace", str(text), str(page), "1"), write=True)
        assert result.status == "rewritten" and result.inserted_coordinates is stale
        R.carry_all(catalog, [result], write=True)
        passages = catalog.artifacts([ref_id], "passages")[ref_id][""]
        assert (passages.fingerprint == R.STALE) is stale
        hit = catalog.payload(passages)["passages"][0]["hits"][0]["span"]
        assert text.read_text(encoding="utf-8")[slice(*hit)] == old[21:33]


def test_the_record_holds_the_page_s_prose_and_every_way_s(tmp_path):
    from ingestion_workflow.extractors.figure_captions import take_html_figures
    from ingestion_workflow.services.download_check import page_prose

    out = built("af3skhwhkgkm", "30699438", tmp_path)
    page = page_prose(take_html_figures(real("af3skhwhkgkm"))[0])
    assert out.text_source["page_prose_words"] == page.words
    assert set(out.text_source["prose_words"]) == {
        ace.READABILITY, ace.WITHOUT_TABLE_PLACEHOLDERS, ace.WITHOUT_READABILITY}
    assert "Download citation file" not in out.text  # the dialog readability kept


def test_scripts_and_styles_stay_out_of_a_rebuild(tmp_path):
    hidden = ("<script>var note = 'This sentence is part of a script and not of the "
              "article at all.';</script><style>p { content: 'This sentence is part of a "
              "style sheet and not of the article.' }</style>")
    page = real("af3skhwhkgkm").replace("</body>", f"<div><p>{hidden}</p></div></body>", 1)
    out = ace.article_text_and_captions(page, "30699438", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_READABILITY
    assert "part of a script" not in out.text and "style sheet" not in out.text


def test_a_nested_table_is_appended_once(tmp_path):
    nested = ("<table><tr><td><table><tr><td>Left insula</td><td>-38</td><td>12</td>"
              "<td>4</td></tr></table></td></tr></table>")
    page = real("gdsqtwkjfl24").replace("</body>", f"<div>{nested}</div></body>", 1)
    out = ace.article_text_and_captions(page, "12456913", tmp_path)
    assert out.text_source["method"] == ace.WITHOUT_TABLE_PLACEHOLDERS
    assert out.text.count("Left insula") == 1


def test_a_body_in_divs_keeps_readability_s_text_and_says_why(tmp_path):
    """Neurology: the body is in <div role="paragraph">s, which neither rebuild reads, so
    readability's 179 words of the page's 1,598 stay."""
    out = built("fih5aai35gwn", "22551729", tmp_path)
    assert out.text_source["method"] == ace.READABILITY
    assert "neither other way kept more" in out.text_source["reason"]


class _Page:
    def __init__(self, words, sections, sentences=None):
        self.words, self.sections = words, sections
        self.sentences = words if sentences is None else sentences


@pytest.fixture()
def stub_ways(monkeypatch):
    """Texts are their own prose word counts; both other ways give `rebuilt`."""
    def use(page, rebuilt="300"):
        monkeypatch.setattr(ace, "page_prose", lambda html: page)
        monkeypatch.setattr(ace, "prose_words", lambda text: int(text))
        monkeypatch.setattr(ace, "_text_without_table_placeholders", lambda *a: rebuilt)
        monkeypatch.setattr(ace, "_text_without_readability", lambda *a: rebuilt)
    return use


@pytest.mark.parametrize("first, page, rebuilt", [
    ("100", _Page(200, ("introduction", "methods")), False),  # exactly a half
    ("99", _Page(200, ("introduction", "methods")), True),
    ("100", _Page(201, ("introduction", "methods")), True),
    ("0", _Page(200, ("methods",)), False),  # one body section
    ("99", _Page(200, ("introduction", "methods"), sentences=10), True),
])
def test_the_gate_is_a_half_of_the_prose_of_a_page_with_two_sections(
        stub_ways, first, page, rebuilt):
    stub_ways(page)
    text, source = ace._whole_article_text(None, "", first, [])
    assert (source is not None) is rebuilt
    assert text == ("300" if rebuilt else first)


def test_ways_that_tie_keep_readability_s_text(stub_ways):
    stub_ways(_Page(200, ("introduction", "methods")), rebuilt="0")
    text, source = ace._whole_article_text(None, "", "0", [])
    assert source["method"] == ace.READABILITY and text == "0"


def test_the_source_reads_with_readability_again_after_a_rebuild():
    class Source:
        use_readability = True
        seen = None

        def decode_html_entities(self, html):
            return html

        def _text_with_tables(self, soup):
            self.seen = self.use_readability
            return "text"

    source = Source()
    assert ace._text_without_readability(source, "<html><body></body></html>", []) == "text"
    assert source.seen is False and source.use_readability is True


def test_a_jama_article_readability_cut_short_is_rebuilt(tmp_path):
    out = built("44yiqcp3u3na", "18838634", tmp_path)
    kept = out.text_source["prose_words"]
    assert out.text_source["method"] != ace.READABILITY
    assert kept[out.text_source["method"]] > 5 * kept[ace.READABILITY]
