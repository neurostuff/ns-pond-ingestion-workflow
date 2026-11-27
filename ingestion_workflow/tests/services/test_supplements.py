"""Supplementary material: what each route lists, what is fetched and read, and how it is
appended to the extraction. The markup below is cut from stored downloads: PMC5520049's
JATS (its Cloud Service package lists ncomms16052-s1.pdf and -s2.pdf), the Elsevier XML of
article djydru4zt34t, and the publisher pages ACE kept for articles of the 2026-11-16 sample.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests
from lxml import etree

from ingestion_workflow.models import DownloadSource, ExtractedContent, ExtractedTable, Identifier
from ingestion_workflow.services import supplements as sp

JATS = b"""<article xmlns:xlink="http://www.w3.org/1999/xlink"><body><p>Body.</p></body><back>
<supplementary-material id="d32e18" content-type="local-data"><caption><title>Supplementary
Information</title></caption><media xlink:href="ncomms16052-s1.pdf"/></supplementary-material>
<supplementary-material id="d32e21" content-type="local-data"><caption><title>Peer Review
File</title></caption><media xlink:href="ncomms16052-s2.pdf"/></supplementary-material>
<supplementary-material id="d32e24"><label>Data 1</label><media xlink:href="ncomms16052-s3.xlsx"/>
</supplementary-material></back></article>"""

PREFIX = "PMC5520049.1/"
KEYS = [f"{PREFIX}{name}" for name in (
    "PMC5520049.1.json", "PMC5520049.1.pdf", "PMC5520049.1.xml", "ncomms16052-f1.jpg",
    "ncomms16052-s1.pdf", "ncomms16052-s2.pdf")]

ELSEVIER = b"""<full-text-retrieval-response xmlns="http://www.elsevier.com/xml/svapi/article/dtd"
 xmlns:ce="http://www.elsevier.com/xml/common/dtd" xmlns:xlink="http://www.w3.org/1999/xlink">
<originalText><ce:e-component id="mmc1"><ce:label>Multimedia component 1</ce:label>
<ce:caption><ce:simple-para>Supplementary Tables S1-S3.</ce:simple-para></ce:caption>
<ce:link locator="mmc1" xlink:href="pii:S0005796721000590/mmc1"/></ce:e-component></originalText>
<objects>
<object ref="mmc1" category="standard" type="APPLICATION" mimetype="application/word"
 size="1799430">https://api.elsevier.com/content/object/eid/1-s2.0-S0005796721000590-mmc1.docx?httpAccept=%2A%2F%2A</object>
<object ref="mmc2" category="thumbnail" type="IMAGE-THUMBNAIL" mimetype="image/gif"
 >https://api.elsevier.com/content/object/eid/1-s2.0-S0005796721000590-mmc2.sml?httpAccept=%2A%2F%2A</object>
<object ref="gr1" category="standard" type="IMAGE-DOWNSAMPLED" mimetype="image/jpeg"
 >https://api.elsevier.com/content/object/eid/1-s2.0-S0005796721000590-gr1.jpg?httpAccept=%2A%2F%2A</object>
</objects></full-text-retrieval-response>"""


def test_the_pmc_route_fetches_only_the_files_its_package_holds_by_name():
    items = sp.pmc_items(etree.fromstring(JATS), KEYS, PREFIX)

    assert [(i.name, i.url) for i in items] == [
        ("ncomms16052-s1.pdf", f"{sp.PMC_CLOUD}/{PREFIX}ncomms16052-s1.pdf"),
        ("ncomms16052-s2.pdf", f"{sp.PMC_CLOUD}/{PREFIX}ncomms16052-s2.pdf"),
        # the JATS names it, the package has no file of that name: no file stands in for it
        ("ncomms16052-s3.xlsx", None),
    ]
    assert items[2].reason == "not in PMC's Cloud Service package"
    assert items[0].caption == "Supplementary Information"


def test_a_peer_review_file_and_an_image_are_passed_over():
    items = sp.pmc_items(etree.fromstring(JATS), KEYS, PREFIX)

    assert [sp.passed_over(i) for i in items[:2]] == [None, "about the paper's review or reporting"]
    assert sp.passed_over(sp.Supplement("pmc", "https://x/f1.jpg", "f1.jpg")) == "not read: .jpg"


def test_the_elsevier_route_lists_the_mmc_attachments_at_the_cdn_with_their_captions():
    items = sp.elsevier_items(etree.fromstring(ELSEVIER))

    assert [(i.name, i.url, i.label, i.caption) for i in items] == [(
        "1-s2.0-S0005796721000590-mmc1.docx",
        "https://ars.els-cdn.com/content/image/1-s2.0-S0005796721000590-mmc1.docx",
        "Multimedia component 1", "Supplementary Tables S1-S3.")]


def test_the_springer_route_resolves_a_bare_name_to_the_esm_host():
    jats = b"""<article xmlns:xlink="http://www.w3.org/1999/xlink"><back><supplementary-material>
    <label>Supplementary file1</label><media xlink:href="406_2022_1485_MOESM1_ESM.docx"/>
    </supplementary-material></back></article>"""

    [item] = sp.springer_items(etree.fromstring(jats), "10.1007/s00406-022-01485-7")

    assert item.url == ("https://static-content.springer.com/esm/art%3A10.1007%2Fs00406-022-01485-7"
                        "/MediaObjects/406_2022_1485_MOESM1_ESM.docx")


PAGE = b"""<html><head><link rel="canonical" href="https://www.pnas.org/doi/10.1073/pnas.2021745118"/>
</head><body>
<a href="https://ars.els-cdn.com/content/image/1-s2.0-S2451902219300692-mmc1.pdf">Download PDF</a>
<a href="http://www.sciencedirect.com/science/MiamiMultiMediaURL/1-s2.0-S105381191100276X/1-s2.0-S105381191100276X-mmc1.doc/272508/FULL/S105381191100276X/6275577bf8c7d89">Supplementary data</a>
<a href="https://static-content.springer.com/esm/art%3A10.1007%2Fs11682-018-9960-7/MediaObjects/11682_2018_9960_MOESM1_ESM.pdf">ESM 1</a>
<a href="/doi/suppl/10.1073/pnas.2021745118/suppl_file/pnas.2021745118.sapp.pdf">Appendix 01 (PDF)</a>
<a href="https://www.jneurosci.org/content/jneuro/43/9/1600/DC1/embed/inline-supplementary-material-1.docx?download=true">Supplemental Material</a>
<a href="http://scholar.google.com/scholar_lookup?&amp;title=supplementary%20motor%20area">Google Scholar</a>
<a href="/topics/neuroscience/supplementary-motor-area">Supplementary motor area</a>
<a href="https://www.pnas.org/lookup/doi/10.1073/pnas.2021745118#supplementary-materials">Supplementary Materials</a>
</body></html>"""


def test_a_publisher_page_yields_its_supplementary_files_and_none_of_its_other_links():
    items = sp.publisher_items(PAGE)

    assert [i.url for i in items] == [
        "https://ars.els-cdn.com/content/image/1-s2.0-S2451902219300692-mmc1.pdf",
        # ScienceDirect's old viewer link, as the CDN file it names
        "https://ars.els-cdn.com/content/image/1-s2.0-S105381191100276X-mmc1.doc",
        "https://static-content.springer.com/esm/art%3A10.1007%2Fs11682-018-9960-7/MediaObjects/"
        "11682_2018_9960_MOESM1_ESM.pdf",
        # relative, resolved against the page's canonical address
        "https://www.pnas.org/doi/suppl/10.1073/pnas.2021745118/suppl_file/pnas.2021745118.sapp.pdf",
        "https://www.jneurosci.org/content/jneuro/43/9/1600/DC1/embed/inline-supplementary-material-1.docx"
        "?download=true",
    ]
    assert items[-1].name == "inline-supplementary-material-1.docx"
    assert items[3].label == "Appendix 01 (PDF)"


@pytest.mark.parametrize("caption, label", [
    ("Table S3. Peak coordinates for the conjunction.", "Table S3"),
    ("Table 2 Regions showing group differences", "Table S2"),
    ("Supplementary Table 4b: Clusters", "Table S4b"),
    ("Sheet1", "Supplementary file 2, table #1"),
])
def test_a_supplementary_table_is_labelled_as_the_paper_cites_it(caption, label):
    assert sp.table_label(caption, 2, 1) == label


def test_a_workbook_is_read_sheet_by_sheet(tmp_path):
    from openpyxl import Workbook

    book = Workbook()
    book.active.title = "Table S1 Clusters"
    for row in (["Region", "x", "y", "z", "t"], ["L insula", -34, 16, -6, 5.2]):
        book.active.append(row)
    other = book.create_sheet("Demographics")
    other.append(["Group", "N"])
    other.append(["Patients", 24])
    book.create_sheet("Empty")
    path = tmp_path / "data.xlsx"
    book.save(path)
    item = sp.Supplement("pmc", "https://x/data.xlsx", "data.xlsx", path=str(path))

    sp.read(item, tmp_path / "read")

    assert item.read and item.read_by == "sheet"
    assert [t["caption"] for t in item.tables] == ["Table S1 Clusters", "Demographics"]
    assert Path(item.tables[0]["raw_content_path"]).read_text().splitlines()[1] == "L insula,-34,16,-6,5.2"
    assert "L insula\t-34\t16\t-6\t5.2" in Path(item.text_path).read_text()


def _content(tmp_path) -> ExtractedContent:
    text = tmp_path / "extract" / "article.txt"
    text.parent.mkdir(exist_ok=True)
    body = "## Methods\n\nWe scanned 24 patients.\n\n## Figure legends\n\nFig. 1. Activation."
    text.write_text(body, encoding="utf-8")
    return ExtractedContent(
        slug="s", source=DownloadSource.PUBGET, identifier=Identifier(pmcid="PMC1"),
        full_text_path=text,
        tables=[ExtractedTable(table_id="t1", raw_content_path=tmp_path / "t1.xml", table_number=1,
                               caption="Table 1. Demographics")],
        figure_captions=[{"ids": ["f1"], "span": [len(body) - 19, len(body)]}],
    )


def _read_supplement(tmp_path, name="mmc1.docx") -> sp.Supplement:
    out = tmp_path / "read"
    (out / "tables").mkdir(parents=True, exist_ok=True)
    (out / "tables" / "table-s1.csv").write_text("Region,x,y,z\nL insula,-34,16,-6\n")
    (out / "text.txt").write_text("Table S1. Peaks of the conjunction.\nL insula -34 16 -6\n")
    table = ExtractedTable(table_id="table-s1", raw_content_path=out / "tables" / "table-s1.csv",
                           table_number=1, caption="Table S1. Peaks of the conjunction.")
    return sp.Supplement("elsevier", "https://ars.els-cdn.com/content/image/x-mmc1.docx", name,
                         label="Multimedia component 1", path=str(tmp_path / name),
                         text_path=str(out / "text.txt"), tables=[table.to_dict()], read_by="docling")


def test_a_supplement_is_appended_after_the_text_leaving_every_offset_in_it_as_it_was(tmp_path):
    content = _content(tmp_path)
    before = Path(content.full_text_path).read_text()

    joined = sp.append(content, [_read_supplement(tmp_path)], tmp_path / "joined")

    text = Path(joined.full_text_path).read_text()
    assert text.startswith(before) and Path(joined.base_text_path).read_text() == before
    # never beside the extractor's text, which may sit in a shared source cache
    assert Path(joined.full_text_path).parent == tmp_path / "joined"
    assert sorted(p.name for p in Path(joined.base_text_path).parent.iterdir()) == ["article.txt"]
    [entry] = joined.supplements
    a, b = entry["span"]
    assert text[a:b].strip() == "Table S1. Peaks of the conjunction.\nL insula -34 16 -6"
    assert "## Supplementary material\n\n### Supplementary file 1 (mmc1.docx): Multimedia component 1" in text
    span = joined.figure_captions[0]["span"]
    assert text[span[0]:span[1]] == "Fig. 1. Activation."
    supp = joined.tables[-1]
    assert (supp.table_id, supp.table_number, entry["tables"]) == ("s1-table-s1", None, ["s1-table-s1"])
    assert supp.metadata == {"supplement": 1, "table_label": "Table S1"}
    assert [t.table_id for t in joined.tables] == ["t1", "s1-table-s1"]


def test_appending_again_replaces_what_was_appended_and_none_takes_it_off(tmp_path):
    content = _content(tmp_path)
    once = sp.append(content, [_read_supplement(tmp_path)], tmp_path / "joined")
    payload = ExtractedContent.from_dict(once.to_dict())

    twice = sp.append(payload, [_read_supplement(tmp_path)], tmp_path / "joined")
    assert [t.table_id for t in twice.tables] == ["t1", "s1-table-s1"]

    bare = sp.append(ExtractedContent.from_dict(twice.to_dict()), [], tmp_path / "joined")
    assert (bare.supplements, [t.table_id for t in bare.tables]) == ([], ["t1"])
    assert bare.full_text_path.name == "article.txt"


def test_an_extraction_without_supplements_serialises_as_before(tmp_path):
    assert "supplements" not in _content(tmp_path).to_dict()


class _Files(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        answers = {
            "/mmc1.docx": (200, "application/msword", b"PK\x03\x04 docx bytes"),
            "/gone.pdf": (404, "text/html", b"not found"),
            "/login.pdf": (200, "text/html; charset=utf-8", b"<html>sign in</html>"),
            "/busy.pdf": (503, "text/plain", b"slow down"),
            "/big.pdf": (200, "application/pdf", b"%PDF" + b"0" * 4096),
        }
        status, kind, body = answers[self.path]
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture()
def host():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Files)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


@pytest.mark.parametrize("name, reason", [
    ("mmc1.docx", None),
    ("gone.pdf", "HTTP 404"),
    ("login.pdf", "a page, not the file"),
    ("big.pdf", "larger than the cap (1,024 bytes)"),
])
def test_what_a_fetch_records(host, tmp_path, name, reason):
    item = sp.Supplement("pmc", f"{host}/{name}", name)

    sp.fetch(item, tmp_path / name, requests.Session(), max_bytes=1024)

    assert item.reason == reason
    assert (item.path is not None) == (reason is None)
    assert not list(tmp_path.glob("*.part"))


def test_a_busy_host_is_noted_for_a_retry_not_raised(host, tmp_path):
    item = sp.Supplement("pmc", f"{host}/busy.pdf", "busy.pdf")

    sp.fetch(item, tmp_path / "busy.pdf", requests.Session(), max_bytes=1024)

    assert (item.reason, sp.is_transient(item), item.path) == ("transient: HTTP 503", True, None)


def test_a_word_file_is_read_with_its_tables(tmp_path):
    import docx

    document = docx.Document()
    document.add_paragraph("Supplementary Methods. Images were normalised to MNI space.")
    document.add_paragraph("Table S1. Peaks of the group difference.")
    rows = [["Region", "x", "y", "z", "t"], ["L insula", "-34", "16", "-6", "5.2"],
            ["R amygdala", "22", "-4", "-18", "4.8"]]
    table = document.add_table(rows=len(rows), cols=len(rows[0]))
    for r, row in enumerate(rows):
        for c, cell in enumerate(row):
            table.cell(r, c).text = cell
    path = tmp_path / "mmc1.docx"
    document.save(path)
    item = sp.Supplement("elsevier", "https://x/mmc1.docx", "mmc1.docx", path=str(path))

    sp.read(item, tmp_path / "read")

    assert item.read and item.read_by == "docling"
    text = Path(item.text_path).read_text()
    assert "normalised to MNI space" in text and "L insula" in text
    [read] = item.tables
    assert "-34" in Path(read["raw_content_path"]).read_text()
    # Docling links no caption to a Word table: the "Table S1." paragraph before it is taken
    assert read["caption"] == "Table S1. Peaks of the group difference."


@pytest.mark.parametrize("runs, caption", [
    # PMC13791214's supplement: "Supplementary" and "Table 5." in bold, the rest plain. Docling
    # reads the paragraph as an inline group of three texts; the caption was "Table 5." alone.
    ([("Supplementary", "b"), ("Table 5.", "bi"),
      ("Whole-brain associations between gray matter volume and aggression.", "")],
     "Supplementary Table 5. Whole-brain associations between gray matter volume and aggression."),
    # A run that starts with its punctuation takes no space before it.
    ([("Table S3", "b"), (". Brain areas showing gene interaction effects on ReHo", "")],
     "Table S3. Brain areas showing gene interaction effects on ReHo"),
    # r5i3sldauyg4's supplement: a figure's note, a page break, then the next table's caption,
    # all one paragraph. The caption starts at its own run.
    ([("Abbreviations", "bi"), (": B=Baclofen; P=Placebo; 1=time point 1; 2=time point 2", "i"),
      ("", "bp"), ("Supplementary Table 3: Analyses of reaction times and misses in the baclofen group", "b")],
     "Supplementary Table 3: Analyses of reaction times and misses in the baclofen group"),
])
def test_a_word_caption_whose_runs_differ_in_format_is_read_whole(tmp_path, runs, caption):
    import docx
    from docx.enum.text import WD_BREAK

    document = docx.Document()
    paragraph = document.add_paragraph()
    for text, style in runs:
        run = paragraph.add_run(text)
        run.bold, run.italic = "b" in style, "i" in style
        if "p" in style:
            run.add_break(WD_BREAK.PAGE)
    table = document.add_table(rows=2, cols=4)
    for r, row in enumerate([["Region", "x", "y", "z"], ["IFG", "-39", "20", "19"]]):
        for c, cell in enumerate(row):
            table.cell(r, c).text = cell
    path = tmp_path / "fcag426_supplementary_data.docx"
    document.save(path)
    item = sp.Supplement("pmc", "https://x/s.docx", path.name, path=str(path))

    sp.read(item, tmp_path / "read")

    [read] = item.tables
    assert read["caption"] == caption


@pytest.mark.parametrize("paragraphs, caption", [
    # A supplement prints "Table S6" alone, its title as the next paragraph.
    (["Voxel coordinates are in MNI space.", "Table S6",
      "Conjunction analysis of MACM and RSFC results for left middle insula"],
     "Table S6 Conjunction analysis of MACM and RSFC results for left middle insula"),
    # A caption with its title keeps no later paragraph.
    (["Table S1. Peaks of the group difference.", "Coordinates are in MNI space."],
     "Table S1. Peaks of the group difference."),
])
def test_a_word_caption_that_is_only_its_label_takes_the_next_paragraph(tmp_path, paragraphs, caption):
    import docx

    document = docx.Document()
    for text in paragraphs:
        document.add_paragraph(text)
    table = document.add_table(rows=2, cols=4)
    for r, row in enumerate([["Region", "x", "y", "z"], ["Insula", "-38", "4", "2"]]):
        for c, cell in enumerate(row):
            table.cell(r, c).text = cell
    path = tmp_path / "s1.docx"
    document.save(path)
    item = sp.Supplement("pmc", "https://x/s1.docx", path.name, path=str(path))

    sp.read(item, tmp_path / "read")

    assert item.tables[0]["caption"] == caption


@pytest.mark.parametrize("item", [
    sp.Supplement("pmc", None, "s3.xlsx", reason="not in PMC's Cloud Service package"),
    sp.Supplement("publisher", "https://www.pnas.org/doi/suppl/x", "", reason="HTTP 403"),
])
def test_an_item_with_no_address_or_no_name_survives_its_payload(item):
    assert sp.Supplement.from_dict(item.to_dict()) == item


# -- the review of #221 ------------------------------------------------------------------


def _numbered(tmp_path, name, caption):
    item = _read_supplement(tmp_path / name, name)
    item.tables[0]["caption"] = caption
    return item


def test_two_files_that_each_number_a_table_1_are_not_both_the_paper_s_table_s1(tmp_path):
    content = _content(tmp_path)
    items = [_numbered(tmp_path, "s1.docx", "Table 1. Peaks, study 1"),
             _numbered(tmp_path, "s2.docx", "Table 1. Peaks, study 2")]

    joined = sp.append(content, items, tmp_path / "joined")

    labels = [t.metadata["table_label"] for t in joined.tables[1:]]
    assert labels == ["Supplementary file 1, table #1", "Supplementary file 2, table #1"]
    from ingestion_workflow.services.set_roles.common import citing_sentences
    assert all(citing_sentences("As Table 1 shows, patients differed.", label) == [] for label in labels)


def test_one_file_numbering_its_tables_names_them_as_the_paper_s_s_tables(tmp_path):
    joined = sp.append(_content(tmp_path), [_numbered(tmp_path, "s1.docx", "Table 3. Peaks")], tmp_path / "joined")

    assert joined.tables[-1].metadata["table_label"] == "Table S3"


def test_a_supplementary_table_s_caption_says_which_file_it_is_from(tmp_path):
    joined = sp.append(_content(tmp_path), [_numbered(tmp_path, "s1.docx", "Table 3. Peaks")], tmp_path / "joined")

    assert joined.tables[-1].caption == "Supplementary file 1 (s1.docx): Table 3. Peaks"
    assert joined.tables[0].caption == "Table 1. Demographics"


def test_supplement_text_past_the_cap_is_left_out_and_its_tables_kept(tmp_path):
    item = _read_supplement(tmp_path)
    Path(item.text_path).write_text("x" * 500)

    joined = sp.append(_content(tmp_path), [item], tmp_path / "joined", max_chars=100)

    [entry] = joined.supplements
    a, b = entry["span"]
    text = Path(joined.full_text_path).read_text()
    assert entry["truncated"] and text[a:a + 100] == "x" * 100 and "left out" in text[a:b]
    assert [t.table_id for t in joined.tables] == ["t1", "s1-table-s1"]


def test_a_long_table_in_a_supplement_s_text_is_cut_and_the_table_kept_whole(tmp_path):
    """Docling writes a Word table as padded Markdown, a sheet as tab-separated rows: each is
    cut after its first row and APPENDED_TABLE_ROWS more, a short one left whole."""
    item = _read_supplement(tmp_path)
    word = ["| Region       | x     | y   | z   |", "|--------------|-------|-----|-----|"]
    word += [f"| Gene {i:<7} | {i:<5} | 1   | 2   |" for i in range(100)]
    demographics = ["Group\tN\tAge", "Patients\t24\t31.2", "Controls\t22\t30.8"]
    text = ("Table S1. Expression.\n" + "\n".join(word)
            + "\n\nTable S2. Demographics.\n" + "\n".join(demographics) + "\n")
    Path(item.text_path).write_text(text)

    joined = sp.append(_content(tmp_path), [item], tmp_path / "joined")

    a, b = joined.supplements[0]["span"]
    text = Path(joined.full_text_path).read_text()[a:b]
    rows = [line for line in text.split("\n") if line.startswith("|")]
    assert rows[0] == "| Region | x | y | z |" and len(rows) == 1 + sp.APPENDED_TABLE_ROWS
    assert ("[... 40 more rows of this table are left out of the text; the table keeps them]"
            in text)
    assert "\n".join(demographics) in text and "|---" not in text
    assert [t.table_id for t in joined.tables] == ["t1", "s1-table-s1"]


def test_how_supplements_are_appended_is_in_the_extraction_s_fingerprint(monkeypatch):
    from ingestion_workflow.catalog import Artifact
    from ingestion_workflow.pipeline.stages import extract

    download = Artifact(article_id="a", stage="download", source="pubget", fingerprint="dl")
    before = extract.extraction_fingerprint("pubget", download, "digest")
    monkeypatch.setattr(sp, "APPEND_VERSION", sp.APPEND_VERSION + 1)
    assert extract.extraction_fingerprint("pubget", download, "digest") != before
    # an article without supplements keeps its fingerprint
    assert extract.extraction_fingerprint("pubget", download) == extract.extraction_fingerprint(
        "pubget", download, None)


def test_the_read_digest_names_each_file_s_bytes_and_reader(tmp_path):
    item = _read_supplement(tmp_path)
    item.sha256 = "a" * 64
    same = sp.read_digest([item])
    item.sha256 = "b" * 64
    changed = sp.read_digest([item])
    item.read_by = "mineru"

    assert same and changed != same and sp.read_digest([item]) not in (same, changed)
    assert sp.read_digest([sp.Supplement("pmc", "u", "x.pdf", reason="HTTP 404")]) is None


def test_a_workbook_that_would_unpack_to_far_more_than_it_holds_is_not_opened(tmp_path):
    import zipfile

    path = tmp_path / "bomb.xlsx"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("xl/worksheets/sheet1.xml", "0" * (5 * 2**20))
    item = sp.Supplement("pmc", "u", "bomb.xlsx", path=str(path))

    sp.read(item, tmp_path / "read")

    assert not item.read and item.reason.startswith("unpacks to")


@pytest.mark.parametrize("extension, head, why", [
    ("pdf", b"%PDF-1.7 ...", None),
    ("pdf", b"<!DOCTYPE html><html>sign in", "a page, not the file"),
    ("docx", b"PK\x03\x04", None),
    ("docx", b"%PDF-1.4", "not a .docx file"),
    ("txt", b"<html>a text supplement may be anything", None),
])
def test_what_a_fetched_file_must_start_with(extension, head, why):
    assert sp.not_the_file(extension, head) == why


def test_a_publisher_link_to_a_host_that_is_not_public_is_not_fetched(host, tmp_path):
    item = sp.Supplement("publisher", f"{host}/mmc1.docx", "mmc1.docx")

    sp.fetch(item, tmp_path / "mmc1.docx", requests.Session(), max_bytes=1024)

    assert (item.reason, item.path) == ("not a public host", None)


def test_a_file_already_fetched_is_fetched_again_on_a_refresh(host, tmp_path):
    dest = tmp_path / "mmc1.docx"
    dest.write_bytes(b"PK an older copy")
    kept = sp.Supplement("pmc", f"{host}/mmc1.docx", "mmc1.docx")
    fresh = sp.Supplement("pmc", f"{host}/mmc1.docx", "mmc1.docx")

    sp.fetch(kept, dest, requests.Session(), max_bytes=1024)
    sp.fetch(fresh, dest, requests.Session(), max_bytes=1024, reuse=False)

    assert kept.size == len(b"PK an older copy")
    assert dest.read_bytes() == b"PK\x03\x04 docx bytes" and fresh.sha256 != kept.sha256


class _Declares(BaseHTTPRequestHandler):
    """Declares a body larger than the cap and sends a small one."""

    def do_GET(self):  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Type", "application/pdf")
        self.send_header("Content-Length", str(10 * 2**20))
        self.end_headers()
        self.wfile.write(b"%PDF")

    def log_message(self, *args):
        pass


def test_a_file_declared_larger_than_the_cap_is_not_downloaded(tmp_path):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Declares)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    item = sp.Supplement("pmc", f"http://127.0.0.1:{server.server_address[1]}/big.pdf", "big.pdf")
    try:
        sp.fetch(item, tmp_path / "big.pdf", requests.Session(), max_bytes=1024)
    finally:
        server.shutdown()

    assert item.reason == "larger than the cap (1,024 bytes)" and not (tmp_path / "big.pdf").exists()


@pytest.mark.parametrize("url, name", [
    ("https://www.frontiersin.org/api/v4/articles/1234/file/Data_Sheet_1.docx/1234/1", "Data_Sheet_1.docx"),
    ("https://onlinelibrary.wiley.com/action/downloadSupplement?doi=10.1002%2Fhbm.1&file=hbm1-sup-0001.docx",
     "hbm1-sup-0001.docx"),
    ("https://ars.els-cdn.com/content/image/1-s2.0-S1-mmc1.pdf", "1-s2.0-S1-mmc1.pdf"),
])
def test_a_link_is_named_by_the_file_it_names(url, name):
    assert sp.link_name(url) == name


def test_a_doc_is_converted_with_an_absolute_profile_from_a_relative_cache(tmp_path, monkeypatch):
    calls = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sp.shutil, "which", lambda name: "/usr/bin/soffice")
    monkeypatch.setattr(sp.subprocess, "run", lambda args, **kw: calls.append(args))
    (tmp_path / "s1.doc").write_bytes(b"\xd0\xcf\x11\xe0")
    item = sp.Supplement("pmc", "u", "s1.doc", path="s1.doc")

    sp.read_doc(item, Path("cache/read/01"))

    profile = next(a for a in calls[0] if a.startswith("-env:UserInstallation="))
    assert profile.startswith("-env:UserInstallation=file:///") and Path(calls[0][-1]).is_absolute()
    assert item.reason == "LibreOffice made no .docx"


@pytest.mark.parametrize("name, body", [
    ("peaks.csv", "Region,x,y,z\nL insula,-34,16,-6\n"),
    ("peaks.tsv", "Region\tx\ty\tz\nL insula\t-34\t16\t-6\n"),
])
def test_a_delimited_file_is_one_table(tmp_path, name, body):
    path = tmp_path / name
    path.write_text(body)
    item = sp.Supplement("pmc", "u", name, path=str(path))

    sp.read(item, tmp_path / "read")

    [table] = item.tables
    assert item.read_by == "sheet"
    assert Path(table["raw_content_path"]).read_text().splitlines() == ["Region,x,y,z", "L insula,-34,16,-6"]
