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
    ("Sheet1", "Supplementary file 2 table 1"),
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
    text = tmp_path / "article.txt"
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

    joined = sp.append(content, [_read_supplement(tmp_path)])

    text = Path(joined.full_text_path).read_text()
    assert text.startswith(before) and Path(joined.base_text_path).read_text() == before
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
    once = sp.append(content, [_read_supplement(tmp_path)])
    payload = ExtractedContent.from_dict(once.to_dict())

    twice = sp.append(payload, [_read_supplement(tmp_path)])
    assert [t.table_id for t in twice.tables] == ["t1", "s1-table-s1"]

    bare = sp.append(ExtractedContent.from_dict(twice.to_dict()), [])
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
    item = sp.Supplement("publisher", f"{host}/{name}", name)

    sp.fetch(item, tmp_path / name, requests.Session(), max_bytes=1024)

    assert item.reason == reason
    assert (item.path is not None) == (reason is None)
    assert not list(tmp_path.glob("*.part"))


def test_a_busy_host_is_asked_again_later(host, tmp_path):
    item = sp.Supplement("publisher", f"{host}/busy.pdf", "busy.pdf")

    with pytest.raises(sp.TransientFetch):
        sp.fetch(item, tmp_path / "busy.pdf", requests.Session(), max_bytes=1024)


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
