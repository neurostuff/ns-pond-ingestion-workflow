"""Every extractor writes each figure caption, as the source words it, under "Figure legends"
at the end of the text, and records its span there."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from lxml import etree

from ingestion_workflow.extractors.figure_captions import HEADING, append_legends, dedupe
from ingestion_workflow.services.prose_passages import find

CAPTION = "Greater insula activation in patients (x = -34, y = 16, z = -6; p < 0.05)."


def _check(text, spans, n=1):
    """One caption span per figure, each holding the caption's coordinate, after the heading
    and nowhere else in the text."""
    assert len(spans) == n
    start = text.index(HEADING)
    for c in spans:
        a, b = c["span"]
        assert a > start
        caption = text[a:b]
        assert [(h.x, h.y, h.z) for h in find(caption)] == [(-34.0, 16.0, -6.0)]
        assert text.count(caption) == 1
    return text[:start]


def test_append_legends_leaves_the_text_before_them_unchanged():
    text, spans = append_legends("## Results\nBody.", [(["F1"], "Figure 1. A."), ([], "Figure 2. B.")])
    assert text.startswith("## Results\nBody.\n\n## Figure legends\n\n")
    assert [text[slice(*c["span"])] for c in spans] == ["Figure 1. A.", "Figure 2. B."]
    assert append_legends("Bare text.", [([], "Figure 1. A.")])[0] == "Bare text.\n\nFigure legends\n\nFigure 1. A."
    assert append_legends("Body.", []) == ("Body.", [])


def test_dedupe_keeps_one_entry_per_caption_with_every_figure_id():
    got = dedupe([("F1", "Same  caption."), ("F2", "Same caption."), ("F3", ""), (None, "Other.")])
    assert got == [(["F1", "F2"], "Same caption."), ([], "Other.")]


JATS = f"""<article><front><article-meta><title-group><article-title>T</article-title></title-group>
</article-meta></front><body>
<sec><title>Results</title><p>Patients showed more activity (Fig. 1).</p>
<fig id="F1"><label>Figure 1</label><caption><title>Insula.</title><p>{CAPTION.replace("<", "&lt;")}</p></caption></fig>
<p>After the figure.</p></sec></body>
<floats-group><fig id="F2"><label>Figure 2</label><caption><p>Panel B. {CAPTION.replace("<", "&lt;")}</p></caption></fig></floats-group>
</article>"""


def test_pubget_writes_body_and_floats_group_figures_after_the_text(tmp_path):
    from ingestion_workflow.extractors.pubget_extractor import article_text, article_text_and_captions

    xml = tmp_path / "article.xml"
    xml.write_text(JATS)
    tree = etree.parse(str(xml))
    text, spans = article_text_and_captions(tree, tmp_path)
    body = _check(text, spans, n=2)
    assert "After the figure." in body and "insula" not in body.lower()
    assert text[slice(*spans[0]["span"])].startswith("Figure 1 Insula. Greater insula")
    assert [c["ids"] for c in spans] == [["F1"], ["F2"]]
    assert article_text(tree, tmp_path) == text
    assert tree.find(".//fig") is not None  # the caller's tree is left alone


ELSEVIER = f"""<?xml version="1.0" encoding="utf-8"?>
<full-text-retrieval-response xmlns="http://www.elsevier.com/xml/svapi/article/dtd"
  xmlns:ce="http://www.elsevier.com/xml/common/dtd" xmlns:ja="http://www.elsevier.com/xml/ja/dtd"
  xmlns:dc="http://purl.org/dc/elements/1.1/">
<coredata><dc:title>T</dc:title></coredata>
<originalText><ja:article><ja:body><ce:sections>
<ce:section id="s1"><ce:section-title>Results</ce:section-title>
<ce:para>Patients showed more activity.</ce:para>
<ce:figure id="f1"><ce:label>Fig. 1</ce:label><ce:caption><ce:simple-para>{CAPTION.replace("<", "&lt;")}</ce:simple-para></ce:caption></ce:figure>
<ce:para>After the figure.</ce:para>
</ce:section></ce:sections></ja:body></ja:article></originalText>
</full-text-retrieval-response>"""


def test_elsevier_writes_its_figure_captions_after_the_text():
    from ingestion_workflow.extractors.elsevier_extractor import article_text_and_captions

    text, spans = article_text_and_captions(ELSEVIER.encode())
    body = _check(text, spans)
    assert "After the figure." in body and "Patients showed more activity." in body
    assert text[slice(*spans[0]["span"])].startswith("Fig. 1 Greater insula")
    assert spans[0]["ids"] == ["f1"]


PAGE = Path(__file__).parent.parent / "data" / "test_html" / "Nature neuroscience" / "33106677.html"


@pytest.mark.vcr()
def test_ace_writes_its_figcaptions_after_the_text(tmp_path, monkeypatch):
    """A Nature page, its first figure's caption given a coordinate."""
    import re

    from ingestion_workflow.extractors.ace_extractor import article_text_and_captions
    from ingestion_workflow.extractors.figure_captions import take_html_figures
    from ingestion_workflow.patches.ace_patch import SKIP_REMOTE_ENV

    monkeypatch.setenv(SKIP_REMOTE_ENV, "1")
    html = re.sub(r"(?s)(<figcaption\b[^>]*>).*?(</figcaption>)",
                  lambda m: m.group(1) + "<b>Figure 1.</b> " + CAPTION.replace("<", "&lt;") + m.group(2),
                  PAGE.read_text(encoding="utf-8"), count=1)
    page, captions = take_html_figures(html)
    assert "<figcaption" not in page and len(captions) >= 2
    assert captions[0][1] == "Figure 1. " + CAPTION

    built = article_text_and_captions(html, None, tmp_path)
    text, spans = built.text, built.figure_captions
    assert len(spans) == len(captions)
    _check(text, spans[:1])
    assert text[slice(*spans[0]["span"])] == "Figure 1. " + CAPTION
    for (_, caption), c in zip(captions, spans):
        assert text[slice(*c["span"])] == caption  # the page's own words (a bare "Fig. 4" too)


class _Ref:
    def __init__(self, item):
        self.item = item

    def resolve(self, doc):
        return self.item


class _Doc:
    """The part of a docling document the PDF text uses: a picture and its caption,
    exported in reading order, until they are deleted."""

    def __init__(self):
        self.caption = SimpleNamespace(text="Figure 1. " + CAPTION)
        self.pictures = [SimpleNamespace(self_ref="#/pictures/0", captions=[_Ref(self.caption)])]
        self.items = [SimpleNamespace(text="Results: patients showed more activity."), self.caption,
                      SimpleNamespace(text="Table 1. Peaks.")]

    def model_copy(self, deep):
        return self

    def delete_items(self, node_items):
        self.items = [i for i in self.items if i not in node_items]
        self.pictures = [p for p in self.pictures if p not in node_items]

    def export_to_text(self):
        return "\n\n".join(i.text for i in self.items)


def test_pdf_writes_its_picture_captions_after_the_text_and_keeps_table_captions():
    from ingestion_workflow.extractors.pdf_extractor import pdf_text_and_captions

    text, spans = pdf_text_and_captions(_Doc(), lambda s: s)
    body = _check(text, spans)
    assert "Table 1. Peaks." in body
    assert spans[0]["ids"] == ["#/pictures/0"]


MARKED_UP = """<article><body><sec><title>Results</title><p>Text.</p>
<fig id="F1"><label>Figure 1</label><caption><title>V<sub>1</sub> response.</title>
<p>Activation at <italic>p</italic>&lt;10<sup>−3</sup>; peak (−4<bold>2</bold>, 10, 5).</p>
<p>Second paragraph.</p></caption></fig></sec></body></article>"""


def test_a_caption_keeps_its_inline_markup_s_text_unspaced():
    """As the stylesheet writes "p<sub>FWE</sub>" in the body: no space where the source has none."""
    from ingestion_workflow.extractors.figure_captions import take_html_figures, take_xml_figures

    root = etree.fromstring(MARKED_UP)
    ((ids, caption),) = take_xml_figures(root, "fig")
    assert caption == ("Figure 1 V1 response. Activation at p<10−3; peak (−42, 10, 5). Second paragraph.")
    _, ((_, html_caption),) = take_html_figures(
        '<figure id="Fig1"><figcaption><b>Fig. 1</b> V<sub>1</sub> at 10<sup>−3</sup>'
        '<p>Second.</p></figcaption><img src="f.png"></figure>')
    assert html_caption == "Fig. 1 V1 at 10−3 Second."


SPRINGER = """<html><head><meta name="citation_publisher" content="Springer"></head><body>
<p>Patients showed more activity in the insula.</p>
<figure id="Fig1"><figcaption class="Caption"><div class="CaptionContent"><span class="CaptionNumber">Fig. 1</span>
<p>Insula activation (x = -34, y = 16, z = -6).</p></div></figcaption></figure>
<figure id="Tab1"><figcaption class="Caption"><div class="CaptionContent"><span class="CaptionNumber">Table 1</span>
<p>Peaks</p></div></figcaption><table><thead><tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr></thead>
<tbody><tr><td>Insula</td><td>-34</td><td>16</td><td>-6</td></tr></tbody></table></figure>
<figure id="T2"><figcaption>Table 2 Peaks, set apart</figcaption></figure>
</body></html>"""


def test_a_table_set_in_a_figure_keeps_its_caption_for_ace():
    """Springer's <figure id="TabN">: ACE reads the table's label from its figcaption
    (ace/sources.py OldSpringerSource), and cutting it lost every table on the page."""
    from ace.sources import SourceManager

    from ingestion_workflow.extractors.figure_captions import take_html_figures

    page, captions = take_html_figures(SPRINGER)
    assert captions == [(["Fig1"], "Fig. 1 Insula activation (x = -34, y = 16, z = -6).")]
    assert "Insula activation" not in page
    tables = SPRINGER[SPRINGER.index('<figure id="Tab1">'):]
    assert page.endswith(tables)  # byte for byte

    article = SourceManager(table_dir="/nonexistent").sources["OldSpringer"].parse_article(
        page, pmid="1", skip_metadata=True, keep_tables=True)
    (table,) = article.tables
    assert table.label == "Table 1" and table.caption == "Peaks"


ELIFE = """<article><body><sec><title>Results</title><p>Overlapping activations (Figure 2).</p>
<fig id="fig2" position="float"><label>Figure 2.</label><caption><title>Control sub-networks.</title>
<p>Activations for temporal control (red).</p></caption><graphic href="elife-fig2.jpg"/>
<p content-type="supplemental-figure"><fig id="fig2s1" specific-use="child-fig" position="anchor">
<label>Figure 2—figure supplement 1.</label><caption><title>Contrasts using reduced, 4 mm smoothing.</title></caption>
<graphic href="elife-fig2-figsupp1.jpg"/></fig></p>
<p content-type="supplemental-figure"><fig id="fig2s2" specific-use="child-fig" position="anchor">
<label>Figure 2—figure supplement 2.</label><caption><title>Contrasts using surface-based processing.</title></caption>
<graphic href="elife-fig2-figsupp2.jpg"/></fig></p></fig>
<p>After the figure.</p></sec></body></article>"""


def test_pubget_writes_an_elife_figure_supplement_s_caption_after_its_parent_s(tmp_path):
    """PMC's eLife JATS nests each figure supplement, a figure with its own label and caption,
    inside its parent figure: the supplement is a figure of its own, not a panel."""
    from ingestion_workflow.extractors.pubget_extractor import article_text_and_captions

    xml = tmp_path / "article.xml"
    xml.write_text(ELIFE)
    text, spans = article_text_and_captions(etree.parse(str(xml)), tmp_path)
    assert [text[slice(*c["span"])] for c in spans] == [
        "Figure 2. Control sub-networks. Activations for temporal control (red).",
        "Figure 2—figure supplement 1. Contrasts using reduced, 4 mm smoothing.",
        "Figure 2—figure supplement 2. Contrasts using surface-based processing.",
    ]
    assert [c["ids"] for c in spans] == [["fig2"], ["fig2s1"], ["fig2s2"]]
    assert "After the figure." in text[: text.index(HEADING)]


STATEMENT = """<article><body><sec><title>Methods</title><p>Each participant viewed five clips (Fig. 1).</p>
<fig id="F1" position="float"><label>Fig. 1</label><graphic href="bi-8-295-g001"/>
<statement><p>Example of a complete scan session for the participants.</p></statement></fig>
</sec></body></article>"""


def test_pubget_writes_a_caption_the_figure_holds_in_a_statement(tmp_path):
    """Some JATS put a figure's words in <statement>, with no <caption>: the legend is those words,
    not the bare label."""
    from ingestion_workflow.extractors.pubget_extractor import article_text_and_captions

    xml = tmp_path / "article.xml"
    xml.write_text(STATEMENT)
    text, spans = article_text_and_captions(etree.parse(str(xml)), tmp_path)
    assert [text[slice(*c["span"])] for c in spans] == [
        "Fig. 1 Example of a complete scan session for the participants."]


ALT_TEXT = """<article><body><sec><title>Methods</title><p>The device (Fig. 1).</p>
<fig id="f1" position="float"><label>Fig. 1.</label><caption><p>(Left) Exploded view of the StretchWrist.</p></caption>
<alt-text>A labeled 3D rendering of a mechanical device beside a photo of a person inside an MRI scanner.</alt-text>
<graphic href="fig1.webp"/></fig></sec></body></article>"""


def test_pubget_writes_a_figure_s_alt_text_into_its_legend_marked_as_an_image_description(tmp_path):
    """A journal's description of the image is the source's words about the figure: it follows the
    caption in the same legend, marked so a reader can tell it from the authors' caption."""
    from ingestion_workflow.extractors.pubget_extractor import article_text_and_captions

    xml = tmp_path / "article.xml"
    xml.write_text(ALT_TEXT)
    text, spans = article_text_and_captions(etree.parse(str(xml)), tmp_path)
    assert [text[slice(*c["span"])] for c in spans] == [
        "Fig. 1. (Left) Exploded view of the StretchWrist. Image description: A labeled 3D rendering of a "
        "mechanical device beside a photo of a person inside an MRI scanner."]
