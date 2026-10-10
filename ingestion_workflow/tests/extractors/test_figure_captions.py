"""Every extractor writes each figure caption, as the source words it, under "Figure legends"
at the end of the text, and records its span there."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

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

    _, text, spans = article_text_and_captions(html, None, tmp_path)
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
