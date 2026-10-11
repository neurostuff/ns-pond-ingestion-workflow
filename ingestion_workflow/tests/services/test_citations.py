from pathlib import Path

from lxml import etree

from ingestion_workflow.extractors.pubget_extractor import KEEPS_SUBSCRIPTS, KEEPS_SUPERSCRIPTS, article_text
from ingestion_workflow.services import citations as C

JATS = """<?xml version="1.0"?>
<article><front><article-meta>
<title-group><article-title>Attention and perception</article-title></title-group>
<abstract><p>An abstract.</p></abstract>
</article-meta></front>
<body><sec><title>Introduction</title>
<p>Corrected at p<sub>FWE</sub> &lt; 0.05.</p>
<p>Attention shapes perception (<xref ref-type="bibr" rid="B1">Smith et al., 2001</xref>). It is a filter.<sup><xref ref-type="bibr" rid="B2">2</xref></sup> Many agree [<xref ref-type="bibr" rid="B3">3</xref>&#8211;<xref ref-type="bibr" rid="B5">5</xref>].</p>
</sec></body>
<back><ref-list>
<ref id="B1"><label>1</label><element-citation publication-type="journal"><person-group><name><surname>Smith</surname></name><name><surname>Jones</surname></name></person-group><article-title>Attention</article-title><source>J Neurosci</source><year>2001</year><pub-id pub-id-type="doi">10.1523/x.2001</pub-id><pub-id pub-id-type="pmid">111</pub-id></element-citation></ref>
<ref id="B2"><label>2</label><mixed-citation>Lee B. Filters. 2002.</mixed-citation></ref>
<ref id="B3"><label>3</label><mixed-citation>Three C. 2003.</mixed-citation></ref>
<ref id="B4"><label>4</label><mixed-citation>Four D. 2004.</mixed-citation></ref>
<ref id="B5"><label>5</label><mixed-citation>Five E. 2005.</mixed-citation></ref>
</ref-list></back></article>
"""

ELSEVIER = b"""<?xml version="1.0" encoding="UTF-8"?>
<full-text-retrieval-response xmlns="http://www.elsevier.com/xml/svapi/article/dtd" xmlns:ce="http://www.elsevier.com/xml/common/dtd" xmlns:sb="http://www.elsevier.com/xml/common/struct-bib/dtd" xmlns:ja="http://www.elsevier.com/xml/ja/dtd" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:prism="http://prismstandard.org/namespaces/basic/2.0/" xmlns:xocs="http://www.elsevier.com/xml/xocs/dtd">
<coredata><dc:title>A test article</dc:title><prism:doi>10.1016/j.test.2020.1</prism:doi></coredata>
<originalText><xocs:doc><xocs:serial-item><ja:article>
<ja:head><ce:title>A test article</ce:title></ja:head>
<ja:body><ce:sections><ce:section id="s1"><ce:section-title>Introduction</ce:section-title>
<ce:para>Attention shapes perception (<ce:cross-ref refid="bib1">Smith et al., 2001</ce:cross-ref>). Others disagree <ce:cross-refs refid="bib2 bib3">[2,3]</ce:cross-refs>.</ce:para>
</ce:section></ce:sections></ja:body>
<ja:tail><ce:bibliography><ce:bibliography-sec>
<ce:bib-reference id="bib1"><ce:label>Smith et al., 2001</ce:label><sb:reference><sb:contribution><sb:authors><sb:author><ce:given-name>J.</ce:given-name><ce:surname>Smith</ce:surname></sb:author></sb:authors><sb:title><sb:maintitle>Attention</sb:maintitle></sb:title></sb:contribution><sb:host><sb:issue><sb:series><sb:title><sb:maintitle>J Neurosci</sb:maintitle></sb:title></sb:series><sb:date>2001</sb:date></sb:issue><ce:doi>10.1523/x.2001</ce:doi></sb:host></sb:reference></ce:bib-reference>
<ce:bib-reference id="bib2"><ce:label>[2]</ce:label><ce:other-ref><ce:textref>Jones A. Perception. 2003.</ce:textref></ce:other-ref></ce:bib-reference>
<ce:bib-reference id="bib3"><ce:label>[3]</ce:label><ce:other-ref><ce:textref>Lee B. Vision. 2004.</ce:textref></ce:other-ref></ce:bib-reference>
</ce:bibliography-sec></ce:bibliography></ja:tail>
</ja:article></xocs:serial-item></xocs:doc></originalText></full-text-retrieval-response>"""

HTML = """<html><body>
<p>Attention shapes perception (<a href="#bib1">Smith et al., 2001</a>), see <a href="#tbl1">Table 1</a>.</p>
<p>A second claim rests on older work (<a href="#bib2">Jones, 1999</a>).</p>
<table id="tbl1"><tr><td>Region 2004</td><td>x y z values for the cortex in 2004</td></tr></table>
<ol>
<li id="bib1">Smith J, Jones K (2001) Attention. J Neurosci 21:1-10. <a href="https://doi.org/10.1523/x.2001">doi</a></li>
<li id="bib2">Jones A (1999) Older work on perception. Vision Res 9:1-2.</li>
</ol></body></html>"""


def _jats(tmp_path: Path) -> Path:
    path = tmp_path / "article.xml"
    path.write_text(JATS)
    return path


def _span(text, c):
    return text[c["text_span"]["start_char"] : c["text_span"]["end_char"]]


def _sentence(text, c):
    return text[c["sentence"]["start_char"] : c["sentence"]["end_char"]]


def test_jats_links_land_on_their_markers_in_the_extracted_text(tmp_path):
    path = _jats(tmp_path)
    text = article_text(etree.parse(str(path)), tmp_path)

    result = C.read_jats(path, text)

    refs = {r["id"]: r for r in result.references}
    assert refs["B1"]["doi"] == "10.1523/x.2001" and refs["B1"]["pmid"] == "111"
    assert refs["B1"]["authors"] == ["Smith", "Jones"] and refs["B1"]["year"] == 2001
    first = result.citations[0]
    assert _span(text, first) == "Smith et al., 2001" and first["references"] == ["B1"]
    assert first["method"] == "xref" and first["marker_in_text"]
    assert _sentence(text, first).startswith("Attention shapes perception")


def test_a_superscript_marker_names_its_sentence_whether_or_not_pubget_keeps_it(tmp_path):
    path = _jats(tmp_path)
    text = article_text(etree.parse(str(path)), tmp_path)

    sup = next(c for c in C.read_jats(path, text).citations if c["references"] == ["B2"])

    assert _sentence(text, sup).startswith("It is a filter")
    if KEEPS_SUPERSCRIPTS:
        assert sup["marker_in_text"] and _span(text, sup) == "2"
    else:  # an older pubget drops it: a zero-width anchor where it was
        assert not sup["marker_in_text"]
        assert sup["text_span"]["start_char"] == sup["text_span"]["end_char"]


def test_a_range_linked_at_its_ends_cites_everything_between(tmp_path):
    path = _jats(tmp_path)
    text = article_text(etree.parse(str(path)), tmp_path)

    ranged = C.read_jats(path, text).citations[-1]

    assert ranged["references"] == ["B3", "B4", "B5"]
    assert _span(text, ranged).replace(" ", "") == "3–5"


def test_offsets_carry_over_to_a_stored_text_rendered_differently(tmp_path):
    path = _jats(tmp_path)
    rebuilt = article_text(etree.parse(str(path)), tmp_path)
    stored = "A line an older extractor added\n" + rebuilt.replace("An abstract.", "An abstract, longer.")

    result = C.read_jats(path, stored)

    assert result.notes.get("text_rebuilt_differs") == 1
    assert [_span(stored, c) for c in result.citations if c["marker_in_text"]][0] == "Smith et al., 2001"


def test_elsevier_cross_references_are_exact(tmp_path):
    from elsevier_coordinate_extraction.extract.text import extract_text_from_article, format_article_text

    path = tmp_path / "content.xml"
    path.write_bytes(ELSEVIER)
    text = format_article_text(extract_text_from_article(ELSEVIER, True, True))

    result = C.read_elsevier(path, text)

    assert [r["id"] for r in result.references] == ["bib1", "bib2", "bib3"]
    assert result.references[0]["authors"] == ["Smith"] and result.references[0]["doi"] == "10.1523/x.2001"
    assert [(_span(text, c), c["references"]) for c in result.citations] == [
        ("Smith et al., 2001", ["bib1"]), ("[2,3]", ["bib2", "bib3"])]


def test_html_links_are_placed_by_their_context_and_tables_are_not_references(tmp_path):
    path = tmp_path / "page.html"
    path.write_text(HTML)
    text = ("Attention shapes perception (Smith et al., 2001), see Table 1.\n"
            "A second claim rests on older work (Jones, 1999).\n")

    result = C.read_html(path, text)

    assert [r["id"] for r in result.references] == ["bib1", "bib2"]
    assert result.references[0]["doi"] == "10.1523/x.2001"
    assert [(_span(text, c), c["references"], c["method"]) for c in result.citations] == [
        ("Smith et al., 2001", ["bib1"], "link"), ("Jones, 1999", ["bib2"], "link")]
    assert _sentence(text, result.citations[1]).startswith("A second claim")


def test_a_marker_after_a_full_stop_belongs_to_the_sentence_before():
    text = "Attention is a filter. [12] Perception follows."
    at = text.index("[12]")

    # the splitter alone starts the next sentence at "[12]"
    assert any(start == at for start, _ in C.sentence_spans(text))
    assert C.Sentences(text).of(at) == (0, text.index(" [12]"))


def test_markers_are_carried_past_inserted_lines_and_dropped_inside_changed_text():
    rebuilt = "one\ntwo [1]\nthree [22]\n"
    stored = "zero\none\ntwo [1]\nthree [9]\n"
    # the second marker ends inside "22", which the stored text replaced with "9"
    at = {0: (rebuilt.index("[1]"), rebuilt.index("[1]") + 3), 1: (rebuilt.index("[22]"), rebuilt.index("[22]") + 2)}
    notes = {}
    placed = C._place(rebuilt, at, stored, notes)
    assert stored[slice(*placed[0])] == "[1]"
    assert 1 not in placed and notes == {"text_rebuilt_differs": 1}


def test_a_space_the_stored_text_adds_after_a_marker_is_not_the_marker_s():
    rebuilt, stored = "picture. 3At first", "picture. 3 At first"
    at = rebuilt.index("3")
    assert C._place(rebuilt, {0: (at, at + 1)}, stored, {}) == {0: (at, at + 1)}



def test_space_the_rebuild_puts_inside_a_marker_is_trimmed_before_the_map():
    """The rebuild's space is mapped onto the stored text's comma, which no trim after the map removes."""
    rebuilt, stored = "found 19 Disruptions", "found,19 Disruptions"
    at = rebuilt.index(" 19")
    assert C._place(rebuilt, {0: (at, at + 3)}, stored, {}) == {0: (6, 8)}


def test_space_the_map_carries_onto_a_marker_s_edge_is_trimmed_after_it():
    rebuilt, stored = "found [19] Disruptions", "found  19  Disruptions"
    at = rebuilt.index("[")
    assert C._place(rebuilt, {0: (at, at + 4)}, stored, {}) == {0: (7, 9)}


def test_an_elsevier_marker_between_spaces_the_stored_text_has_fewer_of_is_kept():
    """The rebuild puts two spaces around each marker where the stored text has one. Diffed
    character by character, runs like this lost 26 markers in 13 of 80 Elsevier articles;
    matched word by word, any run of space matching any other, each space is its own edit
    and every marker keeps its place."""
    from ingestion_workflow.services.offsets import diff

    sentences = [f"Finding {k} was replicated in the cortex of patients with the disease." for k in range(8)]
    rebuilt = "".join(f"{t}  {k + 1}  " for k, t in enumerate(sentences))
    stored = "".join(f"{t} {k + 1} " for k, t in enumerate(sentences))
    spans = {k: (rebuilt.index(f"  {k + 1}  "), rebuilt.index(f"  {k + 1}  ") + 4) for k in range(8)}
    placed = C._place(rebuilt, spans, stored, {}, words=True)
    assert [stored[a:b] for a, b in placed.values()] == [str(k + 1) for k in range(8)]
    edits = diff(rebuilt, stored, words=True).edits
    assert len(edits) == 16 and all(not (rebuilt[a:b] + stored[c:d]).strip() for a, b, c, d in edits)


def test_subscripts_stay_in_the_text_when_pubget_keeps_them(tmp_path):
    text = article_text(etree.parse(str(_jats(tmp_path))), tmp_path)

    assert ("FWE" in text) == KEEPS_SUBSCRIPTS
