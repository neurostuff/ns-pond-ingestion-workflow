"""Reading a download's Methods and Results, and the filter run before it."""

from __future__ import annotations

from ingestion_workflow.services.prose_text import (
    kept_spans,
    main_file,
    may_hold_coordinates,
    read_download,
)

JATS = """<article><body>
<sec><title>Introduction</title><p>Earlier work found x = 30, y = 2, z = -20.</p></sec>
<sec><title>Materials and methods</title><p>We scanned 20 people.</p></sec>
<sec><title>Results</title><p>A peak in the ACC (x = 4, y = 30, z = 22).</p>
<table-wrap><table><tr><td>-40</td><td>20</td><td>10</td></tr></table></table-wrap>
<fig><caption><p>Crosshairs at MNI -6, 22, -8.</p></caption></fig></sec>
</body></article>"""

ELSEVIER = """<doc xmlns:ce="http://www.elsevier.com/xml/common/dtd"><body><ce:sections>
<ce:section><ce:section-title>Results</ce:section-title><ce:para>In the amygdala (x = &#x2212;22, y = &#x2212;4,
z = &#x2212;18).</ce:para><ce:table><row><entry>1</entry></row></ce:table></ce:section>
</ce:sections></body></doc>"""

HTML = """<html><body><nav>x = 1, y = 2, z = 3</nav><h2>Results</h2>
<p>Peak at (x = 10, y = 12, z = 14; t = 3.2).</p><table><tr><td>40</td><td>20</td><td>10</td></tr></table>
<figure><figcaption>Figure 1. Slices at the peak (MNI 10, 12, 14).</figcaption></figure></body></html>"""


def test_jats_keeps_methods_results_and_legends(tmp_path):
    path = tmp_path / "a.xml"
    path.write_text(JATS)
    text, legends = read_download(path, "xml")
    spans, how = kept_spans(text)
    prose = "\n\n".join(text[a:b] for a, b in spans) + "\n\n" + legends
    assert how == "methods+results"
    assert "x = 4, y = 30, z = 22" in prose and "-6, 22, -8" in prose
    assert "x = 30" not in prose and "-40" not in prose


def test_elsevier_sections_and_entity_minus(tmp_path):
    path = tmp_path / "e.xml"
    path.write_text(ELSEVIER)
    text, _ = read_download(path, "xml")
    assert text.lstrip().startswith("## Results")
    assert "−22" in text and "<" not in text


def test_html_drops_tables_and_navigation(tmp_path):
    path = tmp_path / "h.html"
    path.write_text(HTML)
    text, legends = read_download(path, "html")
    assert "## Results" in text and "x = 10" in text and "x = 1," not in text and "40" not in text
    assert "MNI 10, 12, 14" in legends


def test_the_filter_ignores_table_cells_and_keeps_prose():
    assert may_hold_coordinates(JATS)
    assert not may_hold_coordinates("<p>Accuracy was 85% (70, 85, 92).</p>"
                                    "<table-wrap><table><tr><td>x = -40, y = 20, z = 10</td></tr></table></table-wrap>")


def test_without_sections_the_whole_text_is_read():
    text = "Peak at x = 1, y = 2, z = 3 somewhere."
    assert kept_spans(text) == ([(0, len(text))], "full text")


def test_the_article_file_not_its_tables(tmp_path):
    (tmp_path / "tables").mkdir()
    table = tmp_path / "tables" / "t.csv"
    table.write_text("x")
    article = tmp_path / "article.xml"
    article.write_text("<article/>")
    got = main_file([{"file_path": str(table), "file_type": "csv"}, {"file_path": str(article), "file_type": "xml"}])
    assert got["file_path"] == str(article)


def test_legend_spans_finds_a_legend_written_with_other_spaces_and_minus_signs():
    from ingestion_workflow.services.prose_text import legend_spans

    text = "## Results\nBody.\n\nFigure 1. Insula peak at\n(−34,  16, -6).\n## Discussion\n"
    legends = "Figure 1. Insula peak at (-34, 16, −6).\n\nFigure 2. A legend the text left out."
    (span,) = legend_spans(text, legends)
    assert text[span[0]:span[1]] == "Figure 1. Insula peak at\n(−34,  16, -6)."
    assert legend_spans(text, "") == []
