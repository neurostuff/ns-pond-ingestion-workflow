"""Real tables whose coordinates the extractor stored wrong, from the roles audit's
geometry-guard holds (2026-11-19).

Each file in `data/garbled_coordinate_tables` is the raw table production read, cut to a
few rows. The assertions are on what the extractor is sent (`_serialise`) or on how a
PDF's tables are pieced together (`read_middle_json`): the numbers the model can read
are the numbers it stores.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_workflow.extractors.mineru_convert import MineruTable, continued, read_middle_json
from ingestion_workflow.extractors.utils import normalize_minus
from ingestion_workflow.services.create_analyses import CreateAnalysesService

DATA = Path(__file__).parents[1] / "data" / "garbled_coordinate_tables"


def _serialised(name: str) -> list[str]:
    markup = (DATA / name).read_text(encoding="utf-8")
    return CreateAnalysesService._serialise(markup, "t").split("\n")


def test_a_footnote_mark_is_not_read_as_more_digits():
    """PMC3985019 Table 2 cites each seed's source by a superscript footnote: `46<sup>1</sup>`
    read `461`, and every z of the table's 53 seeds was stored out of the brain."""
    lines = _serialised("pmc3985019_t2.xml")

    assert lines[1].endswith("| -23, -66, 46")
    assert lines[3].endswith("| -56, -18, 37")
    assert not any("461" in line or "3711" in line for line in lines)


def test_a_minus_set_as_a_superscript_still_reads_as_a_minus():
    """Elsevier's PMC2602949 Table 2 sets one minus as `<sup>—</sup> 36`. Only numeric
    footnote marks are touched, so it reads `-36` as it did before."""
    lines = _serialised("elsevier_pmc2602949_tbl2.xml")

    assert "L intraparietal sulcus | 7, 39 | -36 | -50 | 40 | 4.06 | 0.00005" in lines


@pytest.mark.parametrize("markup, text", [
    # a power of ten keeps its exponent, glued as the serialiser always wrote it
    ("<table><tr><td>p</td><td>2.1 × 10<sup>5</sup></td></tr></table>", "2.1 × 105"),
    # a unit's power and a letter mark are not numeric footnotes after a number
    ("<table><tr><td>k</td><td>120 mm<sup>3</sup></td></tr></table>", "120 mm3"),
    ("<table><tr><td>t</td><td>4.21<sup>a</sup></td></tr></table>", "4.21a"),
    # a footnote link holding the number itself
    ('<table><tr><td>z</td><td>46<xref ref-type="table-fn" rid="t1">2</xref></td></tr></table>',
     "46"),
])
def test_only_a_numeric_mark_after_a_number_is_dropped(markup, text):
    assert CreateAnalysesService._serialise(markup, "t").split(" | ")[-1] == text


def test_a_wedge_printed_for_a_minus_is_a_minus():
    """PMC4116759 Table 4 prints every negative coordinate as `∧40`; all were stored positive."""
    lines = _serialised("pmc4116759_tbl4.xml")

    assert any(line.startswith("Inferior occipital gyrus | 18 | -40 | -86 | -4 | 8.29 | 2.53")
               for line in lines)
    assert any(line.endswith("| 44 | -74 | 0 | 8.48 | 2.59") for line in lines)
    assert not any("∧" in line for line in lines)


@pytest.mark.parametrize("text", ["Faces ∧ Places", "(A>B)∧1", "x∧2"])
def test_a_wedge_that_is_a_conjunction_stays(text):
    assert normalize_minus(text) == text


def test_a_direction_letter_is_a_sign():
    """doi:10.1111/j.1530-0277.2001.tb02204.x Table 2 (MinerU) prints Talairach coordinates
    as `32R | 92P | 4I`; the letters were dropped and left, posterior and inferior foci
    stored on the other side."""
    lines = _serialised("mineru_11236838_table-2.html")

    assert any(line.endswith("| 18 | 675 | 32 | -92 | -4 | 1.83") for line in lines)
    assert any(line.endswith("| - | 2133 | -14 | -11 | 18 | 1.50") for line in lines)
    assert any(line.endswith("| 10 | 621 | 23 | 62 | 9 | 1.36") for line in lines)
    # Brodmann's area and volume are not coordinates and keep their own form.
    assert any(line.startswith("Right inferior occipital gyrus^{a} | 18 | 675 |")
               for line in lines)


def test_a_lone_direction_letter_is_left_alone():
    from ingestion_workflow.services.create_analyses import _signed_by_direction

    assert _signed_by_direction("Region | 6L | 12 | 3") == "Region | 6L | 12 | 3"
    assert _signed_by_direction("Region | 6L | 12P | 3") == "Region | 6L | 12P | 3"


# -- a row whose first cell spans columns ----------------------------------------------


def test_a_row_whose_first_cell_spans_columns_lines_up_with_its_header():
    """PMC5622290 Table 2 spans each sub-peak's region over the cluster-size column. Sent as
    `<2:Caudate (L/R) | 12 | 6 | 16 | 8.34`, nu-v23 read 6, 16, 8 as x, y, z and 12 as a cluster
    size, in all 21 sub-peaks (supplements production run, 2026-11-24)."""
    lines = _serialised("pmc5622290_t2.xml")

    assert "Caudate (L/R) |  | 12 | 6 | 16 | 8.34" in lines
    assert "Cluster #1b | 9,727 |  |  |  | " in lines
    assert not any(line.startswith("<2:") for line in lines)


def test_a_spanned_region_before_the_coordinates_leaves_x_in_its_column():
    """Lancet 1998 (doi:10.1016/S0140-6736(98)02470-2) Table 1 spans four regions over the
    Brodmann column; nu-v23 dropped their x and read the Z score as z."""
    lines = _serialised("elsevier_9690407_tbl1.xml")

    assert "Left hypothalamus |  | -2 | -18 | -8 | 3·68" in lines
    assert "Right cingulate cortex | 24 | 2 | 22 | 24 | 4·9" in lines


@pytest.mark.parametrize("line", [
    "<6:Alcohol group > healthy controls",  # a row-group banner
    "<2:PBO |  |  |  | ",                   # a banner with empty cells after it
    "#<3:MNI coordinates | #T",             # a header
    "Caudate | <2:12 | 16 | 8.34",          # a span after the first cell
    # two banners side by side (luukio3kr5kr tbl3)
    "<5:Encoding-reading | <5:Reading-encoding",
    # a header row the source did not mark (4txarhgtrzng s2-table-003)
    "<3:Coordinate | Accuracy | p | Extent | Hemi",
    # an empty spanned cell (hxphsknjjo5e s1-table-010)
    "<8:^6: | 26 | 2.74 | -42 | -42 | 54",
    # a block's banner set beside another block's rows (vk5rtp22s3da s9-table-001)
    "<6:K=1, Negative connectivity map | L Cerebellum | -6 | -27 | -18 | 6.41 | 5.55",
])
def test_a_banner_a_header_and_a_later_span_are_left_alone(line):
    from ingestion_workflow.services.create_analyses import _leading_span_cells

    assert _leading_span_cells(line) == line


def test_spans_are_spelt_out_only_where_other_rows_keep_those_columns_apart():
    """3odyz2h6hh7b s1-table-003 spans every row's region over two columns, under a header row
    the source did not mark that spans them too: consistent as written. PMC5622290 prints
    `Cluster #3 | 946` in the columns `<2:Caudate (L/R)` spans."""
    from ingestion_workflow.services.create_analyses import _leading_span_rows

    consistent = ["<2:Anatomical area | BA | Coordinates | Cluster Size | t-value",
                  "<2:Right Orbitofrontal Cortex. | 10, 11 | 34 60 -12 | 1679 | 5.34",
                  "<2:Left Angular Gyrus. | 40, 7 | -46 -50 58 | 2911 | 5.52"]
    assert _leading_span_rows(consistent) == consistent
    apart = ["#Brain region | #Cluster size (voxels) | #x | #y | #z | #z-statistic",
             "Cluster #3 | 946 |  |  |  | ",
             "<2:Caudate (L/R) | 12 | 6 | 16 | 8.34"]
    assert _leading_span_rows(apart)[2] == "Caudate (L/R) |  | 12 | 6 | 16 | 8.34"


def test_a_span_as_wide_as_the_headers_first_cell_is_left_alone():
    """An ALE table (crmamvju5ixf, tbl0015) heads its label column `#<2:Domain & Anatomical
    Label`, so `<2:L Caudate | 2384 | 1 | -10` lines up with it as written. Spelt out, the row
    was one cell wider than its header and nu-v23 read the cluster number as x."""
    from ingestion_workflow.services.create_analyses import _header_first_span, _leading_span_cells

    lines = ["#<2:Domain & Anatomical Label | #Volume | #Cluster | #x | #y | #z",
             "<2:L Caudate | 2384 | 1 | -10 | 12 | 0"]
    assert _header_first_span(lines) == 2
    assert _leading_span_cells(lines[1], 2) == lines[1]
    assert _header_first_span(["# | # | #<3:MNI", "#Region | #Side | #x | #y | #z"]) == 1


# -- a table carried over a page break ------------------------------------------------


def _middle() -> dict:
    return json.loads((DATA / "mineru_jnr25176_continued.json").read_text())


def test_a_headless_piece_on_the_next_page_gets_its_tables_header():
    """doi:10.1002/jnr.25176 Supplementary Table 1.b runs onto the next page, which MinerU
    reads as a table with no caption and no header. Read alone, its cluster size and x
    were stored as x and y (3366.77, 16.7, -60.7)."""
    head, piece = read_middle_json(_middle()).tables

    assert piece.caption == head.caption + " (continued)"
    lines = CreateAnalysesService._serialise(piece.html, "t").split("\n")
    assert "Cluster Number" in lines[0] and "Coordinate (MNI)" in lines[0]
    # x, y and z head the same columns as in the head piece, where the piece's numbers sit.
    assert lines[1].split(" | ")[3:6] == ["x", "y", "z"]
    assert lines[2:] == [
        " | 1 | 3366.77 | 16.7 | -60.7 | 27.4 | Parieto-occipital sulcus",
        " | 2 | 1108.36 | 27.0 | -81.8 | -9.2 | Posterior transverse collateral sulcus",
    ]
    # The head keeps its own rows, and the piece only its own.
    assert "2446.08" in head.html and "3366.77" not in head.html
    assert "2446.08" not in piece.html


def test_the_article_text_is_not_changed_by_the_stitch():
    text = read_middle_json(_middle()).text

    assert text.count("Cluster Number") == 1


def test_a_piece_two_pages_on_is_not_a_continuation():
    middle = _middle()
    middle["pages"][1]["page_idx"] = 13

    _, piece = read_middle_json(middle).tables
    assert piece.caption == ""
    assert "Cluster Number" not in piece.html


@pytest.mark.parametrize("html", [
    # a header of its own
    "<table><tr><td>Region</td><td>t</td><td>x</td><td>y</td><td>z</td><td>k</td><td>BA</td></tr>"
    "<tr><td>A</td><td>4.1</td><td>-2</td><td>3</td><td>4</td><td>12</td><td>6</td></tr></table>",
    # another width
    "<table><tr><td></td><td>1</td><td>3366.77</td><td>16.7</td><td>-60.7</td></tr></table>",
])
def test_a_piece_with_its_own_header_or_width_is_left_alone(html):
    head = read_middle_json(_middle()).tables[0]

    assert continued(head, html, "") == ("", html)


def test_a_head_without_a_header_gives_nothing():
    head = MineruTable(caption="", footer="", html="<table><tr><td>1</td><td>2</td></tr></table>")
    html = "<table><tr><td>3</td><td>4</td></tr></table>"

    assert continued(head, html, "") == ("", html)


def _mineru(name: str, caption: str = "") -> MineruTable:
    return MineruTable(caption=caption, footer="", html=(DATA / name).read_text(encoding="utf-8"))


def test_a_piece_whose_columns_hold_other_kinds_is_left_alone():
    """doi:10.1148/radiol.2016151809: Table 4 opens with an empty column the next table
    lacks, so at equal width its header would put x over the next table's y. The
    extractor read that table right without it."""
    head = _mineru("mineru_27002420_table-4.html", "Table 4. Cluster table")
    piece = (DATA / "mineru_27002420_table-005.html").read_text(encoding="utf-8")

    assert continued(head, piece, "") == ("", piece)


def test_a_piece_with_a_title_row_and_its_own_header_is_left_alone():
    """doi:10.1111/pcn.12502: the piece opens "Table 1. (Continued)" and names its columns."""
    head = _mineru("mineru_28032396_table-1-2.html", "Table 1. (Continued)")
    piece = (DATA / "mineru_28032396_table-003.html").read_text(encoding="utf-8")

    assert continued(head, piece, "") == ("", piece)


def test_a_header_left_alone_on_its_page_heads_the_rows_on_the_next():
    """doi:10.1111/joor.12657: MinerU read the conjunction table's header as a table of its
    own and its one row as another, whose cluster size and x were stored as x and y."""
    head = _mineru("mineru_29782041_table-005.html", "Conjunction analysis")
    piece = (DATA / "mineru_29782041_table-006.html").read_text(encoding="utf-8")

    caption, html = continued(head, piece, "")
    assert caption == "Conjunction analysis (continued)"
    lines = CreateAnalysesService._serialise(html, "t").split("\n")
    assert lines[0].split(" | ")[1:4] == ["x", "y", "z"]
    assert lines[1] == "48 | 60 | -6 | 32 | 0.008 | R | Primary somatosensory cortex"
