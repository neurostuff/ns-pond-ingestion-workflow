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


# -- a triple printed in one cell -----------------------------------------------------


def test_a_packed_triple_is_split_into_three_columns():
    """Psychiatry Res Neuroimaging 2025 (doi:10.1016/j.pscychresns.2025.111946) Table 7 prints
    `15 24 33` in one "Coordinate x/y/z" cell; nu-v23 read (15, -6.61, 33), taking the t value
    for y, on every row."""
    lines = _serialised("elsevier_39823711_tbl0007.xml")

    assert "#<3:Coordinatex/y/z | #t-value" in lines[0]
    assert any(line.endswith("| 4630 | R | 45 | 3 | -51 | -5.38 | < 0.001") for line in lines)


def test_a_number_before_a_packed_triple_keeps_its_own_column():
    """PMC12499305 Table 6 prints an ROI number before each `x y z` cell; nu-v23 read the ROI
    number as x and dropped z on 41 of 42 rows."""
    lines = _serialised("pmc12499305_t6.xml")

    assert "#<3:MNI Coordinates (mm)" in lines[0]
    assert "^17:Nback-fMRI | 10 | 10 | -46 | 73 | Precuneus R | SM Hand" in lines


@pytest.mark.parametrize("text", [
    # two packed columns side by side: two tables printed as one
    "#Node | #MNI | #Label | #Node | #MNI | #Label\n18 | 34 32 7 | R IFG | 42 | 43 1 12 | R insula\n"
    "53 | 44 -11 38 | R PoCG | 62 | -38 -15 59 | L PrCG",
    # a header that names no coordinates (thresholds)
    "#Region | #Thresholds\nA | 0.01 0.005 0.001\nB | 0.01 0.005 0.001",
    # a column that also prints something other than a triple
    "#Region | #MNI\nA | 1 2 3\nB | 4 5 6\nC | see text",
    # one packed row is not a packed column
    "#Region | #MNI\nA | 1 2 3\nB | 4",
])
def test_a_packed_cell_is_left_alone_unless_its_column_is_one_of_coordinates(text):
    from ingestion_workflow.services.create_analyses import _split_packed_triples

    assert _split_packed_triples(text) == text


def test_a_spanned_header_and_a_continued_cell_take_the_new_columns():
    from ingestion_workflow.services.create_analyses import _split_packed_triples

    text = ("#<2:Peak | #T\n~ | #x y z | ~\n<3:Patients > controls\n"
            "A | 1 -2 3 | 4.1\n~ | 5 6 -7 | 3.9")
    assert _split_packed_triples(text).split("\n") == [
        "#<4:Peak | #T",
        "~ | #<3:x y z | ~",
        "<5:Patients > controls",
        "A | 1 | -2 | 3 | 4.1",
        "~ | 5 | 6 | -7 | 3.9",
    ]
