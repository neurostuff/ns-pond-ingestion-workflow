"""Points read off a table's headed x, y and z columns, read back from their rows.

The tables and answers are production's (`data/column_shift/*.json` name each: the
article, the table, and the analyses artifact the answer was rebuilt from)."""

import json
from pathlib import Path

import pytest

from ingestion_workflow.services import column_shift
from ingestion_workflow.services.nuextract_payload import parse_payload

DATA = Path(__file__).resolve().parents[1] / "data" / "column_shift"


def _recorded(name):
    stored = json.loads((DATA / f"{name}.json").read_text())
    markup = next(DATA.glob(f"{name}.*[!n]")).read_text()  # the .html or .xml beside the .json
    parsed, _space = parse_payload(stored["answer"])
    return parsed, markup


def _xyz(parsed):
    return [tuple(p.coordinates) for a in parsed.analyses for p in a.points]


@pytest.mark.parametrize(
    "name, printed, note",
    [
        # Klausen 2022 (JCI Insight) Table S14: `Clustersize | x | y | z`, read as
        # (Clustersize, x, y); the FDCR review sheet has the printed peaks.
        ("shifted_left_of_headed_xyz",
         [(-56, -10, -12), (60, 6, -22), (28, -20, -20), (-6, -12, -20)],
         "4 of 4 points read 1 column left"),
        # `BA | x | y | z` under a header that spans the coordinate columns, read as (BA, x, y).
        ("shifted_left_under_spanned_header",
         [(-46, 12, -34), (-50, -34, -6), (44, 0, -26), (-38, -40, -22)],
         "4 of 4 points read 1 column left"),
        # `X | Y | Z | Voxels`, read as (Y, Z, Voxels).
        ("shifted_right_of_headed_xyz",
         [(35, 11, 2), (-32, 14, -6), (-16, -62, -2), (12, -12, 7), (-11, -13, 6), (51, 22, 29)],
         "6 of 6 points read 1 column right"),
    ],
)
def test_points_read_off_the_headed_columns_are_read_back_from_their_rows(name, printed, note):
    parsed, markup = _recorded(name)
    assert _xyz(parsed) != printed
    out, moved = column_shift.realign(parsed, markup)
    assert _xyz(out) == printed
    assert moved.startswith(note)


def test_values_beside_the_coordinates_are_kept_as_answered():
    parsed, markup = _recorded("shifted_left_of_headed_xyz")
    out, _ = column_shift.realign(parsed, markup)
    before = [(p.values, p.cluster_size) for a in parsed.analyses for p in a.points]
    assert [(p.values, p.cluster_size) for a in out.analyses for p in a.points] == before


def test_an_answer_read_right_under_a_heading_set_off_its_columns_is_left_alone():
    # The table's sub-header row puts x over the Brodmann-area column, one left of the
    # coordinates; the answer read the coordinates. Most rows print an area list there,
    # not a number, so the headed window is not taken over the one the answer read.
    parsed, markup = _recorded("read_right_under_spanned_header")
    out, moved = column_shift.realign(parsed, markup)
    assert moved is None
    assert _xyz(out) == _xyz(parsed)


def test_an_answer_read_right_under_headings_set_off_onto_statistics_is_left_alone():
    # Headed x, y and z sit over the coordinates' z and two statistic columns; reading them
    # back would turn (28, -68, 42) into (42, 3.25, 4.97).
    parsed, markup = _recorded("read_right_under_headings_off_onto_statistics")
    out, moved = column_shift.realign(parsed, markup)
    assert moved is None
    assert _xyz(out) == _xyz(parsed)


def _table(header, *rows):
    cells = lambda tag, row: "".join(f"<{tag}>{c}</{tag}>" for c in row)  # noqa: E731
    body = "".join(f"<tr>{cells('td', r)}</tr>" for r in rows)
    return f"<table><thead><tr>{cells('th', header)}</tr></thead><tbody>{body}</tbody></table>"


def _answer(*points):
    return parse_payload(json.dumps({"analyses": [{"name": "A", "points": [list(p) for p in points]}]}))[0]


def test_without_one_headed_column_each_for_x_y_and_z_nothing_is_read_back():
    two_blocks = _table(["k", "x", "y", "z", "x", "y", "z"], [12, -40, 22, 8, 30, -6, 4])
    assert column_shift.realign(_answer((12, -40, 22)), two_blocks)[1] is None
    unheaded = _table(["k", "", "", ""], [12, -40, 22, 8])
    assert column_shift.realign(_answer((12, -40, 22)), unheaded)[1] is None


def test_a_row_a_cell_short_of_the_table_is_not_read_back():
    # A sub-peak row without its region cell is placed from the left: its x sits under the
    # side column. The answer read it right.
    table = ("<table><thead><tr><th>Region</th><th>Side</th><th>x</th><th>y</th><th>z</th>"
             "<th>t</th></tr></thead><tbody>"
             "<tr><td>Precentral</td><td>L</td><td>-45</td><td>-12</td><td>-33</td><td>4.0</td></tr>"
             "<tr><td>R</td><td>51</td><td>-18</td><td>18</td><td>4.0</td></tr></tbody></table>")
    out, moved = column_shift.realign(_answer((-45, -12, -33), (51, -18, 18)), table)
    assert moved is None and _xyz(out) == [(-45, -12, -33), (51, -18, 18)]


def test_a_point_is_not_read_back_to_a_triple_outside_the_brain():
    # A row laid out apart from the rest: its window one to the right of the headed
    # columns prints the answer's point, and its headed cells a cluster size as x.
    table = _table(["k", "x", "y", "z", "t"], [12, -40, 22, -8, 3.2], [7, 1666, -40, 2, 38])
    out, moved = column_shift.realign(_answer((-40, 2, 38)), table)
    assert moved is None and _xyz(out) == [(-40, 2, 38)]


def test_a_window_that_two_rows_print_with_different_coordinates_is_left_alone():
    table = _table(["k", "x", "y", "z"], [12, -40, 22, 8], [12, -40, 22, -6])
    out, moved = column_shift.realign(_answer((12, -40, 22)), table)
    assert moved is None and _xyz(out) == [(12, -40, 22)]


def test_a_point_printed_under_the_headed_columns_is_kept_even_beside_a_shifted_one():
    table = _table(["k", "x", "y", "z"], [12, -40, 22, -8], [30, 16, -4, 50])
    out, moved = column_shift.realign(_answer((-40, 22, -8), (30, 16, -4)), table)
    # (-40, 22, -8) is the first row's printed point: kept. (30, 16, -4) is the second row
    # read one column left: read back as (16, -4, 50).
    assert _xyz(out) == [(-40, 22, -8), (16, -4, 50)]
    assert moved.startswith("1 of 2 points read 1 column left")


def test_a_window_whose_columns_print_no_more_negatives_than_the_answer_s_is_not_taken():
    # z prints no negative number, so the headed columns are no more coordinate-like than
    # (k, x, y): the answer is kept.
    table = _table(["k", "x", "y", "z"], [12, -40, 22, 8], [30, 16, -4, 50])
    out, moved = column_shift.realign(_answer((30, 16, -4)), table)
    assert moved is None and _xyz(out) == [(30, 16, -4)]
