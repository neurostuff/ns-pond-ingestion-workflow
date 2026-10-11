"""Where a coordinate triple is printed: the matcher the parse and LOCAL share."""

from __future__ import annotations

import pytest
from ingestion_workflow.services.coordinate_text import find_all, find_point, find_points


def _printed(point, text):
    return [text[a:b] for a, b in find_all(point, text)]


# Printed forms from the x1 prose units, each with the unit it came from.
@pytest.mark.parametrize(
    "text, point, printed",
    [
        # p:silver-1005: an en dash glued to the value before it.
        ("intertemporal coordinates [0 46–4],  z = 4.25", (0, 46, -4), "0 46–4"),
        ("intertemporal coordinates [−58–40 24],  z = 4.82", (-58, -40, 24), "−58–40 24"),
        # p:silver-1099 and p:silver-1104: "and" before the last value.
        (
            "Gyrus (  x   = –48,   y   = 8, and   z   = 16) and",
            (-48, 8, 16),
            "–48,   y   = 8, and   z   = 16",
        ),
        ("was x = 27, y = -1 and z = 58 based", (27, -1, 58), "27, y = -1 and z = 58"),
        # p:silver-1136: a spread after each value.
        (
            "STS ( x , y , z = − 54 ± 5, − 40 ± 2, − 1 ± 2).",
            (-54, -40, -1),
            "− 54 ± 5, − 40 ± 2, − 1 ± 2",
        ),
        # p:silver-1140: a space before the comma and after the minus.
        ("MNI = [–55,  13 , – 4 ]) ( Fig 1", (-55, 13, -4), "–55,  13 , – 4"),
        # p:silver-1101: a plus sign, spaced.
        ("coordinates −38, 44, 26 and + 38, 44, 26. Participants", (38, 44, 26), "+ 38, 44, 26"),
        ("(x = -42, y = 18, z = 6)", (-42, 18, 6), "-42, y = 18, z = 6"),
        ("peak (−42; 18; 6)", (-42, 18, 6), "−42; 18; 6"),
        ("peak −42/18/6 here", (-42, 18, 6), "−42/18/6"),
        ("peak (–42, 18, 6)", (-42, 18, 6), "–42, 18, 6"),
        ("peak (-42.0, 18, 6)", (-42, 18, 6), "-42.0, 18, 6"),
        ("peak (−42, 18,\n6)", (-42, 18, 6), "−42, 18,\n6"),
        ("at -42, 18, 6.", (-42, 18, 6), "-42, 18, 6"),
        ("peak (4.50, 1, 2)", (4.5, 1, 2), "4.50, 1, 2"),
        ("peak (4.53, 1, 2)", (4.5, 1, 2), "4.53, 1, 2"),
        # p:silver-1505, p:silver-1363, p:silver-1597.
        ("x, y, z = + 10-98 + 04; cluster size", (10, -98, 4), "+ 10-98 + 04"),
        (
            " x −  24.2,   y  \u2009+\u200916.3,   z  \u2009+\u200918.1",
            (-24.2, 16.3, 18.1),
            "−  24.2,   y  \u2009+\u200916.3,   z  \u2009+\u200918.1",
        ),
        (
            "(  x   = −26,   y   = \n−64,   z   = 40) ",
            (-26, -64, 40),
            "−26,   y   = \n−64,   z   = 40",
        ),
        # A table's decimals printed rounded in the prose.
        ("peak (-42, 18, 6)", (-41.6, 18.2, 6), "-42, 18, 6"),
        ("peak (-41.6, 18.2, 6)", (-41.63, 18.2, 6), "-41.6, 18.2, 6"),
    ],
)
def test_a_point_is_found_in_every_way_papers_print_it(text, point, printed):
    assert _printed(point, text) == [printed]


@pytest.mark.parametrize(
    "text, point",
    [
        # p:silver-1107 and p:silver-1101: the mirror hemisphere is another point.
        ("prefrontal cortex (vlPFC) [−40 20 −2], the", (40, 20, -2)),
        ("coordinates −38, 44, 26 and", (38, 44, 26)),
        ("peak (- 42, 18, 6)", (42, 18, 6)),
        ("peak (−  42, 18, 6)", (42, 18, 6)),
        ("peak (42, -18, 6)", (42, 18, 6)),
        ("peak (42, 18, -6)", (42, 18, 6)),
        ("peak (-42, 18, 6)", (-42, -18, 6)),
        # Every sign extractors.utils reads as a minus is one here too.
        ("peak (\u201442, 18, 6)", (42, 18, 6)),
        ("peak (\u201142, 18, 6)", (42, 18, 6)),
        ("peak (\uff0d42, 18, 6)", (42, 18, 6)),
        ("peak (\ufe6342, 18, 6)", (42, 18, 6)),
        # A longer number.
        ("peak (142, 18, 6)", (42, 18, 6)),
        ("peak (-142, 18, 6)", (-42, 18, 6)),
        ("peak (42, 18, 60)", (42, 18, 6)),
        ("peak (−42, 18, 6.5)", (-42, 18, 6)),
        ("peak (-42.5, 18, 6)", (-42, 18, 6)),
        # Rows of a table in the parsed text are not one triple.
        ("insula -42 18\n6 putamen", (-42, 18, 6)),
        ("peak (-4218, 6)", (-42, 18, 6)),
    ],
)
def test_another_point_is_not_taken_for_this_one(text, point):
    assert _printed(point, text) == []


def test_a_decimal_point_prefers_its_printed_value_to_a_rounded_copy():
    text = "Rounded (-42, 18, 6) in the text; the table gives (-41.6, 18.2, 6)."
    assert _printed((-41.6, 18.2, 6), text) == ["-41.6, 18.2, 6"]


def test_find_point_prefers_its_window_then_an_untaken_copy():
    text = "(1, 2, 3) once. Later (1, 2, 3) and (1, 2, 3)."
    second, third = text.index("1, 2, 3", 10), text.rindex("1, 2, 3")
    window = [(15, len(text))]
    assert find_point((1, 2, 3), text, window) == (second, second + 7)
    assert find_point((1, 2, 3), text, window, {(second, second + 7)}) == (third, third + 7)
    # Printed only outside its window: where it is printed once.
    assert find_point((4, 5, 6), "x (4, 5, 6)", [(0, 1)]) == (3, 10)


def test_a_set_s_repeated_point_goes_to_its_next_copy():
    text = "Seed (1, 2, 3); peak (1, 2, 3)."
    points = [{"x": 1, "y": 2, "z": 3}, {"x": 1, "y": 2, "z": 3}, {"x": 9}]
    assert find_points(points, text) == [(6, 13), (22, 29), None]
