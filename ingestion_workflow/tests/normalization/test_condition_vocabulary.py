"""Condition vocabulary v3: the file is consistent, and labellers' atoms from the 2026-11-20 pilot normalize onto it."""
import re

import pytest

from ingestion_workflow.normalization import vocabulary as V

VOCAB = V.load()
SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")


def test_axes_and_values_are_snake_case():
    for axis, spec in VOCAB["axes"].items():
        assert SNAKE.match(axis), axis
        for value in spec["values"]:
            assert SNAKE.match(value), (axis, value)


def test_aliases_land_in_the_vocabulary():
    for alias, target in VOCAB["aliases"]["axes"].items():
        assert alias not in VOCAB["axes"], alias
        assert target in VOCAB["axes"], target
    for alias, targets in VOCAB["aliases"]["values"].items():
        for t in targets:
            axis, value = t.split(":", 1)
            assert V.status(axis, value) in ("in", "open_value"), (alias, t)


def test_structural_axes_are_not_condition_axes():
    assert not set(VOCAB["structural"]) & set(VOCAB["axes"])
    assert "level" not in VOCAB["axes"]


@pytest.mark.parametrize(
    "axis, value, expected",
    [
        # pilot labels: an economic game's partner move, a mental-rotation angle, a pre/post period, block order
        ("partner_action", "cooperate", [("cooperation", "cooperate"), ("referent", "other")]),
        ("orientation", "deg_17", [("orientation", "17_deg")]),
        ("rate", "hz_0_5", [("rate", "0_5_hz")]),
        ("session", "pre", [("period", "pre")]),
        ("session", "extinction", [("phase", "extinction")]),
        ("block_order", "2", [("stage", "block_2")]),
        ("deception", "lie", [("veracity", "lie")]),
        ("Saccade Type", "anti", [("saccade", "anti")]),
        ("stimulus", ["numbers", "letters"], [("stimulus", "numbers"), ("stimulus", "letters")]),
        ("group", "patients_mondo_0005090", []),
        ("score", "bdi_ii", []),
    ],
)
def test_normalize_atom(axis, value, expected):
    assert V.normalize_atom(axis, value) == expected


@pytest.mark.parametrize(
    "axis, value, expected",
    [
        ("stimulus", "faces", "in"),
        ("timing", "minus_300_ms", "in"),
        ("orientation", "17_deg", "in"),
        ("content", "erotic", "open_value"),
        ("emotion", "sympathy", "new_value"),
        ("dilemma_type", "personal", "local"),
    ],
)
def test_status(axis, value, expected):
    assert V.status(axis, value) == expected
