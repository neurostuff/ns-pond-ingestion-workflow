"""The Methods and Results of an article's text, and the filter run before the detector."""

from __future__ import annotations

from ingestion_workflow.services.prose_text import kept_spans, may_hold_coordinates

TEXT = """# Title

## Introduction

Earlier work found x = 30, y = 2, z = -20.

## Materials and methods

We scanned 20 people.

## Results

A peak in the ACC (x = 4, y = 30, z = 22).
"""


def test_the_methods_and_results_are_kept():
    spans, how = kept_spans(TEXT)
    prose = "\n\n".join(TEXT[a:b] for a, b in spans)
    assert how == "methods+results"
    assert "x = 4, y = 30, z = 22" in prose and "x = 30" not in prose


def test_the_filter_keeps_prose_and_drops_a_text_without_a_coordinate():
    assert may_hold_coordinates(TEXT)
    assert not may_hold_coordinates("Accuracy was 85% (70, 85, 92).")


def test_without_sections_the_whole_text_is_read():
    text = "Peak at x = 1, y = 2, z = 3 somewhere."
    assert kept_spans(text) == ([(0, len(text))], "full text")


def test_a_text_s_less_than_and_greater_than_signs_are_kept():
    assert may_hold_coordinates("Insula (p < 0.05; x = -34, y = 16, z = -6; Z > 3.1).")
