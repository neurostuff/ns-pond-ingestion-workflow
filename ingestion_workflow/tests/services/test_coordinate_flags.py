"""The two flags a table decides for itself."""

from __future__ import annotations

from ingestion_workflow.services.coordinate_flags import (
    is_deactivation,
    reports_extent,
    subpeak_flags,
)


# -- is_deactivation ------------------------------------------------------

def test_a_negative_statistic_is_a_deactivation():
    assert is_deactivation(-4.31) is True
    assert is_deactivation(4.31) is False


def test_no_statistic_is_not_a_deactivation():
    """Absence of evidence. A row with no statistic says nothing about
    direction, and defaulting to True would mark most of the corpus."""
    assert is_deactivation(None) is False


def test_zero_is_not_a_deactivation():
    """`< 0`, not `<= 0`. A zero statistic is not a decrease, and it is
    usually a parse of an empty cell anyway."""
    assert is_deactivation(0.0) is False


def test_an_unparseable_statistic_is_not_a_deactivation():
    """The field reaches here from a model, so it can hold anything. It must
    not raise in the middle of converting an article."""
    assert is_deactivation("n.s.") is False


def test_a_magnitude_only_table_yields_no_deactivations():
    """Many papers print unsigned magnitudes and put the direction in the
    contrast name. Inferring a deactivation from an unsigned number would
    invent a result, so this stays deliberately conservative -- reading the
    contrast name is the model's job."""
    assert [is_deactivation(v) for v in (3.1, 4.8, 2.2)] == [False, False, False]


# -- is_subpeak -----------------------------------------------------------

def test_a_row_without_extent_is_a_subpeak_when_others_have_one():
    """A table that reports extent prints it once per cluster, on the peak,
    and leaves it blank for the local maxima that follow."""
    assert subpeak_flags([218, None, None, 57]) == [False, True, True, False]


def test_a_table_that_never_reports_extent_has_no_subpeaks():
    """Otherwise every row of every table without a size column becomes a
    subpeak -- which is most of the corpus, and wrong. A table of peaks with
    no sizes is a table of peaks."""
    assert subpeak_flags([None, None, None]) == [False, False, False]


def test_a_table_that_reports_extent_on_every_row_has_no_subpeaks():
    assert subpeak_flags([12, 34, 56]) == [False, False, False]


def test_the_decision_is_taken_over_the_whole_analysis_not_per_row():
    """This is the reason the flag cannot be computed one point at a time:
    the same blank extent means subpeak in the first analysis and nothing in
    the second."""
    assert subpeak_flags([100, None]) == [False, True]
    assert subpeak_flags([None, None]) == [False, False]


def test_no_points_is_not_an_error():
    assert subpeak_flags([]) == []
    assert reports_extent([]) is False


def test_reports_extent_needs_only_one():
    assert reports_extent([None, None, 5]) is True
    assert reports_extent([None, None]) is False
