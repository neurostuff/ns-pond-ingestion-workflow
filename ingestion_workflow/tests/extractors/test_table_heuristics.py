"""Tests for the coordinate-table heuristics."""

from __future__ import annotations

import pytest

from ingestion_workflow.extractors.table_heuristics import looks_like_coordinate_table
from ingestion_workflow.models import ExtractedTable

COORDINATE_CSV = """Region,Cluster size,x,y,z,T
Left IFG,1225,-37,-57,44,4.63
Right MOG,731,45,-79,-18,5.13
"""

DEMOGRAPHICS_CSV = """Measure,Patients (n = 17),Controls (n = 17),p
Age (years),25.0,24.0,0.87
Height (cm),163.5,173.0,0.001
"""

# Coordinates present, but no header row the deterministic parser can map.
HEADERLESS_COORDINATE_CSV = """Superior frontal,-12,38,46,5.2
Precuneus,6,-62,38,4.1
"""


def _table(tmp_path, content: str, *, caption: str = "", footer: str = "") -> ExtractedTable:
    path = tmp_path / "table.csv"
    path.write_text(content, encoding="utf-8")
    return ExtractedTable(
        table_id="t1",
        raw_content_path=path,
        caption=caption,
        footer=footer,
    )


def test_detects_coordinate_table_by_headers(tmp_path):
    table = _table(
        tmp_path,
        COORDINATE_CSV,
        caption="Table 2. Peak MNI coordinates of significant clusters",
    )
    assert looks_like_coordinate_table(table) is True


def test_detects_coordinate_table_without_usable_headers(tmp_path):
    """The case the deterministic parser misses: no mappable x/y/z columns."""
    table = _table(
        tmp_path,
        HEADERLESS_COORDINATE_CSV,
        caption="Table 3. Local maxima of activation (Talairach)",
    )
    assert looks_like_coordinate_table(table) is True


def test_rejects_demographics_table(tmp_path):
    table = _table(
        tmp_path,
        DEMOGRAPHICS_CSV,
        caption="Table 1. Demographic and clinical characteristics",
    )
    assert looks_like_coordinate_table(table) is False


def test_rejects_coordinate_wording_without_coordinates(tmp_path):
    """A methods-ish table that talks about clusters but reports none."""
    table = _table(
        tmp_path,
        "Parameter,Value\nCluster-forming threshold,p < 0.001\nSmoothing,8 mm\n",
        caption="Table 1. Cluster thresholding parameters",
    )
    assert looks_like_coordinate_table(table) is False


def test_rejects_all_positive_integers_as_coordinates(tmp_path):
    """Ages and counts are positive triples too; brains have a left side."""
    table = _table(
        tmp_path,
        "Group,N,Age,Education\nPatients,17,25,12\nControls,17,24,13\n",
        caption="Table 1. Sample characteristics by cluster",
    )
    assert looks_like_coordinate_table(table) is False


def test_missing_file_is_not_a_coordinate_table(tmp_path):
    table = ExtractedTable(
        table_id="gone",
        raw_content_path=tmp_path / "does-not-exist.csv",
        caption="Table 2. MNI coordinates",
    )
    assert looks_like_coordinate_table(table) is False
