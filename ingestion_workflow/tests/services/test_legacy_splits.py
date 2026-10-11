"""Payloads stored before splits were declared mark a split only by name.

Sync declares it in memory.
"""

from __future__ import annotations

from ingestion_workflow.models import Analysis, AnalysisCollection, Coordinate
from ingestion_workflow.services.create_analyses import declare_legacy_splits


def _pt(value, x=1.0):
    return Coordinate(x=x, y=2.0, z=3.0, statistic_value=value, statistic_type="T")


def _declared(*analyses):
    collection = AnalysisCollection(slug="s::t1")
    for a in analyses:
        collection.add_analysis(a)
    blob = collection.to_dict()
    out = declare_legacy_splits(AnalysisCollection.from_dict(blob))
    assert collection.to_dict() == blob  # the stored payload is not touched
    return [(a.name, len(a.coordinates), a.metadata.get("split")) for a in out.analyses]


def test_a_legacy_pair_named_inverse_is_declared_too():
    assert _declared(
        Analysis(name="A > B", table_id="t1", coordinates=[_pt(4.0)]),
        Analysis(name="A > B (inverse)", table_id="t1", coordinates=[_pt(-4.0, x=3)]),
    ) == [
        ("A > B", 1, {"half": "original", "index": 0}),
        ("A > B (inverse)", 1, {"half": "inverse", "original_index": 0}),
    ]


def test_a_legacy_pair_is_declared_as_stored():
    assert _declared(
        Analysis(name="A > B", table_id="t1", coordinates=[_pt(4.0), _pt(2.0, x=2)]),
        Analysis(name="A > B (negative)", table_id="t1", coordinates=[_pt(-4.0, x=3)]),
        Analysis(name="C", table_id="t1", coordinates=[_pt(3.0, x=4)]),
    ) == [
        ("A > B", 2, {"half": "original", "index": 0}),
        ("A > B (negative)", 1, {"half": "inverse", "original_index": 0}),
        ("C", 1, None),
    ]


def test_an_unpaired_inverse_half_is_declared_with_no_original():
    """rsk26miv2pjw: "Encoding (negative)" first in its collection."""
    assert _declared(
        Analysis(name="Encoding (negative)", table_id="t1", coordinates=[_pt(-4.0)]),
        Analysis(name="Retrieval", table_id="t1", coordinates=[_pt(3.0, x=2)]),
    ) == [
        ("Encoding (negative)", 1, {"half": "inverse", "original_index": None}),
        ("Retrieval", 1, None),
    ]


def test_a_pair_never_crosses_tables():
    assert _declared(
        Analysis(name="X", table_id="t1", coordinates=[_pt(4.0)]),
        Analysis(name="X (negative)", table_id="t2", coordinates=[_pt(-4.0, x=2)]),
    ) == [
        ("X", 1, None),
        ("X (negative)", 1, {"half": "inverse", "original_index": None}),
    ]


def test_a_declared_payload_is_left_alone():
    halves = [
        Analysis(name="X", table_id="t1", coordinates=[_pt(4.0)],
                 metadata={"split": {"half": "original", "index": 0}}),
        Analysis(name="X (negative)", table_id="t1", coordinates=[_pt(-4.0, x=2)],
                 metadata={"split": {"half": "inverse", "original_index": 0}}),
    ]
    assert [s for _, _, s in _declared(*halves)] == [
        {"half": "original", "index": 0}, {"half": "inverse", "original_index": 0}]
