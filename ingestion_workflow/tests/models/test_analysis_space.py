"""A space nothing states is null, never MNI, on the way in and out of the cache."""

from __future__ import annotations

import pytest
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    Coordinate,
    CoordinateSpace,
    ExtractedTable,
)


def test_a_point_with_no_space_key_decodes_to_none():
    """It used to decode to MNI: a silent default for a point nothing placed."""
    assert Coordinate.from_dict({"x": 1, "y": 2, "z": 3}).space is None
    assert Coordinate.from_dict({"x": 1, "y": 2, "z": 3, "space": None}).space is None
    assert Coordinate(x=1.0, y=2.0, z=3.0).space is None


def test_a_collection_with_no_space_key_decodes_to_none():
    collection = AnalysisCollection.from_dict({"slug": "a::t", "analyses": []})
    assert collection.coordinate_space is None
    assert AnalysisCollection(slug="a::t").coordinate_space is None


@pytest.mark.parametrize("space", [None, *CoordinateSpace])
def test_every_space_round_trips_through_the_cache(space):
    collection = AnalysisCollection(slug="a::t", coordinate_space=space, analyses=[
        Analysis(name="A > B", coordinates=[Coordinate(x=1.0, y=2.0, z=3.0, space=space)])])
    payload = collection.to_dict()
    assert payload["coordinate_space"] == (space.value if space else None)
    again = AnalysisCollection.from_dict(payload)
    assert again.coordinate_space is space
    assert again.analyses[0].coordinates[0].space is space
    assert again.to_dict() == payload


def test_an_extracted_point_with_no_space_anywhere_stays_none():
    table = ExtractedTable.from_dict({
        "table_id": "t1", "raw_content_path": "t1.html", "space": None,
        "coordinates": [{"x": 1.0, "y": 2.0, "z": 3.0, "space": None}],
    })
    assert table.coordinates[0].space is None
