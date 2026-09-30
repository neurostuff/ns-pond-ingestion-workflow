"""Reading the fine-tuned extractor's compact answer."""

from __future__ import annotations

import json

from ingestion_workflow.services.nuextract_payload import parse_payload


def _payload(**kwargs):
    base = {"space": "MNI", "analyses": []}
    base.update(kwargs)
    return json.dumps(base)


def test_a_positional_point_becomes_a_coordinate_point():
    """The tuple is [x, y, z, statistic_type, statistic, extent]."""
    out, space = parse_payload(_payload(analyses=[
        {"name": "Smoking > Neutral", "measure": "voxels",
         "points": [[-42, 18, 4, "T", 4.31, 218]]}]))
    assert space == "MNI"
    point = out.analyses[0].points[0]
    assert point.coordinates == [-42.0, 18.0, 4.0]
    assert point.values[0].kind == "T"
    assert point.values[0].value == 4.31
    assert point.cluster_size == 218
    assert point.cluster_measure == "voxels"


def test_the_space_is_stated_once_and_copied_onto_every_point():
    """A paper normalises once, so v19 reports the space for the table rather
    than repeating it per coordinate. Downstream wants it per point."""
    out, space = parse_payload(_payload(space="TAL", analyses=[
        {"name": "a", "points": [[1, 2, 3], [4, 5, 6]]}]))
    assert space == "TAL"
    assert [p.space for p in out.analyses[0].points] == ["TAL", "TAL"]


def test_the_measure_is_per_analysis_not_per_point():
    out, _ = parse_payload(_payload(analyses=[
        {"name": "a", "measure": "mm3", "points": [[1, 2, 3, "Z", 3.0, 10]]}]))
    assert out.analyses[0].points[0].cluster_measure == "mm^3"


def test_no_flags_are_read_from_the_payload():
    """v19's points carry no flag fields at all. is_subpeak and
    is_deactivation are derived from the numbers in `coordinate_flags`, and
    inventing defaults here would shadow that."""
    out, _ = parse_payload(_payload(analyses=[
        {"name": "a", "points": [[1, 2, 3, "T", -2.0, None]]}]))
    point = out.analyses[0].points[0]
    assert point.is_subpeak is False and point.is_deactivation is False
    assert point.values[0].value == -2.0      # the sign survives, to be read later


def test_an_empty_answer_is_an_answer_not_a_failure():
    """v19 is trained to return nothing for a table holding no coordinates.
    Treating that as a parse failure would retry every negative table."""
    out, space = parse_payload('{"space":null,"analyses":[]}')
    assert out.analyses == []
    assert space is None


def test_a_short_row_still_yields_its_coordinate():
    """The tail is optional: a table with no statistic and no size column
    still has usable coordinates in it."""
    out, _ = parse_payload(_payload(analyses=[{"name": "a", "points": [[1, 2, 3]]}]))
    point = out.analyses[0].points[0]
    assert point.coordinates == [1.0, 2.0, 3.0]
    assert point.values is None and point.cluster_size is None


def test_a_row_whose_first_three_entries_are_not_numbers_is_dropped():
    out, _ = parse_payload(_payload(analyses=[
        {"name": "a", "points": [["L", "MFG", "x"], [1, 2, 3]]}]))
    assert len(out.analyses[0].points) == 1


def test_an_analysis_with_no_usable_point_is_dropped():
    """It would otherwise add a named, empty analysis to the database."""
    out, _ = parse_payload(_payload(analyses=[
        {"name": "empty", "points": []},
        {"name": "real", "points": [[1, 2, 3]]}]))
    assert [a.name for a in out.analyses] == ["real"]


def test_an_unnamed_statistic_is_still_kept():
    """Its sign is what decides is_deactivation, so discarding it for lacking
    a type would lose the only evidence of direction."""
    out, _ = parse_payload(_payload(analyses=[
        {"name": "a", "points": [[1, 2, 3, None, -4.5]]}]))
    value = out.analyses[0].points[0].values[0]
    assert value.value == -4.5 and value.kind is None


def test_malformed_output_costs_one_table_not_the_article():
    """Two of 869 benchmark tables came back unparseable. None of these may
    raise."""
    for bad in ("", "   ", "no json here", '{"space": "MNI", ', "[]",
                '{"analyses": "nope"}', '{"analyses": [null, 3]}'):
        out, space = parse_payload(bad)
        assert out.analyses == []


def test_prose_around_the_json_is_tolerated():
    out, _ = parse_payload('here you go: {"space":"MNI","analyses":'
                           '[{"name":"a","points":[[1,2,3]]}]} done')
    assert out.analyses[0].points[0].coordinates == [1.0, 2.0, 3.0]


def test_a_negative_extent_is_read_as_a_magnitude():
    out, _ = parse_payload(_payload(analyses=[
        {"name": "a", "points": [[1, 2, 3, "T", 2.0, -40]]}]))
    assert out.analyses[0].points[0].cluster_size == 40
