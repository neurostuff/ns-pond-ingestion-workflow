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
    """v19's points carry no flag fields at all. is_subpeak and the sign are
    derived from the numbers in `coordinate_flags`, and inventing defaults
    here would shadow that."""
    out, _ = parse_payload(_payload(analyses=[
        {"name": "a", "points": [[1, 2, 3, "T", -2.0, None]]}]))
    point = out.analyses[0].points[0]
    assert point.is_subpeak is False and not hasattr(point, "is_deactivation")
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


def test_a_named_analysis_with_no_points_is_kept():
    """A table that names a contrast and reports `n.s.` has run it and found
    nothing, which is a result. Dropping it would say the paper never looked.
    This is the opposite of an empty payload, where the table reports no
    contrasts at all."""
    out, _ = parse_payload(_payload(analyses=[
        {"name": "Down-regulate vs. look aversive", "points": []},
        {"name": "real", "points": [[1, 2, 3]]}]))
    assert [a.name for a in out.analyses] == [
        "Down-regulate vs. look aversive", "real"]
    assert out.analyses[0].points == []


def test_an_analysis_with_neither_name_nor_point_is_dropped():
    """Nothing was reported, so there is no analysis to record."""
    out, _ = parse_payload(_payload(analyses=[
        {"name": "", "points": []},
        {"name": None, "points": []},
        {"name": "real", "points": [[1, 2, 3]]}]))
    assert [a.name for a in out.analyses] == ["real"]


def test_an_unnamed_statistic_is_still_kept():
    """Its sign is what decides the sign split, so discarding it for lacking
    a type would lose the only evidence of direction."""
    out, _ = parse_payload(_payload(analyses=[
        {"name": "a", "points": [[1, 2, 3, None, -4.5]]}]))
    value = out.analyses[0].points[0].values[0]
    assert value.value == -4.5 and value.kind is None


def test_an_unreadable_answer_is_a_failure_not_an_empty_result():
    """`{"analyses": []}` is the model saying the table holds nothing, which
    is a result. Output that is not JSON is a failure, and recording the two
    the same way hid data loss behind a legitimate-looking outcome: re-asking
    25 tables recorded as "no analyses", 3 (12%) were actually this."""
    import pytest

    from ingestion_workflow.services.nuextract_payload import UnreadableAnswer

    for bad in ("", "   ", "no json here", '{"space": "MNI", ', "[]"):
        with pytest.raises(UnreadableAnswer):
            parse_payload(bad)


def test_the_number_that_broke_a_real_table_raises():
    """A serialiser that fused two cells turned two statistics into
    `7.264.17`, which has two decimal points. The model echoed it and the
    whole table's 57 coordinates were lost."""
    import pytest

    from ingestion_workflow.services.nuextract_payload import UnreadableAnswer

    with pytest.raises(UnreadableAnswer):
        parse_payload('{"space":"MNI","analyses":[{"name":"a",'
                      '"points":[[33,39,15,"T",7.264.17,178]]}]}')


def test_a_well_formed_answer_with_odd_contents_is_still_read():
    """Only unreadable output raises; a parseable object that says something
    strange is handled as before."""
    out, _ = parse_payload('{"analyses": "nope"}')
    assert out.analyses == []
    out, _ = parse_payload('{"analyses": [null, 3]}')
    assert out.analyses == []


def test_prose_around_the_json_is_tolerated():
    out, _ = parse_payload('here you go: {"space":"MNI","analyses":'
                           '[{"name":"a","points":[[1,2,3]]}]} done')
    assert out.analyses[0].points[0].coordinates == [1.0, 2.0, 3.0]


def test_a_negative_extent_is_read_as_a_magnitude():
    out, _ = parse_payload(_payload(analyses=[
        {"name": "a", "points": [[1, 2, 3, "T", 2.0, -40]]}]))
    assert out.analyses[0].points[0].cluster_size == 40
