"""Re-upload into an existing study version: which existing analysis each incoming one is."""

import pytest
from ingestion_workflow.config import Settings, UploadBehavior
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    Coordinate,
    CoordinateSpace,
    Identifier,
)
from ingestion_workflow.services.db import SessionFactory
from ingestion_workflow.services.upload import UploadService
from ingestion_workflow.services.upload_models import Analysis as DbAnalysis
from ingestion_workflow.services.upload_models import Annotation as DbAnnotation
from ingestion_workflow.services.upload_models import AnnotationAnalysis as DbAnnotationAnalysis
from ingestion_workflow.services.upload_models import Base as UploadBase
from ingestion_workflow.services.upload_models import Point as DbPoint
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session
from study_schema import keys

IDENTIFIER = Identifier(doi="10.1/abc", pmid="123")


@pytest.fixture()
def upload(tmp_path):
    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns",
        upload_use_ssh=False,
        upload_source="llm",
    )
    engine = create_engine("sqlite:///:memory:", future=True)
    UploadBase.metadata.create_all(engine)
    service = UploadService(settings, SessionFactory(settings, engine=engine))

    def run(*analyses):
        collection = AnalysisCollection(
            slug="slug::t1",
            analyses=list(analyses),
            coordinate_space=CoordinateSpace.MNI,
            identifier=IDENTIFIER,
        )
        items = service.prepare_work_items(
            {"slug": {"t1": collection}}, {}, metadata_mode=settings.upload_metadata_mode
        )
        (outcome,) = service.run(
            items,
            behavior=UploadBehavior.UPDATE,
            metadata_only=False,
            metadata_mode=settings.upload_metadata_mode,
        )
        assert outcome.success, outcome.error
        return outcome

    run.engine = engine
    return run


def _table(name, cells, *points):
    """A table analysis as the upload stage hands it over: with its place in the parse."""
    return Analysis(
        name=name,
        table_id="T1",
        coordinates=[Coordinate(x=x, y=y, z=z, space=CoordinateSpace.MNI) for x, y, z in points],
        metadata={
            "sanitized_table_id": "t1",
            "parse": {
                "key": keys.table_key("t1", cells, name),
                "origin": "table",
                "table_id": "t1",
                "cells": [{"row": r, "column_group": g} for r, g in cells],
            },
        },
    )


def _legacy(name, *points):
    """An analysis with no place in a parse, as every upload before keys made one."""
    return Analysis(
        name=name,
        table_id="T1",
        coordinates=[Coordinate(x=x, y=y, z=z, space=CoordinateSpace.MNI) for x, y, z in points],
        metadata={"sanitized_table_id": "t1"},
    )


def _all_rows(engine):
    """(id, name, source_id, points) of every analysis."""
    with Session(engine, future=True) as session:
        return [
            (row.id, row.name, row.source_id, sorted((p.x, p.y, p.z) for p in row.points))
            for row in session.execute(select(DbAnalysis)).scalars()
        ]


def _rows(engine):
    """name -> (id, source_id, points), for studies whose names are distinct."""
    rows = _all_rows(engine)
    assert len({name for _, name, _, _ in rows}) == len(rows)
    return {name: (id_, key, points) for id_, name, key, points in rows}


def _annotate(engine, analysis_id, note):
    with Session(engine, future=True) as session:
        session.add(DbAnnotation(id="ann1", name="a"))
        session.add(DbAnnotationAnalysis(annotation_id="ann1", analysis_id=analysis_id, note=note))
        session.commit()


def _annotated_name(engine):
    with Session(engine, future=True) as session:
        link = session.execute(select(DbAnnotationAnalysis)).scalar_one()
        return session.get(DbAnalysis, link.analysis_id).name


def _by_decision(outcome):
    out = {}
    for entry in outcome.reconcile:
        out.setdefault(entry["decision"], []).append(entry)
    return out


SHARED = [(30, -60, 12), (-30, -60, 12), (42, 18, -6)]


def test_a_contrast_sharing_most_points_does_not_take_an_annotated_analysis(upload):
    """The conjunction table case: B shares three of A's four peaks but is another contrast."""
    first = upload(_table("Patients > Controls", [(1, 0), (2, 0), (3, 0), (4, 0)],
                          *SHARED, (0, 50, 20)))
    a_id = first.analysis_ids[0]
    _annotate(upload.engine, a_id, {"included": False, "note": "checked by hand"})

    second = upload(_table("Patients > Controls AND Task", [(1, 0), (2, 0), (3, 0), (5, 0)],
                           *SHARED, (8, 8, 8)))

    rows = _rows(upload.engine)
    assert rows["Patients > Controls"][0] == a_id
    assert sorted(rows["Patients > Controls"][2]) == sorted([*SHARED, (0, 50, 20)])
    assert rows["Patients > Controls AND Task"][0] != a_id
    assert _annotated_name(upload.engine) == "Patients > Controls"
    decided = _by_decision(second)
    assert [e["name"] for e in decided["new"]] == ["Patients > Controls AND Task"]
    [left] = decided["unmatched_existing"]
    assert left["analysis_id"] == a_id and left["annotated"] is True
    assert "left in place" in left["reason"]


@pytest.mark.parametrize(
    "build",
    [lambda name, row: _table(name, [(row, 0)]), lambda name, row: _legacy(name)],
    ids=["keyed", "legacy"],
)
def test_two_pointless_contrasts_with_near_names_are_never_swapped(upload, build):
    """'PO > Silence' and 'PC > Silence' are 92% alike as strings, and have no points."""
    first = upload(build("PO > Silence", 1))
    po_id = first.analysis_ids[0]
    _annotate(upload.engine, po_id, {"included": False})

    second = upload(build("PC > Silence", 2))

    rows = _rows(upload.engine)
    assert rows["PO > Silence"][0] == po_id
    assert rows["PC > Silence"][0] != po_id
    assert _annotated_name(upload.engine) == "PO > Silence"
    decided = _by_decision(second)
    assert [e["name"] for e in decided["new"]] == ["PC > Silence"]
    assert [e["analysis_id"] for e in decided["unmatched_existing"]] == [po_id]


def test_both_contrasts_read_again_keep_their_own_rows_in_any_order(upload):
    first = upload(_table("PO > Silence", [(1, 0)]), _table("PC > Silence", [(2, 0)]))
    po_id, pc_id = first.analysis_ids

    second = upload(_table("PC > Silence", [(2, 0)]), _table("PO > Silence", [(1, 0)]))

    rows = _rows(upload.engine)
    assert (rows["PO > Silence"][0], rows["PC > Silence"][0]) == (po_id, pc_id)
    assert {e["decision"] for e in second.reconcile} == {"matched_key"}


def test_an_analysis_read_again_from_other_cells_keeps_its_row_and_takes_the_new_key(upload):
    """neurostore's same-analysis rule: same normalized name, overlapping cells, one match each."""
    first = upload(_table("PO > Sil", [(1, 0)], (1, 2, 3)))
    a_id = first.analysis_ids[0]
    _annotate(upload.engine, a_id, {"included": False})

    second = upload(_table("PO>Sil", [(1, 0), (2, 0)], (1, 2, 3), (4, 5, 6)))

    rows = _rows(upload.engine)
    assert list(rows) == ["PO>Sil"]
    assert rows["PO>Sil"][0] == a_id
    assert rows["PO>Sil"][1] == keys.table_key("t1", [(1, 0), (2, 0)], "PO > Sil")
    [entry] = second.reconcile
    assert entry["decision"] == "matched_cells_changed"
    assert entry["previous_key"] == keys.table_key("t1", [(1, 0)], "PO > Sil")


def test_a_rename_at_the_same_cells_is_a_candidate_not_a_match(upload):
    """Another name at exactly the old cells: a rename or another contrast, a curator decides."""
    first = upload(_table("PO > Sil", [(1, 0)], (1, 2, 3)))
    a_id = first.analysis_ids[0]
    _annotate(upload.engine, a_id, {"included": False})
    old_key = keys.table_key("t1", [(1, 0)], "PO > Sil")
    new_key = keys.table_key("t1", [(1, 0)], "PO > Silence")

    second = upload(_table("PO > Silence", [(1, 0)], (1, 2, 3)))

    rows = _rows(upload.engine)
    assert rows["PO > Sil"][:2] == (a_id, old_key)
    assert rows["PO > Silence"][0] != a_id and rows["PO > Silence"][1] == new_key
    assert _annotated_name(upload.engine) == "PO > Sil"
    decided = _by_decision(second)
    [new] = decided["new"]
    assert new["rename_candidate_of"] == old_key and "rename candidate" in new["reason"]
    [left] = decided["unmatched_existing"]
    assert left["rename_candidate"] == new_key


def test_a_legacy_row_is_matched_only_on_its_name_and_identical_points(upload):
    first = upload(
        _legacy("Faces > Houses", (1, 2, 3), (4, 5, 6)),
        _legacy("Houses > Faces", (7, 8, 9)),
        _legacy("Main effect", (10, 11, 12)),
    )
    faces, houses, main = first.analysis_ids
    _annotate(upload.engine, houses, {"included": False})

    second = upload(
        # the same name and points: the same analysis, and it takes its key
        _table("faces  >  houses", [(1, 0), (2, 0)], (1, 2, 3), (4, 5, 6)),
        # the same points under another name: another contrast
        _table("Faces > Objects", [(3, 0)], (7, 8, 9)),
        # the same name, one point more: not guessed
        _table("Main effect", [(4, 0), (5, 0)], (10, 11, 12), (13, 14, 15)),
    )

    rows = {id_: (name, key, points) for id_, name, key, points in _all_rows(upload.engine)}
    assert rows[faces][:2] == (
        "faces  >  houses", keys.table_key("t1", [(1, 0), (2, 0)], "Faces > Houses")
    )
    # annotated, so kept; the unannotated legacy "Main effect" is replaced and deleted
    assert rows[houses][:2] == ("Houses > Faces", None)
    assert main not in rows
    added = {name: key for id_, (name, key, _) in rows.items() if id_ not in first.analysis_ids}
    assert added == {
        "Faces > Objects": keys.table_key("t1", [(3, 0)], "Faces > Objects"),
        "Main effect": keys.table_key("t1", [(4, 0), (5, 0)], "Main effect"),
    }
    decided = _by_decision(second)
    assert [e["analysis_id"] for e in decided["matched_exact"]] == [faces]
    assert sorted(e["name"] for e in decided["new"]) == ["Faces > Objects", "Main effect"]
    [kept] = decided["unmatched_existing"]
    assert kept["analysis_id"] == houses and kept["annotated"] is True
    [gone] = decided["deleted"]
    assert gone["analysis_id"] == main and gone["key"] is None and gone["points"] == 1


def test_a_legacy_row_whose_repeat_suffix_moved_is_still_matched(upload):
    """The -2 upload gives a repeated name is positional: it goes when the first copy does."""
    first = upload(
        _legacy("Patients > Controls", (1, 2, 3)), _legacy("Patients > Controls", (4, 5, 6))
    )
    second_id = first.analysis_ids[1]

    second = upload(_table("Patients > Controls", [(2, 0)], (4, 5, 6)))

    assert second.analysis_ids == [second_id]
    [matched] = [e for e in second.reconcile if e["decision"] == "matched_exact"]
    assert matched["analysis_id"] == second_id
    assert matched["previous_name"] == "Patients > Controls-2"


def test_a_name_that_differs_by_more_than_a_repeat_suffix_is_another_analysis(upload):
    """'Run-1' and 'Run-2' are the paper's names, not upload's suffixes, whatever their points."""
    first = upload(_legacy("Run-1", (1, 2, 3)))

    second = upload(_table("Run-2", [(1, 0)], (1, 2, 3)))

    assert second.analysis_ids != first.analysis_ids
    assert sorted(e["decision"] for e in second.reconcile) == ["deleted", "new"]


def test_a_re_upload_of_the_same_parse_writes_nothing(upload):
    analyses = lambda: (  # noqa: E731 - fresh objects each upload, as each run reads its own
        _table("PO > Sil", [(1, 0), (2, 0)], (1, 2, 3), (4, 5, 6)),
        _table("PC > Sil", [(1, 0), (2, 0)], (1, 2, 3), (4, 5, 6)),
        _table("Houses > Faces", [(3, 0)]),
    )
    first = upload(*analyses())
    with Session(upload.engine, future=True) as session:
        points_before = sorted(session.execute(select(DbPoint.id)).scalars())

    writes = []

    @event.listens_for(upload.engine, "before_cursor_execute")
    def _record(conn, cursor, statement, *args):
        verb = statement.lstrip().split(None, 1)[0].upper()
        if verb in ("INSERT", "UPDATE", "DELETE") and any(
            f" {t}" in statement for t in ("analyses", "points", "point_values")
        ):
            writes.append(statement)

    second = upload(*analyses())

    assert writes == []
    assert second.analysis_ids == first.analysis_ids
    assert {e["decision"] for e in second.reconcile} == {"matched_key"}
    with Session(upload.engine, future=True) as session:
        assert sorted(session.execute(select(DbPoint.id)).scalars()) == points_before


def test_an_unmatched_analysis_is_deleted_unless_annotated_and_each_deletion_is_recorded(upload):
    """A new reading replaced it, so keeping an unannotated one would count it twice."""
    first = upload(
        _table("PO > Sil", [(1, 0)], (1, 2, 3)),
        _legacy("Main effect", (7, 8, 9), (1, 1, 1)),
        _table("Faces > Houses", [(3, 0)], (2, 2, 2)),
    )
    po_id, main_id, faces_id = first.analysis_ids
    _annotate(upload.engine, faces_id, {"included": False, "note": "checked"})

    second = upload(_table("PC > Sil", [(2, 0)], (4, 5, 6)))

    rows = _rows(upload.engine)
    assert set(rows) == {"PC > Sil", "Faces > Houses"}
    assert rows["Faces > Houses"][0] == faces_id
    with Session(upload.engine, future=True) as session:
        stored = set(session.execute(select(DbPoint.analysis_id)).scalars())
    assert po_id not in stored and main_id not in stored
    decided = _by_decision(second)
    gone = {e["analysis_id"]: e for e in decided["deleted"]}
    assert set(gone) == {po_id, main_id}
    assert gone[po_id]["key"] == keys.table_key("t1", [(1, 0)], "PO > Sil")
    assert gone[main_id]["key"] is None
    assert (gone[po_id]["points"], gone[main_id]["points"]) == (1, 2)
    assert all(not e["annotated"] and e["reason"].endswith("so deleted") for e in gone.values())
    [kept] = decided["unmatched_existing"]
    assert kept["analysis_id"] == faces_id and kept["annotated"] is True
    assert kept["reason"].endswith("so left in place")


def test_every_incoming_and_existing_analysis_has_a_decision_with_a_reason(upload):
    upload(_table("A", [(1, 0)], (1, 1, 1)), _legacy("B", (2, 2, 2)))
    second = upload(_table("A", [(1, 0)], (1, 1, 1)), _table("C", [(3, 0)], (3, 3, 3)))
    assert sorted((e["decision"], e["name"]) for e in second.reconcile) == [
        ("deleted", "B"),
        ("matched_key", "A"),
        ("new", "C"),
    ]
    assert all(e["reason"] and e["analysis_id"] for e in second.reconcile)
