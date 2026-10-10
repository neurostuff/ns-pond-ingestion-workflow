"""A person's "not coordinates" verdict, from the catalog to neurostore and the corpus."""

from __future__ import annotations

import json

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.cli.main import app
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import exclusions as excl
from ingestion_workflow.pipeline.stages.upload import UploadStage
from ingestion_workflow.services.db import SessionFactory
from ingestion_workflow.services.nspond import write_corpus_manifest
from ingestion_workflow.services.upload import UploadService
from ingestion_workflow.services.upload_models import Analysis as DbAnalysis
from ingestion_workflow.services.upload_models import Study as DbStudy
from ingestion_workflow.services.upload_models import StudysetStudy as DbStudysetStudy
from ingestion_workflow.services.upload_models import Table as DbTable
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from test_upload_service import (
    _analysis,
    _annotate,
    _article_metadata,
    _collection,
    _coords,
    _engine,
    _settings,
    _upload_once,
)


# -- the catalog -------------------------------------------------------------


def test_a_mark_is_recorded_replaced_and_withdrawn(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        ref = catalog.register(Identifier(pmid="1"))
        catalog.exclude([(ref.id, "t1", "not coordinates", "crystal structure")])
        catalog.exclude([(ref.id, "t1", "not coordinates", "atomic positions")])
        catalog.exclude([(ref.id, "t2", "not coordinates", "")])
        marks = catalog.exclusions([ref.id])
        assert set(marks[ref.id]) == {"t1", "t2"}
        assert marks[ref.id]["t1"]["note"] == "atomic positions"
        assert catalog.unexclude(ref.id, "t1") == 1
        assert set(catalog.exclusions()[ref.id]) == {"t2"}
        assert catalog.unexclude(ref.id) == 1
        assert catalog.exclusions([ref.id]) == {}


def test_an_old_catalog_gains_the_table_when_opened(tmp_path):
    """The table is created with IF NOT EXISTS, so no migration is needed."""
    Catalog.open(tmp_path / "cat").close()
    with Catalog.open(tmp_path / "cat") as catalog:
        assert catalog.exclusions() == {}


# -- what upload and sync keep -----------------------------------------------


def test_only_marked_tables_are_left_out():
    payload = {"t1": {"analyses": [1]}, "t2": {"analyses": [2]}}
    assert excl.kept(payload, {"t2": {}}) == {"t1": {"analyses": [1]}}
    assert excl.kept(payload, {excl.WHOLE_ARTICLE: {}}) == {}
    assert excl.kept(payload, {}) == payload


def test_marking_nothing_leaves_every_fingerprint_as_it_was():
    """Otherwise adding the mechanism would make the whole corpus stale."""

    class Upstream:
        fingerprint = "abc"

    stage = UploadStage(Settings(upload_source="nuextract-v21"))
    plain = stage.fingerprint_for(Upstream())
    assert stage.fingerprint_for(Upstream(), {}) == plain
    assert stage.fingerprint_for(Upstream(), None) == plain
    marked = stage.fingerprint_for(Upstream(), {"t1": {}})
    assert marked != plain
    assert stage.fingerprint_for(Upstream(), {"t1": {"note": "different note"}}) == marked
    assert stage.fingerprint_for(Upstream(), {"t1": {}, "t2": {}}) != marked


# -- retraction in neurostore ------------------------------------------------


def _uploaded(tmp_path):
    identifier = Identifier(doi="10.1/abc", pmid="123")
    settings = _settings(tmp_path)
    engine = _engine()
    service = UploadService(settings, SessionFactory(settings, engine=engine))
    collection = _collection(identifier, [_analysis("A", _coords((1, 2, 3), (4, 5, 6)))])
    outcome = _upload_once(service, settings, identifier, collection, _article_metadata())[0]
    return service, engine, outcome


def test_a_version_nothing_uses_is_deleted(tmp_path):
    service, engine, uploaded = _uploaded(tmp_path)
    [done] = service.retract([("slug", uploaded.base_study_id)])
    assert done.success and done.action == "deleted" and done.removed == 1
    with Session(engine, future=True) as session:
        assert list(session.execute(select(DbStudy)).scalars()) == []
        assert list(session.execute(select(DbAnalysis)).scalars()) == []
        assert list(session.execute(select(DbTable)).scalars()) == []


def test_a_version_in_a_studyset_is_emptied_not_deleted(tmp_path):
    """Deleting it would cascade through studyset_studies and drop it from a
    user's studyset in Compose without anyone being told."""
    service, engine, uploaded = _uploaded(tmp_path)
    with Session(engine, future=True) as session:
        session.add(DbStudysetStudy(study_id=uploaded.study_id, studyset_id="ss1"))
        session.commit()
    [done] = service.retract([("slug", uploaded.base_study_id)])
    assert done.action == "emptied" and done.studysets == ["ss1"]
    with Session(engine, future=True) as session:
        assert session.execute(select(DbStudy.id)).scalar_one() == uploaded.study_id
        assert list(session.execute(select(DbAnalysis)).scalars()) == []


def test_a_retraction_nobody_asked_for_leaves_a_studyset_member_untouched(tmp_path):
    """When the article's text was taken back, no person decided: the coordinates under
    someone's meta-analysis stay, and the study comes back held for review."""
    service, engine, uploaded = _uploaded(tmp_path)
    with Session(engine, future=True) as session:
        session.add(DbStudysetStudy(study_id=uploaded.study_id, studyset_id="ss1"))
        session.commit()
    [done] = service.retract([("slug", uploaded.base_study_id)], hold_studyset_members=True)
    assert (done.success, done.action, done.studysets, done.removed) == (True, "held", ["ss1"], 0)
    with Session(engine, future=True) as session:
        assert session.execute(select(DbStudy.id)).scalar_one() == uploaded.study_id
        assert len(list(session.execute(select(DbAnalysis)).scalars())) == 1


def test_holding_studyset_members_still_retracts_a_study_no_studyset_holds(tmp_path):
    service, engine, uploaded = _uploaded(tmp_path)
    [done] = service.retract([("slug", uploaded.base_study_id)], hold_studyset_members=True)
    assert done.action == "deleted" and done.removed == 1


def test_an_annotated_analysis_survives_a_retraction(tmp_path):
    service, engine, uploaded = _uploaded(tmp_path)
    with Session(engine, future=True) as session:
        analysis_id = session.execute(select(DbAnalysis.id)).scalar_one()
        _annotate(session, analysis_id, {"included": False, "note": "checked by hand"})
    [done] = service.retract([("slug", uploaded.base_study_id)])
    assert done.action == "emptied" and done.kept_annotated == 1
    with Session(engine, future=True) as session:
        assert session.execute(select(DbAnalysis.id)).scalar_one() == analysis_id


def test_retracting_where_this_source_never_wrote_touches_nothing(tmp_path):
    service, engine, uploaded = _uploaded(tmp_path)
    service.settings = service.settings.model_copy(update={"upload_source": "another-extractor"})
    [done] = service.retract([("slug", uploaded.base_study_id)])
    assert done.success and done.action == "absent"
    with Session(engine, future=True) as session:
        assert session.execute(select(DbStudy.id)).scalar_one() == uploaded.study_id


# -- the corpus manifest -----------------------------------------------------


class _Bundle:
    def __init__(self, pmid):
        class Source:
            value = "pubget"

        class Data:
            identifier = Identifier(pmid=pmid)
            source = Source()

        self.article_data = Data()


def test_the_manifest_is_merged_not_overwritten(tmp_path):
    """Writing only the run's articles once shrank 37,135 rows to 3,341."""
    path = tmp_path / "pmids.tsv"
    path.write_text("1\tA\tace\n2\tB\tace\n3\tC\tace\n", encoding="utf-8")
    write_corpus_manifest(path, [("B", _Bundle("22")), ("D", _Bundle("4"))], drop=["C"])
    assert path.read_text().splitlines() == ["1\tA\tace", "22\tB\tpubget", "4\tD\tpubget"]


def test_retracting_alone_rewrites_the_manifest(tmp_path):
    path = tmp_path / "pmids.tsv"
    path.write_text("1\tA\tace\n2\tB\tace\n", encoding="utf-8")
    write_corpus_manifest(path, [], drop=["A"])
    assert path.read_text().splitlines() == ["2\tB\tace"]


# -- the command -------------------------------------------------------------


@pytest.fixture()
def config(tmp_path):
    path = tmp_path / "settings.yaml"
    path.write_text(json.dumps({
        "data_root": str(tmp_path / "data"), "cache_root": str(tmp_path / "cache"),
        "catalog_root": str(tmp_path / "catalog"), "ns_pond_root": str(tmp_path / "pond"),
        "log_to_file": False, "show_progress": False,
    }), encoding="utf-8")
    return path


def test_a_review_file_marks_only_its_non_coordinate_tables(config, tmp_path):
    with Catalog.open(tmp_path / "catalog") as catalog:
        a = catalog.register(Identifier(pmid="1")).id
        b = catalog.register(Identifier(pmid="2")).id
    review = tmp_path / "review.json"
    review.write_text(json.dumps([
        {"article_id": a, "table_id": "t1", "verdict": "not coordinates", "kind": "crystal", "note": "atoms"},
        {"article_id": a, "table_id": "t2", "verdict": "coordinates"},
        {"article_id": b, "table_id": "t9", "verdict": "not coordinates"},
        {"article_id": "zzzzzzzzzzzz", "table_id": "t1", "verdict": "not coordinates"},
    ]))
    runner = CliRunner()
    out = runner.invoke(app, ["exclude", "--file", str(review), "--config", str(config)])
    assert out.exit_code == 0, out.output
    assert "marked 2 tables on 2 articles" in out.output
    assert "skipped, not an article id" in out.output
    with Catalog.open(tmp_path / "catalog") as catalog:
        marks = catalog.exclusions()
    assert set(marks) == {a, b}
    assert set(marks[a]) == {"t1"} and marks[a]["t1"]["reason"] == "crystal"


def test_an_article_named_on_the_command_line_is_marked_whole(config, tmp_path):
    with Catalog.open(tmp_path / "catalog") as catalog:
        a = catalog.register(Identifier(pmid="1")).id
    runner = CliRunner()
    out = runner.invoke(app, ["exclude", "1", "--note", "lat/long", "--config", str(config)])
    assert out.exit_code == 0, out.output
    assert f"-> article {a}" in out.output
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert set(catalog.exclusions()[a]) == {Catalog.WHOLE_ARTICLE}
    out = runner.invoke(app, ["exclude", "1", "--remove", "--config", str(config)])
    assert out.exit_code == 0 and "withdrew 1 marks" in out.output


def _base_flag(engine):
    from ingestion_workflow.services.upload_models import BaseStudy as DbBaseStudy
    with Session(engine, future=True) as session:
        return session.execute(select(DbBaseStudy.has_coordinates)).scalar_one()


def test_retracting_clears_the_base_study_flag(tmp_path):
    """Upload set has_coordinates and nothing unset it: 287 retracted base studies
    still came back from the coordinate search with no points."""
    service, engine, uploaded = _uploaded(tmp_path)
    assert _base_flag(engine) is True
    service.retract([("slug", uploaded.base_study_id)])
    assert _base_flag(engine) is False


def test_a_reupload_with_no_points_clears_the_flag(tmp_path):
    identifier = Identifier(doi="10.1/abc", pmid="123")
    settings = _settings(tmp_path)
    engine = _engine()
    service = UploadService(settings, SessionFactory(settings, engine=engine))
    _upload_once(service, settings, identifier, _collection(identifier, [_analysis("A", _coords((1, 2, 3)))]), _article_metadata())
    assert _base_flag(engine) is True
    _upload_once(service, settings, identifier, _collection(identifier, [_analysis("A", [])]), _article_metadata())
    assert _base_flag(engine) is False


def test_another_versions_points_keep_the_flag(tmp_path):
    service, engine, uploaded = _uploaded(tmp_path)
    with Session(engine, future=True) as session:
        other = DbStudy(base_study_id=uploaded.base_study_id, source="neurosynth")
        session.add(other); session.flush()
        a = DbAnalysis(study_id=other.id, name="ns"); session.add(a); session.flush()
        from ingestion_workflow.services.upload_models import Point as DbPoint
        session.add(DbPoint(analysis_id=a.id, x=1, y=2, z=3)); session.commit()
    service.retract([("slug", uploaded.base_study_id)])
    assert _base_flag(engine) is True
