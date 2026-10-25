"""A person's "not coordinates" verdict, from the catalog to neurostore and the corpus."""

from __future__ import annotations

import json

import pytest
from ingestion_workflow.catalog import WHOLE_ARTICLE, Catalog
from ingestion_workflow.cli.main import app
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import exclusions as excl
from ingestion_workflow.pipeline.stages.sync import SyncStage
from ingestion_workflow.services.nspond import write_corpus_manifest
from typer.testing import CliRunner


# -- the catalog -------------------------------------------------------------


def test_a_mark_is_recorded_replaced_and_withdrawn(tmp_path, evidence):
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
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


def test_an_old_catalog_gains_the_table_when_opened(tmp_path, evidence):
    """The table is created with IF NOT EXISTS, so no migration is needed."""
    Catalog.open(tmp_path / "cat", evidence=evidence).close()
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
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
        summary = {}

    stage = SyncStage(Settings())
    plain = stage.fingerprint_for(Upstream(), "bs-1")
    assert stage.fingerprint_for(Upstream(), "bs-1", {}) == plain
    assert stage.fingerprint_for(Upstream(), "bs-1", None) == plain
    marked = stage.fingerprint_for(Upstream(), "bs-1", {"t1": {}})
    assert marked != plain
    assert stage.fingerprint_for(Upstream(), "bs-1", {"t1": {"note": "different note"}}) == marked
    assert stage.fingerprint_for(Upstream(), "bs-1", {"t1": {}, "t2": {}}) != marked


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
        "log_to_file": False, "show_progress": False, "pubmed_email": "replay@example.com",
    }), encoding="utf-8")
    return path


def test_a_review_file_marks_only_its_non_coordinate_tables(config, tmp_path, evidence):
    with Catalog.open(tmp_path / "catalog", evidence=evidence) as catalog:
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
    with Catalog.open(tmp_path / "catalog", evidence=evidence) as catalog:
        marks = catalog.exclusions()
    assert set(marks) == {a, b}
    assert set(marks[a]) == {"t1"} and marks[a]["t1"]["reason"] == "crystal"


def test_an_article_named_on_the_command_line_is_marked_whole(config, tmp_path, evidence):
    with Catalog.open(tmp_path / "catalog", evidence=evidence) as catalog:
        a = catalog.register(Identifier(pmid="1")).id
    runner = CliRunner()
    out = runner.invoke(app, ["exclude", "1", "--note", "lat/long", "--config", str(config)])
    assert out.exit_code == 0, out.output
    assert f"-> article {a}" in out.output
    with Catalog.open(tmp_path / "catalog", evidence=evidence) as catalog:
        assert set(catalog.exclusions()[a]) == {WHOLE_ARTICLE}
    out = runner.invoke(app, ["exclude", "1", "--remove", "--config", str(config)])
    assert out.exit_code == 0 and "withdrew 1 marks" in out.output


