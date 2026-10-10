"""`space` fills in what `analyses` could not name, and nothing else."""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import STAGE_ORDER
from ingestion_workflow.pipeline.stages.space import SpaceStage, fill_spaces

TEXT = "## Methods\nMNI coordinates were converted to Talairach space using mni2tal.\n"


def _collection(space, *, point_space=None, footer=""):
    return {
        "slug": "a::t",
        "coordinate_space": space,
        "identifier": None,
        "analyses": [{
            "name": "A > B",
            "table_caption": "Activations",
            "table_footer": footer,
            "coordinates": [{"x": 1.0, "y": 2.0, "z": 3.0, "space": point_space or space}],
        }],
    }


def test_space_runs_between_analyses_and_upload():
    order = list(STAGE_ORDER)
    assert order.index("analyses") < order.index("space") < order.index("upload")


def test_an_unknown_table_takes_the_space_its_article_states():
    filled, summary = fill_spaces({"t1": _collection("OTHER")}, TEXT)
    collection = AnalysisCollection.from_dict(filled["t1"])
    assert collection.coordinate_space.value == "TAL"
    assert [c.space.value for c in collection.analyses[0].coordinates] == ["TAL"]
    assert summary["read"]["t1"]["where"] == "methods"
    assert summary["read"]["t1"]["rule"] == "converted"
    assert summary["unknown"] == 0
    assert summary["tables"] == 1


def test_a_known_table_is_left_alone():
    payload = {"t1": _collection("MNI")}
    filled, summary = fill_spaces(payload, TEXT)
    assert filled == payload
    assert summary["read"] == {}


def test_a_point_that_names_its_own_space_keeps_it():
    filled, _ = fill_spaces({"t1": _collection("OTHER", point_space="MNI")}, TEXT)
    assert filled["t1"]["analyses"][0]["coordinates"][0]["space"] == "MNI"


def test_the_tables_own_footer_outranks_the_methods():
    filled, summary = fill_spaces(
        {"t1": _collection("OTHER", footer="x, y, z: MNI coordinates.")}, TEXT)
    assert filled["t1"]["coordinate_space"] == "MNI"
    assert summary["read"]["t1"]["where"] == "table"


def test_an_article_that_never_says_stays_unknown():
    payload = {"t1": _collection("OTHER")}
    filled, summary = fill_spaces(payload, "## Methods\nWe scanned people.\n")
    assert filled == payload
    assert summary["unknown"] == 1


@pytest.fixture()
def env(tmp_path):
    settings = Settings(
        data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k")
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, tmp_path


def test_the_stage_reads_the_text_of_the_extraction_triage_judged(env):
    settings, catalog, tmp_path = env
    judged, other = tmp_path / "judged.txt", tmp_path / "other.txt"
    judged.write_text(TEXT)
    other.write_text("## Methods\nCoordinates are reported in MNI space.\n")
    ref = catalog.register(Identifier(pmid="123"))
    catalog.record([
        Outcome(article_id=ref.id, stage="extract", source="pubget", fingerprint="ex-1",
                payload={"full_text_path": str(judged)}, summary={}),
        Outcome(article_id=ref.id, stage="extract", source="ace", fingerprint="ex-2",
                payload={"full_text_path": str(other)}, summary={}),
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1",
                payload={}, summary={"source": "pubget", "passed": 2}),
        Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                payload={"t1": _collection("OTHER"), "t2": _collection("MNI")},
                summary={"tables": 2}),
    ])
    ctx = Context(settings, catalog)
    stage = SpaceStage(settings)
    plan = stage.plan(
        ctx, [ref], catalog.artifacts([ref.id], "space"),
        catalog.artifacts([ref.id], "analyses"))
    assert len(plan.pending) == 1

    (outcome,) = list(stage.execute(ctx, plan.pending))
    assert outcome.status is Status.OK
    assert outcome.payload["t1"]["coordinate_space"] == "TAL"
    assert outcome.payload["t2"]["coordinate_space"] == "MNI"
    assert outcome.summary["tables"] == 2

    catalog.record([outcome])
    again = stage.plan(
        ctx, [ref], catalog.artifacts([ref.id], "space"),
        catalog.artifacts([ref.id], "analyses"))
    assert again.fresh == 1 and not again.pending



def test_no_other_extraction_s_text_stands_in_for_the_judged_one(env):
    settings, catalog, tmp_path = env
    other = tmp_path / "other.txt"
    other.write_text(TEXT)
    ref = catalog.register(Identifier(pmid="124"))
    catalog.record([
        Outcome.failure(ref.id, "extract", "pubget", "unreadable", fingerprint="ex-1"),
        Outcome(article_id=ref.id, stage="extract", source="ace", fingerprint="ex-2",
                payload={"full_text_path": str(other)}, summary={}),
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1",
                payload={}, summary={"source": "pubget", "passed": 1}),
        Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                payload={"t1": _collection("OTHER")}, summary={"tables": 1}),
    ])
    ctx = Context(settings, catalog)
    stage = SpaceStage(settings)
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], "space"), catalog.artifacts([ref.id], "analyses"))
    (outcome,) = list(stage.execute(ctx, plan.pending))
    assert outcome.payload["t1"]["coordinate_space"] == "OTHER"

def test_an_article_with_nothing_to_fill_writes_the_analyses_blob_again(env):
    """Content-addressed: the same payload is the same blob, stored once."""
    settings, catalog, _ = env
    ref = catalog.register(Identifier(pmid="456"))
    payload = {"t1": _collection("MNI")}
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="",
                            fingerprint="an-1", payload=payload, summary={"tables": 1})])
    ctx = Context(settings, catalog)
    stage = SpaceStage(settings)
    plan = stage.plan(ctx, [ref], {}, catalog.artifacts([ref.id], "analyses"))
    catalog.record(list(stage.execute(ctx, plan.pending)))
    space, analyses = (catalog.artifact(ref.id, stage, "") for stage in ("space", "analyses"))
    assert space.blob == analyses.blob


def test_an_article_with_nothing_to_fill_never_reads_its_text(env, monkeypatch):
    from ingestion_workflow.pipeline.stages import space

    def unexpected(*args):
        raise AssertionError("read the text of an article with nothing to fill")

    monkeypatch.setattr(space, "_article_text", unexpected)
    settings, catalog, _ = env
    ref = catalog.register(Identifier(pmid="789"))
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                            payload={"t1": _collection("TAL")}, summary={"tables": 1})])
    ctx = Context(settings, catalog)
    stage = SpaceStage(settings)
    plan = stage.plan(ctx, [ref], {}, catalog.artifacts([ref.id], "analyses"))
    (outcome,) = list(stage.execute(ctx, plan.pending))
    assert outcome.status is Status.OK
    assert outcome.payload["t1"]["coordinate_space"] == "TAL"
