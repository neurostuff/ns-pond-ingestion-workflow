"""`--select pending` means whatever a stage would do something about.

Fresh finished work is left alone; work that is finished but stale (its inputs
or the stage's code changed), never run, or failed in a way worth retrying is
selected; permanent failures are not.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Select, everything, narrow
from ingestion_workflow.pipeline.stages import ExtractStage


@pytest.fixture()
def world(tmp_path, request):
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        download_sources=["pdf"],
        retry_after_hours=getattr(request, "param", 0),
    )
    stage = ExtractStage(settings)
    ids = {}
    with Catalog.open(settings.catalog_root) as catalog:
        for name in ("fresh", "stale", "permanent", "transient", "never"):
            ref = catalog.register(Identifier(pmid=f"9{len(ids)}"))
            ids[name] = ref.id
            if name == "never":
                continue
            catalog.record([Outcome(article_id=ref.id, stage="download", source="pdf",
                                    fingerprint="dl", payload={"files": []})])
            download = catalog.artifact(ref.id, "download", "pdf")
            if name in ("fresh", "stale"):
                fp = stage.fingerprint_for("pdf", download)
                catalog.record([Outcome(
                    article_id=ref.id, stage="extract", source="pdf",
                    fingerprint=fp if name == "fresh" else "built-by-older-code",
                    payload={"tables": []})])
            else:
                catalog.record([Outcome.failure(
                    ref.id, "extract", "pdf", "boom", permanent=name == "permanent")])
        yield catalog, settings, ids


def selected(world, mode=Select.PENDING, **kwargs):
    catalog, settings, ids = world
    got = narrow(catalog, everything(catalog), mode, "extract", settings=settings, **kwargs)
    by_id = {v: k for k, v in ids.items()}
    return {by_id[ref.id] for ref in got.refs}


def test_stale_ok_is_selected(world):
    assert "stale" in selected(world)


def test_fresh_ok_is_not(world):
    assert "fresh" not in selected(world)


def test_permanent_failure_is_not(world):
    assert "permanent" not in selected(world)


def test_transient_failure_is_selected(world):
    assert "transient" in selected(world)


@pytest.mark.parametrize("world", [24], indirect=True)
def test_transient_failure_still_backing_off_is_not(world):
    assert "transient" not in selected(world)


def test_the_rest_of_the_set(world):
    # An article extract has no download for is blocked, not pending.
    assert selected(world) == {"stale", "transient"}


def test_refresh_still_reaches_fresh_work(world):
    assert "fresh" in selected(world, refresh=["extract"])
