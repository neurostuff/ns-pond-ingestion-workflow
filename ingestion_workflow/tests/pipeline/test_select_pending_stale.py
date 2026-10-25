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
def world(tmp_path, request, evidence):
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        download_sources=["pdf"],
        retry_after_hours=getattr(request, "param", 0),
    )
    stage = ExtractStage(settings)
    ids = {}
    with Catalog.open(settings.catalog_root, evidence=evidence) as catalog:
        for name in ("fresh", "stale", "permanent", "transient", "undone", "never"):
            ref = catalog.register(Identifier(pmid=f"9{len(ids)}"))
            ids[name] = ref.id
            if name == "never":
                continue
            catalog.record([Outcome(article_id=ref.id, stage="download", source="pdf",
                                    fingerprint="dl", payload={"files": []})])
            download = catalog.artifact(ref.id, "download", "pdf")
            if name == "undone":
                continue
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


def test_never_run_with_a_download_is_selected(world):
    assert "undone" in selected(world)


def test_downloadless_article_is_blocked_not_selected(world):
    assert "never" not in selected(world)


def test_transient_failure_past_max_attempts_is_not(world):
    catalog, settings, ids = world
    for _ in range(settings.max_attempts):
        catalog.record([Outcome.failure(ids["transient"], "extract", "pdf", "boom")])
    assert "transient" not in selected(world)


def test_transient_failure_is_selected(world):
    assert "transient" in selected(world)


@pytest.mark.parametrize("world", [24], indirect=True)
def test_transient_failure_still_backing_off_is_not(world):
    assert "transient" not in selected(world)


def test_the_rest_of_the_set(world):
    # An article extract has no download for is blocked, not pending.
    assert selected(world) == {"stale", "transient", "undone"}


def test_refresh_still_reaches_fresh_work(world):
    assert "fresh" in selected(world, refresh=["extract"])


def test_only_the_named_stages_are_planned(world):
    catalog, settings, ids = world
    got = narrow(catalog, everything(catalog), Select.PENDING, None, settings=settings,
                 stages=["download", "extract"])
    only_extract = narrow(catalog, everything(catalog), Select.PENDING, None,
                          settings=settings, stages=["extract"])
    assert {r.id for r in only_extract.refs} <= {r.id for r in got.refs}
    assert ids["never"] in {r.id for r in got.refs}      # download is still to do
    assert ids["never"] not in {r.id for r in only_extract.refs}


def test_pending_needs_settings(world):
    catalog, _, _ = world
    with pytest.raises(ValueError, match="needs settings"):
        narrow(catalog, everything(catalog), Select.PENDING, "extract")
