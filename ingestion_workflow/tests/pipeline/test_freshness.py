"""Freshness is the whole cache policy: these pin it down."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from ingestion_workflow.catalog import Artifact, Catalog, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.pipeline import Context


@pytest.fixture()
def ctx(tmp_path):
    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        catalog_root=tmp_path / "cat",
        ns_pond_root=tmp_path / "pond",
    )
    with Catalog.open(settings.catalog_root) as catalog:
        yield Context(settings, catalog, max_attempts=3, retry_after=timedelta(hours=24))


def artifact(**kwargs) -> Artifact:
    base = dict(article_id="a", stage="extract", source="pubget", fingerprint="fp1")
    base.update(kwargs)
    return Artifact(**base)


def test_matching_fingerprint_is_fresh(ctx):
    assert ctx.is_fresh(artifact(), "fp1")


def test_changed_fingerprint_is_stale(ctx):
    assert not ctx.is_fresh(artifact(), "fp2")


def test_missing_artifact_is_not_fresh(ctx):
    assert not ctx.is_fresh(None, "fp1")


def test_failed_artifact_is_never_fresh(ctx):
    assert not ctx.is_fresh(artifact(status=Status.FAILED), "fp1")


def test_refresh_overrides_a_matching_fingerprint(tmp_path):
    settings = Settings(catalog_root=tmp_path / "c", data_root=tmp_path / "d")
    with Catalog.open(settings.catalog_root) as catalog:
        ctx = Context(settings, catalog, refresh=["extract"])
        assert not ctx.is_fresh(artifact(), "fp1")
        assert ctx.is_fresh(artifact(stage="download"), "fp1")


def test_a_vanished_blob_makes_an_artifact_stale(ctx):
    assert not ctx.is_fresh(artifact(blob="deadbeef"), "fp1")


def test_permanent_failures_are_not_retried(ctx):
    assert not ctx.should_attempt(artifact(status=Status.PERMANENT), 0, None, "extract")


def test_transient_failures_are_retried_until_the_cap(ctx):
    failed = artifact(status=Status.FAILED)
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    assert ctx.should_attempt(failed, 1, old, "extract")
    assert not ctx.should_attempt(failed, 3, old, "extract")


def test_a_recent_failure_waits_for_the_backoff(ctx):
    failed = artifact(status=Status.FAILED)
    just_now = datetime.now(timezone.utc).isoformat()
    assert not ctx.should_attempt(failed, 1, just_now, "extract")


def test_refresh_reaches_even_permanent_failures(tmp_path):
    settings = Settings(catalog_root=tmp_path / "c", data_root=tmp_path / "d")
    with Catalog.open(settings.catalog_root) as catalog:
        ctx = Context(settings, catalog, refresh=["extract"])
        assert ctx.should_attempt(artifact(status=Status.PERMANENT), 9, None, "extract")
