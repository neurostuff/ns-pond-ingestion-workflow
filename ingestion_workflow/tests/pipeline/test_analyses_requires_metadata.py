"""Analyses waits for metadata to have been attempted, not to have succeeded.

The prompt carries the article's title and abstract. An article whose metadata
stage has not run yet would be parsed without them, and the result cached -- so
a later metadata run could not improve it. Attempted is the bar: plenty of
articles have no metadata to find, and those should still be parsed.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages.analyses import AnalysesStage


@pytest.fixture()
def env(tmp_path):
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
    )
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog


def _extracted(catalog, ref) -> None:
    catalog.record(
        [
            Outcome(
                article_id=ref.id,
                stage="extract",
                source="pubget",
                fingerprint="ex-1",
                payload={"tables": []},
                summary={"tables": 1, "tables_with_coordinates": 1},
            )
        ]
    )


def _metadata(catalog, ref, *, status: Status = Status.OK) -> None:
    if status is Status.OK:
        outcome = Outcome(
            article_id=ref.id,
            stage="metadata",
            source="",
            fingerprint="md-1",
            payload={"title": "A title"},
            summary={},
        )
    else:
        outcome = Outcome.failure(ref.id, "metadata", "", "nothing found", fingerprint="md-1")
    catalog.record([outcome])


def _plan(settings, catalog, refs):
    stage = AnalysesStage(settings)
    ctx = Context(settings, catalog)
    ids = [r.id for r in refs]
    return stage.plan(
        ctx,
        refs,
        catalog.artifacts(ids, "analyses"),
        catalog.artifacts(ids, "extract"),
    )


def test_blocked_until_metadata_has_run(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC1"))
    _extracted(catalog, ref)

    plan = _plan(settings, catalog, [ref])

    assert plan.pending == []
    assert plan.blocked == 1


def test_runs_once_metadata_has_run(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC2"))
    _extracted(catalog, ref)
    _metadata(catalog, ref)

    plan = _plan(settings, catalog, [ref])

    assert [w.ref.id for w in plan.pending] == [ref.id]


def test_a_failed_metadata_attempt_still_lets_analyses_run(env):
    """Many articles have no metadata to find; that must not block parsing."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC3"))
    _extracted(catalog, ref)
    _metadata(catalog, ref, status=Status.FAILED)

    plan = _plan(settings, catalog, [ref])

    assert [w.ref.id for w in plan.pending] == [ref.id]


def test_metadata_alone_is_not_enough(env):
    """The extraction is still what analyses parses."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC4"))
    _metadata(catalog, ref)

    plan = _plan(settings, catalog, [ref])

    assert plan.pending == []
    assert plan.blocked == 1
