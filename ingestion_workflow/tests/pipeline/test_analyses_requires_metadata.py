"""Parsing waits for metadata to have been attempted, not to have succeeded.

The prompt carries the article's title and abstract. An article whose metadata
stage has not run yet would be parsed without them, and the result cached -- so
a later metadata run could not improve it. Attempted is the bar: plenty of
articles have no metadata to find, and those should still be parsed.

The bar now sits in `triage` rather than in `analyses`. Triage requires
metadata, analyses requires triage, so the guarantee is unchanged and is
enforced one stage earlier -- which also means the gate's own verdicts are
made with the abstract in hand.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages.analyses import AnalysesStage
from ingestion_workflow.pipeline.stages.triage import TriageStage


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
            payload={"title": "A title", "abstract": "Coordinates are in MNI space."},
            summary={},
        )
    else:
        outcome = Outcome.failure(ref.id, "metadata", "", "nothing found", fingerprint="md-1")
    catalog.record([outcome])


def _triaged(catalog, ref, *, passes: bool = True) -> None:
    catalog.record(
        [
            Outcome(
                article_id=ref.id,
                stage="triage",
                source="",
                fingerprint="tr-1",
                payload={"source": "pubget",
                         "tables": [{"table_id": "t1", "passes": passes}]},
                summary={"tables": 1, "passed": int(passes)},
            )
        ]
    )


def _plan_triage(settings, catalog, refs):
    stage = TriageStage(settings)
    ctx = Context(settings, catalog)
    ids = [r.id for r in refs]
    return stage.plan(ctx, refs, catalog.artifacts(ids, "triage"),
                      catalog.artifacts(ids, "metadata"))


def _plan_analyses(settings, catalog, refs):
    stage = AnalysesStage(settings)
    ctx = Context(settings, catalog)
    ids = [r.id for r in refs]
    return stage.plan(ctx, refs, catalog.artifacts(ids, "analyses"),
                      catalog.artifacts(ids, "triage"))


def test_blocked_until_metadata_has_run(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC1"))
    _extracted(catalog, ref)

    plan = _plan_triage(settings, catalog, [ref])

    assert plan.pending == []
    assert plan.blocked == 1


def test_runs_once_metadata_has_run(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC2"))
    _extracted(catalog, ref)
    _metadata(catalog, ref)

    plan = _plan_triage(settings, catalog, [ref])

    assert [w.ref.id for w in plan.pending] == [ref.id]


def test_a_failed_metadata_attempt_still_lets_parsing_run(env):
    """Many articles have no metadata to find; that must not block parsing."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC3"))
    _extracted(catalog, ref)
    _metadata(catalog, ref, status=Status.FAILED)

    plan = _plan_triage(settings, catalog, [ref])

    assert [w.ref.id for w in plan.pending] == [ref.id]


def test_metadata_alone_is_not_enough(env):
    """The extraction is still what gets judged, and then parsed."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC4"))
    _metadata(catalog, ref)

    plan = _plan_triage(settings, catalog, [ref])

    assert plan.pending == []
    assert plan.blocked == 1


def test_analyses_waits_for_triage(env):
    """Which tables are worth a call is triage's answer, so analyses cannot
    start guessing before it has one."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC5"))
    _extracted(catalog, ref)
    _metadata(catalog, ref)

    assert _plan_analyses(settings, catalog, [ref]).blocked == 1

    _triaged(catalog, ref)
    assert [w.ref.id for w in _plan_analyses(settings, catalog, [ref]).pending] == [ref.id]
