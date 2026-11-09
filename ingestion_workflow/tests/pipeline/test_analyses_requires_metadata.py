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
from ingestion_workflow.pipeline.selection import Select, everything, narrow
from ingestion_workflow.pipeline.stage import metadata_digests
from ingestion_workflow.pipeline.stages.analyses import AnalysesStage
from ingestion_workflow.pipeline.stages.triage import TriageStage


@pytest.fixture()
def env(tmp_path):
    # Triage digests the gate into its fingerprint, so a stage built without
    # one cannot plan. Raising then rather than mid-run is the point: a run
    # with no gate should stop before it does any work.
    gate = tmp_path / "gate.joblib"
    gate.write_bytes(b"a fitted gate")
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        coordinate_gate_path=gate,
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
            summary={"digests": metadata_digests({"title": "A title",
                                                  "abstract": "Coordinates are in MNI space."})},
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
                summary={"source": "pubget", "tables": 1, "passed": int(passes),
                         "passed_tables": ["t1"] if passes else []},
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


def test_blocked_until_metadata_has_run(env, evidence):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC1"), evidence)
    _extracted(catalog, ref)

    plan = _plan_triage(settings, catalog, [ref])

    assert plan.pending == []
    assert plan.blocked == 1


def test_runs_once_metadata_has_run(env, evidence):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC2"), evidence)
    _extracted(catalog, ref)
    _metadata(catalog, ref)

    plan = _plan_triage(settings, catalog, [ref])

    assert [w.ref.id for w in plan.pending] == [ref.id]


def test_a_failed_metadata_attempt_still_lets_parsing_run(env, evidence):
    """Many articles have no metadata to find; that must not block parsing."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC3"), evidence)
    _extracted(catalog, ref)
    _metadata(catalog, ref, status=Status.FAILED)

    plan = _plan_triage(settings, catalog, [ref])

    assert [w.ref.id for w in plan.pending] == [ref.id]


def test_metadata_alone_is_not_enough(env, evidence):
    """The extraction is still what gets judged, and then parsed."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC4"), evidence)
    _metadata(catalog, ref)

    plan = _plan_triage(settings, catalog, [ref])

    assert plan.pending == []
    assert plan.blocked == 1


def test_analyses_waits_for_triage(env, evidence):
    """Which tables are worth a call is triage's answer, so analyses cannot
    start guessing before it has one."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC5"), evidence)
    _extracted(catalog, ref)
    _metadata(catalog, ref)

    assert _plan_analyses(settings, catalog, [ref]).blocked == 1

    _triaged(catalog, ref)
    assert [w.ref.id for w in _plan_analyses(settings, catalog, [ref]).pending] == [ref.id]


def test_a_skip_runs_again_once_triage_passes_tables(env, evidence):
    """"No coordinate tables" is a verdict on what triage passed. When a new
    read passes tables, the old skip is stale and analyses runs; until then it
    stands."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC6"), evidence)
    _extracted(catalog, ref)
    _metadata(catalog, ref)
    _triaged(catalog, ref, passes=False)
    work = _plan_analyses(settings, catalog, [ref]).pending[0]
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="",
                            status=Status.SKIPPED, fingerprint=work.fingerprint,
                            summary={"tables": 0, "reason": "no coordinate tables"})])
    assert _plan_analyses(settings, catalog, [ref]).permanent == 1

    # A new read that passes no tables leaves the skip standing (2lrnm23oqj2g: MinerU's
    # read moved the fingerprint, and upload held the article on the "stale" skip).
    catalog.record([Outcome(article_id=ref.id, stage="triage", source="",
                            fingerprint="tr-1b",
                            payload={"source": "pdf",
                                     "tables": [{"table_id": "t1", "passes": False}]},
                            summary={"source": "pdf", "tables": 1, "passed": 0,
                                     "passed_tables": []})])
    assert _plan_analyses(settings, catalog, [ref]).permanent == 1

    catalog.record([Outcome(article_id=ref.id, stage="triage", source="",
                            fingerprint="tr-2",
                            payload={"source": "pubget",
                                     "tables": [{"table_id": "t1", "passes": True}]},
                            summary={"source": "pubget", "tables": 1, "passed": 1,
                                     "passed_tables": ["t1"]})])

    assert [w.ref.id for w in _plan_analyses(settings, catalog, [ref]).pending] == [ref.id]


def test_analyses_go_stale_skip_and_return_as_triage_reads_change(env, evidence):
    """The round trip: OK analyses, then a triage that passes nothing, then one that
    passes t1 again. The gate keeps the article while it holds analyses, the plan
    records the skip, and the skip runs again once tables pass."""
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC7"), evidence)
    _extracted(catalog, ref)
    _metadata(catalog, ref)

    def plan():
        sel = narrow(catalog, everything(catalog), Select.ALL, "analyses", settings=settings)
        return _plan_analyses(settings, catalog, list(sel.refs))

    _triaged(catalog, ref)
    work = plan().pending[0]
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", status=Status.OK,
                            fingerprint=work.fingerprint, summary={"tables": 1})])
    assert plan().fresh == 1

    _triaged(catalog, ref, passes=False)
    work = plan().pending[0]
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="",
                            status=Status.SKIPPED, fingerprint=work.fingerprint,
                            summary={"tables": 0, "reason": "no coordinate tables"})])
    assert plan().pending == []

    _triaged(catalog, ref)
    assert [w.ref.id for w in plan().pending] == [ref.id]
