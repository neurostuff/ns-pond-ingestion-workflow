"""`sync` writes the coordinate parse, then `upload` posts it to neurostore's ingester.

Upload holds back a parse built from a table or prose reading that is stale or failed:
the ingester keys analyses by the parse's keys and deletes or supersedes the ones a new
parse lacks, so such a parse would take live analyses out of neurostore.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Artifact, Catalog, Outcome, Status
from ingestion_workflow.config import Settings, UploadBackend
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.plan import StagePlan, Work
from ingestion_workflow.pipeline.stage import NO_TEXT
from ingestion_workflow.pipeline.stages import (
    LEGACY_UPLOAD_ORDER,
    STAGE_ORDER,
    STAGE_TYPES,
    build,
    stage_order,
    stage_types,
)
from ingestion_workflow.pipeline.stages.analyses import AnalysesStage
from ingestion_workflow.pipeline.stages.resolve import ResolveStage
from ingestion_workflow.pipeline.stages.roles import RolesStage
from ingestion_workflow.pipeline.stages.space import SpaceStage
from ingestion_workflow.pipeline.stages.sync import (
    NOT_IN_NEUROSTORE,
    SyncBeforeUploadStage,
    SyncStage,
)
from ingestion_workflow.pipeline.stages.upload import (
    GATED,
    IngesterUploadStage,
    UploadStage,
    upstream_state,
)
from ingestion_workflow.services import neurostore_ingest
from ingestion_workflow.services.set_roles import ANCHOR_KINDS, COORDINATE_ROLES, Prediction
from ingestion_workflow.services.set_roles.model import CONTEXT_VERSIONS, META_FILE
from test_prose_stages import ARTICLE, _read_prose, _Reader, _record_upstream


def _meta(path: Path, origin: str) -> Path:
    """A role model's metadata, which is all the plan reads of it."""
    path.mkdir(parents=True, exist_ok=True)
    (path / META_FILE).write_text(json.dumps({
        "name": f"set-roles-{origin}", "version": "1", "roles": list(COORDINATE_ROLES),
        "anchor_kinds": list(ANCHOR_KINDS), "origin": origin,
        "context_version": CONTEXT_VERSIONS[origin]}))
    return path


class _ByName:
    """A role classifier: a set named from `others` is `other`, the rest are results."""

    source = "by-name@1"

    def __init__(self, others=()):
        self.others = tuple(others)

    def predict(self, texts):
        out = []
        for text in texts:
            role = "other" if any(f"[NAME] {o}" in text for o in self.others) else "result"
            out.append(Prediction(0.99, {r: float(r == role) for r in COORDINATE_ROLES}, {}, 0.0))
        return out


class _Ingester:
    """Stands in for neurostore's ingester: reads the parse the way the client does."""

    def __init__(self, base_study_id):
        self.base_study_id = base_study_id
        self.sent = []

    def upload(self, study_dir):
        payload, checks = neurostore_ingest.build_payload(Path(study_dir))
        self.sent.append((Path(study_dir), payload))
        parse_id = payload["coordinate_parse"]["parse_id"]
        record = {"status": "uploaded", "study_id": Path(study_dir).name, "parse_id": parse_id,
                  "checks": checks, "dry_run": False,
                  "summary": {"kind": "parse", "base_study_id": self.base_study_id,
                              "parse_id": parse_id, "study_id": "S1"}}
        return {**record, "last_uploaded": {"x": record}, "history": [record]}

    def close(self):
        pass


class _Refuses:
    def upload(self, study_dir):
        raise AssertionError(f"a held-back parse was sent: {study_dir}")

    def close(self):
        pass


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


def _roles(settings, monkeypatch, classifier):
    stage = RolesStage(settings)
    monkeypatch.setattr(stage, "classifier", lambda origin: classifier)
    return stage


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond",
                        prose_model="nu-prose", llm_api_key="x",
                        role_model_table=_meta(tmp_path / "table", "table"),
                        role_model_prose=_meta(tmp_path / "text", "text"))
    path = tmp_path / "article.xml"
    path.write_text(ARTICLE)
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, path


def _prose_article(env, monkeypatch, pmid, neurostore=None):
    """An article read through prose, resolve, roles and space; triage passed no table."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid=pmid, neurostore=neurostore))
    _record_upstream(catalog, ref, path)
    catalog.record([Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1",
                            summary={"source": "pubget", "tables": 0, "passed": 0})])
    ctx = Context(settings, catalog)
    _read_prose(ctx, catalog, ref, _Reader())
    _run(ResolveStage(settings), ctx, catalog, ref)
    _run(_roles(settings, monkeypatch, _ByName()), ctx, catalog, ref)
    _, (spaced,) = _run(SpaceStage(settings), ctx, catalog, ref)
    assert spaced.status is Status.OK, spaced.error
    return ctx, ref


def test_sync_runs_before_upload_and_the_legacy_upload_runs_the_other_way_round():
    assert STAGE_ORDER[-2:] == ("sync", "upload")
    assert LEGACY_UPLOAD_ORDER[-2:] == ("upload", "sync")
    default = Settings()
    legacy = Settings(upload_backend="legacy")
    assert default.upload_backend is UploadBackend.INGESTER
    assert [type(s) for s in build(["upload", "sync"], default)] == [
        SyncBeforeUploadStage, IngesterUploadStage]
    assert [type(s) for s in build(["upload", "sync"], legacy)] == [UploadStage, SyncStage]
    for settings in (default, legacy):
        order, types = stage_order(settings), stage_types(settings)
        for name in order:
            stage = types[name](settings) if name in ("sync", "upload", "space", "roles") else None
            required = stage.requires if stage is not None else types[name].requires
            assert required is None or order.index(required) < order.index(name), name
        # Selection and the funnel ask the registry's classes, so they follow the backend too.
        for name in ("sync", "upload"):
            assert STAGE_TYPES[name].upstream_for(settings)[0] == types[name](settings).requires
    assert SyncBeforeUploadStage(default).requires == "space"
    assert IngesterUploadStage(default).requires == "sync"


def test_sync_writes_the_parse_with_no_upload_and_upload_sends_what_it_wrote(env, monkeypatch):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "21", "BS21")
    sync, upload = build(["sync", "upload"], settings)
    assert catalog.artifacts([ref.id], "upload") == {}
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert synced.status is Status.OK, synced.error
    written = settings.ns_pond_root / "BS21" / "parse" / "coordinate_parse.json"
    assert json.loads(written.read_text())["header"]["identifiers"]["neurostore_base_study_id"] \
        == "BS21"
    assert (settings.ns_pond_root / "BS21" / "stage1" / "analyses.json").exists()

    fake = _Ingester("BS21")
    monkeypatch.setattr(upload, "ingester", lambda: fake)
    _, (done,) = _run(upload, ctx, catalog, ref)
    upload.finish()
    assert done.status is Status.OK, done.error
    ((sent_from, payload),) = fake.sent
    assert sent_from == settings.ns_pond_root / "BS21"
    assert payload["coordinate_parse"] == json.loads(written.read_text())
    summary = done.summary
    assert (summary["backend"], summary["dry_run"], summary["gate"]) == ("ingester", False, None)
    assert summary["parse_id"] == synced.summary["parse_id"] == summary["ingester"]["parse_id"]
    assert summary["ingester"]["summary"]["kind"] == "parse"
    assert "history" not in summary["ingester"] and "last_uploaded" not in summary["ingester"]
    assert summary["analyses"] == synced.summary["analyses"] == 1
    # Uploaded: fresh until sync writes another parse.
    plan, outcomes = _run(upload, ctx, catalog, ref)
    assert (plan.fresh, outcomes) == (1, [])


def test_an_article_neurostore_holds_no_base_study_for_waits_for_sync(env, monkeypatch):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "23")
    sync = SyncBeforeUploadStage(settings)
    plan, outcomes = _run(sync, ctx, catalog, ref)
    assert (plan.blocked, plan.reasons, outcomes) == (1, {NOT_IN_NEUROSTORE: 1}, [])
    # A base study an earlier upload wrote to names it.
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-0",
                            summary={"base_study_id": "BS23", "analyses": 1})])
    plan, (synced,) = _run(sync, ctx, catalog, ref)
    assert synced.summary["base_study_id"] == "BS23"


def _dry(settings):
    return settings.model_copy(update={"dry_run": True})


def _dry_lines(settings):
    runs = Path(settings.data_root) / neurostore_ingest.DRY_RUN_DIR / "upload_runs"
    (log,) = runs.glob("*.jsonl")
    return [json.loads(line) for line in log.read_text().splitlines()]


def test_a_dry_run_writes_only_under_its_own_root_and_records_nothing(env, monkeypatch):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "24", "BS24")
    _run(SyncBeforeUploadStage(settings), ctx, catalog, ref)
    dry = _dry(settings)
    upload = IngesterUploadStage(dry)
    _, outcomes = _run(upload, Context(dry, catalog), catalog, ref)
    upload.finish()
    assert outcomes == []
    assert catalog.artifacts([ref.id], "upload") == {}
    assert not (settings.ns_pond_root / "BS24" / "upload").exists()
    root = Path(settings.data_root) / neurostore_ingest.DRY_RUN_DIR
    assert (root / "BS24" / neurostore_ingest.PAYLOAD_NAME).exists()
    (line,) = _dry_lines(settings)
    assert (line["article_id"], line["status"], line["backend"], line["dry_run"],
            line["gate"]) == (ref.id, "dry_run", "ingester", True, None)
    assert line["ingester"]["checks"][:2] == ["model", "check_paper"]


def test_a_dry_run_of_a_missing_parse_never_reaches_the_upload_history(env, monkeypatch):
    """A missing (or invalid) parse in a dry run is the dry run's record, not the article's."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "25", "BS25")
    _run(SyncBeforeUploadStage(settings), ctx, catalog, ref)
    for name in ("coordinate_parse.json", "parsed_paper.json"):
        (settings.ns_pond_root / "BS25" / "parse" / name).unlink()
    dry = _dry(settings)
    upload = IngesterUploadStage(dry)
    _, outcomes = _run(upload, Context(dry, catalog), catalog, ref)
    assert outcomes == [] and catalog.artifacts([ref.id], "upload") == {}
    assert list(settings.ns_pond_root.rglob(neurostore_ingest.ARTIFACT_NAME)) == []
    (line,) = _dry_lines(settings)
    assert line["status"] == "missing" and line["error"].startswith("missing")


#: icafz5gtrxbb as the catalog held it on 2026-10-11: two tables, 12 sets, read on 2026-10-02
#: from a triage that ran again on 2026-10-05, so its table analyses are stale. Against
#: the re-extracted text no row prints the points, and its parse holds none of the 12
#: sets that the last upload sent.
ICAFZ5_TABLES = {
    "2": ["Source 1 (bootstrap)", "Source 2 (bootstrap)", "Source 1 (average)",
          "Source 2 (average)"],
    "html-table-2": ["Right thumb", "Right index", "Right middle", "Right small",
                     "Left thumb", "Left index", "Left middle", "Left small"],
}


def _icafz5_payload():
    return {table_id: {"slug": f"s::{table_id}", "coordinate_space": "OTHER", "identifier": None,
                       "analyses": [{"name": name, "table_caption": "", "table_footer": "",
                                     "metadata": {}, "coordinates": [
                                         {"x": 10.0 + i, "y": -20.0, "z": 50.0, "space": "OTHER"},
                                         {"x": 12.0 + i, "y": -22.0, "z": 52.0, "space": "OTHER"}]}
                                    for i, name in enumerate(names)]}
            for table_id, names in ICAFZ5_TABLES.items()}


def _icafz5(tmp_path, monkeypatch, catalog, *, analyses_fresh=False):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond",
                        role_model_table=_meta(tmp_path / "table", "table"))
    ref = catalog.register(Identifier(pmid="16139528", neurostore="NTKEuAF4Autf"))
    catalog.record([Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1005",
                            summary={"source": "ace", "tables": 3, "passed": 2})])
    read_from = catalog.artifact(ref.id, "triage", "") if analyses_fresh else Artifact(
        ref.id, "triage", fingerprint="tr-1002")
    catalog.record([
        Outcome(article_id=ref.id, stage="analyses", source="",
                fingerprint=AnalysesStage(settings).fingerprint_for(read_from),
                payload=_icafz5_payload(), summary={"tables": 2, "coordinates": 24}),
        # The legacy upload of 2026-10-03, which sent all 12.
        Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-legacy",
                summary={"base_study_id": "NTKEuAF4Autf", "study_id": "ETMWa8HbYhqa",
                         "analyses": 12}),
    ])
    ctx = Context(settings, catalog)
    _, (roled,) = _run(_roles(settings, monkeypatch, _ByName(["Source"])), ctx, catalog, ref)
    assert roled.summary["sets"] == 12 and roled.summary["roles"] == {"other": 4, "result": 8}
    _run(SpaceStage(settings), ctx, catalog, ref)
    _sync_as_written(ctx, catalog, ref, analyses=0)
    return settings, ctx, ref


def _sync_as_written(ctx, catalog, ref, analyses):
    """The sync record that catalog held: fresh against space, its parse holding `analyses`."""
    sync = SyncBeforeUploadStage(ctx.settings)
    plan = sync.plan(ctx, [ref], catalog.artifacts([ref.id], "sync"),
                     catalog.artifacts([ref.id], "space"))
    (work,) = plan.pending
    omitted = [f"{t}: {n!r}: no row of the table prints its points"
               for t, names in ICAFZ5_TABLES.items() for n in names] if not analyses else []
    catalog.record([Outcome(
        article_id=ref.id, stage="sync", source="", status=Status.OK, fingerprint=work.fingerprint,
        summary={"base_study_id": "NTKEuAF4Autf",
                 "path": str(ctx.settings.ns_pond_root / "NTKEuAF4Autf"),
                 "parse_id": f"parse-{analyses}", "analyses": analyses,
                 **({"parse_omitted": omitted} if omitted else {})})])


def test_a_parse_made_from_stale_table_analyses_is_held_back(tmp_path, monkeypatch):
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog)
        upload = IngesterUploadStage(settings)
        monkeypatch.setattr(upload, "ingester", lambda: _Refuses())
        _, (held,) = _run(upload, ctx, catalog, ref)
        assert held.status is Status.FAILED
        assert held.error == (f"{GATED}: analyses stale; the parse holds 0 analyses, "
                              "the last upload 12 (12 missing)")
        gate = held.summary["gate"]
        assert gate["upstream"] == {"analyses": "stale"}
        assert (gate["parse_analyses"], gate["last_uploaded_analyses"], gate["missing"],
                gate["sets"]) == (0, 12, 12, 12)
        assert (held.summary["backend"], held.summary["dry_run"]) == ("ingester", False)
        # The last upload is carried, so the count holds on the next run, which records
        # nothing new and sends nothing.
        assert held.summary["last_upload"]["analyses"] == 12
        plan, outcomes = _run(upload, ctx, catalog, ref)
        assert (plan.blocked, plan.reasons, outcomes) == (1, {GATED: 1}, [])
        # A dry run holds it back the same way, in its own record.
        dry = _dry(settings)
        dry_upload = IngesterUploadStage(dry)
        monkeypatch.setattr(dry_upload, "ingester", lambda: _Refuses())
        _, outcomes = _run(dry_upload, Context(dry, catalog), catalog, ref)
        assert outcomes == []
        (line,) = _dry_lines(settings)
        assert (line["status"], line["error"]) == ("held back", held.error)
        assert catalog.artifact(ref.id, "upload", "").error == held.error


def test_once_the_tables_are_read_again_the_parse_goes_up(tmp_path, monkeypatch):
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog)
        upload = IngesterUploadStage(settings)
        monkeypatch.setattr(upload, "ingester", lambda: _Refuses())
        _run(upload, ctx, catalog, ref)
        # The analyses read again from today's triage; everything after them follows.
        triage = catalog.artifact(ref.id, "triage", "")
        catalog.record([Outcome(article_id=ref.id, stage="analyses", source="",
                                fingerprint=AnalysesStage(settings).fingerprint_for(triage),
                                payload=_icafz5_payload(), summary={"tables": 2})])
        _run(_roles(settings, monkeypatch, _ByName(["Source"])), ctx, catalog, ref)
        _run(SpaceStage(settings), ctx, catalog, ref)
        _sync_as_written(ctx, catalog, ref, analyses=12)
        assert upload.held_back(ctx, [ref], {ref.id: catalog.artifact(ref.id, "sync", "")},
                                catalog.artifacts([ref.id], "upload")) == {}
        sent = []
        monkeypatch.setattr(upload, "ingester", lambda: type("I", (), {
            "upload": lambda self, d: sent.append(d) or {
                "status": "uploaded", "summary": {"base_study_id": "NTKEuAF4Autf"}},
            "close": lambda self: None})())
        _, (done,) = _run(upload, ctx, catalog, ref)
        assert done.status is Status.OK and done.summary["gate"] is None
        assert sent == [settings.ns_pond_root / "NTKEuAF4Autf"]


def test_a_person_can_leave_the_tables_out_with_a_reason(tmp_path, monkeypatch):
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog)
        upload = IngesterUploadStage(settings)
        monkeypatch.setattr(upload, "ingester", lambda: _Refuses())
        _run(upload, ctx, catalog, ref)
        catalog.exclude([(ref.id, "source:tables", "MEG dipole fits, not results", "")])
        # The mark re-syncs the article, and its upload is planned again.
        _sync_as_written(ctx, catalog, ref, analyses=0)
        sent = []
        monkeypatch.setattr(upload, "ingester", lambda: type("I", (), {
            "upload": lambda self, d: sent.append(d) or {
                "status": "uploaded", "summary": {"base_study_id": "NTKEuAF4Autf"}},
            "close": lambda self: None})())
        _, (done,) = _run(upload, ctx, catalog, ref)
        assert done.status is Status.OK and len(sent) == 1
        assert done.summary["gate"]["excused"] == {"analyses": "MEG dipole fits, not results"}


def test_a_failed_table_reading_holds_back_a_parse_with_only_the_prose(env, monkeypatch):
    """The tables failed in the model, so the parse holds the prose's analyses alone."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "26", "BS26")
    _run(SyncBeforeUploadStage(settings), ctx, catalog, ref)
    catalog.record([
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-2",
                summary={"source": "pubget", "tables": 1, "passed": 1}),
        Outcome.failure(ref.id, "analyses", "", "UnreadableAnswer: Expecting ',' delimiter: "
                        "line 1 column 1481 (char 1480)",
                        fingerprint=AnalysesStage(settings).fingerprint_for(
                            Artifact(ref.id, "triage", fingerprint="tr-2"))),
    ])
    upload = IngesterUploadStage(settings)
    monkeypatch.setattr(upload, "ingester", lambda: _Refuses())
    _, (held,) = _run(upload, ctx, catalog, ref)
    assert held.error == (
        f"{GATED}: analyses failed: UnreadableAnswer: Expecting ',' delimiter: line 1 column "
        "1481 (char 1480); the parse holds 1 analyses, and nothing was uploaded before")


@pytest.mark.parametrize("plan, existing, expected", [
    (StagePlan("x", fresh=1), None, None),
    (StagePlan("x", skipped=1), None, None),
    (StagePlan("x", blocked=1, reasons={"its resolve was made from other prose": 1}),
     Artifact("a", "roles", status=Status.OK), "blocked (its resolve was made from other prose)"),
    (StagePlan("x", pending=[Work(ref=None, source="", fingerprint="f")]),
     Artifact("a", "x", status=Status.FAILED, error="ReadTimeout"), "failed: ReadTimeout"),
    (StagePlan("x", pending=[Work(ref=None, source="", fingerprint="f")]),
     Artifact("a", "x", status=Status.OK), "stale"),
    (StagePlan("x", pending=[Work(ref=None, source="", fingerprint=NO_TEXT)]),
     Artifact("a", "x", status=Status.OK), "taken back"),
    (StagePlan("x", permanent=1), Artifact("a", "x", status=Status.SKIPPED), None),
    (StagePlan("x", permanent=1), Artifact("a", "x", status=Status.PERMANENT, error="refused"),
     "failed, not retried: refused"),
])
def test_what_each_plan_says_of_a_stage_s_reading(plan, existing, expected):
    assert upstream_state("roles", plan, existing) == expected


@pytest.mark.parametrize("passed, expected", [(0, None), (2, "not run"), (None, "not run")])
def test_tables_never_read_are_behind_unless_triage_passed_none(passed, expected):
    plan = StagePlan("analyses", pending=[Work(ref=None, source="", fingerprint="an-1")])
    triage = Artifact("a", "triage", status=Status.OK,
                      summary={} if passed is None else {"passed": passed})
    assert upstream_state("analyses", plan, None, triage) == expected
    assert upstream_state("prose", plan, None, triage) == "not run"


def test_a_stop_records_the_paper_and_stops_before_the_next_batch(env, monkeypatch):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "27", "BS27")
    _run(SyncBeforeUploadStage(settings), ctx, catalog, ref)
    upload = IngesterUploadStage(settings)

    class Unauthorized:
        def upload(self, study_dir):
            raise neurostore_ingest.NeurostoreAuthError("401 from the ingester")

        def close(self):
            pass

    monkeypatch.setattr(upload, "ingester", lambda: Unauthorized())
    _, (stopped,) = _run(upload, ctx, catalog, ref)
    assert stopped.status is Status.FAILED and "401 from the ingester" in stopped.error
    with pytest.raises(neurostore_ingest.NeurostoreAuthError):
        list(upload.execute(ctx, [Work(ref=ref, source="", fingerprint="x")]))


def test_the_legacy_upload_refuses_a_dry_run_before_it_connects(tmp_path, monkeypatch):
    from ingestion_workflow.services import db

    monkeypatch.setattr(db, "SSHTunnel", lambda *a, **k: pytest.fail("connected"))
    stage = UploadStage(Settings(upload_source="nuextract-v21", dry_run=True,
                                 data_root=tmp_path / "d"))
    with Catalog.open(tmp_path / "k") as catalog:
        ref = catalog.register(Identifier(pmid="9"))
        with pytest.raises(ValueError, match="has no dry run"):
            list(stage.execute(Context(stage.settings, catalog),
                               [Work(ref=ref, source="", fingerprint="f")]))
