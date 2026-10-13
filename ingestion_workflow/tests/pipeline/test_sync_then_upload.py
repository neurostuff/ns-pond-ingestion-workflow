"""`sync` writes the coordinate parse, then `upload` posts it to neurostore's ingester.

Upload holds back a parse built from a table or prose reading that is stale or failed:
the ingester keys analyses by the parse's keys and deletes or supersedes the ones a new
parse lacks, so such a parse would take live analyses out of neurostore.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Artifact, Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.plan import StagePlan, Work
from ingestion_workflow.pipeline.stage import NO_ANALYSES, NO_TEXT
from ingestion_workflow.pipeline.stages import STAGE_ORDER, STAGE_TYPES, build
from ingestion_workflow.pipeline.stages.analyses import AnalysesStage
from ingestion_workflow.pipeline.stages.resolve import ResolveStage
from ingestion_workflow.pipeline.stages.roles import RolesStage
from ingestion_workflow.pipeline.stages.space import SpaceStage
from ingestion_workflow.pipeline.stages.sync import (
    NO_IDENTIFIER,
    RETRACTION_NOTICE,
    SyncStage,
    new_articles_root,
)
from ingestion_workflow.pipeline.stages.upload import UploadStage
from ingestion_workflow.pipeline.upstream import (
    GATED,
    RETRACTION_NEEDED,
    Kind,
    Reading,
    Verdict,
    decide,
    prose_reading,
    tables_reading,
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


def test_sync_runs_before_upload_and_upload_reads_what_sync_wrote():
    assert STAGE_ORDER[-2:] == ("sync", "upload")
    settings = Settings()
    assert [type(s) for s in build(["upload", "sync"], settings)] == [SyncStage, UploadStage]
    for name in STAGE_ORDER:
        required = STAGE_TYPES[name](settings).requires
        assert required is None or STAGE_ORDER.index(required) < STAGE_ORDER.index(name), name
    assert (SyncStage.requires, SyncStage.requires_flag) == ("space", "tables")
    assert UploadStage(settings).requires == "sync"
    assert not hasattr(Settings(), "upload_backend")


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


@pytest.mark.parametrize("fingerprint", ["", "up-legacy"])
def test_an_upload_the_ingester_did_not_make_is_never_fresh(env, monkeypatch, fingerprint):
    """A direct-database upload, a migrated one with no fingerprint among them, did not send
    this parse; the parse goes up through the ingester once."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "42", "BS42")
    _run(SyncStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint=fingerprint,
                            summary={"base_study_id": "BS42", "analyses": 1})])
    upload = UploadStage(settings)
    fake = _Ingester("BS42")
    monkeypatch.setattr(upload, "ingester", lambda: fake)
    plan, (done,) = _run(upload, ctx, catalog, ref)
    assert (plan.fresh, done.status, len(fake.sent)) == (0, Status.OK, 1)
    plan, outcomes = _run(upload, ctx, catalog, ref)
    assert (plan.fresh, outcomes) == (1, [])


def test_an_article_neurostore_holds_no_base_study_for_is_uploaded_and_given_one(env, monkeypatch):
    """The ingester creates the base study from the parse's identifiers and the ParsedPaper's
    bibliography; until then the copy sits outside the tree pondie reads."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "23")
    sync, upload = build(["sync", "upload"], settings)
    _, (first,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    new = new_articles_root(settings) / ref.id
    assert (first.status, first.summary["base_study_id"], first.summary["path"]) == (
        Status.OK, None, str(new))
    assert first.summary["new_to_neurostore"] is True
    header = json.loads((new / "parse" / "coordinate_parse.json").read_text())["header"]
    assert header["identifiers"]["neurostore_base_study_id"] is None
    assert header["identifiers"]["pmid"] == "23"
    assert not (settings.ns_pond_root / "pmids.tsv").exists()
    assert not settings.ns_pond_root.exists() or list(settings.ns_pond_root.iterdir()) == []

    fake = _Ingester("NEW23")
    monkeypatch.setattr(upload, "ingester", lambda: fake)
    _, (done,) = _run(upload, ctx, catalog, ref)
    assert done.status is Status.OK and done.summary["base_study_id"] == "NEW23"
    assert [d for d, _ in fake.sent] == [new]
    assert catalog.resolve(Identifier(neurostore="NEW23")).id == ref.id

    # The next sync writes it into the tree under its base study and drops the copy.
    _, (second,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    target = settings.ns_pond_root / "NEW23"
    assert (second.summary["base_study_id"], second.summary["path"]) == ("NEW23", str(target))
    assert second.summary["replaces"] == str(new) and not new.exists()
    assert second.summary["parse_id"] == first.summary["parse_id"]
    header = json.loads((target / "parse" / "coordinate_parse.json").read_text())["header"]
    assert header["identifiers"]["neurostore_base_study_id"] == "NEW23"
    assert (settings.ns_pond_root / "pmids.tsv").read_text().split("\t")[:2] == ["23", "NEW23"]
    _run(upload, ctx, catalog, ref)
    assert [d for d, _ in fake.sent] == [new, target]
    assert fake.sent[1][1]["coordinate_parse"]["parse_id"] == first.summary["parse_id"]


def test_an_article_with_nothing_to_create_a_base_study_by_waits(env, monkeypatch):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "28")
    bare = dataclasses.replace(ref, identifier=Identifier())
    sync = SyncStage(settings)
    plan = sync.plan(ctx, [bare], catalog.artifacts([ref.id], "sync"),
                     catalog.artifacts([ref.id], "space"))
    assert (plan.blocked, plan.reasons, plan.pending) == (1, {NO_IDENTIFIER: 1}, [])


def _take_back_space(catalog, ref, reason=NO_TEXT):
    catalog.record([Outcome.failure(ref.id, "space", "", reason, fingerprint=NO_TEXT)])


@pytest.mark.parametrize("reason", [NO_TEXT, NO_ANALYSES])
def test_a_taken_back_article_gets_no_parse_and_waits_for_a_retraction(env, monkeypatch, reason):
    """Neurostore's ingester only takes a parse: nothing there withdraws a study, so the
    upload says a retraction is needed rather than pretending it made one."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "29", "BS29")
    sync, upload = build(["sync", "upload"], settings)
    _run(sync, ctx, catalog, ref)
    monkeypatch.setattr(upload, "ingester", lambda: _Ingester("BS29"))
    _, (done,) = _run(upload, ctx, catalog, ref)
    assert done.status is Status.OK

    _take_back_space(catalog, ref, reason)
    _, (back,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert (back.status, back.error, back.fingerprint) == (Status.FAILED, reason, NO_TEXT)
    assert back.summary["retracted"] is True and back.summary["reason"] == reason
    assert not (settings.ns_pond_root / "BS29").exists()
    assert Path(back.summary["moved_to"]).exists()
    assert list(settings.ns_pond_root.rglob("coordinate_parse.json")) == []

    monkeypatch.setattr(upload, "ingester", lambda: _Refuses())
    _, (held,) = _run(upload, ctx, catalog, ref)
    assert (held.status, held.error) == (Status.FAILED, f"{RETRACTION_NEEDED}: {reason}")
    assert held.summary["retraction_needed"] is True and held.summary["reason"] == reason
    assert held.summary["base_study_id"] == "BS29"
    assert held.summary["last_upload"]["analyses"] == 1
    # Recorded once; after that the article is blocked on it, and nothing is sent.
    plan, outcomes = _run(upload, ctx, catalog, ref)
    assert (plan.blocked, plan.reasons, outcomes) == (1, {RETRACTION_NEEDED: 1}, [])


def test_a_taken_back_article_never_uploaded_has_nothing_to_retract(env, monkeypatch):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "30")
    sync, upload = build(["sync", "upload"], settings)
    _run(sync, ctx, catalog, ref)
    new = new_articles_root(settings) / ref.id
    assert new.exists()
    _take_back_space(catalog, ref)
    _, (back,) = _run(sync, ctx, catalog, ref)
    assert back.error == NO_TEXT and back.summary["removed"] == str(new) and not new.exists()
    monkeypatch.setattr(upload, "ingester", lambda: _Refuses())
    _, (recorded,) = _run(upload, ctx, catalog, ref)
    assert (recorded.error, recorded.fingerprint) == (NO_TEXT, NO_TEXT)
    assert "retraction_needed" not in recorded.summary
    plan, outcomes = _run(upload, ctx, catalog, ref)
    assert (plan.blocked, plan.reasons, outcomes) == (1, {}, [])


def test_notices_are_the_parsed_paper_s_corrections_and_a_notice_is_not_synced(
        env, monkeypatch):
    """Neurostore's ingester marks a paper retracted from its ParsedPaper's corrections;
    unanswered notices leave them out, which it reads as not looked up."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "41", "BS41")
    sync = SyncStage(settings)
    _run(sync, ctx, catalog, ref)
    paper = settings.ns_pond_root / "BS41" / "parse" / "parsed_paper.json"
    assert json.loads(paper.read_text())["bibliography"].get("corrections") is None
    retraction = {"kind": "retraction", "pmid": "99999999", "doi": None, "source": "openalex"}
    catalog.record([Outcome(article_id=ref.id, stage="notices", source="", fingerprint="no-1",
                            payload={"corrections": [retraction], "retraction_notice": False},
                            summary={"retracted": True, "retraction_notice": False})])
    _, (again,) = _run(sync, ctx, catalog, ref)
    assert again.status is Status.OK, again.error
    assert json.loads(paper.read_text())["bibliography"]["corrections"] == [
        {"kind": "retraction", "pmid": "99999999", "doi": None}]
    catalog.record([Outcome(article_id=ref.id, stage="notices", source="", fingerprint="no-2",
                            payload={"corrections": [], "retraction_notice": True},
                            summary={"retracted": False, "retraction_notice": True})])
    plan, outcomes = _run(sync, ctx, catalog, ref)
    assert (plan.blocked, plan.reasons, outcomes) == (1, {RETRACTION_NOTICE: 1}, [])


def _dry(settings):
    return settings.model_copy(update={"dry_run": True})


def _dry_lines(settings):
    runs = Path(settings.data_root) / neurostore_ingest.DRY_RUN_DIR / "upload_runs"
    (log,) = runs.glob("*.jsonl")
    return [json.loads(line) for line in log.read_text().splitlines()]


def test_a_dry_run_writes_only_under_its_own_root_and_records_nothing(env, monkeypatch):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "24", "BS24")
    _run(SyncStage(settings), ctx, catalog, ref)
    dry = _dry(settings)
    upload = UploadStage(dry)
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
    _run(SyncStage(settings), ctx, catalog, ref)
    for name in ("coordinate_parse.json", "parsed_paper.json"):
        (settings.ns_pond_root / "BS25" / "parse" / name).unlink()
    dry = _dry(settings)
    upload = UploadStage(dry)
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


def _icafz5(tmp_path, monkeypatch, catalog, *, analyses_fresh=False, unread=None):
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
                payload=_icafz5_payload(),
                summary={"tables": 2, "coordinates": 24,
                         **({"unread": unread} if unread else {})}),
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
    sync = SyncStage(ctx.settings)
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
        upload = UploadStage(settings)
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
        dry_upload = UploadStage(dry)
        monkeypatch.setattr(dry_upload, "ingester", lambda: _Refuses())
        _, outcomes = _run(dry_upload, Context(dry, catalog), catalog, ref)
        assert outcomes == []
        (line,) = _dry_lines(settings)
        assert (line["status"], line["error"]) == ("held back", held.error)
        assert catalog.artifact(ref.id, "upload", "").error == held.error


def test_once_the_tables_are_read_again_the_parse_goes_up(tmp_path, monkeypatch):
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog)
        upload = UploadStage(settings)
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
        upload = UploadStage(settings)
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


@pytest.mark.parametrize("excuse", [None, "the third table is a figure, not coordinates"])
def test_a_table_left_unread_holds_back_unless_a_person_excuses_it(tmp_path, monkeypatch, excuse):
    """The analyses ran, fresh, but one table's raw content was missing: its analyses are
    not in the parse, so it is not sent unless a person leaves the tables out."""
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog, analyses_fresh=True,
                                     unread={"t3": "raw content missing"})
        if excuse:
            catalog.exclude([(ref.id, "source:tables", excuse, "")])
            _sync_as_written(ctx, catalog, ref, analyses=0)
        upload = UploadStage(settings)
        sent = []
        monkeypatch.setattr(upload, "ingester", lambda: type("I", (), {
            "upload": lambda self, d: sent.append(d) or {
                "status": "uploaded", "summary": {"base_study_id": "NTKEuAF4Autf"}},
            "close": lambda self: None})())
        _, (done,) = _run(upload, ctx, catalog, ref)
        if excuse:
            assert done.status is Status.OK and len(sent) == 1
            assert done.summary["gate"] == {"excused": {"analyses": excuse}}
        else:
            assert sent == [] and done.error == (
                f"{GATED}: tables not read: raw content missing; the parse holds 0 analyses, "
                "the last upload 12 (12 missing)")
            assert done.summary["gate"]["upstream"] == {
                "analyses": "tables not read: raw content missing"}


def test_a_failed_table_reading_holds_back_a_parse_with_only_the_prose(env, monkeypatch):
    """The tables failed in the model, so the parse holds the prose's analyses alone."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "26", "BS26")
    _run(SyncStage(settings), ctx, catalog, ref)
    catalog.record([
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-2",
                summary={"source": "pubget", "tables": 1, "passed": 1}),
        Outcome.failure(ref.id, "analyses", "", "UnreadableAnswer: Expecting ',' delimiter: "
                        "line 1 column 1481 (char 1480)",
                        fingerprint=AnalysesStage(settings).fingerprint_for(
                            Artifact(ref.id, "triage", fingerprint="tr-2"))),
    ])
    upload = UploadStage(settings)
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
    (StagePlan("x", pending=[Work(ref=None, source="", fingerprint="f")]), None, "not run"),
    (StagePlan("x", pending=[Work(ref=None, source="", fingerprint=NO_TEXT)]),
     Artifact("a", "x", status=Status.OK), "taken back"),
    (StagePlan("x", permanent=1), Artifact("a", "x", status=Status.SKIPPED), None),
    (StagePlan("x", permanent=1), Artifact("a", "x", status=Status.PERMANENT, error="refused"),
     "failed, not retried: refused"),
])
def test_what_each_plan_says_of_a_stage_s_reading(plan, existing, expected):
    assert upstream_state(plan, existing) == expected


EMPTY, HOLDS = Reading(Kind.EMPTY), Reading(Kind.HOLDS)
UNREAD = Reading(Kind.UNKNOWN, "tables not read: APIConnectionError: Connection error.")
PROSE_FAILED = Reading(Kind.UNKNOWN, "prose not read: 1 of 1 passages failed")


@pytest.mark.parametrize("tables, prose, behind, expected", [
    (EMPTY, EMPTY, {}, (Verdict.TAKE_BACK, NO_ANALYSES)),
    (EMPTY, None, {}, (Verdict.TAKE_BACK, NO_ANALYSES)),
    (HOLDS, EMPTY, {}, (Verdict.PROCEED, None)),
    (EMPTY, HOLDS, {}, (Verdict.PROCEED, None)),
    (UNREAD, EMPTY, {}, (Verdict.HOLD, UNREAD.why)),
    (EMPTY, PROSE_FAILED, {}, (Verdict.HOLD, PROSE_FAILED.why)),
    (Reading(Kind.EXCUSED, UNREAD.why), EMPTY, {}, (Verdict.PROCEED, None)),
    (HOLDS, HOLDS, {"roles": "stale"}, (Verdict.HOLD, "roles stale")),
    (EMPTY, EMPTY, {"resolve": "taken back"}, (Verdict.HOLD, "resolve taken back")),
    # A reading's own stage says it for the reading, once.
    (UNREAD, HOLDS, {"analyses": "failed: APIConnectionError"},
     (Verdict.HOLD, "analyses failed: APIConnectionError")),
    (UNREAD, PROSE_FAILED, {"space": "stale"},
     (Verdict.HOLD, f"{UNREAD.why}; {PROSE_FAILED.why}; space stale")),
])
def test_one_rule_holds_back_or_takes_back(tables, prose, behind, expected):
    """Only two readings that were read whole and hold nothing take back; anything not
    known holds."""
    assert decide(tables, prose, behind) == expected


def _tables_ctx(tmp_path, *records):
    catalog = Catalog.open(tmp_path / "k")
    ref = catalog.register(Identifier(pmid="40"))
    catalog.record([Outcome(article_id=ref.id, **r) for r in records])
    return Context(Settings(data_root=tmp_path / "d"), catalog), ref


@pytest.mark.parametrize("analyses, records, expected", [
    (dict(status=Status.SKIPPED, summary={"tables": 0}), [], Kind.EMPTY),
    (dict(status=Status.OK, summary={"tables": 0}), [], Kind.EMPTY),
    (dict(status=Status.OK, summary={"tables": 2}), [], Kind.HOLDS),
    (dict(status=Status.OK, summary={"tables": 0, "unread": {"t1": "raw content missing"}}),
     [], Kind.UNKNOWN),
    (dict(status=Status.OK, summary={"tables": 1, "unread": {"t2": "raw content missing"}}),
     [], Kind.UNKNOWN),
    (dict(status=Status.OK, summary={"tables": 0,
                                     "unread": {"t2": "repeats another table's numbers"}}),
     [], Kind.EMPTY),
    (dict(status=Status.FAILED, error="ReadTimeout", summary={}), [], Kind.UNKNOWN),
    (None, [dict(stage="triage", source="", fingerprint="t", payload={"tables": [
        {"table_id": "t1", "passes": False}]})], Kind.EMPTY),
    (None, [dict(stage="triage", source="", fingerprint="t", payload={"tables": [
        {"table_id": "t1", "passes": True}]})], Kind.UNKNOWN),
    (None, [dict(stage="extract", source="ace", fingerprint="e", summary={"tables": 0})],
     Kind.EMPTY),
    (None, [dict(stage="extract", source="ace", fingerprint="e", summary={"tables": 2})],
     Kind.UNKNOWN),
    (None, [], Kind.UNKNOWN),
])
def test_tables_are_empty_only_when_read_whole(tmp_path, analyses, records, expected):
    ctx, ref = _tables_ctx(tmp_path, *records)
    try:
        artifact = None if analyses is None else Artifact(ref.id, "analyses", **analyses)
        reading = tables_reading(ctx, ref.id, artifact)
        assert reading.kind is expected
        assert (reading.why or "").startswith("tables not read") == (expected is Kind.UNKNOWN)
    finally:
        ctx.catalog.close()


@pytest.mark.parametrize("prose, expected", [
    (None, Kind.UNKNOWN),
    (Artifact("a", "prose", status=Status.FAILED, error="1 of 3 passages failed"), Kind.UNKNOWN),
    (Artifact("a", "prose", status=Status.OK, summary={"coordinates": 0}), Kind.EMPTY),
    (Artifact("a", "prose", status=Status.OK, summary={"coordinates": 2}), Kind.HOLDS),
    (Artifact("a", "prose", status=Status.OK, summary={"analyses": 1}), Kind.HOLDS),
])
def test_prose_is_empty_only_when_read(prose, expected):
    assert prose_reading(prose).kind is expected


def test_resolve_and_upload_ask_the_same_rule():
    """One copy: the take-back in resolve and the gate in upload both call `decide`."""
    import inspect

    from ingestion_workflow.pipeline import upstream
    from ingestion_workflow.pipeline.stages import resolve, upload

    assert "decide(" in inspect.getsource(resolve.ResolveStage.plan)
    assert "decide(" in inspect.getsource(upstream.Assessor.assess)
    assert "assess(" in inspect.getsource(upload.UploadStage.held_back)
    assert not hasattr(resolve, "tables_unread")


def test_a_stop_records_the_paper_and_stops_before_the_next_batch(env, monkeypatch):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "27", "BS27")
    _run(SyncStage(settings), ctx, catalog, ref)
    upload = UploadStage(settings)

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
