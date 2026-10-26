"""`sync` writes the coordinate parse, then `upload` posts it to neurostore's ingester.

Upload holds back a parse built from a table or prose reading that is stale or failed:
the ingester keys analyses by the parse's keys and supersedes the ones a new parse lacks,
so such a parse would take live analyses out of neurostore.

Upload goes through the real client. The tests that send replay cassettes recorded
against the private, local neurostore `test_neurostore_ingest` describes, which holds
base studies BS21, BS29 and BS42; the rest allow no request at all, so a parse sent that
should not be fails them.
"""

from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Artifact, Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadCopy
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.plan import StagePlan, Work
from ingestion_workflow.pipeline.stage import NO_ANALYSES, NO_TEXT, metadata_fingerprint
from ingestion_workflow.pipeline.stages import STAGE_ORDER, STAGE_TYPES, build
from ingestion_workflow.pipeline.stages.analyses import METADATA_READ as ANALYSES_READ
from ingestion_workflow.pipeline.stages.analyses import AnalysesStage
from ingestion_workflow.pipeline.stages.prose import ProseStage
from ingestion_workflow.pipeline.stages.resolve import ResolveStage
from ingestion_workflow.pipeline.stages.roles import RolesStage
from ingestion_workflow.pipeline.stages.space import SpaceStage
from ingestion_workflow.pipeline.stages.sync import (
    NO_IDENTIFIER,
    RETRACTION_NOTICE,
    SHARED_BASE_STUDY,
    SyncStage,
    is_preprint,
    new_articles_root,
    preprints_among,
)
from ingestion_workflow.pipeline.stages.upload import NO_API_URL, UploadStage
from ingestion_workflow.pipeline.upstream import (
    GATED,
    Kind,
    Reading,
    Verdict,
    decide,
    prose_reading,
    tables_reading,
    upstream_state,
)
from ingestion_workflow.services import neurostore_ingest
from ingestion_workflow.services.funnel_report import FILTERED, build_funnel, open_readonly
from ingestion_workflow.services.set_roles import COORDINATE_ROLES, Prediction
from ingestion_workflow.tests.services.test_neurostore_ingest import (
    LOCAL,
    REPLAY_TOKEN,
    _closed_port,
)
from ingestion_workflow.tests import cassette_secrets
from ingestion_workflow.tests.upload_body import without_input_fingerprints
from ingestion_workflow.pipeline.stage import metadata_digests
from study_schema.models.paper_parse import CopyAccess, CopyRoute, CopyVersion
from test_prose_stages import ARTICLE, _read_prose, _Reader, _record_upstream
from test_roles_stage import PROSE_MODEL, TABLE_MODEL, served  # noqa: F401 - a fixture

CASSETTES = Path(__file__).parents[1] / "cassettes" / "sync_then_upload"


@pytest.fixture(scope="module")
def vcr_cassette_dir():
    return str(CASSETTES)


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": ["authorization", "cookie", "set-cookie"],
        "before_record_response": cassette_secrets.scrub_response,
        "record_mode": "none",
        "match_on": ["method", "scheme", "host", "port", "path", "query", "upload_body"],
    }


#: For the tests that send: they record their cassette once, against the local neurostore.
RECORDS = pytest.mark.vcr(record_mode="once")


class _ByName:
    """A role classifier: a set named from `others` is `other`, the rest are results."""

    source = "by-name@1"

    def __init__(self, others=()):
        self.others = tuple(others)

    def predict(self, texts):
        out = []
        for text in texts:
            role = "other" if any(f"[NAME] {o}" in text for o in self.others) else "result"
            out.append(Prediction(role, None, {r: float(r == role) for r in COORDINATE_ROLES}))
        return out


def _neurostore_plays(vcr):
    """Each recorded neurostore request's replay count; the catalog's checks of a PMID are
    recorded in the same cassette."""
    return [vcr.play_counts[i] for i, r in enumerate(vcr.requests) if r.host == "127.0.0.1"]


def _sent(vcr):
    """The body of each upload the cassette replayed."""
    return [json.loads(r.body) for r in vcr.requests if r.method == "POST"]


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
def token(monkeypatch):
    value = os.environ.get("NEUROSTORE_TOKEN") or REPLAY_TOKEN
    monkeypatch.setenv("NEUROSTORE_TOKEN", value)
    return value


@pytest.fixture()
def env(tmp_path, served, token):  # noqa: F811 - the imported fixture
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond",
                        prose_model="nu-prose-v2", llm_api_key="x",
                        role_model_table=TABLE_MODEL, role_model_prose=PROSE_MODEL,
                        neurostore_api_url=LOCAL)
    path = tmp_path / "article.xml"
    path.write_text(ARTICLE)
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, path


def _prose_article(env, monkeypatch, pmid, neurostore=None, doi=None, *, evidence):
    """An article read through prose, resolve, roles and space; triage passed no table."""
    settings, catalog, path = env
    ref = catalog.register(Identifier(pmid=pmid, neurostore=neurostore, doi=doi), evidence)
    _record_upstream(catalog, ref, path)
    catalog.record([Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1",
                            summary={"source": "pubget", "tables": 0, "passed": 0,
                                     "passed_tables": []})])
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


@RECORDS
def test_sync_writes_the_parse_with_no_upload_and_upload_sends_what_it_wrote(env, monkeypatch,
                                                                             vcr, evidence):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "21", "BS21", evidence=evidence)
    sync, upload = build(["sync", "upload"], settings)
    assert catalog.artifacts([ref.id], "upload") == {}
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert synced.status is Status.OK, synced.error
    written = settings.ns_pond_root / "BS21" / "parse" / "coordinate_parse.json"
    assert json.loads(written.read_text())["header"]["neurostore_base_study_id"] \
        == "BS21"
    assert (settings.ns_pond_root / "BS21" / "stage1" / "analyses.json").exists()

    _, (done,) = _run(upload, ctx, catalog, ref)
    upload.finish()
    assert done.status is Status.OK, done.error
    (payload,) = _sent(vcr)
    # the replayed body is the recorded one: its input fingerprints are the recorded run's
    assert without_input_fingerprints(payload["coordinate_parse"]) == without_input_fingerprints(
        json.loads(written.read_text()))
    assert without_input_fingerprints(payload["parsed_paper"]) == without_input_fingerprints(json.loads(
        (settings.ns_pond_root / "BS21" / "parse" / "parsed_paper.json").read_text()))
    # The matcher blanks the input fingerprints of the replayed body; the parse written here is
    # where they are held to the catalog's artifacts.
    held = {"extraction": catalog.artifacts([ref.id], "extract")[ref.id]["pubget"].fingerprint,
            "metadata": catalog.artifacts([ref.id], "metadata")[ref.id][""].fingerprint,
            "triage": catalog.artifacts([ref.id], "triage")[ref.id][""].fingerprint,
            "analyses": catalog.artifacts([ref.id], "space")[ref.id][""].fingerprint}
    for name in ("parsed_paper", "coordinate_parse"):
        inputs = json.loads((settings.ns_pond_root / "BS21" / "parse" / f"{name}.json").read_text())[
            "header"]["inputs"]
        assert inputs and all(i["fingerprint"] == held.get(i["artifact_kind"], i["fingerprint"])
                              for i in inputs)
    assert {i["artifact_kind"] for i in json.loads(written.read_text())["header"]["inputs"]} >= {"analyses"}
    assert done.summary["base_study_id"] == "BS21"
    summary = done.summary
    assert (summary["backend"], summary["dry_run"], summary["gate"]) == ("ingester", False, None)
    assert summary["parse_id"] == synced.summary["parse_id"] == summary["ingester"]["parse_id"]
    assert summary["ingester"]["summary"]["kind"] == "parse"
    assert "history" not in summary["ingester"] and "last_uploaded" not in summary["ingester"]
    assert summary["analyses"] == synced.summary["analyses"] == 1
    # Uploaded: fresh until sync writes another parse.
    plan, outcomes = _run(upload, ctx, catalog, ref)
    assert (plan.fresh, outcomes) == (1, [])


@RECORDS
@pytest.mark.parametrize("fingerprint", ["", "up-legacy"], ids=["unfingerprinted", "legacy"])
def test_an_upload_the_ingester_did_not_make_is_never_fresh(
        env, monkeypatch, fingerprint, vcr, evidence):
    """A direct-database upload, a migrated one with no fingerprint among them, did not send
    this parse; the parse goes up through the ingester once."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "42", "BS42", evidence=evidence)
    _run(SyncStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="", fingerprint=fingerprint,
                            summary={"base_study_id": "BS42", "analyses": 1})])
    upload = UploadStage(settings)
    plan, (done,) = _run(upload, ctx, catalog, ref)
    assert (plan.fresh, done.status, len(_sent(vcr))) == (0, Status.OK, 1)
    plan, outcomes = _run(upload, ctx, catalog, ref)
    assert (plan.fresh, outcomes) == (1, [])


def test_a_licence_change_makes_sync_and_upload_stale_and_nothing_before_them(
        env, monkeypatch, evidence):
    """The parse carries the metadata's licences, which the stages before sync do not read."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "21", "BS21", evidence=evidence)
    sync, upload = build(["sync", "upload"], settings)
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert synced.status is Status.OK, synced.error
    (work,) = upload.plan(ctx, [ref], {}, catalog.artifacts([ref.id], "sync")).pending
    catalog.record([Outcome(article_id=ref.id, stage="upload", source="",
                            fingerprint=work.fingerprint,
                            summary={"base_study_id": "BS21", "backend": "ingester"})])
    before = ("prose", "resolve", "roles", "space")
    by_name = {"prose": ProseStage(settings), "resolve": ResolveStage(settings),
               "roles": _roles(settings, monkeypatch, _ByName()), "space": SpaceStage(settings),
               "sync": sync, "upload": upload}

    def pending():
        return {name for name, stage in by_name.items()
                if stage.plan(ctx, [ref], catalog.artifacts([ref.id], name),
                              catalog.artifacts([ref.id], stage.requires)).pending}

    assert pending() == set()
    licensed = {**catalog.payload(catalog.artifact(ref.id, "metadata", "")),
                "license": {"license_id": "cc-by", "determination": "stated"}}
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="", fingerprint="me-3",
                            payload=licensed, summary={"digests": metadata_digests(licensed)})])

    assert pending() == {"sync"}
    assert not pending() & set(before)
    _run(sync, ctx, catalog, ref)
    sync.finish()
    assert pending() == {"upload"}


def test_the_same_metadata_written_again_leaves_sync_fresh(env, monkeypatch, evidence):
    """Sync holds the metadata's content: a re-fetch that finds the same record is no
    change."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "21", "BS21", evidence=evidence)
    catalog._conn.execute("UPDATE artifacts SET updated_at='2000-01-01T00:00:00+00:00' "
                          "WHERE article_id=? AND stage='metadata'", (ref.id,))
    (sync,) = build(["sync"], settings)
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    stored = catalog.artifact(ref.id, "metadata", "")
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="",
                            fingerprint=stored.fingerprint, payload=catalog.payload(stored),
                            summary=stored.summary)])
    assert catalog.artifact(ref.id, "metadata", "").updated_at != "2000-01-01T00:00:00+00:00"

    plan = sync.plan(ctx, [ref], catalog.artifacts([ref.id], "sync"),
                     catalog.artifacts([ref.id], "space"))
    assert (plan.fresh, plan.pending) == (1, [])


def test_triage_judged_again_re_syncs_the_parse_that_holds_its_verdicts(
        env, monkeypatch, evidence):
    """The parse holds triage's meta-analysis mark, publication types and each table's
    score and route. A triage judged again (a refitted gate, a new meta-analysis rule)
    re-syncs, though analyses follows only the tables it passed and the metadata stands;
    the same triage recorded again does not."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "21", "BS21", evidence=evidence)
    (sync,) = build(["sync"], settings)
    _, (synced,) = _run(sync, ctx, catalog, ref)
    sync.finish()

    def plan():
        found = sync.plan(ctx, [ref], catalog.artifacts([ref.id], "sync"),
                          catalog.artifacts([ref.id], "space"))
        return found.fresh, len(found.pending)

    triaged = catalog.artifact(ref.id, "triage", "")
    catalog.record([Outcome(article_id=ref.id, stage="triage", source="",
                            fingerprint=triaged.fingerprint, summary=triaged.summary)])
    assert plan() == (1, 0)

    catalog.record([Outcome(article_id=ref.id, stage="triage", source="",
                            fingerprint="tr-refit",
                            summary={**triaged.summary, "is_meta_analysis": True})])
    assert plan() == (0, 1)


@RECORDS
def test_an_article_neurostore_holds_no_base_study_for_is_uploaded_and_given_one(env, monkeypatch,
                                                                                vcr, evidence):
    """The ingester creates the base study from the parse's identifiers and the ParsedPaper's
    bibliography; until then the copy sits outside the tree pondie reads."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "23", evidence=evidence)
    sync, upload = build(["sync", "upload"], settings)
    _, (first,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    new = new_articles_root(settings) / ref.id
    assert (first.status, first.summary["base_study_id"], first.summary["path"]) == (
        Status.OK, None, str(new))
    assert first.summary["new_to_neurostore"] is True
    header = json.loads((new / "parse" / "coordinate_parse.json").read_text())["header"]
    assert header["neurostore_base_study_id"] is None
    assert header["identifiers"]["pmid"] == "23"
    assert not (settings.ns_pond_root / "pmids.tsv").exists()
    assert not settings.ns_pond_root.exists() or list(settings.ns_pond_root.iterdir()) == []

    _, (done,) = _run(upload, ctx, catalog, ref)
    assert done.status is Status.OK, done.error
    created = done.summary["base_study_id"]
    assert created and done.summary["ingester"]["study_id"] == ref.id
    assert catalog.resolve(Identifier(neurostore=created)).id == ref.id

    # The next sync writes it into the tree under its base study and drops the copy.
    _, (second,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    target = settings.ns_pond_root / created
    assert (second.summary["base_study_id"], second.summary["path"]) == (created, str(target))
    assert second.summary["replaces"] == str(new) and not new.exists()
    assert second.summary["parse_id"] == first.summary["parse_id"]
    header = json.loads((target / "parse" / "coordinate_parse.json").read_text())["header"]
    assert header["neurostore_base_study_id"] == created
    assert (settings.ns_pond_root / "pmids.tsv").read_text().split("\t")[:2] == ["23", created]
    _, (again,) = _run(upload, ctx, catalog, ref)
    assert again.status is Status.OK and again.summary["base_study_id"] == created
    # An upload's own record is what it sent; the one before it is not carried on.
    assert "last_upload" not in again.summary
    first_sent, second_sent = _sent(vcr)
    assert first_sent["coordinate_parse"]["header"]["neurostore_base_study_id"] is None
    assert second_sent["coordinate_parse"]["header"]["neurostore_base_study_id"] == created
    assert second_sent["coordinate_parse"]["parse_id"] == first.summary["parse_id"]


def _plan_sync(sync, ctx, catalog, refs):
    ids = [ref.id for ref in refs]
    return sync.plan(ctx, refs, catalog.artifacts(ids, "sync"), catalog.artifacts(ids, "space"))


@pytest.mark.parametrize("identifier, waits", [
    (Identifier(), True),
    (Identifier(doi="10.5555/doi.only"), False),
    (Identifier(pmcid="PMC9990028"), False),
    (Identifier(preprint_dois=["10.1101/2025.05.13.653896"]), False),
    (Identifier(preprint_pmids=["34845462"]), False),
    (Identifier(preprint_pmcids=["PMC8629206"]), False),
], ids=["none", "doi-only", "pmcid-only", "preprint-doi-only", "preprint-pmid-only",
        "preprint-pmcid-only"])
def test_an_article_with_nothing_to_create_a_base_study_by_is_recorded_with_why(
        env, monkeypatch, identifier, waits, evidence):
    """Any one of doi, pmid or pmcid is enough for the ingester to find or create the base
    study; with none, sync records the article skipped with the reason, once."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "28", evidence=evidence)
    only = dataclasses.replace(ref, identifier=identifier)
    sync = SyncStage(settings)
    (work,) = _plan_sync(sync, ctx, catalog, [only]).pending
    (out,) = sync.execute(ctx, [work])
    if not waits:
        assert out.status is Status.OK, out.error
        assert out.summary["new_to_neurostore"] is True
        return
    assert (out.status, out.error, out.summary) == (
        Status.SKIPPED, NO_IDENTIFIER, {"reason": NO_IDENTIFIER, "base_study_id": None})
    assert not new_articles_root(settings).exists()
    catalog.record([out])
    plan = _plan_sync(sync, ctx, catalog, [only])
    assert (plan.blocked, plan.reasons, plan.pending) == (1, {NO_IDENTIFIER: 1}, [])


def _claimed_twice(env, monkeypatch, dois=(None, None), *, evidence):
    """om54glpssubo and udjyuoxv2dbo's shape in the catalog of 2026-10-12: one holds
    neurostore's alias for base study 5dsFAUgmBz36, the other an upload record naming the
    same base study."""
    settings, catalog, _ = env
    ctx, held = _prose_article(env, monkeypatch, "19452633", "5dsFAUgmBz36",
                               doi=dois[0], evidence=evidence)
    _, named = _prose_article(env, monkeypatch, "19176279", doi=dois[1], evidence=evidence)
    catalog.record([Outcome(article_id=named.id, stage="upload", source="",
                            fingerprint="up-legacy",
                            summary={"base_study_id": "5dsFAUgmBz36", "analyses": 1})])
    return ctx, held, named


def _read_from_a_preprint(catalog, ref):
    """The download its text came from is a submitted version, as PMC marks a preprint
    it hosts."""
    download = catalog.artifact(ref.id, "download", "pubget")
    preprint = {**download.summary["copy"], "version": "submitted"}
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            fingerprint=download.fingerprint, payload=catalog.payload(download),
                            summary={**download.summary, "copy": preprint})])


@pytest.mark.vcr
def test_two_articles_naming_one_base_study_are_both_recorded_and_neither_is_sent(
        env, monkeypatch, evidence):
    """Each upload would supersede the other's analyses in that base study, so neither is
    written or uploaded; each is recorded with why."""
    settings, catalog, _ = env
    ctx, held, named = _claimed_twice(env, monkeypatch, evidence=evidence)
    sync = SyncStage(settings)
    # Planned alone, each still finds the other: the claims are the whole catalog's.
    for ref, other in ((held, named), (named, held)):
        _, (skipped,) = _run(sync, ctx, catalog, ref)
        assert (skipped.status, skipped.error) == (
            Status.SKIPPED, f"shares base study 5dsFAUgmBz36 with {other.id}")
        assert skipped.summary == {"reason": SHARED_BASE_STUDY, "base_study_id": "5dsFAUgmBz36",
                                   "shared_with": [other.id]}
    assert not (settings.ns_pond_root / "5dsFAUgmBz36").exists()
    plan = _plan_sync(sync, ctx, catalog, [held, named])
    assert (plan.pending, plan.reasons) == ([], {SHARED_BASE_STUDY: 2})
    ids = [held.id, named.id]
    plan = UploadStage(settings).plan(ctx, [held, named], catalog.artifacts(ids, "upload"),
                                      catalog.artifacts(ids, "sync"))
    assert (plan.pending, plan.blocked) == ([], 2)
    conn = open_readonly(settings.catalog_root)
    try:
        funnel = build_funnel(conn, samples=10)
    finally:
        conn.close()
    (count, sample), = [found for key, found in funnel.cells.items()
                        if key[:3] == ("sync", FILTERED, SHARED_BASE_STUDY)]
    assert count == 2 and {raw for _, raw in sample} == {
        f"shares base study 5dsFAUgmBz36 with {i}" for i in (held.id, named.id)}


def test_of_a_preprint_and_its_published_paper_on_one_base_study_the_paper_is_written(
        env, monkeypatch, evidence):
    """HXrFSBcmoxhc's case: neurostore's alias is the Research Square preprint's, and the
    published paper names the same base study. The paper is written under it and uploaded;
    the preprint is recorded with why."""
    settings, catalog, _ = env
    ctx, preprint, paper = _claimed_twice(env, monkeypatch, evidence=evidence)
    _read_from_a_preprint(catalog, preprint)
    sync = SyncStage(settings)
    plan = _plan_sync(sync, ctx, catalog, [preprint, paper])
    outcomes = {o.article_id: o for o in sync.execute(ctx, plan.pending)}
    sync.finish()
    assert (outcomes[preprint.id].status, outcomes[preprint.id].error) == (
        Status.SKIPPED, f"shares base study 5dsFAUgmBz36 with {paper.id}")
    written = outcomes[paper.id]
    assert written.status is Status.OK, written.error
    assert written.summary["path"] == str(settings.ns_pond_root / "5dsFAUgmBz36")
    header = json.loads((settings.ns_pond_root / "5dsFAUgmBz36" / "parse"
                         / "coordinate_parse.json").read_text())["header"]
    assert header["identifiers"]["pmid"] == "19176279"


@pytest.mark.parametrize("dois, written", [
    (("10.1101/2020.01.01.123456", "10.1016/j.neuroimage.2020.1"), 1),
    (("10.21203/rs.3.rs-123456/v1", "10.1016/j.neuroimage.2020.1"), 1),
    (("10.1016/j.neuroimage.2020.2", "10.1016/j.neuroimage.2020.1"), None),
    (("10.1101/2020.01.01.123456", "10.21203/rs.3.rs-123456/v1"), None),
], ids=["biorxiv and published", "research square and published", "both published",
        "both preprints"])
def test_a_preprint_server_doi_makes_the_other_article_the_one_that_is_written(
        env, monkeypatch, dois, written, evidence):
    """Of a preprint and its published paper, by DOI prefix alone, the paper is written
    and the preprint recorded with why; two of a kind are all recorded, none written."""
    settings, catalog, _ = env
    ctx, first, second = _claimed_twice(env, monkeypatch, dois, evidence=evidence)
    sync = SyncStage(settings)
    plan = _plan_sync(sync, ctx, catalog, [first, second])
    outcomes = {o.article_id: o for o in sync.execute(ctx, plan.pending)}
    sync.finish()
    pair = {first.id: second, second.id: first}
    kept = second if written else None
    for ref in (first, second):
        out = outcomes[ref.id]
        if ref is kept:
            assert out.status is Status.OK, out.error
        else:
            assert (out.status, out.error) == (
                Status.SKIPPED, f"shares base study 5dsFAUgmBz36 with {pair[ref.id].id}")
    assert (settings.ns_pond_root / "5dsFAUgmBz36").exists() == bool(written)


def test_is_preprint_by_a_preprint_servers_doi_or_by_the_copy_of_the_download():
    def held(route, version=None, server=None):
        return DownloadCopy(route=route, version=version, access=CopyAccess.public,
                            preprint_server=server)

    published = held(CopyRoute.publisher, CopyVersion.vor)
    assert is_preprint("10.1101/2019.12.11.873174", None)
    assert is_preprint("10.1101/2019.12.11.873174".upper(), None)
    assert is_preprint("10.64898/2025.11.28.690060", None)
    assert is_preprint("10.21203/rs.3.rs-2556690/v2", None)
    assert is_preprint("10.48550/arxiv.2111.08743", None)
    assert is_preprint("10.2139/ssrn.3349212", published)
    assert is_preprint("10.1016/j.x", held(CopyRoute.repository, CopyVersion.submitted))
    # a journal article whose accepted manuscript arXiv holds is the article
    assert not is_preprint("10.1016/j.x",
                           held(CopyRoute.preprint_server, CopyVersion.am, "arxiv"))
    assert is_preprint(None, held(CopyRoute.preprint_server, CopyVersion.submitted, "medrxiv"))
    assert not is_preprint("10.1016/j.x", published)
    assert not is_preprint("10.1016/j.x", held(CopyRoute.repository, CopyVersion.am))
    assert not is_preprint("10.1016/j.x", DownloadCopy(route=CopyRoute.unrecorded,
                                                      access=CopyAccess.unrecorded))
    assert not is_preprint(None, None)
    assert not is_preprint("10.11011/x", None)
    assert not is_preprint("10.1101/gr.112680.110", None)
    assert not is_preprint("10.1101/lm.022889.111", published)


def test_a_download_that_failed_since_the_extraction_leaves_the_doi_to_decide(
        env, monkeypatch, evidence):
    """A refreshed download that failed keeps no copy: the bioRxiv DOI still says which is
    the preprint, and the other article's copy is read as before."""
    _, catalog, _ = env
    ctx, preprint, paper = _claimed_twice(
        env, monkeypatch, ("10.1101/2020.01.01.123456", "10.1016/j.neuroimage.2020.1"),
                evidence=evidence)
    catalog.record([Outcome.failure(preprint.id, "download", "pubget", "no full text")])

    assert preprints_among(ctx, [preprint.id, paper.id]) == {preprint.id}


def test_a_download_stored_before_copies_were_recorded_names_the_backfill(
        env, monkeypatch, evidence):
    _, catalog, _ = env
    ctx, held, named = _claimed_twice(env, monkeypatch, evidence=evidence)
    download = catalog.artifact(held.id, "download", "pubget")
    catalog.record([Outcome(article_id=held.id, stage="download", source="pubget",
                            fingerprint=download.fingerprint, payload=catalog.payload(download),
                            summary={k: v for k, v in download.summary.items() if k != "copy"})])

    with pytest.raises(RuntimeError, match="scripts/backfill_download_copies.py"):
        preprints_among(ctx, [held.id, named.id])


def test_the_parsed_paper_s_text_copy_is_the_copy_its_text_was_downloaded_as(
        env, monkeypatch, evidence):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "41", "BS41", evidence=evidence)
    _read_from_a_preprint(catalog, ref)
    _, (synced,) = _run(SyncStage(settings), ctx, catalog, ref)
    assert synced.status is Status.OK, synced.error

    paper = json.loads(
        (settings.ns_pond_root / "BS41" / "parse" / "parsed_paper.json").read_text())
    copy = {k: v for k, v in paper["text_copy"].items() if v is not None}
    assert copy.pop("license") == {"license_id": "all-rights-reserved", "determination": "assumed",
                                   "stated_by": None, "stated_as": None}
    assert copy == catalog.artifact(ref.id, "download", "pubget").summary["copy"]
    assert copy["version"] == "submitted"


@pytest.mark.parametrize("status", [Status.FAILED, Status.PERMANENT])
def test_a_text_whose_download_since_failed_is_not_synced_and_says_why(
        env, monkeypatch, status, evidence):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "42", "BS42", evidence=evidence)
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget", status=status,
                            fingerprint="dl-2", error="no full text")])
    _, (synced,) = _run(SyncStage(settings), ctx, catalog, ref)

    assert synced.status is Status.FAILED
    assert synced.error == "no ok download of the pubget copy the text was read from"
    assert not (settings.ns_pond_root / "BS42").exists()


def test_the_header_holds_every_doi_the_catalog_has_for_the_article(env, monkeypatch, evidence):
    """dpl6phhniqbj's two DOIs, its journal's and its bioRxiv preprint's."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "45", "BS45", doi="10.1101/2020.03.25.008318",
            evidence=evidence)
    catalog.add_aliases([(ref.id, "doi", "10.1002/hbm.25137")])
    _, (synced,) = _run(SyncStage(settings), ctx, catalog, catalog.ref(ref.id))
    assert synced.status is Status.OK, synced.error

    header = json.loads(
        (settings.ns_pond_root / "BS45" / "parse" / "parsed_paper.json").read_text())["header"]
    assert (header["identifiers"]["doi"], header["identifiers"]["preprint_dois"]) == (
        "10.1002/hbm.25137", [{"doi": "10.1101/2020.03.25.008318", "server": "biorxiv"}])


def test_an_article_holding_two_doi_rows_is_not_synced(env, monkeypatch, evidence):
    """298 catalog articles hold two version-of-record DOIs, most of them two works fused;
    sync must not write one and drop the other."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "46", "BS46", doi="10.1002/hbm.25137",
            evidence=evidence)
    catalog.add_aliases([(ref.id, "doi", "10.1371/journal.pone.0041873")])
    _, (synced,) = _run(SyncStage(settings), ctx, catalog, catalog.ref(ref.id))

    assert synced.status is not Status.OK
    assert "two DOIs of a version of record" in synced.error
    assert not (settings.ns_pond_root / "BS46" / "parse").exists()


def _held_by_pubget_and_wiley(env, monkeypatch, pmid, base, read, evidence):
    """An article pubget holds and whose Wiley PDF is read into its `pdf` text; triage
    judged the `read` extraction."""
    from ingestion_workflow.extractors.copies import copy_of
    from ingestion_workflow.extractors.wiley_extractor import TDM_URL
    from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType

    settings, catalog, path = env
    ctx, ref = _prose_article(env, monkeypatch, pmid, base, evidence=evidence)
    wiley = DownloadResult(
        identifier=ref.identifier, source=DownloadSource.WILEY, success=True,
        files=[DownloadedFile(file_path=path, file_type=FileType.PDF,
                              content_type="application/pdf", source=DownloadSource.WILEY)],
        url_attempts=[{"url": TDM_URL + "10.1111%2Fj.1440-1614.2006.01765.x",
                       "provider": "wiley", "status": 200, "outcome": "pdf"}])
    pubget = catalog.artifact(ref.id, "extract", "pubget")
    catalog.record([
        Outcome(article_id=ref.id, stage="download", source="wiley", fingerprint="dl-w",
                payload=wiley.to_dict(), summary={"copy": copy_of(wiley).to_dict()}),
        Outcome(article_id=ref.id, stage="extract", source="pdf", fingerprint="ex-pdf",
                payload={**catalog.payload(pubget), "source": "pdf"},
                summary={**pubget.summary, "download_source": "wiley"}),
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1",
                summary={"source": read, "tables": 0, "passed": 0, "passed_tables": []}),
        Outcome(article_id=ref.id, stage="metadata", source="", fingerprint="me-2",
                payload=(licensed := {**catalog.payload(catalog.artifact(ref.id, "metadata", "")),
                                      "copy_licenses": COPY_LICENSES}),
                summary={"digests": metadata_digests(licensed)}),
    ])
    return ctx, ref


#: Each held copy's licence, as the metadata stage would decide it: the Wiley copy's
#: Crossref's version-of-record deposit, the pubget copy's nobody's.
COPY_LICENSES = {
    "pubget": {"license_id": "all-rights-reserved", "determination": "assumed"},
    "wiley": {"license_id": "other", "determination": "stated", "stated_by": "crossref",
              "stated_as": "http://onlinelibrary.wiley.com/termsAndConditions#vor"},
}


@pytest.mark.parametrize("read, download", [("pdf", "wiley"), ("pubget", "pubget")])
def test_the_text_copy_is_the_download_the_judged_text_was_read_from(
        env, monkeypatch, read, download, evidence):
    settings, catalog, _ = env
    ctx, ref = _held_by_pubget_and_wiley(env, monkeypatch, "44", "BS44", read, evidence=evidence)
    _, (synced,) = _run(SyncStage(settings), ctx, catalog, ref)
    assert synced.status is Status.OK, synced.error

    paper = json.loads(
        (settings.ns_pond_root / "BS44" / "parse" / "parsed_paper.json").read_text())
    assert paper["source"] == read
    copy = {k: v for k, v in paper["text_copy"].items() if v is not None and k != "license"}
    assert copy == catalog.artifact(ref.id, "download", download).summary["copy"]
    stated = {k: v for k, v in paper["text_copy"]["license"].items() if v is not None}
    assert stated == COPY_LICENSES[download]


def test_sync_stops_at_a_text_download_stored_before_copies_were_recorded(
        env, monkeypatch, evidence):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "43", "BS43", evidence=evidence)
    download = catalog.artifact(ref.id, "download", "pubget")
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            fingerprint=download.fingerprint, payload=catalog.payload(download),
                            summary={k: v for k, v in download.summary.items() if k != "copy"})])

    with pytest.raises(RuntimeError, match="scripts/backfill_download_copies.py"):
        _run(SyncStage(settings), ctx, catalog, ref)


def _never_downloaded(env, monkeypatch, pmid, base, evidence):
    return env[1].register(Identifier(pmid=pmid, neurostore=base), evidence)


def _space_taken_back(env, monkeypatch, pmid, base, evidence):
    _, ref = _prose_article(env, monkeypatch, pmid, base, evidence=evidence)
    _take_back_space(env[1], ref)
    return ref


def _roles_failed(env, monkeypatch, pmid, base, evidence):
    _, ref = _prose_article(env, monkeypatch, pmid, base, evidence=evidence)
    env[1].record([Outcome.failure(ref.id, "roles", "", "TimeoutError: timed out")])
    return ref


def _a_retraction_notice(env, monkeypatch, pmid, base, evidence):
    _, ref = _prose_article(env, monkeypatch, pmid, base, evidence=evidence)
    env[1].record([Outcome(article_id=ref.id, stage="notices", source="", fingerprint="no-1",
                           payload={"corrections": [], "retraction_notice": True},
                           summary={"retracted": False, "retraction_notice": True})])
    return ref


@pytest.mark.parametrize("other", [
    _never_downloaded, _space_taken_back, _roles_failed, _a_retraction_notice,
], ids=["never downloaded", "space taken back", "roles failed", "retraction notice"])
def test_an_article_that_never_reaches_sync_holds_no_claim_on_its_base_study(env, monkeypatch,
                                                                             other, evidence):
    """aTRpMgJ82Ysx's case in the catalog of 2026-10-12: qjgzxkryyvbg holds neurostore's
    alias for it and has no download, so it never reaches sync and sends nothing;
    vzlh56dlbzjy uploaded under it. vzlh56dlbzjy is written, planned alone or with the
    other, and what sync records of the other is no claim either."""
    settings, catalog, _ = env
    ctx, uploaded = _prose_article(env, monkeypatch, "40402904", evidence=evidence)
    catalog.record([Outcome(article_id=uploaded.id, stage="upload", source="",
                            fingerprint="up-legacy",
                            summary={"base_study_id": "aTRpMgJ82Ysx", "analyses": 1})])
    alias_only = other(env, monkeypatch, "40402903", "aTRpMgJ82Ysx", evidence)
    sync = SyncStage(settings)
    plan = _plan_sync(sync, ctx, catalog, [uploaded])
    assert [(w.article_id, w.skip) for w in plan.pending] == [(uploaded.id, None)]
    plan = _plan_sync(sync, ctx, catalog, [alias_only, uploaded])
    assert [w.skip for w in plan.pending if w.article_id == uploaded.id] == [None]
    outcomes = {o.article_id: o for o in sync.execute(ctx, plan.pending)}
    catalog.record(list(outcomes.values()))
    written = outcomes[uploaded.id]
    assert written.status is Status.OK, written.error
    assert written.summary["path"] == str(settings.ns_pond_root / "aTRpMgJ82Ysx")
    assert _plan_sync(sync, ctx, catalog, [uploaded]).fresh == 1


def test_an_article_taken_back_from_the_corpus_holds_no_claim_on_its_base_study(env,
                                                                                monkeypatch,
                                                                                        evidence):
    """Its sync record is the take-back: nothing of it is in the tree for upload to send."""
    settings, catalog, _ = env
    ctx, gone = _prose_article(env, monkeypatch, "40402910", "BS40402910", evidence=evidence)
    sync = SyncStage(settings)
    _run(sync, ctx, catalog, gone)
    _take_back_space(catalog, gone)
    _, (back,) = _run(sync, ctx, catalog, gone)
    assert (back.error, back.summary["base_study_id"]) == (NO_TEXT, "BS40402910")
    _, paper = _prose_article(env, monkeypatch, "40402911", evidence=evidence)
    catalog.record([Outcome(article_id=paper.id, stage="upload", source="",
                            fingerprint="up-legacy",
                            summary={"base_study_id": "BS40402910", "analyses": 1})])
    plan = _plan_sync(sync, ctx, catalog, [paper])
    assert [(w.article_id, w.skip) for w in plan.pending] == [(paper.id, None)]


def test_the_gate_reads_the_claims_on_base_studies_once_for_all_its_articles(env,
                                                                            monkeypatch, evidence):
    """The claims are one query over every sync and upload record in the catalog, so the
    gate asks it once for a batch, not once per article."""
    from ingestion_workflow.pipeline.upstream import Assessor

    settings, catalog, _ = env
    refs = []
    for pmid in ("40402907", "40402908", "40402909"):
        ctx, ref = _prose_article(env, monkeypatch, pmid, f"BS{pmid}", evidence=evidence)
        _run(SyncStage(settings), ctx, catalog, ref)
        refs.append(ref)
    asked = []
    catalog._conn.set_trace_callback(
        lambda sql: asked.append(sql) if "'$.base_study_id') IN" in sql else None)
    try:
        assessed = Assessor(settings).assess(ctx, refs)
    finally:
        catalog._conn.set_trace_callback(None)
    assert {found.verdict for found in assessed.values()} == {Verdict.PROCEED}
    assert len(asked) == 1


def _take_back_space(catalog, ref, reason=NO_TEXT):
    catalog.record([Outcome.failure(ref.id, "space", "", reason, fingerprint=NO_TEXT)])


@RECORDS
@pytest.mark.parametrize("reason", [NO_TEXT, NO_ANALYSES], ids=["no-text", "no-analyses"])
def test_a_taken_back_article_gets_no_parse_and_is_recorded_once_with_its_reason(
        env, monkeypatch, reason, vcr, evidence):
    """Nothing is deleted from neurostore: it keeps the last upload, and the upload stage
    records the take-back's reason and sends nothing again."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "29", "BS29", evidence=evidence)
    sync, upload = build(["sync", "upload"], settings)
    _run(sync, ctx, catalog, ref)
    _, (done,) = _run(upload, ctx, catalog, ref)
    assert done.status is Status.OK, done.error

    _take_back_space(catalog, ref, reason)
    _, (back,) = _run(sync, ctx, catalog, ref)
    sync.finish()
    assert (back.status, back.error, back.fingerprint) == (Status.FAILED, reason, NO_TEXT)
    assert back.summary["retracted"] is True and back.summary["reason"] == reason
    assert not (settings.ns_pond_root / "BS29").exists()
    assert Path(back.summary["moved_to"]).exists()
    assert list(settings.ns_pond_root.rglob("coordinate_parse.json")) == []

    _, (back,) = _run(upload, ctx, catalog, ref)
    assert (back.status, back.error, back.fingerprint) == (Status.FAILED, reason, NO_TEXT)
    assert back.summary["reason"] == reason and back.summary["base_study_id"] == "BS29"
    assert back.summary["last_upload"]["analyses"] == 1
    # Recorded once; after that the article waits, and nothing is sent.
    plan, outcomes = _run(upload, ctx, catalog, ref)
    assert (plan.blocked, plan.pending, outcomes) == (1, [], [])
    assert len(_sent(vcr)) == 1


@pytest.mark.vcr
def test_a_taken_back_article_never_uploaded_has_nothing_to_retract(env, monkeypatch, evidence):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "30", evidence=evidence)
    sync, upload = build(["sync", "upload"], settings)
    _run(sync, ctx, catalog, ref)
    new = new_articles_root(settings) / ref.id
    assert new.exists()
    _take_back_space(catalog, ref)
    _, (back,) = _run(sync, ctx, catalog, ref)
    assert back.error == NO_TEXT and back.summary["removed"] == str(new) and not new.exists()
    _, (recorded,) = _run(upload, ctx, catalog, ref)
    assert (recorded.error, recorded.fingerprint) == (NO_TEXT, NO_TEXT)
    assert recorded.summary["base_study_id"] is None and "last_upload" not in recorded.summary
    plan, outcomes = _run(upload, ctx, catalog, ref)
    assert (plan.blocked, plan.reasons, outcomes) == (1, {}, [])


def test_notices_are_the_parsed_paper_s_corrections_and_a_notice_is_not_synced(
        env, monkeypatch, evidence):
    """Neurostore's ingester marks a paper retracted from its ParsedPaper's corrections;
    unanswered notices leave them out, which it reads as not looked up."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "41", "BS41", evidence=evidence)
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
    _, (skipped,) = _run(sync, ctx, catalog, ref)
    assert (skipped.status, skipped.error, skipped.summary["reason"]) == (
        Status.SKIPPED, RETRACTION_NOTICE, RETRACTION_NOTICE)
    plan, outcomes = _run(sync, ctx, catalog, ref)
    assert (plan.blocked, plan.reasons, outcomes) == (1, {RETRACTION_NOTICE: 1}, [])


def _dry(settings):
    return settings.model_copy(update={"dry_run": True})


def _dry_lines(settings):
    runs = Path(settings.data_root) / neurostore_ingest.DRY_RUN_DIR / "upload_runs"
    (log,) = runs.glob("*.jsonl")
    return [json.loads(line) for line in log.read_text().splitlines()]


def test_a_dry_run_writes_only_under_its_own_root_and_records_nothing(env, monkeypatch, evidence):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "24", "BS24", evidence=evidence)
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


def test_a_dry_run_of_a_missing_parse_never_reaches_the_upload_history(env, monkeypatch, evidence):
    """A missing (or invalid) parse in a dry run is the dry run's record, not the article's."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "25", "BS25", evidence=evidence)
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


def _analysed_from(settings, catalog, triage):
    """The fingerprint of analyses read from `triage`, and from the extraction and
    metadata the catalog holds."""
    stage = AnalysesStage(settings)
    extraction = stage._extraction_for(
        catalog.artifacts([triage.article_id], "extract").get(triage.article_id, {}), triage)
    metadata = catalog.artifact(triage.article_id, "metadata", "")
    return stage.fingerprint_for(triage, extraction,
                                 metadata_fingerprint(metadata, ANALYSES_READ))


def _icafz5(tmp_path, monkeypatch, catalog, evidence, *, analyses_fresh=False, unread=None,
            uploaded=True):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond",
                        role_model_table=TABLE_MODEL, neurostore_api_url=LOCAL)
    ref = catalog.register(Identifier(pmid="16139528", neurostore="NTKEuAF4Autf"), evidence)
    catalog.record([Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1005",
                            summary={"source": "ace", "tables": 3, "passed": 2,
                                     "passed_tables": list(ICAFZ5_TABLES)})])
    # The triage of 2026-10-02 passed one of the two tables today's passes.
    read_from = catalog.artifact(ref.id, "triage", "") if analyses_fresh else Artifact(
        ref.id, "triage", fingerprint="tr-1002",
        summary={"source": "ace", "tables": 3, "passed": 1, "passed_tables": ["2"]})
    catalog.record([
        Outcome(article_id=ref.id, stage="analyses", source="",
                fingerprint=_analysed_from(settings, catalog, read_from),
                payload=_icafz5_payload(),
                summary={"tables": 2, "coordinates": 24,
                         **({"unread": unread} if unread else {})}),
        # The legacy upload of 2026-10-03, which sent all 12.
        *([Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-legacy",
                   summary={"base_study_id": "NTKEuAF4Autf", "study_id": "ETMWa8HbYhqa",
                            "analyses": 12})] if uploaded else []),
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


def _handed_to_the_client(done, settings):
    """The gate let the parse through: the client was given the directory sync recorded,
    which holds no files here, since these sync records stand for what sync wrote."""
    record = done.summary["ingester"]
    assert (record["status"], record["study_id"]) == ("missing", "NTKEuAF4Autf")
    assert done.error.startswith("missing: ")
    assert str(settings.ns_pond_root / "NTKEuAF4Autf") in record["error"]["detail"]


@pytest.fixture()
def icafz5_env(served, token):  # noqa: F811 - the imported fixture
    """The role server's model card and the token, which `_icafz5`'s settings need."""


@pytest.mark.vcr
def test_a_parse_made_from_stale_table_analyses_is_held_back(
        tmp_path, monkeypatch, icafz5_env, evidence):
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog, evidence=evidence)
        upload = UploadStage(settings)
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
        _, outcomes = _run(dry_upload, Context(dry, catalog), catalog, ref)
        assert outcomes == []
        (line,) = _dry_lines(settings)
        assert (line["status"], line["error"]) == ("held back", held.error)
        assert catalog.artifact(ref.id, "upload", "").error == held.error


@pytest.mark.vcr
def test_once_the_tables_are_read_again_the_parse_goes_up(
        tmp_path, monkeypatch, icafz5_env, evidence):
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog, evidence=evidence)
        upload = UploadStage(settings)
        _run(upload, ctx, catalog, ref)
        # The analyses read again from today's triage; everything after them follows.
        triage = catalog.artifact(ref.id, "triage", "")
        catalog.record([Outcome(article_id=ref.id, stage="analyses", source="",
                                fingerprint=_analysed_from(settings, catalog, triage),
                                payload=_icafz5_payload(), summary={"tables": 2})])
        _run(_roles(settings, monkeypatch, _ByName(["Source"])), ctx, catalog, ref)
        _run(SpaceStage(settings), ctx, catalog, ref)
        _sync_as_written(ctx, catalog, ref, analyses=12)
        assert upload.held_back(ctx, [ref], {ref.id: catalog.artifact(ref.id, "sync", "")},
                                catalog.artifacts([ref.id], "upload")) == {}
        _, (done,) = _run(upload, ctx, catalog, ref)
        assert done.summary["gate"] is None
        _handed_to_the_client(done, settings)


@pytest.mark.vcr
def test_a_person_can_leave_the_tables_out_with_a_reason(
        tmp_path, monkeypatch, icafz5_env, evidence):
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog, evidence=evidence)
        upload = UploadStage(settings)
        _run(upload, ctx, catalog, ref)
        catalog.exclude([(ref.id, "source:tables", "MEG dipole fits, not results", "")])
        # The mark re-syncs the article, and its upload is planned again.
        _sync_as_written(ctx, catalog, ref, analyses=0)
        _, (done,) = _run(upload, ctx, catalog, ref)
        _handed_to_the_client(done, settings)
        assert done.summary["gate"]["excused"] == {"analyses": "MEG dipole fits, not results"}


@pytest.mark.parametrize("excuse", [None, "the third table is a figure, not coordinates"])
@pytest.mark.vcr
def test_a_table_left_unread_holds_back_unless_a_person_excuses_it(tmp_path, monkeypatch, excuse,
                                                                   icafz5_env, evidence):
    """The analyses ran, fresh, but one table's raw content was missing: its analyses are
    not in the parse, so it is not sent unless a person leaves the tables out."""
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog, analyses_fresh=True,
                                     unread={"t3": "raw content missing"}, evidence=evidence)
        if excuse:
            catalog.exclude([(ref.id, "source:tables", excuse, "")])
            _sync_as_written(ctx, catalog, ref, analyses=0)
        upload = UploadStage(settings)
        _, (done,) = _run(upload, ctx, catalog, ref)
        if excuse:
            _handed_to_the_client(done, settings)
            assert done.summary["gate"] == {"excused": {"analyses": excuse}}
        else:
            assert "ingester" not in done.summary and done.error == (
                f"{GATED}: tables not read: raw content missing; the parse holds 0 analyses, "
                "the last upload 12 (12 missing)")
            assert done.summary["gate"]["upstream"] == {
                "analyses": "tables not read: raw content missing"}


@pytest.mark.vcr
def test_a_failed_table_reading_holds_back_a_parse_with_only_the_prose(env, monkeypatch, evidence):
    """The tables failed in the model, so the parse holds the prose's analyses alone."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "26", "BS26", evidence=evidence)
    _run(SyncStage(settings), ctx, catalog, ref)
    catalog.record([
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-2",
                summary={"source": "pubget", "tables": 1, "passed": 1, "passed_tables": ["t1"]}),
        Outcome.failure(ref.id, "analyses", "", "UnreadableAnswer: Expecting ',' delimiter: "
                        "line 1 column 1481 (char 1480)",
                        fingerprint=_analysed_from(settings, catalog, Artifact(
                            ref.id, "triage", fingerprint="tr-2",
                            summary={"source": "pubget", "passed_tables": ["t1"]}))),
    ])
    upload = UploadStage(settings)
    _, (held,) = _run(upload, ctx, catalog, ref)
    # The parse was synced from the triage before, which passed no table: sync is stale too.
    assert held.error == (
        f"{GATED}: analyses failed: UnreadableAnswer: Expecting ',' delimiter: line 1 column "
        "1481 (char 1480); sync stale; the parse holds 1 analyses, and nothing was uploaded "
        "before")


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


def _tables_ctx(tmp_path, *records, evidence):
    catalog = Catalog.open(tmp_path / "k")
    ref = catalog.register(Identifier(pmid="40"), evidence)
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
def test_tables_are_empty_only_when_read_whole(tmp_path, analyses, records, expected, evidence):
    ctx, ref = _tables_ctx(tmp_path, *records, evidence=evidence)
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


def _two_synced(env, monkeypatch, evidence):
    settings, catalog, _ = env
    refs = []
    for pmid in ("27", "31"):
        ctx, ref = _prose_article(env, monkeypatch, pmid, f"BS{pmid}", evidence=evidence)
        _run(SyncStage(settings), ctx, catalog, ref)
        refs.append(ref)
    return ctx, refs


def _stops(upload, ctx, catalog, refs, caplog):
    """Both articles' plans, run as two batches: the first stops, the second sends nothing."""
    outcomes = []
    for ref in refs:
        plan = upload.plan(ctx, [ref], catalog.artifacts([ref.id], "upload"),
                           catalog.artifacts([ref.id], "sync"))
        assert len(plan.pending) == 1
        outcomes += list(upload.execute(ctx, plan.pending))
    assert outcomes == []
    assert catalog.artifacts([r.id for r in refs], "upload") == {}
    assert "upload stopped" in caplog.text and "1 article(s) left pending" in caplog.text
    # Pending still: a later run plans both again.
    for ref in refs:
        plan = upload.plan(ctx, [ref], {}, catalog.artifacts([ref.id], "sync"))
        assert len(plan.pending) == 1


@pytest.mark.vcr(
    str(Path(__file__).parents[1] / "cassettes" / "neurostore_ingest"
        / "test_a_key_the_ingester_refuses_is_the_paper_s_failure[KEY_MISMATCH].yaml"),
    match_on=["method", "scheme", "host", "port", "path", "query"])
def test_a_key_the_ingester_refuses_is_recorded_as_the_article_s_failure_with_its_code(
        env, monkeypatch, evidence):
    """neurostore's real KEY_MISMATCH refusal (its answer to another parse: this client's
    own key check would not send one it derives differently) is a permanent failure of
    that article whose reason names the code; nothing stops."""
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "62", "BS62", evidence=evidence)
    sync, upload = build(["sync", "upload"], settings)
    _run(sync, ctx, catalog, ref)
    _, (done,) = _run(upload, ctx, catalog, ref)
    assert done.status is Status.PERMANENT
    assert done.error == ("refused: KEY_MISMATCH: Analysis keys must be derived from their "
                          "name, space and points.")
    assert upload._stopped is None


def _upload_together(upload, ctx, catalog, refs):
    """One batch: the plan of every article, then one execute."""
    ids = [ref.id for ref in refs]
    plan = upload.plan(ctx, refs, catalog.artifacts(ids, "upload"), catalog.artifacts(ids, "sync"))
    outcomes = list(upload.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, {o.article_id: o for o in outcomes}


def _recorded_body(name):
    import yaml

    found = yaml.safe_load((CASSETTES / name).read_text())["interactions"]
    return [json.loads(i["request"]["body"]) for i in found if i["request"]["method"] == "POST"]


@pytest.mark.vcr(match_on=["method", "scheme", "host", "port", "path", "query"])
@pytest.mark.parametrize("first, status, answers", [
    ("refused", Status.PERMANENT, 2),
    # five tries, the health check, and the next paper
    ("blocked", Status.FAILED, neurostore_ingest.RETRIES + 3),
], ids=["refused", "blocked"])
def test_a_refused_or_blocked_paper_leaves_the_next_one_to_upload(env, monkeypatch, vcr, first,
                                                                  status, answers, evidence):
    """One paper's failure is that paper's: the stage goes on to the next. Blocked is
    retried later by backoff; refused is not. The cassettes replay real answers in the
    order sent, not matched on the body, so the second paper's parse is checked against
    the request its 200 answered."""
    settings, catalog, _ = env
    monkeypatch.setattr(neurostore_ingest.time, "sleep", lambda s: None)
    ctx, failing = _prose_article(env, monkeypatch, "60",
                                  "doesnotexist1" if first == "refused" else "BS60",
                                          evidence=evidence)
    _, sent = _prose_article(env, monkeypatch, "21", "BS21", evidence=evidence)
    sync, upload = build(["sync", "upload"], settings)
    for ref in (failing, sent):
        _run(sync, ctx, catalog, ref)
    _, done = _upload_together(upload, ctx, catalog, [failing, sent])
    upload.finish()
    assert (done[failing.id].status, done[failing.id].error.split(":")[0]) == (status, first)
    assert done[sent.id].status is Status.OK, done[sent.id].error
    played = _neurostore_plays(vcr)
    assert (sum(played), all(played)) == (answers, True)
    (answered,) = _recorded_body(
        "test_sync_writes_the_parse_with_no_upload_and_upload_sends_what_it_wrote.yaml")
    assert without_input_fingerprints(answered) == without_input_fingerprints(
        {name: json.loads((settings.ns_pond_root / "BS21" / "parse" / f"{name}.json").read_text())
         for name in ("coordinate_parse", "parsed_paper")})
    # Neither is tried again in this run: one is fresh, the other waits out its retry.
    plan, again = _upload_together(UploadStage(settings), ctx, catalog, [failing, sent])
    assert (plan.fresh, plan.permanent, again) == (1, 1, {})


@pytest.mark.vcr(str(CASSETTES / "test_sync_writes_the_parse_with_no_upload_and_upload_sends_"
                     "what_it_wrote.yaml"))
def test_a_paper_the_client_cannot_read_fails_alone_and_the_next_is_sent(
        env, monkeypatch, vcr, evidence):
    """An error the client does not turn into an outcome is that paper's failure, retried
    by backoff, and the stage goes on."""
    settings, catalog, _ = env
    ctx, broken = _prose_article(env, monkeypatch, "61", "BS61", evidence=evidence)
    _, sent = _prose_article(env, monkeypatch, "21", "BS21", evidence=evidence)
    sync, upload = build(["sync", "upload"], settings)
    for ref in (broken, sent):
        _run(sync, ctx, catalog, ref)
    parse = settings.ns_pond_root / "BS61" / "parse" / "coordinate_parse.json"
    parse.unlink()
    parse.mkdir()  # unreadable, and not missing
    _, done = _upload_together(upload, ctx, catalog, [broken, sent])
    upload.finish()
    assert done[broken.id].status is Status.FAILED
    assert done[broken.id].error.startswith("IsADirectoryError: ")
    assert done[sent.id].status is Status.OK and sum(_neurostore_plays(vcr)) == 1


@pytest.mark.vcr
def test_a_parse_with_no_analysis_never_uploaded_is_skipped_once(tmp_path, monkeypatch,
                                                                 icafz5_env, evidence):
    """The stage did its job: a failure would be retried on every run and never succeed,
    and an upload would create a study claiming the paper reports no coordinates."""
    with Catalog.open(tmp_path / "k") as catalog:
        settings, ctx, ref = _icafz5(tmp_path, monkeypatch, catalog, analyses_fresh=True,
                                     uploaded=False, evidence=evidence)
        upload = UploadStage(settings)
        _, (done,) = _run(upload, ctx, catalog, ref)
        assert (done.status, done.summary["reason"]) == (Status.SKIPPED, "no analyses to upload")
        plan, outcomes = _run(upload, ctx, catalog, ref)
        assert (plan.skipped, outcomes) == (1, [])


@RECORDS
def test_a_token_neurostore_refuses_stops_the_upload_and_records_nothing(env, monkeypatch,
                                                                         caplog, vcr, evidence):
    """Recorded with NEUROSTORE_TOKEN set to a token the local neurostore does not know."""
    settings, catalog, _ = env
    ctx, refs = _two_synced(env, monkeypatch, evidence=evidence)
    upload = UploadStage(settings)
    _stops(upload, ctx, catalog, refs, caplog)
    assert "NeurostoreAuthError" in caplog.text and "with 401" in caplog.text
    assert len(_sent(vcr)) == 1
    assert list(settings.ns_pond_root.rglob(neurostore_ingest.ARTIFACT_NAME)) == []


def test_a_down_neurostore_stops_the_upload_and_records_nothing(
        env, monkeypatch, caplog, evidence):
    settings, catalog, _ = env
    ctx, refs = _two_synced(env, monkeypatch, evidence=evidence)
    down = settings.model_copy(update={"neurostore_api_url": f"http://127.0.0.1:{_closed_port()}"})
    upload = UploadStage(down)
    monkeypatch.setattr(neurostore_ingest.time, "sleep", lambda s: None)
    _stops(upload, Context(down, catalog), catalog, refs, caplog)
    assert "NeurostoreUnavailable" in caplog.text
    assert list(settings.ns_pond_root.rglob(neurostore_ingest.ARTIFACT_NAME)) == []


def test_a_stop_leaves_pending_the_article_it_stopped_at_and_every_one_after(env, monkeypatch,
                                                                            caplog, evidence):
    """The first paper fails alone, before anything is sent; neurostore is down for the
    second, so the stage stops there with one article left."""
    settings, catalog, _ = env
    ctx, (broken, stopped) = _two_synced(env, monkeypatch, evidence=evidence)
    parse = settings.ns_pond_root / "BS27" / "parse" / "coordinate_parse.json"
    parse.unlink()
    parse.mkdir()
    down = settings.model_copy(update={"neurostore_api_url": f"http://127.0.0.1:{_closed_port()}"})
    monkeypatch.setattr(neurostore_ingest.time, "sleep", lambda s: None)
    _, done = _upload_together(UploadStage(down), Context(down, catalog), catalog,
                               [broken, stopped])
    assert list(done) == [broken.id]
    assert done[broken.id].error.startswith("IsADirectoryError: ")
    assert "NeurostoreUnavailable" in caplog.text and "1 article(s) left pending" in caplog.text


@pytest.mark.vcr
def test_no_token_stops_the_upload_before_anything_is_sent(env, monkeypatch, caplog, evidence):
    settings, catalog, _ = env
    ctx, refs = _two_synced(env, monkeypatch, evidence=evidence)
    monkeypatch.delenv("NEUROSTORE_TOKEN")
    _stops(UploadStage(settings), ctx, catalog, refs, caplog)
    assert "no neurostore token" in caplog.text


@pytest.mark.vcr
def test_with_no_neurostore_named_every_article_waits(env, monkeypatch, evidence):
    settings, catalog, _ = env
    ctx, ref = _prose_article(env, monkeypatch, "33", "BS33", evidence=evidence)
    _run(SyncStage(settings), ctx, catalog, ref)
    unnamed = settings.model_copy(update={"neurostore_api_url": None})
    plan, outcomes = _run(UploadStage(unnamed), Context(unnamed, catalog), catalog, ref)
    assert (plan.blocked, plan.reasons, outcomes) == (1, {NO_API_URL: 1}, [])


def test_the_neurostore_api_is_part_of_the_upload_fingerprint(tmp_path):
    """A different API is a different neurostore: pointing the run at production after
    staging sends every parse there, rather than leaving them looking fresh."""
    synced = Artifact("a1", "sync", fingerprint="sy")
    staging, production = (
        UploadStage(Settings(data_root=tmp_path, neurostore_api_url=url)).fingerprint_for(synced)
        for url in ("https://staging.neurostore.xyz", "https://neurostore.org"))
    assert staging != production
