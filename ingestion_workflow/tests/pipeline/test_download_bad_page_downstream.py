"""A download failed as a page that is not the article takes its extraction's text out of use."""

import gzip
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    FileType,
)
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages import DownloadStage
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint
from ingestion_workflow.pipeline.stages.prose import ProseStage
from ingestion_workflow.pipeline.stages.triage import TriageStage

PAGES = Path(__file__).resolve().parents[1] / "data" / "download_pages"


class Source:
    def __init__(self, source, result):
        self.source, self.result = source, result

    def download(self, identifiers, dead_urls=None):
        return [self.result(identifier) for identifier in identifiers]


def page(tmp_path, name):
    path = tmp_path / f"{name}.html"
    path.write_bytes(gzip.decompress((PAGES / f"{name}.html.gz").read_bytes()))
    return path


def served(source, path):
    return lambda identifier: DownloadResult(identifier, source, True, files=[
        DownloadedFile(path, FileType.HTML, "text/html", source)])


@pytest.fixture()
def env(tmp_path):
    gate = tmp_path / "gate.joblib"
    gate.write_bytes(b"a fitted gate")
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", download_sources=["ace", "pdf"],
                        coordinate_gate_path=gate)
    with Catalog.open(settings.catalog_root) as catalog:
        ref = catalog.register(Identifier(pmid="123", doi="10.1/x"))
        stage = DownloadStage(settings)
        # the ACE download and extraction a run stored before the check existed
        ace = served(DownloadSource.ACE, page(tmp_path, "onv6piwppvwo"))(ref.identifier)
        catalog.record([Outcome(ref.id, "download", "ace", Status.OK,
                                stage.fingerprint_for(DownloadSource.ACE), ace.to_dict())])
        extracted(catalog, ref, "ace")
        catalog.record([Outcome(ref.id, "metadata", "", Status.OK, "md-1", {"title": "t"})])
        yield settings, catalog, ref, stage


def extracted(catalog, ref, source):
    download = catalog.artifact(ref.id, "download", source)
    catalog.record([Outcome(ref.id, "extract", source, Status.OK,
                            extraction_fingerprint(source, download), {"tables": []},
                            summary={"tables": 0, "has_text": True})])


def refresh(settings, catalog, ref, stage, ace_result, tmp_path):
    """`--refresh download:ace`: ACE answers with `ace_result`, and pdf has the article."""
    stage._extractors = {
        DownloadSource.ACE: Source(DownloadSource.ACE, ace_result),
        DownloadSource.PDF: Source(DownloadSource.PDF,
                                   served(DownloadSource.PDF, page(tmp_path, "ytpicngb4emb"))),
    }
    ctx = Context(settings, catalog, refresh=["download:ace"])
    work = Work(ref=ref, source="ace", fingerprint=stage.fingerprint_for(DownloadSource.ACE))
    outcomes = list(stage.execute(ctx, [work]))
    catalog.record(outcomes)
    return outcomes


def judged(settings, catalog, ref):
    plan = TriageStage(settings).plan(Context(settings, catalog), [ref],
                                      catalog.artifacts([ref.id], "triage"),
                                      catalog.artifacts([ref.id], "metadata"))
    return [w.upstream.source for w in plan.pending]


def test_a_refresh_that_hits_a_bad_page_fails_the_extraction_made_from_it(env, tmp_path):
    settings, catalog, ref, stage = env
    assert judged(settings, catalog, ref) == ["ace"]
    outcomes = refresh(settings, catalog, ref, stage,
                       served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    assert [(o.stage, o.source, o.status) for o in outcomes] == [
        ("download", "ace", Status.FAILED), ("extract", "ace", Status.FAILED),
        ("download", "pdf", Status.OK)]
    extraction = catalog.artifact(ref.id, "extract", "ace")
    assert extraction.error == "download failed validation: paywall"
    assert judged(settings, catalog, ref) == []  # pdf is not extracted yet: nothing to judge


def test_the_bad_page_text_never_beats_a_new_source(env, tmp_path):
    settings, catalog, ref, stage = env
    refresh(settings, catalog, ref, stage,
            served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    extracted(catalog, ref, "pdf")
    # An earlier stage left the ACE extraction OK: no newer text should lose to it on the tie.
    ace = catalog.artifact(ref.id, "extract", "ace")
    catalog.record([Outcome(ref.id, "extract", "ace", Status.OK, ace.fingerprint, {"tables": []},
                            summary={"tables": 0, "has_text": True})])
    assert judged(settings, catalog, ref) == ["pdf"]
    catalog.record([Outcome.failure(ref.id, "extract", "pdf", "timed out")])
    assert judged(settings, catalog, ref) == []  # not the ACE text as a fallback either


def test_ace_rejecting_the_page_itself_records_the_reason_and_where_the_page_went(env, tmp_path):
    settings, catalog, ref, stage = env
    moved = {"/cache/html/x/123.html": "/cache/html-quarantine/x/123.html"}

    def rejected(identifier):
        return DownloadResult(identifier, DownloadSource.ACE, False, moved_aside=moved,
                              error_message="ACE scrape produced invalid HTML: bad page "
                                            "(paywall): page says 'Buy this article'")

    first, second, third = refresh(settings, catalog, ref, stage, rejected, tmp_path)
    assert (first.status, first.summary) == (Status.FAILED, {"bad_page": "paywall",
                                                             "moved_aside": moved})
    assert (second.stage, second.error) == ("extract", "download failed validation: paywall")
    assert (third.source, third.status) == ("pdf", Status.OK)


def test_prose_does_not_read_passages_made_from_a_bad_page(env, tmp_path):
    settings, catalog, ref, stage = env
    catalog.record([Outcome(ref.id, "passages", "", Status.OK, "p-1", {"passages": []},
                            summary={"source": "ace", "passages": 1})])

    def plan():
        return ProseStage(settings).plan(Context(settings, catalog), [ref],
                                         catalog.artifacts([ref.id], "prose"),
                                         catalog.artifacts([ref.id], "passages"))

    assert len(plan().pending) == 1
    refresh(settings, catalog, ref, stage,
            served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    blocked = plan()
    assert (blocked.pending, blocked.blocked) == ([], 1)
    assert catalog.artifact(ref.id, "passages", "").status is Status.OK  # held, not taken back


def test_nothing_after_prose_reads_prose_made_from_a_bad_page(env, tmp_path):
    from ingestion_workflow.pipeline.stages.resolve import ResolveStage
    from ingestion_workflow.pipeline.stages.roles import RolesStage
    from ingestion_workflow.pipeline.stages.sync import SyncStage

    settings, catalog, ref = env[:3]
    settings.ns_pond_root = tmp_path / "pond"
    settings.prose_model = "nu-prose"  # roles reads resolve's sets, prose's among them
    catalog.record([
        Outcome(ref.id, "prose", "", Status.OK, "pr-1", {"analyses": []},
                summary={"source": "ace", "passages": 1, "coordinates": 2, "analyses": 1}),
        Outcome(ref.id, "resolve", "", Status.OK, "r-1", {"prose": {}},
                summary={"tables": 0, "prose_analyses": 1}),
        Outcome(ref.id, "roles", "", Status.OK, "ro-1", {}),
        Outcome(ref.id, "upload", "", Status.OK, "up-1", {}, summary={"base_study_id": "b1"}),
    ])

    def plans():
        ctx = Context(settings, catalog)
        out = {}
        for stage in (ResolveStage(settings), RolesStage(settings), SyncStage(settings)):
            plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                              catalog.artifacts([ref.id], stage.requires))
            out[stage.name] = (len(plan.pending), plan.reasons)
        return out

    before = plans()
    assert before["resolve"][0] == before["sync"][0] == 1
    refresh(*env, served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    reason = {"prose read from a downloaded page that is not the article": 1}
    assert plans() == {"resolve": (0, reason), "roles": (0, reason), "sync": (0, reason)}
    # held, not taken back
    assert {s: catalog.artifact(ref.id, s, "").status for s in ("prose", "resolve", "roles")} == {
        "prose": Status.OK, "resolve": Status.OK, "roles": Status.OK}


def test_a_later_plain_ace_failure_leaves_prose_and_resolve_blocked(env, tmp_path):
    from ingestion_workflow.pipeline.stages.resolve import ResolveStage

    settings, catalog, ref = env[:3]
    catalog.record([
        Outcome(ref.id, "passages", "", Status.OK, "p-1", {"passages": []},
                summary={"source": "ace", "passages": 1}),
        Outcome(ref.id, "prose", "", Status.OK, "pr-1", {"analyses": []},
                summary={"source": "ace", "passages": 1, "coordinates": 2, "analyses": 1}),
    ])
    refresh(*env, served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    # A later waterfall reaches ACE again; the page was moved aside, so ACE scrapes live
    # and is refused, which records the download with no bad page.
    catalog.record([Outcome.failure(ref.id, "download", "ace",
                                    "ACE scrape failed: HTTP 403 Forbidden")])
    assert catalog.artifact(ref.id, "download", "ace").summary == {}

    def pending(stage, upstream):
        plan = stage.plan(Context(settings, catalog), [ref],
                          catalog.artifacts([ref.id], stage.name),
                          catalog.artifacts([ref.id], upstream))
        return len(plan.pending), plan.blocked

    assert pending(ProseStage(settings), "passages") == (0, 1)
    assert pending(ResolveStage(settings), "prose") == (0, 1)


def test_an_extraction_failing_again_keeps_its_bad_page(env, tmp_path):
    from ingestion_workflow.pipeline.stages.extract import ExtractStage

    settings, catalog, ref = env[:3]
    refresh(*env, served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    stage = ExtractStage(settings)
    # a later download of the source, extracted again, fails some other way
    gone = catalog.artifact(ref.id, "download", "pdf")
    work = Work(ref=ref, source="ace", fingerprint="ex-2",
                upstream=type(gone)(ref.id, "download", "ace", Status.OK, "d", blob="missing"))
    (outcome,) = list(stage.execute(Context(settings, catalog), [work]))
    assert outcome.error == "download payload missing from blob store"
    assert outcome.summary == {"bad_page": "paywall"}
    # an extraction that never failed as a bad page gains nothing
    work = Work(ref=ref, source="pdf", fingerprint="ex-3",
                upstream=type(gone)(ref.id, "download", "pdf", Status.OK, "d", blob="missing"))
    (outcome,) = list(stage.execute(Context(settings, catalog), [work]))
    assert outcome.summary == {}


def test_triage_does_not_list_a_bad_page_extraction_as_skipped_for_want_of_text(
        env, tmp_path, monkeypatch):
    settings, catalog, ref, stage = env
    refresh(*env, served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    # An older run left the ACE extraction OK: no text, two tables. pdf has the text.
    ace = catalog.artifact(ref.id, "extract", "ace")
    pdf = catalog.artifact(ref.id, "download", "pdf")
    catalog.record([
        Outcome(ref.id, "extract", "ace", Status.OK, ace.fingerprint, {"tables": []},
                summary={"tables": 2, "has_text": False}),
        Outcome(ref.id, "extract", "pdf", Status.OK, extraction_fingerprint("pdf", pdf),
                ExtractedContent(slug=ref.identifier.slug, source=DownloadSource.PDF).to_dict(),
                summary={"tables": 1, "has_text": True}),
    ])
    triage = TriageStage(settings)
    monkeypatch.setattr(triage, "_judge_all", lambda jobs: [
        (job[0], [{"table_id": "t1", "passes": True, "points": 3}]) for job in jobs])
    plan = triage.plan(Context(settings, catalog), [ref], {},
                       catalog.artifacts([ref.id], "metadata"))
    (work,) = plan.pending
    assert work.upstream.source == "pdf"
    (outcome,) = list(triage.execute(Context(settings, catalog), [work]))
    assert "skipped_no_text" not in outcome.summary


def test_sync_still_retracts_an_article_whose_prose_came_from_a_bad_page(env, tmp_path):
    from ingestion_workflow.pipeline.stage import NO_TEXT
    from ingestion_workflow.pipeline.stages.sync import SyncStage

    settings, catalog, ref = env[:3]
    settings.ns_pond_root = tmp_path / "pond"
    catalog.record([
        Outcome(ref.id, "prose", "", Status.OK, "pr-1", {"analyses": []},
                summary={"source": "ace", "passages": 1}),
        Outcome(ref.id, "upload", "", Status.OK, NO_TEXT, {},
                summary={"base_study_id": "b1", "retracted": True}),
    ])
    refresh(*env, served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    stage = SyncStage(settings)
    plan = stage.plan(Context(settings, catalog), [ref], {}, catalog.artifacts([ref.id], "upload"))
    assert (len(plan.pending), plan.blocked) == (1, 0)


def test_metadata_does_not_reach_an_article_through_passages_from_a_bad_page(env, tmp_path):
    from ingestion_workflow.pipeline.stages.metadata import MetadataStage

    settings, catalog, ref = env[:3]
    settings.prose_model = "nu-prose"  # an article with no extraction is reached through passages
    catalog.record([Outcome(ref.id, "passages", "", Status.OK, "p-1", {"source": "ace"},
                            summary={"source": "ace", "passages": 1})])

    def plan():
        stage = MetadataStage(settings)
        return stage.plan(Context(settings, catalog), [ref],
                          catalog.artifacts([ref.id], "metadata"),
                          catalog.artifacts([ref.id], "extract"))

    assert len(plan().pending) == 1
    refresh(*env, served(DownloadSource.ACE, page(tmp_path, "bvqhehtd4k4d")), tmp_path)
    # a later plain ACE failure clears the download's bad page; the extraction keeps it
    catalog.record([Outcome.failure(ref.id, "download", "ace", "ACE scrape failed: HTTP 403")])
    blocked = plan()
    assert (blocked.pending, blocked.reasons) == (
        [], {"passages read from a downloaded page that is not the article": 1})
    assert catalog.artifact(ref.id, "passages", "").status is Status.OK
