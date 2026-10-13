"""A stored download that is not the article fails without a fetch, and its extraction with it."""

import gzip
import json
import sqlite3
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.cli.main import app
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import DownloadStage
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint
from ingestion_workflow.pipeline.stages.triage import TriageStage
from ingestion_workflow.services import download_revalidate as revalidate
from typer.testing import CliRunner

PAGES = Path(__file__).resolve().parents[1] / "data" / "download_pages"
PAYWALL = "bvqhehtd4k4d"
ARTICLE = "onv6piwppvwo"


class AnySource(BaseExtractor):
    """A source's extractor, to next_source: it asks only whether the article is addressable."""


def page(tmp_path, name):
    path = tmp_path / "pages" / f"{name}.html"
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(gzip.decompress((PAGES / f"{name}.html.gz").read_bytes()))
    return path


@pytest.fixture()
def env(tmp_path):
    gate = tmp_path / "gate.joblib"
    gate.write_bytes(b"a fitted gate")
    settings = Settings(
        data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k",
        download_sources=["pubget", "ace", "pdf"], coordinate_gate_path=gate,
    )
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog


def downloaded(catalog, settings, ref, source, path, file_type=FileType.HTML):
    fp = DownloadStage(settings).fingerprint_for(DownloadSource(source))
    result = DownloadResult(ref.identifier, DownloadSource(source), True, files=[
        DownloadedFile(path, file_type, "text/html", DownloadSource(source))])
    catalog.record([Outcome(ref.id, "download", source, Status.OK, fp, result.to_dict(),
                            summary={"files": 1})])
    return catalog.artifact(ref.id, "download", source)


def extracted(catalog, ref, download, tables, has_text=True):
    catalog.record([Outcome(ref.id, "extract", download.source, Status.OK,
                            extraction_fingerprint(download.source, download), {"tables": []},
                            summary={"tables": tables, "has_text": has_text})])


def article(catalog, settings, tmp_path, pmid, name, *, pubget=False, pubget_text=True):
    """An article whose ACE page is `name`, extracted, with a pubget XML download if asked."""
    ref = catalog.register(Identifier(pmid=pmid))
    ace = downloaded(catalog, settings, ref, "ace", page(tmp_path, name))
    extracted(catalog, ref, ace, tables=2)
    if pubget:
        xml = tmp_path / f"{pmid}.xml"
        xml.write_text("<article/>")
        pubget = downloaded(catalog, settings, ref, "pubget", xml, FileType.XML)
        extracted(catalog, ref, pubget, tables=0, has_text=pubget_text)
    catalog.record([Outcome(ref.id, "metadata", "", Status.OK, "md-1", {"title": "t"})])
    return ref


def test_a_bad_stored_page_fails_and_its_extraction_with_it(env, tmp_path):
    settings, catalog = env
    bad = article(catalog, settings, tmp_path, "1", PAYWALL, pubget=True)
    good = article(catalog, settings, tmp_path, "2", ARTICLE)

    found = revalidate.scan(catalog, [bad.id, good.id])
    assert found.checked == 2  # the pubget XML holds no page
    assert [(b.article_id, b.source, b.reason) for b in found.bad] == [(bad.id, "ace", "paywall")]
    revalidate.apply(catalog, found)

    download = catalog.artifact(bad.id, "download", "ace")
    assert download.status is Status.FAILED
    assert download.summary == {"bad_page": "paywall"}
    assert download.error.startswith("bad page (paywall)")
    extraction = catalog.artifact(bad.id, "extract", "ace")
    assert extraction.status is Status.FAILED
    assert extraction.error == "download failed validation: paywall"
    assert catalog.artifact(bad.id, "extract", "pubget").status is Status.OK
    assert catalog.artifact(good.id, "download", "ace").status is Status.OK
    assert catalog.artifact(good.id, "extract", "ace").status is Status.OK


def test_triage_moves_to_the_other_source(env, tmp_path):
    settings, catalog = env
    ref = article(catalog, settings, tmp_path, "1", PAYWALL, pubget=True)

    def judged():
        ctx = Context(settings, catalog)
        plan = TriageStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "triage"),
                                          catalog.artifacts([ref.id], "metadata"))
        return [w.upstream.source for w in plan.pending]

    assert judged() == ["ace"]  # more tables than pubget's
    revalidate.apply(catalog, revalidate.scan(catalog, [ref.id]))
    assert judged() == ["pubget"]


def test_an_article_with_no_other_download_plans_its_next_source(env, tmp_path):
    settings, catalog = env
    ref = article(catalog, settings, tmp_path, "1", PAYWALL)
    stage = DownloadStage(settings)
    stage._extractors = {s: AnySource() for s in DownloadSource}
    ctx = Context(settings, catalog)

    def pending():
        plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], "download"), {})
        return [w.source for w in plan.pending]

    assert pending() == []  # the stored ACE page counts as fresh
    found = revalidate.scan(catalog, [ref.id])
    assert revalidate.effects(ctx, stage, found)["next"] == {"download:pubget": 1}
    revalidate.apply(catalog, found)
    assert pending() == ["pubget"]


def test_effects_sort_articles_by_what_happens_next(env, tmp_path):
    settings, catalog = env
    switch = article(catalog, settings, tmp_path, "1", PAYWALL, pubget=True)
    replan = article(catalog, settings, tmp_path, "2", PAYWALL)
    article(catalog, settings, tmp_path, "3", ARTICLE)
    article(catalog, settings, tmp_path, "4", PAYWALL, pubget=True, pubget_text=False)
    catalog.record([Outcome(switch.id, "triage", "", Status.OK, "tr-1", {"tables": []}),
                    Outcome(replan.id, "passages", "", Status.FAILED, "p-1")])
    stage = DownloadStage(settings)
    stage._extractors = {s: AnySource() for s in DownloadSource}
    ctx = Context(settings, catalog)
    # every other source failed for good
    catalog.record([Outcome.failure(replan.id, "download", s, "no full text", permanent=True)
                    for s in ("pubget", "pdf")])

    found = revalidate.scan(catalog, list(catalog.all_article_ids()))
    effects = revalidate.effects(ctx, stage, found)
    # ACE itself may be tried again; pubget's extraction holds no text and stays as it is
    assert effects["next"] == {"switch": 1, "download:ace": 1, "no_text": 1}
    assert effects["stale"] == {"metadata": 3, "triage": 1}  # a failed passages made nothing

    # an article whose triage judges another source keeps it
    kept = article(catalog, settings, tmp_path, "5", PAYWALL, pubget=True)
    pubget = catalog.artifact(kept.id, "download", "pubget")
    extracted(catalog, kept, pubget, tables=3)
    effects = revalidate.effects(ctx, stage, revalidate.scan(catalog, [kept.id]))
    assert (effects["next"], effects["stale"]) == ({"kept": 1}, {})


def test_running_again_finds_nothing_new(env, tmp_path):
    settings, catalog = env
    ref = article(catalog, settings, tmp_path, "1", PAYWALL, pubget=True)
    first = revalidate.scan(catalog, [ref.id])
    # a run stopped after the download was failed and before its extraction was
    revalidate.apply(catalog, revalidate.Revalidation(bad=first.bad))
    assert catalog.artifact(ref.id, "extract", "ace").status is Status.OK

    second = revalidate.scan(catalog, [ref.id])
    assert second.bad == []
    assert [(e.source, reason) for e, reason in second.extractions] == [("ace", "paywall")]
    revalidate.apply(catalog, second)
    assert catalog.artifact(ref.id, "extract", "ace").status is Status.FAILED

    third = revalidate.scan(catalog, [ref.id])
    assert (third.bad, third.extractions, third.missing) == ([], [], [])
    assert revalidate.apply(catalog, third) == 0


def statuses(root):
    with sqlite3.connect(root) as conn:
        return sorted(conn.execute(
            "SELECT stage, source, status FROM artifacts WHERE stage != 'metadata'"))


def test_dry_run_writes_nothing_and_apply_backs_up_first(tmp_path, monkeypatch):
    from ingestion_workflow.extractors import ace_extractor

    monkeypatch.setattr(ace_extractor, "prepare_node", lambda settings: None)
    monkeypatch.setattr(DownloadStage, "extractor", lambda self, source: AnySource())
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", download_sources=["pubget", "ace", "pdf"])
    with Catalog.open(settings.catalog_root) as catalog:
        article(catalog, settings, tmp_path, "1", PAYWALL, pubget=True)
    config = tmp_path / "settings.yaml"
    config.write_text(json.dumps({
        "data_root": str(settings.data_root), "cache_root": str(settings.cache_root),
        "catalog_root": str(settings.catalog_root), "download_sources": ["pubget", "ace", "pdf"],
        "log_to_file": False, "show_progress": False, "max_workers": 1,
    }))
    db = settings.catalog_root / "catalog.sqlite"
    before = statuses(db)
    runner = CliRunner()

    dry = runner.invoke(app, ["revalidate-downloads", "-c", str(config)])
    assert dry.exit_code == 0, dry.output
    assert "paywall" in dry.output and "switch 1" in dry.output
    assert statuses(db) == before

    no_backup = runner.invoke(app, ["revalidate-downloads", "--apply", "-c", str(config)])
    assert no_backup.exit_code != 0 and "--backup" in no_backup.output
    assert statuses(db) == before

    backup = tmp_path / "backup" / "catalog.sqlite"
    applied = runner.invoke(app, ["revalidate-downloads", "--apply", "--backup", str(backup),
                                  "-c", str(config)])
    assert applied.exit_code == 0, applied.output
    assert statuses(backup) == before
    assert ("download", "ace", "failed") in statuses(db)
    assert ("extract", "ace", "failed") in statuses(db)

    again = runner.invoke(app, ["revalidate-downloads", "--apply", "--backup", str(backup),
                                "-c", str(config)])
    assert again.exit_code != 0  # an existing backup is never overwritten


def test_a_relative_page_path_is_read_beside_the_catalog_wherever_the_run_starts(
        env, tmp_path, monkeypatch):
    settings, catalog = env
    ref = catalog.register(Identifier(pmid="9"))
    relative = Path("pages") / f"{PAYWALL}.html"
    page(tmp_path, PAYWALL)  # tmp_path is where the catalog root sits
    fp = DownloadStage(settings).fingerprint_for(DownloadSource.ACE)
    result = DownloadResult(ref.identifier, DownloadSource.ACE, True, files=[
        DownloadedFile(relative, FileType.HTML, "text/html", DownloadSource.ACE)])
    catalog.record([Outcome(ref.id, "download", "ace", Status.OK, fp, result.to_dict())])
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    found = revalidate.scan(catalog, [ref.id])
    assert (found.missing, [b.reason for b in found.bad]) == ([], ["paywall"])
    assert found.bad[0].page == str(tmp_path / relative)


def test_apply_leaves_a_row_another_run_rewrote_since_the_scan(env, tmp_path):
    settings, catalog = env
    ref = article(catalog, settings, tmp_path, "1", PAYWALL)
    found = revalidate.scan(catalog, [ref.id])
    # an `ingest run` extracts again meanwhile
    with sqlite3.connect(settings.catalog_root / "catalog.sqlite") as conn:
        conn.execute("UPDATE artifacts SET updated_at='later' WHERE stage='extract'")
    assert revalidate.apply(catalog, found) == 1
    assert found.changed == 1
    assert catalog.artifact(ref.id, "download", "ace").status is Status.FAILED
    assert catalog.artifact(ref.id, "extract", "ace").status is Status.OK


def test_apply_checks_the_backup_path_before_scanning_and_skips_an_empty_run(
        tmp_path, monkeypatch):
    from ingestion_workflow.services import download_revalidate as module

    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k")
    Catalog.open(settings.catalog_root).close()
    config = tmp_path / "settings.yaml"
    config.write_text(json.dumps({
        "data_root": str(settings.data_root), "cache_root": str(settings.cache_root),
        "catalog_root": str(settings.catalog_root), "log_to_file": False,
        "show_progress": False, "max_workers": 1,
    }))
    scans = []
    monkeypatch.setattr(module, "scan", lambda *a, **k: scans.append(1) or module.Revalidation())
    taken = tmp_path / "taken.sqlite"
    taken.write_bytes(b"")
    runner = CliRunner()

    exists = runner.invoke(app, ["revalidate-downloads", "--apply", "--backup", str(taken),
                                 "-c", str(config)])
    assert exists.exit_code == 2 and "exists" in exists.output and scans == []
    assert not isinstance(exists.exception, FileExistsError)

    fresh = tmp_path / "new.sqlite"
    empty = runner.invoke(app, ["revalidate-downloads", "--apply", "--backup", str(fresh),
                                "-c", str(config)])
    assert empty.exit_code == 0, empty.output
    assert "nothing to record" in empty.output and not fresh.exists()


@pytest.fixture()
def two_bad_articles(tmp_path, monkeypatch):
    from ingestion_workflow.extractors import ace_extractor

    monkeypatch.setattr(ace_extractor, "prepare_node", lambda settings: None)
    monkeypatch.setattr(DownloadStage, "extractor", lambda self, source: AnySource())
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", download_sources=["pubget", "ace", "pdf"])
    with Catalog.open(settings.catalog_root) as catalog:
        article(catalog, settings, tmp_path, "1", PAYWALL)
        article(catalog, settings, tmp_path, "2", PAYWALL)
    config = tmp_path / "settings.yaml"
    config.write_text(json.dumps({
        "data_root": str(settings.data_root), "cache_root": str(settings.cache_root),
        "catalog_root": str(settings.catalog_root), "download_sources": ["pubget", "ace", "pdf"],
        "log_to_file": False, "show_progress": False, "max_workers": 1,
    }))
    return settings, config


def manifest_of(tmp_path, *pmids):
    from ingestion_workflow.models.ids import Identifiers

    path = tmp_path / "manifest.jsonl"
    Identifiers([Identifier(pmid=p) for p in pmids]).save(path)
    return path


def failed_downloads(settings):
    with sqlite3.connect(settings.catalog_root / "catalog.sqlite") as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE stage='download' AND status='failed'"
        ).fetchone()[0]


def test_a_manifest_limits_the_check_to_its_articles(tmp_path, two_bad_articles):
    settings, config = two_bad_articles
    runner = CliRunner()
    backup = tmp_path / "backup" / "catalog.sqlite"

    dry = runner.invoke(app, ["revalidate-downloads", "-m", str(manifest_of(tmp_path, "1")),
                              "-c", str(config)])
    assert dry.exit_code == 0, dry.output
    assert "checked 1 stored downloads" in dry.output and "bad pages: 1" in dry.output
    assert failed_downloads(settings) == 0

    applied = runner.invoke(app, ["revalidate-downloads", "--apply", "--backup", str(backup),
                                  "-m", str(manifest_of(tmp_path, "1")), "-c", str(config)])
    assert applied.exit_code == 0, applied.output
    assert failed_downloads(settings) == 1
    with Catalog.open(settings.catalog_root) as catalog:
        failed = catalog.artifact(catalog.resolve(Identifier(pmid="1")).id, "download", "ace")
        left = catalog.artifact(catalog.resolve(Identifier(pmid="2")).id, "download", "ace")
    assert failed.status == Status.FAILED and left.status == Status.OK


def test_without_a_manifest_every_article_is_checked(tmp_path, two_bad_articles):
    settings, config = two_bad_articles
    out = CliRunner().invoke(app, ["revalidate-downloads", "-c", str(config)])
    assert out.exit_code == 0, out.output
    assert "checked 2 stored downloads" in out.output and "bad pages: 2" in out.output
    assert "manifest" not in out.output


def test_a_manifest_entry_the_catalog_does_not_know_is_reported_and_not_registered(
    tmp_path, two_bad_articles
):
    settings, config = two_bad_articles
    out = CliRunner().invoke(app, ["revalidate-downloads", "-m",
                                   str(manifest_of(tmp_path, "1", "999")), "-c", str(config)])
    assert out.exit_code == 0, out.output
    assert "not in the catalog, not checked: 1" in out.output and "999" in out.output
    assert "checked 1 stored downloads" in out.output
    with Catalog.open(settings.catalog_root) as catalog:
        assert catalog.count_articles() == 2
