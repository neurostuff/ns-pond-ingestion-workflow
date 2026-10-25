"""Migration must import everything it can and never write to the source."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Status
from ingestion_workflow.migrate import _identifier_from_slug, migrate_caches
from ingestion_workflow.models.ids import Identifier


def legacy_download_index(path: Path, rows) -> None:
    """A stand-in for the pre-refactor DownloadIndex schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE downloads (
            slug TEXT PRIMARY KEY, payload_json BLOB NOT NULL, cached_at TEXT NOT NULL,
            metadata_json BLOB, pmid TEXT, pmcid TEXT, doi TEXT, source TEXT
        )
        """
    )
    conn.executemany("INSERT INTO downloads VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


def legacy_upload_index(path: Path, slugs) -> None:
    """Upload rows carried no identifier columns; only the composite slug."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE upload_entries (
            slug TEXT PRIMARY KEY, payload_json BLOB NOT NULL, cached_at TEXT NOT NULL,
            metadata_json BLOB, pmid TEXT, pmcid TEXT, doi TEXT,
            base_study_id TEXT, study_id TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO upload_entries VALUES (?,?,?,?,?,?,?,?,?)",
        [
            (slug, json.dumps({"slug": slug, "success": True}), "2026-01-01", None,
             None, None, None, f"base-{n}", f"study-{n}")
            for n, slug in enumerate(slugs)
        ],
    )
    conn.commit()
    conn.close()


@pytest.fixture()
def legacy(tmp_path):
    root = tmp_path / "old-cache"
    legacy_download_index(
        root / "download" / "pubget" / "index.sqlite",
        [
            (
                "37961286-10-1101-x-pmc10634720",
                json.dumps(
                    {
                        "identifier": {"pmcid": "PMC10634720"},
                        "files": [],
                        "source": "pubget",
                        "success": True,
                    }
                ),
                "2026-01-01",
                None,
                "37961286",
                "PMC10634720",
                "10.1101/x",
                "pubget",
            ),
            (
                "12345--pmc999",
                json.dumps(
                    {
                        "identifier": {"pmcid": "PMC999"},
                        "files": [],
                        "source": "pubget",
                        "success": True,
                    }
                ),
                "2026-01-01",
                None,
                "12345",
                "PMC999",
                None,
                "pubget",
            ),
        ],
    )
    legacy_upload_index(
        root / "upload" / "index.sqlite",
        ["37961286-10-1101-x-pmc10634720", "12345--pmc999"],
    )
    return root


def test_migration_imports_every_stage(legacy, tmp_path, evidence):
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
        report = migrate_caches(legacy, catalog)
        assert report.imported["download"] == 2
        assert report.imported["upload"] == 2
        assert catalog.count_articles() == 2


def test_upload_rows_are_recovered_from_their_slug(legacy, tmp_path, evidence):
    """Upload rows have no identifier columns; the slug is all there is."""
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
        migrate_caches(legacy, catalog)
        ref = catalog.resolve(Identifier(pmcid="PMC10634720"))
        upload = catalog.artifact(ref.id, "upload", "")
        assert upload is not None and upload.status is Status.OK


def test_download_and_upload_land_on_the_same_article(legacy, tmp_path, evidence):
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
        migrate_caches(legacy, catalog)
        ref = catalog.resolve(Identifier(pmid="37961286"))
        stages = {a.stage for a in catalog.artifacts_for_article(ref.id)}
        assert stages == {"download", "upload"}


def test_migration_is_idempotent(legacy, tmp_path, evidence):
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
        migrate_caches(legacy, catalog)
        migrate_caches(legacy, catalog)
        assert catalog.count_articles() == 2


def test_migration_never_writes_to_the_source(legacy, tmp_path, evidence):
    db = legacy / "download" / "pubget" / "index.sqlite"
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
        migrate_caches(legacy, catalog)
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
    assert not (legacy / "download" / "pubget" / "index.sqlite-wal").exists()


def test_dry_run_imports_nothing(legacy, tmp_path, evidence):
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
        report = migrate_caches(legacy, catalog, dry_run=True)
        assert report.imported["download"] == 2
        assert catalog.count_articles() == 0


def test_stage_filter_is_honoured(legacy, tmp_path, evidence):
    with Catalog.open(tmp_path / "cat", evidence=evidence) as catalog:
        report = migrate_caches(legacy, catalog, stages=["download"])
        assert "upload" not in report.imported


@pytest.mark.parametrize(
    "slug,expected",
    [
        (
            "37961286-10-1101-2023-10-21-563317-pmc10634720",
            {"pmid": "37961286", "pmcid": "PMC10634720"},
        ),
        ("pmc10634720", {"pmcid": "PMC10634720"}),
        ("34182786-10-1089-brain-2020-0943", {"pmid": "34182786"}),
        (
            "22645660-10-1098-rsob-120001-pmc3352092::table-1",
            {"pmid": "22645660", "pmcid": "PMC3352092"},
        ),
        # the slug above with a zero written in front of the PMCID
        (
            "22645660-10-1098-rsob-120001-pmc03352092",
            {"pmid": "22645660", "pmcid": "PMC3352092"},
        ),
        ("", {}),
    ],
)
def test_slug_parsing_recovers_what_it_can(slug, expected):
    assert _identifier_from_slug(slug) == expected


def test_migrated_artifacts_are_fresh_once_repair_stores_their_fingerprints(
        legacy, tmp_path, evidence):
    """The legacy cache recorded nothing about the versions behind its work, so a
    migrated row carries no fingerprint and is not fresh. `ingest repair` stores the
    one the plan expects, and then the pipeline considers the work done: migrating
    must not mean downloading the entire corpus again."""
    from ingestion_workflow.config import Settings
    from ingestion_workflow.pipeline import Context
    from ingestion_workflow.pipeline.refingerprint import refingerprint
    from ingestion_workflow.pipeline.stages import DownloadStage

    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        catalog_root=tmp_path / "cat",
    )
    with Catalog.open(settings.catalog_root, evidence=evidence) as catalog:
        migrate_caches(legacy, catalog, stages=["download"])
        stage = DownloadStage(settings)
        ctx = Context(settings, catalog)
        refs = [catalog.ref(article_id) for article_id in catalog.all_article_ids()]

        def plan():
            return stage.plan(ctx, refs, catalog.artifacts([r.id for r in refs], "download"), {})

        assert len(plan().pending) == len(refs)
        found = refingerprint(ctx, [stage], refs, catalog.empty_fingerprints())
        assert catalog.store_fingerprints(list(found.rewrites.values())) == len(refs)

        assert plan().pending == []
        assert plan().fresh == len(refs)


def test_refresh_still_reaches_migrated_artifacts(legacy, tmp_path, evidence):
    from ingestion_workflow.config import Settings
    from ingestion_workflow.pipeline import Context
    from ingestion_workflow.pipeline.stages import DownloadStage

    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        catalog_root=tmp_path / "cat",
    )
    with Catalog.open(settings.catalog_root, evidence=evidence) as catalog:
        migrate_caches(legacy, catalog, stages=["download"])
        stage = DownloadStage(settings)
        ctx = Context(settings, catalog, refresh=["download"])
        refs = [catalog.ref(article_id) for article_id in catalog.all_article_ids()]
        plan = stage.plan(ctx, refs, catalog.artifacts([r.id for r in refs], "download"), {})

        assert len(plan.pending) == len(refs)


@pytest.mark.vcr()
def test_the_migrate_command_says_to_run_repair(legacy, tmp_path):
    import ingestion_workflow.cli.main as cli
    from typer.testing import CliRunner

    config = tmp_path / "settings.yaml"
    config.write_text(json.dumps({
        "data_root": str(tmp_path / "data"), "cache_root": str(tmp_path / "cache"),
        "catalog_root": str(tmp_path / "cat"), "log_to_file": False, "show_progress": False,
        # registration checks what it adds against PubMed; nothing here is asked about
        "pubmed_email": "replay@example.com",
    }), encoding="utf-8")
    result = CliRunner().invoke(cli.app, ["migrate", str(legacy), "-s", "download",
                                          "--config", str(config)])
    assert result.exit_code == 0, result.output
    assert "run `ingest repair`" in result.output
