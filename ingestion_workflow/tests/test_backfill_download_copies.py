"""The offline backfill of each stored download's copy, on a real catalog."""

from __future__ import annotations

import importlib.util
import io
import sqlite3
import sys
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.models import (
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    FileType,
    Identifier,
)

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backfill_download_copies.py"
spec = importlib.util.spec_from_file_location("backfill_download_copies", SCRIPT)
backfill = importlib.util.module_from_spec(spec)
# its workers' tasks are pickled by module name
sys.modules[spec.name] = backfill
spec.loader.exec_module(backfill)

MANUSCRIPT = Path(__file__).resolve().parent / "data" / "copies" / "pmc_tdm_manuscript"
WON = {"url": "https://www.biorxiv.org/content/1v1.full.pdf", "provider": "openalex",
       "location_type": "repository", "version": "submittedVersion", "outcome": "pdf"}


def _download(article_id, source, files=(), attempts=()):
    result = DownloadResult(
        identifier=Identifier(doi=f"10.1/{article_id}"), source=source, success=True,
        files=[DownloadedFile(p, FileType.XML, "text/xml", source) for p in files],
        url_attempts=list(attempts))
    return result.to_dict()


@pytest.fixture()
def catalog_root(tmp_path):
    """Downloads stored as production holds them: migrated ones with no fingerprint, a PDF
    labelled by the old version field, one already recorded, one failed, one whose payload
    is gone."""
    root = tmp_path / "cat"
    with Catalog.open(root) as catalog:
        ids = {name: catalog.register(Identifier(doi=f"10.1/{name}")).id
               for name in ("pmc", "elsevier", "pdf", "recorded", "failed", "gone")}
        catalog.record([
            Outcome(ids["pmc"], "download", "pmc", summary={"migrated": True},
                    payload=_download("pmc", DownloadSource.PMC,
                                      sorted(MANUSCRIPT.iterdir()))),
            Outcome(ids["elsevier"], "download", "elsevier", summary={"migrated": True},
                    payload=_download("elsevier", DownloadSource.ELSEVIER)),
            Outcome(ids["pdf"], "download", "pdf", fingerprint="fp-pdf",
                    summary={"files": 1, "url_attempts": [WON],
                             "article_version": "submittedVersion"},
                    payload={**_download("pdf", DownloadSource.PDF, attempts=[WON]),
                             "article_version": "submittedVersion"}),
            Outcome(ids["recorded"], "download", "ace",
                    summary={"copy": {"route": "publisher", "access": "public"}},
                    payload=_download("recorded", DownloadSource.ACE)),
            Outcome.failure(ids["failed"], "download", "pubget", "no full text"),
            Outcome(ids["gone"], "download", "springer", summary={"migrated": True},
                    payload=_download("gone", DownloadSource.SPRINGER)),
        ])
        gone = catalog.artifact(ids["gone"], "download", "springer")
        catalog.blobs._path(gone.blob).unlink()
    return root, ids


def _rows(root):
    with sqlite3.connect(root / "catalog.sqlite") as conn:
        return {(r[0], r[1]): r[1:] for r in conn.execute(
            "SELECT article_id, source, status, fingerprint, summary, updated_at FROM artifacts")}


def test_a_dry_run_counts_each_copy_and_writes_nothing(catalog_root):
    root, ids = catalog_root
    before = _rows(root)
    out = io.StringIO()

    counts = backfill.run(root, out=out)

    assert _rows(root) == before
    assert counts["pmc\tpmc\tam\ttdm_grant"] == 1
    assert counts["elsevier\tpublisher\tvor\ttdm_grant"] == 1
    assert counts["pdf\tpreprint_server\tsubmitted\tpublic"] == 1
    assert counts["already recorded"] == 1
    assert counts["left: payload blob missing"] == 1
    assert f"left\t{ids['gone']}\tspringer\tpayload blob missing" in out.getvalue()
    assert "dry run, nothing written" in out.getvalue()


def test_apply_needs_a_new_backup(catalog_root, tmp_path):
    root, _ = catalog_root
    with pytest.raises(SystemExit):
        backfill.run(root, apply=True)
    taken = tmp_path / "taken.sqlite"
    taken.write_text("")
    with pytest.raises(SystemExit):
        backfill.run(root, apply=True, backup=taken)


def test_apply_records_each_copy_in_its_summary_and_nothing_else(catalog_root, tmp_path):
    root, ids = catalog_root
    before = _rows(root)

    backfill.run(root, apply=True, backup=tmp_path / "before.sqlite", out=io.StringIO())

    after = _rows(root)
    with Catalog.open(root) as catalog:
        def summary(name, source):
            return catalog.artifact(ids[name], "download", source).summary

        assert summary("pmc", "pmc") == {
            "migrated": True, "copy": {"route": "pmc", "version": "am", "access": "tdm_grant"}}
        assert summary("pdf", "pdf") == {"files": 1, "url_attempts": [WON], "copy": {
            "route": "preprint_server", "version": "submitted", "access": "public",
            "url": WON["url"], "preprint_server": "biorxiv"}}
    changed = {key for key in before if before[key] != after[key]}
    assert changed == {(ids["pmc"], "pmc"), (ids["elsevier"], "elsevier"), (ids["pdf"], "pdf")}
    for key in changed:  # status, fingerprint and updated_at stay
        status, fp, _, updated = after[key][1:]
        assert (status, fp, updated) == (before[key][1], before[key][2], before[key][4])
    with sqlite3.connect(tmp_path / "before.sqlite") as kept:
        assert kept.execute("SELECT count(*) FROM artifacts").fetchone()[0] == len(before)

    again = backfill.run(root, out=io.StringIO())
    assert again["already recorded"] == 4
    assert again["left: payload blob missing"] == 1
    assert sum(n for key, n in again.items() if not key.startswith(("already", "left"))) == 0
