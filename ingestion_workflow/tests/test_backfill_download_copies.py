"""The offline backfill of each stored download's copy, on a real catalog."""

from __future__ import annotations

import importlib.util
import io
import sqlite3
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.clients.openalex import OpenAlexClient
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

COPIES = Path(__file__).resolve().parent / "data" / "copies"
MANUSCRIPT = COPIES / "pmc_tdm_manuscript"
WON = {"url": "https://www.biorxiv.org/content/1v1.full.pdf", "provider": "openalex",
       "location_type": "repository", "version": "submittedVersion", "outcome": "pdf"}
#: Production's untyped PDFs of 10.1111/epi.14017 and 10.1111/j.1460-9568.2007.05889.x:
#: OpenAlex lists a repository location of the first on UCL's site, and nothing of the
#: second on its author's.
UCL = {"url": "https://discovery.ucl.ac.uk/10062208/1/Adler%20et%20al.%20Topographic"
              "%20principles_Epilepsia_revised.pdf",
       "provider": "semantic_scholar", "location_type": "unknown", "outcome": "pdf"}
KOELSCH = {"url": "http://www.stefan-koelsch.de/papers/"
                  "Koelsch+_EuropJNeurosci_ECG_personality_fMRI_amygdala.pdf",
           "provider": "semantic_scholar", "location_type": "unknown", "outcome": "pdf"}
DOIS = {"ucl": "10.1111/epi.14017", "koelsch": "10.1111/j.1460-9568.2007.05889.x"}

pytestmark = [pytest.mark.vcr, pytest.mark.default_cassette("backfill_download_copies.yaml")]


def _download(article_id, source, files=(), attempts=()):
    result = DownloadResult(
        identifier=Identifier(doi=DOIS.get(article_id, f"10.1/{article_id}")), source=source,
        success=True, files=[DownloadedFile(p, FileType.XML, "text/xml", source) for p in files],
        url_attempts=list(attempts))
    return result.to_dict()


@pytest.fixture()
def openalex():
    return OpenAlexClient(email="test@example.com")


@pytest.fixture()
def catalog_root(tmp_path):
    """Downloads stored as production holds them: migrated ones with no fingerprint, a PDF
    labelled by the old version field, two untyped PDFs, one already recorded, one failed,
    one whose payload is gone, one whose file is gone."""
    root = tmp_path / "cat"
    lost = tmp_path / "lost" / "article.xml"
    lost.parent.mkdir()
    lost.write_bytes((COPIES / "pubget_manuscript" / "article.xml").read_bytes())
    with Catalog.open(root) as catalog:
        ids = {name: catalog.register(Identifier(doi=DOIS.get(name, f"10.1/{name}"))).id
               for name in ("pmc", "elsevier", "pdf", "ucl", "koelsch", "recorded", "failed",
                            "gone", "lost")}
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
            *(Outcome(ids[name], "download", "pdf", summary={"files": 1},
                      payload=_download(name, DownloadSource.PDF, attempts=[attempt]))
              for name, attempt in (("ucl", UCL), ("koelsch", KOELSCH))),
            Outcome(ids["recorded"], "download", "ace",
                    summary={"copy": {"route": "publisher", "access": "unrecorded"}},
                    payload=_download("recorded", DownloadSource.ACE)),
            Outcome.failure(ids["failed"], "download", "pubget", "no full text"),
            Outcome(ids["gone"], "download", "springer", summary={"migrated": True},
                    payload=_download("gone", DownloadSource.SPRINGER)),
            Outcome(ids["lost"], "download", "pubget", summary={"migrated": True},
                    payload=_download("lost", DownloadSource.PUBGET, [lost])),
        ])
        gone = catalog.artifact(ids["gone"], "download", "springer")
        catalog.blobs._path(gone.blob).unlink()
    lost.unlink()
    return root, ids


#: Every ok row of `catalog_root` is counted once.
DRY_RUN = {
    "pmc\tpmc\tam\ttdm_grant": 1,
    "elsevier\tpublisher\tvor\ttdm_grant": 1,
    "pdf\tpreprint_server\tsubmitted\tpublic": 1,
    "pdf\trepository\t-\tpublic": 1,
    "pdf\tunrecorded\t-\tpublic": 1,
    "already recorded": 1,
    "left: payload blob missing": 1,
    "left: OSError": 1,
}


def _rows(db):
    with sqlite3.connect(db) as conn:
        return {(r[0], r[1]): r[1:] for r in conn.execute(
            "SELECT article_id, source, status, fingerprint, summary, updated_at FROM artifacts")}


def test_a_dry_run_counts_each_copy_and_writes_nothing(catalog_root, openalex):
    root, ids = catalog_root
    before = _rows(root / "catalog.sqlite")
    out = io.StringIO()

    counts = backfill.run(root, openalex=openalex, out=out)

    assert _rows(root / "catalog.sqlite") == before
    assert counts == DRY_RUN
    assert f"left\t{ids['gone']}\tspringer\tpayload blob missing" in out.getvalue()
    assert f"left\t{ids['lost']}\tpubget\tOSError" in out.getvalue()
    assert "dry run, nothing written" in out.getvalue()


def test_every_row_is_counted_across_chunks(catalog_root, openalex, monkeypatch):
    root, _ = catalog_root
    monkeypatch.setattr(backfill, "CHUNK", 2)

    assert backfill.run(root, openalex=openalex, out=io.StringIO()) == DRY_RUN


def test_apply_needs_a_new_backup(catalog_root, openalex, tmp_path):
    root, _ = catalog_root
    with pytest.raises(SystemExit):
        backfill.run(root, openalex=openalex, apply=True)
    taken = tmp_path / "taken.sqlite"
    taken.write_text("")
    with pytest.raises(SystemExit):
        backfill.run(root, openalex=openalex, apply=True, backup=taken)


def test_apply_records_each_copy_in_its_summary_and_nothing_else(catalog_root, openalex,
                                                                 tmp_path):
    root, ids = catalog_root
    before = _rows(root / "catalog.sqlite")

    counts = backfill.run(root, openalex=openalex, apply=True, backup=tmp_path / "before.sqlite",
                          out=io.StringIO())

    assert counts == DRY_RUN
    after = _rows(root / "catalog.sqlite")
    with Catalog.open(root) as catalog:
        def summary(name, source):
            return catalog.artifact(ids[name], "download", source).summary

        assert summary("pmc", "pmc") == {
            "migrated": True, "copy": {"route": "pmc", "version": "am", "access": "tdm_grant"}}
        assert summary("pdf", "pdf") == {"files": 1, "url_attempts": [WON], "copy": {
            "route": "preprint_server", "version": "submitted", "access": "public",
            "url": WON["url"], "preprint_server": "biorxiv"}}
        assert summary("ucl", "pdf") == {"files": 1, "copy": {
            "route": "repository", "access": "public", "url": UCL["url"]}}
        assert summary("koelsch", "pdf")["copy"]["route"] == "unrecorded"
    written = {(ids[name], source) for name, source in (
        ("pmc", "pmc"), ("elsevier", "elsevier"), ("pdf", "pdf"), ("ucl", "pdf"),
        ("koelsch", "pdf"))}
    assert {key for key in before if before[key] != after[key]} == written
    for key in written:  # status, fingerprint and updated_at stay
        status, fp, _, updated = after[key][1:]
        assert (status, fp, updated) == (before[key][1], before[key][2], before[key][4])
    assert _rows(tmp_path / "before.sqlite") == before

    again = backfill.run(root, openalex=openalex, out=io.StringIO())
    assert again == {"already recorded": 6, "left: payload blob missing": 1, "left: OSError": 1}


def test_a_row_written_while_the_copies_were_read_is_left_alone(catalog_root, openalex,
                                                                monkeypatch, tmp_path):
    """The pipeline refreshes the pmc download between the read and the write."""
    root, ids = catalog_root
    read = backfill.copies_of
    refreshed = {"files": 2, "copy": {"route": "pmc", "version": "vor", "access": "public"}}

    def refreshed_meanwhile(*args):
        found = read(*args)
        with Catalog.open(root) as catalog:
            download = catalog.artifact(ids["pmc"], "download", "pmc")
            catalog.record([Outcome(ids["pmc"], "download", "pmc", fingerprint="fp-new",
                                    summary=refreshed, payload=catalog.payload(download))])
        return found

    monkeypatch.setattr(backfill, "copies_of", refreshed_meanwhile)
    out = io.StringIO()

    counts = backfill.run(root, openalex=openalex, apply=True, backup=tmp_path / "b.sqlite",
                          out=out)

    with Catalog.open(root) as catalog:
        assert catalog.artifact(ids["pmc"], "download", "pmc").summary == refreshed
    assert counts == {**{k: n for k, n in DRY_RUN.items() if not k.startswith("pmc")},
                      "left: written meanwhile": 1}
    assert f"left\t{ids['pmc']}\tpmc\twritten meanwhile" in out.getvalue()


class _Down(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(503)
        self.end_headers()

    def log_message(self, *args):
        pass


def test_rows_whose_openalex_lookup_failed_are_left_unwritten_for_a_rerun(catalog_root,
                                                                         tmp_path):
    root, ids = catalog_root
    server = HTTPServer(("localhost", 0), _Down)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    down = OpenAlexClient(email="test@example.com")
    down.BASE_URL = f"http://localhost:{server.server_port}"
    out = io.StringIO()
    try:
        counts = backfill.run(root, openalex=down, apply=True, backup=tmp_path / "b.sqlite",
                              out=out)
    finally:
        server.shutdown()
        server.server_close()

    assert counts["left: OpenAlex lookup failed"] == 2
    assert "pdf\tunrecorded\t-\tpublic" not in counts
    with Catalog.open(root) as catalog:
        for name in ("ucl", "koelsch"):
            assert "copy" not in catalog.artifact(ids[name], "download", "pdf").summary
    assert f"left\t{ids['ucl']}\tpdf\tOpenAlex lookup failed" in out.getvalue()

    # with OpenAlex back, a rerun types them
    again = backfill.run(root, openalex=OpenAlexClient(email="test@example.com"),
                         out=io.StringIO())
    assert again["pdf\trepository\t-\tpublic"] == 1
    assert again["pdf\tunrecorded\t-\tpublic"] == 1
