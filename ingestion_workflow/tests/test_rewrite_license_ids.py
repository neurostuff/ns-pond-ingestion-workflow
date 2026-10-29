"""The one-off respelling of the licence ids stored before study_schema 0.11.0, on a real
catalog."""

from __future__ import annotations

import importlib.util
import io
import sqlite3
import sys
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata
from pydantic import ValidationError

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "rewrite_license_ids.py"
spec = importlib.util.spec_from_file_location("rewrite_license_ids", SCRIPT)
rewrite = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = rewrite
spec.loader.exec_module(rewrite)

DOIS = {"alz": "10.1002/alz.12697", "scan": "10.1093/scan/nsw123",
        "seizure": "10.1016/j.seizure.2015.08.008", "failed": "10.1002/ceat.70128",
        "gone": "10.1016/j.engfracmech.2023.109223", "odd": "10.1002/stab.201590143"}
WILEY_PDF = "https://onlinelibrary.wiley.com/doi/pdfdirect/10.1002/alz.12697"
PMC_COPY = "https://pmc.ncbi.nlm.nih.gov/articles/PMC10518222"
#: OpenAlex's location of 10.1002/alz.12697's PDF, as the provider's raw record holds it: its
#: own `license` and `license_id` are the source's wording, never respelled.
OPENALEX = {"openalex": {"locations": [{
    "pdf_url": WILEY_PDF, "version": "acceptedVersion", "license": "cc-by-nc",
    "license_id": "https://openalex.org/licenses/cc-by-nc"}]}}

#: The licences of 10.1002/alz.12697 as the metadata stage stored them before 0.11.0.
ALZ_BEFORE = {
    "license": {"license_id": "all-rights-reserved", "determination": "assumed"},
    "copy_licenses": {
        "pdf": {"license_id": "cc-by-nc", "determination": "stated", "stated_by": "openalex",
                "stated_as": "cc-by-nc"},
        "pubget": {"license_id": "all-rights-reserved", "determination": "assumed"}},
    "reported_copies": [{
        "route": "pmc", "version": "am", "url": PMC_COPY,
        "reported_by": "openalex",
        "license": {"license_id": "cc-by-nc-nd", "determination": "stated",
                    "stated_by": "openalex", "stated_as": "cc-by-nc-nd"}}],
}
ALZ_AFTER = {
    "license": {"license_id": "all_rights_reserved", "determination": "assumed"},
    "copy_licenses": {
        "pdf": {"license_id": "cc_by_nc", "determination": "stated", "stated_by": "openalex",
                "stated_as": "cc-by-nc"},
        "pubget": {"license_id": "all_rights_reserved", "determination": "assumed"}},
    "reported_copies": [{
        "route": "pmc", "version": "am", "url": PMC_COPY,
        "reported_by": "openalex",
        "license": {"license_id": "cc_by_nc_nd", "determination": "stated",
                    "stated_by": "openalex", "stated_as": "cc-by-nc-nd"}}],
}
SCAN_BEFORE = {"license": {"license_id": "cc-by-nc", "determination": "stated",
                           "stated_by": "crossref",
                           "stated_as": "http://creativecommons.org/licenses/by-nc/4.0/"}}
SCAN_AFTER = {"license": {"license_id": "cc_by_nc", "determination": "stated",
                          "stated_by": "crossref",
                          "stated_as": "http://creativecommons.org/licenses/by-nc/4.0/"}}
#: Elsevier's open-access user licence is `other`, spelled the same before and after.
SEIZURE = {"license": {"license_id": "other", "determination": "stated", "stated_by": "crossref",
                       "stated_as": "http://www.elsevier.com/open-access/userlicense/1.0/"}}
#: An id no study_schema ever had, beside a former one: respelling cannot make it load.
ODD = {"license": {"license_id": "cc-by-4.0", "determination": "stated"},
       "copy_licenses": {"pdf": {"license_id": "cc-by", "determination": "stated"}}}


def _payload(title, licences, raw=None):
    return {**ArticleMetadata(title=title, raw_metadata=raw or {}).to_dict(), **licences}


def _summary(licences):
    """The summary's licences, as the metadata stage lists them for the report."""
    return {"title": "t", "license": licences["license"]["license_id"],
            "license_determination": licences["license"]["determination"],
            "copy_licenses": {source: lic["license_id"]
                              for source, lic in (licences.get("copy_licenses") or {}).items()},
            "license_embargo_until": None, "digests": {"title": "d"}}


@pytest.fixture()
def catalog_root(tmp_path, evidence):
    """Metadata stored before 0.11.0: two articles with former ids, one with none to
    respell, one failed, one whose payload is gone, one that respelling cannot make load."""
    root = tmp_path / "cat"
    with Catalog.open(root) as catalog:
        ids = {name: catalog.register(Identifier(doi=doi), evidence).id
               for name, doi in DOIS.items()}
        catalog.record([
            Outcome(ids["alz"], "metadata", "", fingerprint="me-alz",
                    summary=_summary(ALZ_BEFORE), payload=_payload("alz", ALZ_BEFORE, OPENALEX)),
            Outcome(ids["scan"], "metadata", "", fingerprint="me-scan",
                    summary=_summary(SCAN_BEFORE), payload=_payload("scan", SCAN_BEFORE)),
            Outcome(ids["seizure"], "metadata", "", fingerprint="me-seizure",
                    summary=_summary(SEIZURE), payload=_payload("seizure", SEIZURE)),
            Outcome.failure(ids["failed"], "metadata", "", "crossref: refused the request"),
            Outcome(ids["gone"], "metadata", "", fingerprint="me-gone",
                    summary=_summary(SCAN_BEFORE), payload=_payload("gone", SCAN_BEFORE)),
            Outcome(ids["odd"], "metadata", "", fingerprint="me-odd",
                    summary=_summary(ODD), payload=_payload("odd", ODD)),
        ])
        catalog.blobs._path(catalog.artifact(ids["gone"], "metadata").blob).unlink()
    return root, ids


DRY_RUN = {"respelled": 2, "already current": 2, "left: payload blob missing": 1,
           "left: does not validate": 1}


def _rows(db):
    with sqlite3.connect(db) as conn:
        return {r[0]: r[1:] for r in conn.execute(
            "SELECT article_id, status, fingerprint, blob, summary, updated_at FROM artifacts")}


def _blobs(root):
    return sorted(p.name for p in (root / "blobs").rglob("*.json.gz"))


def test_a_payload_stored_before_does_not_validate(catalog_root):
    root, ids = catalog_root
    with Catalog.open(root) as catalog:
        with pytest.raises(ValidationError):
            ArticleMetadata.from_dict(catalog.payload(catalog.artifact(ids["alz"], "metadata")))


def test_a_dry_run_counts_what_it_would_respell_and_writes_nothing(catalog_root):
    root, ids = catalog_root
    before, blobs = _rows(root / "catalog.sqlite"), _blobs(root)
    out = io.StringIO()

    assert rewrite.run(root, out=out) == DRY_RUN

    assert (_rows(root / "catalog.sqlite"), _blobs(root)) == (before, blobs)
    printed = out.getvalue()
    assert f"left\t{ids['gone']}\tpayload blob missing" in printed
    assert f"left\t{ids['odd']}\tValidationError: " in printed
    # alz: its payload and summary each hold all-rights-reserved twice and cc-by-nc once;
    # scan: each holds cc-by-nc once
    assert "all-rights-reserved -> all_rights_reserved\t4\n" in printed
    assert "cc-by-nc -> cc_by_nc\t4\n" in printed
    assert "cc-by-nc-nd -> cc_by_nc_nd\t1\n" in printed
    assert "dry run, nothing written" in printed


def test_apply_needs_a_new_backup(catalog_root, tmp_path):
    root, _ = catalog_root
    with pytest.raises(SystemExit):
        rewrite.run(root, apply=True)
    taken = tmp_path / "taken.sqlite"
    taken.write_text("")
    with pytest.raises(SystemExit):
        rewrite.run(root, apply=True, backup=taken)


def test_apply_respells_the_ids_and_nothing_else(catalog_root, tmp_path):
    root, ids = catalog_root
    before = _rows(root / "catalog.sqlite")

    counts = rewrite.run(root, apply=True, backup=tmp_path / "before.sqlite", out=io.StringIO())

    assert counts == DRY_RUN
    after = _rows(root / "catalog.sqlite")
    # status, fingerprint and updated_at stand; only the respelled rows' blob and summary move
    assert {k: (v[0], v[1], v[4]) for k, v in after.items()} == {
        k: (v[0], v[1], v[4]) for k, v in before.items()}
    assert {k for k in after if after[k] != before[k]} == {ids["alz"], ids["scan"]}
    with Catalog.open(root) as catalog:
        alz = catalog.artifact(ids["alz"], "metadata")
        assert catalog.payload(alz) == _payload("alz", ALZ_AFTER, OPENALEX)
        assert alz.summary == _summary(ALZ_AFTER)
        scan = catalog.artifact(ids["scan"], "metadata")
        assert catalog.payload(scan) == _payload("scan", SCAN_AFTER)
        assert scan.summary == _summary(SCAN_AFTER)
        # what sync reads now validates
        assert ArticleMetadata.from_dict(catalog.payload(alz)).license.license_id == \
            "all_rights_reserved"
    with sqlite3.connect(tmp_path / "before.sqlite") as backup:
        assert backup.execute("SELECT blob FROM artifacts WHERE article_id=?",
                              (ids["alz"],)).fetchone()[0] == before[ids["alz"]][2]


def test_a_second_run_finds_nothing_left_to_respell(catalog_root, tmp_path):
    root, _ = catalog_root
    rewrite.run(root, apply=True, backup=tmp_path / "one.sqlite", out=io.StringIO())
    once = _rows(root / "catalog.sqlite")

    counts = rewrite.run(root, apply=True, backup=tmp_path / "two.sqlite", out=io.StringIO())

    assert counts == {"already current": 4, "left: payload blob missing": 1,
                      "left: does not validate": 1}
    assert _rows(root / "catalog.sqlite") == once
