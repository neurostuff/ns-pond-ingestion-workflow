"""The metadata stage decides a licence for each copy the article's downloads hold."""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.copies import copy_of
from ingestion_workflow.models import (
    DownloadCopy,
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    FileType,
)
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.pipeline.stage import Context, metadata_digests
from ingestion_workflow.pipeline.stages.metadata import MetadataStage, held_dois

COPIES = Path(__file__).parents[1] / "data" / "copies"
#: PMC10518222: the NIH manuscript, offered for text mining only, of an article whose
#: version of record is CC BY-NC.
TDM_MANUSCRIPT = Identifier(pmid="35820077", pmcid="PMC10518222", doi="10.1002/alz.12697")


@pytest.fixture()
def env(tmp_path):
    # The keys are dropped from the cassettes, so any value replays. The stage sends no
    # email; the evidence sends the placeholder.
    settings = Settings(
        data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k",
        pubmed_email=None,
        semantic_scholar_api_key=os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"),
        openalex_api_key=os.environ.get("OPENALEX_API_KEY", "replay"))
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog


#: 10.1515/cogsem-2026-3005: its publisher deposited a CC BY licence at Crossref that
#: starts on 2026-10-20, the day after the answers in the cassette were recorded.
EMBARGOED = Identifier(doi="10.1515/cogsem-2026-3005")
EMBARGO_ENDS = datetime(2026, 10, 20, tzinfo=timezone.utc)


def _held(catalog, identifier, source="pmc", fixture="pmc_tdm_manuscript", fingerprint="dl-1",
          *, evidence):
    """An article one download holds, extracted, as those stages record it."""
    ref = catalog.register(identifier, evidence)
    files = sorted((COPIES / fixture).iterdir()) if fixture else []
    download = DownloadResult(
        identifier=ref.identifier, source=DownloadSource(source), success=True,
        files=[DownloadedFile(file_path=f, file_type=FileType.XML, content_type="application/xml",
                              source=DownloadSource(source)) for f in files])
    catalog.record([
        Outcome(article_id=ref.id, stage="download", source=source, fingerprint=fingerprint,
                payload=download.to_dict(), summary={"copy": copy_of(download).to_dict()}),
        Outcome(article_id=ref.id, stage="extract", source=source, fingerprint="ex-1",
                payload={"slug": ref.identifier.slug, "source": source, "tables": [],
                         "identifier": ref.identifier.to_dict(),
                         "extracted_at": "2026-10-18T00:00:00"},
                summary={"tables": 0}),
    ])
    return ref


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                      catalog.artifacts([ref.id], "extract"))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


@pytest.mark.vcr()
def test_each_held_copy_and_the_paper_get_a_licence(env, evidence):
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    # a failed download holds no copy, and gets no licence
    catalog.record([Outcome.failure(ref.id, "download", "wiley", "HTTP 403")])
    _, (outcome,) = _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.OK, outcome.error
    assert {k: outcome.summary[k] for k in ("license", "license_determination", "copy_licenses",
                                            "oa_status")} == {
        "license": "cc_by_nc", "license_determination": "stated",
        "copy_licenses": {"pmc": "all_rights_reserved"}, "oa_status": "hybrid"}
    stored = ArticleMetadata.from_dict(catalog.payload(catalog.artifact(ref.id, "metadata", "")))
    assert (stored.copy_licenses["pmc"].stated_by, stored.copy_licenses["pmc"].stated_as) == (
        "pmc_cloud", "TDM")


@pytest.mark.vcr("test_each_held_copy_and_the_paper_get_a_licence.yaml")
def test_the_digests_recorded_are_those_of_the_payload(env, evidence):
    """Triage and prose judge what they read from these, so they must be the payload's."""
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    catalog.record([Outcome.failure(ref.id, "download", "wiley", "HTTP 403")])
    _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    stored = catalog.artifact(ref.id, "metadata", "")
    assert stored.ok and catalog.payload(stored)["title"]
    assert stored.summary["digests"] == metadata_digests(catalog.payload(stored))



@pytest.mark.vcr("test_each_held_copy_and_the_paper_get_a_licence.yaml")
def test_the_corresponding_authors_are_decided_from_every_source(env, evidence):
    """The held manuscript's front matter marks no one, so OpenAlex's mark speaks."""
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    catalog.record([Outcome.failure(ref.id, "download", "wiley", "HTTP 403")])
    _, (outcome,) = _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.summary["corresponding_authors"] == 1
    stored = ArticleMetadata.from_dict(catalog.payload(catalog.artifact(ref.id, "metadata", "")))
    assert stored.authors_provider == "openalex"
    assert [a.name for a in stored.authors if a.corresponding] == ["Stephen Zicha"]
    assert all(a.corresponding is False for a in stored.authors if a.name != "Stephen Zicha")
    # OpenAlex's list carries the author ids it has (all but one of the 25 here) and the ROR
    # institutions of each affiliation.
    assert [a.name for a in stored.authors if not a.openalex_id] == [
        "Emmanouil (Manos) Spanakis"]
    assert any(i.ror for a in stored.authors for f in a.affiliations for i in f.institutions or [])


@pytest.mark.vcr("test_each_held_copy_and_the_paper_get_a_licence.yaml")
def test_an_error_deciding_the_authors_keeps_the_record_and_notes_it(env, evidence, monkeypatch):
    """Triage and what follows are not held back by the authors."""
    import ingestion_workflow.services.metadata as service

    def failing(*args):
        raise ValueError("cannot read its authors")

    monkeypatch.setattr(service, "_decide_corresponding", failing)
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    catalog.record([Outcome.failure(ref.id, "download", "wiley", "HTTP 403")])
    _, (outcome,) = _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.OK, outcome.error
    assert outcome.summary["errors"] == {
        name: "authors: ValueError: cannot read its authors"
        for name in ("authors", "affiliations", "corresponding_authors")}
    assert "transient" not in outcome.summary
    assert {"authors", "affiliations", "ror", "corresponding_authors"} <= set(
        outcome.summary["missing"])
    assert (outcome.summary["authors"], outcome.summary["license"]) == (0, "cc_by_nc")
    stored = ArticleMetadata.from_dict(catalog.payload(catalog.artifact(ref.id, "metadata", "")))
    assert stored.title and stored.authors == []


def test_a_pdf_s_corresponding_authors_are_read_from_its_extracted_text(env, tmp_path):
    """The PDF's own record names the author with an address; OpenAlex only names one."""
    from ingestion_workflow.models.metadata import CorrespondingAuthor
    from ingestion_workflow.services.metadata import HeldDownload, _decide_corresponding
    from study_schema.models.paper_parse import CopyAccess, CopyRoute

    text = tmp_path / "text.txt"
    text.write_text((Path(__file__).parents[1] / "data" / "corresponding_authors" /
                     "pdf_springer_starred.txt").read_text())
    copy = DownloadCopy(route=CopyRoute.unrecorded, access=CopyAccess.unrecorded)
    pdf = HeldDownload("pdf", copy, [tmp_path / "paper.pdf"], text)
    openalex = ArticleMetadata(title="", corresponding_authors=[
        CorrespondingAuthor(name="Christine Lochner", stated_by="openalex")])
    found = _decide_corresponding(ArticleMetadata(title=""), [pdf], openalex)
    assert [(a.name, a.email, a.stated_by) for a in found] == [
        ("Christine Lochner", "xq1002@sun.example", "pdf")]


def test_a_re_extracted_pdf_makes_the_metadata_stale(env, evidence):
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, source="pdf", fixture=None, evidence=evidence)
    stage = MetadataStage(settings)
    downloads = catalog.artifacts([ref.id], "download")[ref.id]
    ids = (ref.identifier.pmid, held_dois(catalog, ref.id))
    first = stage.fingerprint_for(ids, downloads, catalog.artifacts([ref.id], "extract")[ref.id])
    catalog.record([Outcome(article_id=ref.id, stage="extract", source="pdf", fingerprint="ex-2",
                            payload={"slug": ref.identifier.slug, "source": "pdf", "tables": [],
                                     "identifier": ref.identifier.to_dict(),
                                     "extracted_at": "2026-10-23T00:00:00"},
                            summary={"tables": 0})])
    second = stage.fingerprint_for(ids, downloads,
                                   catalog.artifacts([ref.id], "extract")[ref.id])
    assert second != first

def test_a_new_or_changed_download_makes_the_licences_stale(env, evidence):
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)

    def planned():
        return stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                          catalog.artifacts([ref.id], "extract")).pending[0].fingerprint

    first = planned()
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="", fingerprint=first,
                            payload={"title": "T"}, summary={})])
    assert stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                      catalog.artifacts([ref.id], "extract")).fresh == 1
    _held(catalog, TDM_MANUSCRIPT, fingerprint="dl-2", evidence=evidence)
    second = planned()
    assert second != first
    catalog.record([Outcome.failure(ref.id, "download", "wiley", "HTTP 403")])
    # a failed download holds no copy, so it changes nothing
    assert planned() == second


def test_without_semantic_scholar_the_stage_stops_and_records_nothing(env, evidence):
    settings, catalog = env
    settings = settings.model_copy(update={"semantic_scholar_api_key": None})
    ref = _held(catalog, Identifier(doi="10.1037/npe0000112"), "ace", None, evidence=evidence)
    with pytest.raises(RuntimeError, match="needs semantic_scholar configured"):
        _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)
    assert catalog.artifacts([ref.id], "metadata") == {}


def test_two_journal_dois_fail_as_a_catalog_conflict(env, evidence):
    """d62fs2rhxmro holds 10.1016/j.biopsych.2010.04.013 and 10.1038/mp.2009.121."""
    settings, catalog = env
    ref = _held(catalog, Identifier(doi="10.1016/j.biopsych.2010.04.013"), "ace", None,
            evidence=evidence)
    catalog.add_aliases([(ref.id, "doi", "10.1038/mp.2009.121")])
    _, (outcome,) = _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.FAILED
    assert outcome.error == (
        "catalog conflict: the article holds two DOIs of a version of record, "
        "10.1016/j.biopsych.2010.04.013 and 10.1038/mp.2009.121; its aliases need repair")
    assert not outcome.summary


@pytest.mark.vcr()
def test_a_licence_nobody_states_is_recorded_as_assumed(env, evidence):
    settings, catalog = env
    ref = _held(catalog, Identifier(doi="10.1037/npe0000112"), "ace", None, evidence=evidence)
    _, (outcome,) = _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.OK, outcome.error
    assert (outcome.summary["license"], outcome.summary["license_determination"],
            outcome.summary["copy_licenses"]) == (
        "all_rights_reserved", "assumed", {"ace": "all_rights_reserved"})


@pytest.mark.parametrize("summary, retried", [
    # Semantic Scholar was down on every try: the article is asked about again
    ({"unanswered": ["semantic_scholar"]}, True),
    # its own record could not be read on every try: it is given up on
    (None, False),
])
def test_a_provider_s_outage_spends_none_of_the_article_s_tries(env, summary, retried, evidence):
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    for _ in range(3):
        catalog.record([Outcome.failure(ref.id, "metadata", "", "the licence ... is undecided",
                                        fingerprint="old", summary=summary)])
    ctx = Context(settings, catalog, retry_after=timedelta(0))
    plan = MetadataStage(settings).plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                                        catalog.artifacts([ref.id], "extract"))
    assert (len(plan.pending), plan.permanent) == ((1, 0) if retried else (0, 1))


LICENSES = Path(__file__).parents[1] / "data" / "licenses"


@pytest.mark.vcr("test_each_held_copy_and_the_paper_get_a_licence.yaml")
def test_a_copy_with_no_record_takes_the_first_held_record_in_the_order_sources_are_tried(
        env, evidence):
    """pubget is tried before Elsevier, so ACE's page of the version of record takes the
    licence pubget's JATS states, not Elsevier's coredata."""
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, "pubget", "pubget_open_licence", evidence=evidence)
    elsevier = LICENSES / "elsevier_cortex_2017_05_018" / "content.xml"
    download = DownloadResult(identifier=ref.identifier, source=DownloadSource.ELSEVIER,
                              success=True, files=[DownloadedFile(
                                  file_path=elsevier, file_type=FileType.XML,
                                  content_type="application/xml",
                                  source=DownloadSource.ELSEVIER)])
    ace = DownloadResult(identifier=ref.identifier, source=DownloadSource.ACE, success=True)
    catalog.record([
        Outcome(article_id=ref.id, stage="download", source=d.source.value, fingerprint="dl",
                payload=d.to_dict(), summary={"copy": copy_of(d).to_dict()})
        for d in (download, ace)])
    _, (outcome,) = _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.OK, outcome.error
    stored = ArticleMetadata.from_dict(catalog.payload(catalog.artifact(ref.id, "metadata", "")))
    by = {source: (lic.license_id, lic.stated_by) for source, lic in stored.copy_licenses.items()}
    assert by == {"pubget": ("other", "jats"), "elsevier": ("all_rights_reserved", "elsevier"),
                  "ace": ("other", "jats")}


class _Failing(BaseHTTPRequestHandler):
    """Crossref answering every request with `status`, and no wait asked for."""

    status = 503

    def do_GET(self):
        self.send_response(type(self).status)
        self.send_header("Retry-After", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.mark.vcr("test_a_licence_nobody_states_is_recorded_as_assumed.yaml", ignore_localhost=True)
@pytest.mark.parametrize("status, transient", [
    # down: asked about again after the retry delay
    (503, ["license"]),
    # refusing the request: redone only when asked
    (400, None),
])
def test_crossref_s_failure_is_marked_an_outage_only_when_it_is_one(
        env, status, transient, evidence):
    """Semantic Scholar and OpenAlex answer as recorded for 10.1037/npe0000112."""
    settings, catalog = env
    ref = _held(catalog, Identifier(doi="10.1037/npe0000112"), "ace", None, evidence=evidence)
    _Failing.status = status
    server = HTTPServer(("127.0.0.1", 0), _Failing)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        stage = MetadataStage(settings)
        stage.service._crossref_client.BASE_URL = f"http://127.0.0.1:{server.server_port}/works/"
        _, (outcome,) = _run(stage, Context(settings, catalog), catalog, ref)
    finally:
        server.shutdown()
        server.server_close()
    # The record passes, its licences assumed all rights reserved, and the error is noted.
    assert outcome.status is Status.OK, outcome.error
    assert (outcome.summary["license"], outcome.summary["license_determination"]) == (
        "all_rights_reserved", "assumed")
    assert outcome.summary["errors"]["license"].startswith(
        "the licence of the ace copy is assumed: crossref ")
    assert outcome.summary.get("transient") == transient


@pytest.mark.vcr()
def test_an_embargoed_licence_is_decided_again_when_its_embargo_ends(env, evidence):
    settings, catalog = env
    ref = _held(catalog, EMBARGOED, source="pdf", fixture=None, evidence=evidence)
    stage = MetadataStage(settings)

    def at(when):
        return Context(settings, catalog, now=when)

    def planned(when):
        return stage.plan(at(when), [ref], catalog.artifacts([ref.id], "metadata"),
                          catalog.artifacts([ref.id], "extract"))

    plan, (before,) = _run(stage, at(EMBARGO_ENDS - timedelta(days=1)), catalog, ref)

    assert before.status is Status.OK, before.error
    # CC BY is not in force yet: the paper is all rights reserved, and the artifact says when
    # that changes
    assert before.summary["license"] == "all_rights_reserved"
    assert before.summary["license_embargo_until"] == "2026-10-20T00:00:00+00:00"

    # nothing changes until the embargo ends, then the artifact is stale
    assert planned(EMBARGO_ENDS - timedelta(seconds=1)).fresh == 1
    assert planned(EMBARGO_ENDS).fresh == 0

    # a later run is a new process: it reads Crossref's and Semantic Scholar's answers from
    # the cache the first run filled, and asks OpenAlex, which is not cached, again
    _, (after,) = _run(MetadataStage(settings), at(EMBARGO_ENDS), catalog, ref)

    assert after.status is Status.OK, after.error
    assert after.summary["license"] == "cc_by"
    assert after.summary["license_embargo_until"] is None
    assert planned(EMBARGO_ENDS + timedelta(days=400)).fresh == 1
