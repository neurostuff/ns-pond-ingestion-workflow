"""The metadata stage decides a licence for each copy the article's downloads hold."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.copies import copy_of
from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.pipeline.stage import Context
from ingestion_workflow.pipeline.stages.metadata import MetadataStage

COPIES = Path(__file__).parents[1] / "data" / "copies"
#: PMC10518222: the NIH manuscript, offered for text mining only, of an article whose
#: version of record is CC BY-NC.
TDM_MANUSCRIPT = Identifier(pmid="35820077", pmcid="PMC10518222", doi="10.1002/alz.12697")


@pytest.fixture()
def env(tmp_path):
    # The keys are dropped from the cassettes, so any value replays. No email is sent.
    settings = Settings(
        data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k",
        pubmed_email=None,
        semantic_scholar_api_key=os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"),
        openalex_api_key=os.environ.get("OPENALEX_API_KEY", "replay"))
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog


def _held(catalog, identifier, source="pmc", fixture="pmc_tdm_manuscript", fingerprint="dl-1"):
    """An article one download holds, extracted, as those stages record it."""
    ref = catalog.register(identifier)
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
def test_each_held_copy_and_the_paper_get_a_licence(env):
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    _, (outcome,) = _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.OK, outcome.error
    assert {k: outcome.summary[k] for k in ("license", "license_determination", "copy_licenses",
                                            "oa_status")} == {
        "license": "cc-by-nc", "license_determination": "stated",
        "copy_licenses": {"pmc": "all-rights-reserved"}, "oa_status": "hybrid"}
    stored = ArticleMetadata.from_dict(catalog.payload(catalog.artifact(ref.id, "metadata", "")))
    assert (stored.copy_licenses["pmc"].stated_by, stored.copy_licenses["pmc"].stated_as) == (
        "pmc_cloud", "TDM")


def test_a_new_or_changed_download_makes_the_licences_stale(env):
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)

    def planned():
        return stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                          catalog.artifacts([ref.id], "extract")).pending[0].fingerprint

    first = planned()
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="", fingerprint=first,
                            payload={"title": "T"}, summary={})])
    assert stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                      catalog.artifacts([ref.id], "extract")).fresh == 1
    _held(catalog, TDM_MANUSCRIPT, fingerprint="dl-2")
    assert planned() != first
    catalog.record([Outcome.failure(ref.id, "download", "wiley", "HTTP 403")])
    # a failed download holds no copy, so it changes nothing
    assert planned() == stage.fingerprint_for(catalog.artifacts([ref.id], "download")[ref.id])


@pytest.mark.vcr()
def test_a_licence_a_provider_might_still_state_is_left_undecided_and_retried(env):
    settings, catalog = env
    settings = settings.model_copy(update={"semantic_scholar_api_key": None})
    # 10.1037/npe0000112: APA deposits no licence, its page states none, and Crossref and
    # OpenAlex list none; Semantic Scholar is not asked.
    ref = _held(catalog, Identifier(doi="10.1037/npe0000112"), "ace", None)
    _, (outcome,) = _run(MetadataStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.FAILED
    assert outcome.error == ("the licence of the ace copy is undecided: "
                             "semantic_scholar is not configured")
