"""`links` reads an article's links from its text and markup; `linkprobe` asks OSF and
OpenNeuro about the projects and datasets among them.

The article is Europe PMC's JATS of Stoll et al. 2026 (`test_paper_links`), and the PDF
text that of Ashby et al. 2025. OSF's and OpenNeuro's answers are recorded.
"""

from __future__ import annotations

import shutil
from datetime import timedelta

import pytest
import tenacity
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.clients import openneuro, osf
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.pubget_extractor import article_text
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import OPT_IN_STAGES, STAGE_ORDER, STAGE_TYPES, build
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint
from ingestion_workflow.pipeline.stages.linkprobe import LinkprobeStage
from ingestion_workflow.pipeline.stages.links import LinksStage
from ingestion_workflow.services.reference_lists import from_crossref
from ingestion_workflow.tests import cassette_secrets
from ingestion_workflow.tests.services.test_paper_links import JATS, PDF_TEXT
from lxml import etree

#: What links finds in Stoll et al. 2026 that names an OSF project or OpenNeuro dataset.
#: OpenNeuro's GraphQL takes every question at one URL: the body says which.
GRAPHQL = pytest.mark.vcr(match_on=["method", "scheme", "host", "port", "path", "query", "body"])

PROBED = ["https://doi.org/10.17605/osf.io/4drz6", "https://openneuro.org/datasets/ds006128",
          "https://openneuro.org/datasets/ds006128/versions/1.0.9",
          "https://osf.io/4drz6/overview"]


@pytest.fixture(autouse=True)
def unpaced(monkeypatch):
    monkeypatch.setattr(osf, "OSF_REQUEST_LIMIT", float("inf"))
    monkeypatch.setattr(openneuro, "OPENNEURO_REQUEST_LIMIT", float("inf"))
    monkeypatch.delenv("OSF_TOKEN", raising=False)


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k")
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog


def _download(ref, path, source):
    from ingestion_workflow.models import (
        DownloadedFile,
        DownloadResult,
        DownloadSource,
        FileType,
    )

    kind = FileType.PDF if source == "pdf" else FileType.XML
    return DownloadResult(identifier=ref.identifier, source=DownloadSource(source), success=True,
                          files=[DownloadedFile(file_path=path, file_type=kind,
                                                content_type="application/xml",
                                                source=DownloadSource(source))]).to_dict()


def _record(catalog, ref, path, text_path, source):
    catalog.record([Outcome(article_id=ref.id, stage="download", source=source,
                            fingerprint="dl-1", payload=_download(ref, path, source), summary={})])
    download = catalog.artifact(ref.id, "download", source)
    catalog.record([Outcome(article_id=ref.id, stage="extract", source=source,
                            fingerprint=extraction_fingerprint(source, download),
                            payload={"full_text_path": str(text_path)}, summary={"tables": 0})])


def _jats_article(env, tmp_path, evidence, pmid="42212224"):
    """Stoll et al. 2026 downloaded from Europe PMC and extracted as pubget's reader does."""
    settings, catalog = env
    xml = tmp_path / "europepmc" / "article.xml"
    xml.parent.mkdir()
    shutil.copy(JATS, xml)
    text = tmp_path / "europepmc.txt"
    text.write_text(article_text(etree.parse(str(xml)), xml.parent), encoding="utf-8")
    ref = catalog.register(Identifier(pmid=pmid), evidence)
    _record(catalog, ref, xml, text, "europepmc")
    return ref


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


def test_links_and_linkprobe_run_by_default_after_extract_and_before_sync(tmp_path):
    order = list(STAGE_ORDER)
    assert order.index("extract") < order.index("links") < order.index("linkprobe") \
        < order.index("sync")
    assert (STAGE_TYPES["links"].requires, STAGE_TYPES["linkprobe"].requires) == (
        "extract", "links")
    assert not {"links", "linkprobe"} & set(OPT_IN_STAGES)
    names = [s.name for s in build(None, Settings(data_root=tmp_path))]
    assert names[names.index("extract") + 1:names.index("extract") + 3] == ["links", "linkprobe"]


def test_links_reads_the_extraction_s_markup_and_text(env, tmp_path, evidence):
    settings, catalog = env
    ref = _jats_article(env, tmp_path, evidence)
    ctx = Context(settings, catalog)

    _, (outcome,) = _run(LinksStage(settings), ctx, catalog, ref)

    assert (outcome.status, outcome.source) == (Status.OK, "europepmc"), outcome.error
    assert outcome.summary["probe"] == PROBED
    assert outcome.summary["markup_read"] is True
    assert set(outcome.summary["found_by"]) == {"markup", "reference"}
    links = {link["url"]: link for link in outcome.payload["links"]}
    assert len(links) == outcome.summary["links"]
    occurrence = links["https://openneuro.org/datasets/ds006128"]["occurrences"][0]
    assert set(occurrence) == {"found_by", "raw", "start_char", "end_char", "section", "context"}
    again, _ = _run(LinksStage(settings), ctx, catalog, ref)
    assert (again.fresh, again.pending) == (1, [])


@pytest.mark.vcr()
def test_crossref_s_list_adds_the_links_its_entries_print_and_reads_the_text_again(
        env, tmp_path, evidence):
    settings, catalog = env
    ref = _jats_article(env, tmp_path, evidence)
    ctx = Context(settings, catalog)
    _run(LinksStage(settings), ctx, catalog, ref)

    message = CrossrefClient(cassette_secrets.REPLAY_EMAIL).work("10.1162/IMAG.a.1210")
    catalog.record([Outcome(article_id=ref.id, stage="reflist", source="", fingerprint="rl-1",
                            payload={"references": from_crossref(message)},
                            summary={"references": 97})])
    plan, (outcome,) = _run(LinksStage(settings), ctx, catalog, ref)

    assert len(plan.pending) == 1
    (github,) = [link for link in outcome.payload["links"]
                 if link["url"] == "https://github.com/Kriaese/manuscript-zoomprf"]
    assert {(o["found_by"], o.get("reference")) for o in github["occurrences"]} >= {
        ("reference", "cr81"), ("markup", None)}


def test_a_pdf_is_read_by_its_text_alone(env, tmp_path, evidence):
    settings, catalog = env
    ref = catalog.register(Identifier(pmid="41635700"), evidence)
    pdf = tmp_path / "article.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    _record(catalog, ref, pdf, PDF_TEXT, "pdf")

    _, (outcome,) = _run(LinksStage(settings), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.OK, outcome.error
    assert outcome.summary["markup_read"] is False
    assert outcome.summary["found_by"] == {"text": sum(
        len(link["occurrences"]) for link in outcome.payload["links"])}
    assert outcome.summary["probe"] == [
        "https://doi.org/10.18112/openneuro.ds007115.v1.0.0", "https://osf.io/4b9v8"]
    assert outcome.summary["nifti_context"] == 1


def test_an_extraction_that_kept_no_text_fails(env, tmp_path, evidence):
    settings, catalog = env
    ref = catalog.register(Identifier(pmid="41635700"), evidence)
    _record(catalog, ref, tmp_path / "article.pdf", tmp_path / "gone.txt", "pdf")

    _, (outcome,) = _run(LinksStage(settings), Context(settings, catalog), catalog, ref)

    assert (outcome.status, outcome.error) == (Status.FAILED, "extraction kept no text")


# -- linkprobe -----------------------------------------------------------------------


@GRAPHQL
def test_linkprobe_asks_osf_and_openneuro_about_each_project_and_dataset(env, tmp_path, evidence):
    settings, catalog = env
    ref = _jats_article(env, tmp_path, evidence)
    ctx = Context(settings, catalog)
    _run(LinksStage(settings), ctx, catalog, ref)

    _, (outcome,) = _run(LinkprobeStage(settings), ctx, catalog, ref)

    assert outcome.status is Status.OK, outcome.error
    probes = {p["url"]: p for p in outcome.payload["probes"]}
    assert list(probes) == PROBED
    assert {url: p["has_nifti"] for url, p in probes.items()} == {
        "https://doi.org/10.17605/osf.io/4drz6": False,
        "https://openneuro.org/datasets/ds006128": True,
        "https://openneuro.org/datasets/ds006128/versions/1.0.9": True,
        "https://osf.io/4drz6/overview": False}
    assert probes["https://openneuro.org/datasets/ds006128/versions/1.0.9"]["seen"] == {
        "tag": "1.0.9", "modalities": ["mri"]}
    assert probes["https://osf.io/4drz6/overview"]["seen"]["files"] == 35
    assert all(p["checked_at"] for p in probes.values())
    assert outcome.summary == {"probed": 4, "with_nifti": 2, "without_nifti": 2, "unknown": 0}
    again, _ = _run(LinkprobeStage(settings), ctx, catalog, ref)
    assert (again.fresh, again.pending) == (1, [])


def test_an_article_with_no_repository_link_is_probed_without_asking(env, tmp_path, evidence):
    settings, catalog = env
    ref = _jats_article(env, tmp_path, evidence)
    ctx = Context(settings, catalog)
    _run(LinksStage(settings), ctx, catalog, ref)
    links = catalog.artifact(ref.id, "links", "europepmc")
    catalog.record([Outcome(article_id=ref.id, stage="links", source="europepmc",
                            fingerprint=links.fingerprint, payload={"links": []},
                            summary={**links.summary, "probe": []})])

    stage = LinkprobeStage(settings)
    _, (outcome,) = _run(stage, ctx, catalog, ref)

    assert (outcome.payload, outcome.summary["probed"]) == ({"probes": []}, 0)
    assert stage._osf is None


def test_linkprobe_waits_for_links(env, tmp_path, evidence):
    settings, catalog = env
    ref = _jats_article(env, tmp_path, evidence)

    plan, outcomes = _run(LinkprobeStage(settings), Context(settings, catalog), catalog, ref)

    assert (plan.blocked, outcomes) == (1, [])


@GRAPHQL
def test_osf_down_leaves_its_links_to_a_later_run_and_openneuro_still_answers(
        env, tmp_path, evidence, monkeypatch, vcr):
    """Recorded from test_linkprobe_asks_osf_and_openneuro_about_each_project_and_dataset,
    then OSF's guid 4drz6 edited to answer 503 on every try."""
    monkeypatch.setattr(osf.OsfClient._get.retry, "wait", tenacity.wait_none())
    settings, catalog = env
    ref = _jats_article(env, tmp_path, evidence)
    ctx = Context(settings, catalog)
    _run(LinksStage(settings), ctx, catalog, ref)
    stage = LinkprobeStage(settings)

    _, (outcome,) = _run(stage, ctx, catalog, ref)

    assert outcome.status is Status.OK
    probes = {p["url"]: p for p in outcome.payload["probes"]}
    assert probes["https://openneuro.org/datasets/ds006128"]["has_nifti"] is True
    first, second = probes[PROBED[0]], probes[PROBED[3]]
    assert (first["has_nifti"], second["has_nifti"]) == (None, None)
    assert first["reason"].startswith("OSF did not answer: ")
    assert second["reason"].startswith("OSF did not answer earlier in this run: ")
    assert outcome.summary["transient"] == sorted([PROBED[0], PROBED[3]])
    # OSF was asked once, four tries, and not again for the second link
    assert sum("osf.io" in r.uri for r in vcr.requests) == 4
    assert stage.take_notes() == ["1 article(s) wait for a repository that was busy or down",
                                  "repositories down this run: OSF (HTTPError: 503 Server Error: "
                                  "Service Unavailable for url: https://api.osf.io/v2/guids/4drz6/)"]
    # kept, and asked again once retry_after has passed
    assert _run(LinkprobeStage(settings), ctx, catalog, ref)[0].fresh == 1
    later = Context(settings, catalog, now=ctx.now + timedelta(days=2))
    assert len(LinkprobeStage(settings).plan(
        later, [ref], catalog.artifacts([ref.id], "linkprobe"),
        catalog.artifacts([ref.id], "links")).pending) == 1
