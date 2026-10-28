"""`links` reads an article's links from its text and markup; `linkprobe` asks OSF and
OpenNeuro about the projects and datasets among them; sync writes them, each with its
occurrences, into the parsed paper.

The articles are Europe PMC's JATS of Stoll et al. 2026 and Elsevier's XML of Bierlich et
al. 2025 (`test_paper_links`), and the PDF text that of Ashby et al. 2025. OSF's and
OpenNeuro's answers are recorded.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import timedelta

import pytest
import tenacity
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.clients import openneuro, osf
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.copies import copy_of
from ingestion_workflow.extractors.elsevier_extractor import _extract_elsevier_article
from ingestion_workflow.extractors.figure_captions import captions_digest
from ingestion_workflow.extractors.pubget_extractor import article_text
from ingestion_workflow.models import DownloadResult
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stage import metadata_digests
from ingestion_workflow.pipeline.stages import OPT_IN_STAGES, STAGE_ORDER, STAGE_TYPES, build
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint, file_sha256
from ingestion_workflow.pipeline.stages.linkprobe import LinkprobeStage
from ingestion_workflow.pipeline.stages.links import LinksStage
from ingestion_workflow.pipeline.stages.references import ReferencesStage
from ingestion_workflow.pipeline.stages.space import SpaceStage
from ingestion_workflow.pipeline.stages.sync import SyncStage
from ingestion_workflow.services.paper_parse import links_in_section
from ingestion_workflow.tests.services.test_paper_links import ELSEVIER, JATS, PDF_TEXT
from lxml import etree
from study_schema.jsonschema import load as load_json_schema
from study_schema.models.paper_parse import ParsedPaper, SectionKind
from test_prose_stages import _roles

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


def test_references_links_and_linkprobe_run_by_default_after_extract_and_before_sync(tmp_path):
    """links names the references stage's entries, so references runs first."""
    order = list(STAGE_ORDER)
    assert order.index("extract") < order.index("references") < order.index("links") \
        < order.index("linkprobe") < order.index("sync")
    assert (STAGE_TYPES["links"].requires, STAGE_TYPES["linkprobe"].requires) == (
        "extract", "links")
    assert not {"references", "links", "linkprobe"} & set(OPT_IN_STAGES)
    names = [s.name for s in build(None, Settings(data_root=tmp_path))]
    assert names[names.index("extract") + 1:names.index("extract") + 4] == [
        "references", "links", "linkprobe"]


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


def test_the_references_stage_s_list_names_each_entry_and_links_reads_the_text_again(
        env, tmp_path, evidence):
    settings, catalog = env
    ref = _jats_article(env, tmp_path, evidence)
    ctx = Context(settings, catalog)

    def osf_doi():
        links = catalog.payload(catalog.artifact(ref.id, "links", "europepmc"))["links"]
        (link,) = [link for link in links if link["url"] == "https://doi.org/10.17605/osf.io/4drz6"]
        return [(o["found_by"], o.get("reference_id")) for o in link["occurrences"]]

    _run(LinksStage(settings), ctx, catalog, ref)
    # <ext-link> in reference b82, before any list says what its entry is
    assert osf_doi() == [("reference", None)]

    _, (listed,) = _run(ReferencesStage(settings), ctx, catalog, ref)
    assert listed.status is Status.OK, listed.error
    plan, (outcome,) = _run(LinksStage(settings), ctx, catalog, ref)

    assert len(plan.pending) == 1
    assert outcome.status is Status.OK, outcome.error
    assert osf_doi() == [("reference", "IMAG.a.1210-b82")]
    named = {o["reference_id"] for link in outcome.payload["links"]
             for o in link["occurrences"] if "reference_id" in o}
    assert named <= {entry["id"] for entry in listed.payload["references"]}


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


# -- sync ------------------------------------------------------------------------------

#: The licence nobody stated, as the metadata stage records it.
ASSUMED = {"license_id": "all_rights_reserved", "determination": "assumed"}


@pytest.fixture()
def elsevier_article(tmp_path, monkeypatch, evidence):
    """Bierlich et al. 2025 downloaded from Elsevier and extracted by its extractor, its
    Table 2 read into one analysis per contrast as the extractor reads its coordinates,
    and each set given a role and a space: what sync needs before it writes the parse."""
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond")
    download = tmp_path / "elsevier"
    download.mkdir()
    shutil.copy(ELSEVIER, download / "content.xml")
    # the extractor asks only that the download hold one; its fields are not read
    (download / "metadata.json").write_text("{}")
    with Catalog.open(settings.catalog_root) as catalog:
        ref = catalog.register(Identifier(doi="10.1016/j.nicl.2025.103793",
                                          neurostore="BSnicl103793"), evidence)
        result = _elsevier_download(ref, download)
        content = _extract_elsevier_article(DownloadResult.from_dict(result), tmp_path / "ex")
        (table,) = [t for t in content.tables if t.coordinates]
        metadata = {"title": "Neural processing of social reciprocity in autism",
                    "abstract": "A", "source": "pubmed", "license": ASSUMED,
                    "copy_licenses": {"elsevier": ASSUMED}}
        catalog.record([
            Outcome(article_id=ref.id, stage="download", source="elsevier", fingerprint="dl-1",
                    payload=result,
                    summary={"copy": copy_of(DownloadResult.from_dict(result)).to_dict()}),
            Outcome(article_id=ref.id, stage="metadata", source="", fingerprint="me-1",
                    payload=metadata, summary={"digests": metadata_digests(metadata)}),
        ])
        catalog.record([
            Outcome(article_id=ref.id, stage="extract", source="elsevier",
                    fingerprint=extraction_fingerprint(
                        "elsevier", catalog.artifact(ref.id, "download", "elsevier")),
                    payload=content.to_dict(),
                    summary={"tables": len(content.tables), "has_text": True,
                             "text_sha256": file_sha256(content.full_text_path),
                             "captions_sha256": captions_digest(content.figure_captions)}),
            Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1",
                    summary={"source": "elsevier", "tables": len(content.tables), "passed": 1,
                             "passed_tables": [table.table_id]}),
            Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                    payload={table.table_id: {
                        "slug": content.slug, "coordinate_space": "MNI", "identifier": None,
                        "analyses": [{"name": "autistic vs non-autistic", "table_caption": "",
                                      "table_footer": "", "metadata": {}, "coordinates": [
                                          {"x": c.x, "y": c.y, "z": c.z, "space": "MNI"}
                                          for c in table.coordinates]}]}},
                    summary={"tables": 1, "coordinates": len(table.coordinates)}),
        ])
        ctx = Context(settings, catalog)
        _, (roled,) = _run(_roles(settings, monkeypatch), ctx, catalog, ref)
        assert roled.status is Status.OK, roled.error
        _, (spaced,) = _run(SpaceStage(settings), ctx, catalog, ref)
        assert spaced.status is Status.OK, spaced.error
        yield settings, catalog, ctx, ref


def _elsevier_download(ref, directory):
    from ingestion_workflow.models import DownloadedFile, DownloadSource, FileType

    files = [DownloadedFile(file_path=directory / "content.xml", file_type=FileType.XML,
                            content_type="text/xml", source=DownloadSource.ELSEVIER),
             DownloadedFile(file_path=directory / "metadata.json", file_type=FileType.JSON,
                            content_type="application/json", source=DownloadSource.ELSEVIER)]
    return DownloadResult(identifier=ref.identifier, source=DownloadSource.ELSEVIER,
                          success=True, files=files).to_dict()


@pytest.fixture()
def synced(elsevier_article):
    """The Elsevier article through references, links and sync: (the parsed paper as sync
    wrote it, its text, the links artifact's payload, the references artifact's payload)."""
    settings, catalog, ctx, ref = elsevier_article
    read = {}
    for stage in (ReferencesStage(settings), LinksStage(settings), SyncStage(settings)):
        _, (outcome,) = _run(stage, ctx, catalog, ref)
        assert outcome.status is Status.OK, outcome.error
        read[stage.name] = outcome
    parse = settings.ns_pond_root / "BSnicl103793" / "parse"
    paper = json.loads((parse / "parsed_paper.json").read_text("utf-8"))
    raw = (parse.parent / paper["text_path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == paper["text_sha256"]
    return paper, raw.decode("utf-8"), read["links"].payload, read["references"].payload


def test_each_link_s_occurrences_go_into_the_parsed_paper_in_text_order(synced):
    paper, text, links, _ = synced

    # as the links stage read them, without what stays in ns-pond (raw, section, context)
    assert [(link["url"], link["occurrences"]) for link in paper["links"]] == [
        (link["url"], [{"found_by": o["found_by"], "start_char": o["start_char"],
                        "end_char": o["end_char"], "reference_id": o.get("reference_id")}
                       for o in link["occurrences"]])
        for link in links["links"]]
    osf = next(link for link in paper["links"] if link["url"] == "https://osf.io/cw7n4")
    starts = [o["start_char"] for o in osf["occurrences"]]
    assert len(starts) == 2 and starts == sorted(starts)
    load = pytest.importorskip("jsonschema")
    load.validate(paper, load_json_schema("parsed-paper"))


def test_each_span_prints_its_link_or_the_words_its_markup_anchors(synced):
    """Elsevier's inter-refs here print their own URL: the span is the link."""
    paper, text, _, _ = synced
    placed = [(link["url"], text[o["start_char"]:o["end_char"]])
              for link in paper["links"] for o in link["occurrences"]
              if o["start_char"] is not None]

    assert placed == [("https://osf.io/cw7n4", "https://osf.io/cw7n4")] * 2


def test_a_link_the_reference_list_alone_gives_names_its_entry(synced):
    paper, _, _, references = synced
    entries = {entry["id"]: entry for entry in paper["references"]}

    (cknxm,) = [link for link in paper["links"]
                if link["url"] == "https://doi.org/10.17605/osf.io/cknxm"]
    assert cknxm["occurrences"] == [{"found_by": "reference", "start_char": None,
                                     "end_char": None, "reference_id": "b0150"}]
    assert entries["b0150"]["text"].endswith("https://doi.org/10.17605/OSF.IO/CKNXM.")
    # the list is the references stage's, whole
    assert [entry["id"] for entry in paper["references"]] == [
        entry["id"] for entry in references["references"]]
    named = {o["reference_id"] for link in paper["links"] for o in link["occurrences"]
             if o["reference_id"]}
    assert named and named <= set(entries)


def test_the_data_availability_statement_s_links_are_those_in_its_section(synced):
    paper, text, _, _ = synced
    parsed = ParsedPaper.model_validate(paper)

    (osf,) = links_in_section(parsed, SectionKind.data_availability)
    (occurrence,) = osf.occurrences
    assert (osf.url, text[occurrence.start_char:occurrence.end_char]) == (
        "https://osf.io/cw7n4", "https://osf.io/cw7n4")
    statement = text.index("## Data availability")
    assert statement < occurrence.start_char < text.index("## Funding")
    # the Methods give it too, and nothing in them is the statement's
    assert [link.url for link in links_in_section(parsed, SectionKind.methods)] == [
        "https://osf.io/cw7n4"]
    assert links_in_section(parsed, SectionKind.results) == []


def _another_text(payload):
    return {**payload, "text_sha256": hashlib.sha256(b"another text").hexdigest()}


def _an_entry_the_list_lacks(payload):
    links = [{**link, "occurrences": [{**o, "reference_id": "b9999"} if o.get("reference_id")
                                      else o for o in link["occurrences"]]}
             for link in payload["links"]]
    return {**payload, "links": links}


@pytest.mark.parametrize("changed, error", [
    (_another_text, "the links were read from another text than the parsed paper's"),
    (_an_entry_the_list_lacks, "the links name reference entries the paper's list does not "
                               "hold: ['b9999']"),
], ids=["another-text", "unknown-entry"])
def test_links_that_do_not_index_the_parsed_paper_fail_its_sync(elsevier_article, changed,
                                                                 error):
    settings, catalog, ctx, ref = elsevier_article
    _run(ReferencesStage(settings), ctx, catalog, ref)
    _, (read,) = _run(LinksStage(settings), ctx, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="links", source="elsevier",
                            fingerprint="li-changed", payload=changed(read.payload),
                            summary=read.summary)])

    _, (synced,) = _run(SyncStage(settings), ctx, catalog, ref)

    assert (synced.status, synced.error) == (Status.FAILED, f"parse: ValueError: {error}")


def test_the_reference_list_read_again_re_syncs_the_parse_that_holds_it(elsevier_article):
    settings, catalog, ctx, ref = elsevier_article
    _, (listed,) = _run(ReferencesStage(settings), ctx, catalog, ref)
    sync = SyncStage(settings)
    _run(sync, ctx, catalog, ref)

    def stale():
        return bool(sync.plan(ctx, [ref], catalog.artifacts([ref.id], "sync"),
                              catalog.artifacts([ref.id], "space")).pending)

    assert not stale()
    catalog.record([Outcome(article_id=ref.id, stage="references", source="elsevier",
                            fingerprint="re-again", payload=listed.payload,
                            summary=listed.summary)])
    assert stale()
