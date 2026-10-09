from __future__ import annotations

import pytest
from lxml import etree

from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.pubget_extractor import KEEPS_SUPERSCRIPTS, article_text
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import OPT_IN_STAGES, STAGE_ORDER, build
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint
from ingestion_workflow.pipeline.stages.references import ReferencesStage
from ingestion_workflow.tests.services.test_citations import JATS


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k")
    article = tmp_path / "pmcid_1" / "article.xml"
    article.parent.mkdir()
    article.write_text(JATS)
    text = tmp_path / "article.txt"
    text.write_text(article_text(etree.parse(str(article)), article.parent), encoding="utf-8")
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, article, text


def _download(ref, path, source):
    from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType

    kind = FileType.PDF if source == "pdf" else FileType.XML
    return DownloadResult(identifier=ref.identifier, source=DownloadSource(source), success=True, files=[
        DownloadedFile(file_path=path, file_type=kind, content_type="application/xml",
                       source=DownloadSource(source))]).to_dict()


def _record(catalog, ref, path, text, source="pubget"):
    catalog.record([Outcome(article_id=ref.id, stage="download", source=source, fingerprint="dl-1",
                            payload=_download(ref, path, source), summary={})])
    download = catalog.artifact(ref.id, "download", source)
    catalog.record([Outcome(article_id=ref.id, stage="extract", source=source,
                            fingerprint=extraction_fingerprint(source, download),
                            payload={"full_text_path": str(text)}, summary={})])


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


def test_references_follow_extract_and_run_only_when_asked(tmp_path):
    assert STAGE_ORDER.index("extract") < STAGE_ORDER.index("references")
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c")
    assert "references" in OPT_IN_STAGES
    assert "references" not in {s.name for s in build(None, settings)}
    assert [s.name for s in build(["references"], settings)] == ["references"]


def test_an_extraction_gets_its_references_and_citations(env):
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="1"))
    _record(catalog, ref, article, text)
    ctx = Context(settings, catalog)

    _, (outcome,) = _run(ReferencesStage(settings), ctx, catalog, ref)

    assert outcome.status is Status.OK and outcome.source == "pubget"
    payload = outcome.payload
    assert len(payload["references"]) == 5 and payload["references"][0]["doi"] == "10.1523/x.2001"
    assert outcome.summary["citations"] == 3
    assert outcome.summary["markers_not_in_text"] == (0 if KEEPS_SUPERSCRIPTS else 1)
    stored = text.read_text(encoding="utf-8")
    first = payload["citations"][0]["text_span"]
    assert stored[first["start_char"] : first["end_char"]] == "Smith et al., 2001"

    again, _ = _run(ReferencesStage(settings), ctx, catalog, ref)
    assert again.fresh == 1 and not again.pending


def test_a_pdf_marks_no_citations_and_is_not_read(env):
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="2"))
    _record(catalog, ref, article, text, source="pdf")

    plan, outcomes = _run(ReferencesStage(settings), Context(settings, catalog), catalog, ref)

    assert plan.blocked == 1 and not outcomes
