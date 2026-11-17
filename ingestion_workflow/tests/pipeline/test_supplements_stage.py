"""The supplements stage's plan, and what extract makes of what it read: one extraction
holding the article and its supplements, whose fingerprint changes only for an article
that has some."""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion_workflow.catalog import ArticleRef, Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models import (DownloadResult, DownloadSource, DownloadedFile, ExtractedContent,
                                       ExtractedTable, FileType, Identifier)
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import STAGE_ORDER, OPT_IN_STAGES, ExtractStage, SupplementsStage
from ingestion_workflow.pipeline.stages.extract import current_extractions, extraction_fingerprint
from ingestion_workflow.pipeline.stages.passages import find_passages
from ingestion_workflow.pipeline.stages.supplements import NO_ROUTE
from ingestion_workflow.services import supplements as sp
from ingestion_workflow.services.paper_parse import sections


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", download_sources=["pubget", "elsevier", "ace"])
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, Context(settings, catalog)


def _article(catalog, article_id, **ids) -> ArticleRef:
    catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')", (article_id,))
    return ArticleRef(id=article_id, identifier=Identifier(**ids))


def _download(catalog, tmp_path, ref, source):
    path = tmp_path / f"{ref.id}-{source}.xml"
    path.write_text("<article/>")
    result = DownloadResult(identifier=ref.identifier, source=DownloadSource(source), success=True,
                            files=[DownloadedFile(path, FileType.XML, "application/xml", DownloadSource(source))])
    catalog.record([Outcome(ref.id, "download", source, Status.OK, f"dl-{source}",
                            payload=result.to_dict(), summary={"files": 1})])
    return catalog.artifact(ref.id, "download", source)


def _plan(stage, ctx, catalog, refs):
    ids = [r.id for r in refs]
    return stage.plan(ctx, refs, catalog.artifacts(ids, stage.name), catalog.artifacts(ids, stage.requires))


def test_supplements_run_by_default_between_download_and_extract():
    assert STAGE_ORDER.index("download") < STAGE_ORDER.index("supplements") < STAGE_ORDER.index("extract")
    assert "supplements" not in OPT_IN_STAGES


def test_an_article_is_asked_by_each_route_it_has(env, tmp_path):
    settings, catalog, ctx = env
    pmc = _article(catalog, "a1", pmcid="PMC5520049")
    _download(catalog, tmp_path, pmc, "pubget")
    pdf_only = _article(catalog, "a2", doi="10.1/x")
    _download(catalog, tmp_path, pdf_only, "pdf")
    stage = SupplementsStage(settings)

    plan = _plan(stage, ctx, catalog, [pmc, pdf_only])

    assert [w.article_id for w in plan.pending] == ["a1"]
    assert plan.reasons == {NO_ROUTE: 1}
    before = plan.pending[0].fingerprint
    _download(catalog, tmp_path, pmc, "elsevier")
    assert _plan(stage, ctx, catalog, [pmc]).pending[0].fingerprint != before


def _extraction(tmp_path, ref):
    text = tmp_path / "x" / "article.txt"
    text.parent.mkdir(parents=True, exist_ok=True)
    text.write_text("## Methods\n\nTwenty-four patients.\n", encoding="utf-8")
    return ExtractedContent(slug=ref.id, source=DownloadSource.PUBGET, identifier=ref.identifier,
                            full_text_path=text,
                            tables=[ExtractedTable(table_id="t1", raw_content_path=tmp_path / "t1.xml")])


def _read_supplements(catalog, tmp_path, ref):
    out = tmp_path / "supp"
    out.mkdir()
    (out / "text.txt").write_text("Table S1. Peaks.\nL insula x = -34, y = 16, z = -6 (t = 5.2).\n")
    csv = out / "table-s1.csv"
    csv.write_text("Region,x,y,z\nL insula,-34,16,-6\n")
    item = sp.Supplement("pmc", "https://pmc/x.docx", "x.docx", path=str(out / "x.docx"),
                         text_path=str(out / "text.txt"), read_by="docling",
                         tables=[ExtractedTable(table_id="table-s1", raw_content_path=csv,
                                                caption="Table S1. Peaks.").to_dict()])
    catalog.record([Outcome(ref.id, "supplements", "", Status.OK, "supp-fp",
                            payload={"supplements": [item.to_dict()]},
                            summary={"listed": 1, "fetched": 1, "read": 1, "tables": 1})])


def test_an_article_without_supplements_keeps_its_extraction_fresh(env, tmp_path):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    catalog.record([Outcome(ref.id, "extract", "pubget", Status.OK,
                            extraction_fingerprint("pubget", download),
                            payload=_extraction(tmp_path, ref).to_dict(), summary={"tables": 1})])
    catalog.record([Outcome(ref.id, "supplements", "", Status.OK, "supp-fp",
                            payload={"supplements": []}, summary={"listed": 0, "read": 0})])

    plan = _plan(ExtractStage(settings), ctx, catalog, [ref])

    assert (plan.fresh, plan.pending) == (1, [])


def test_supplements_read_later_are_appended_without_reading_the_download_again(env, tmp_path):
    settings, catalog, ctx = env
    ref = _article(catalog, "a1", pmcid="PMC1")
    download = _download(catalog, tmp_path, ref, "pubget")
    catalog.record([Outcome(ref.id, "extract", "pubget", Status.OK,
                            extraction_fingerprint("pubget", download),
                            payload=_extraction(tmp_path, ref).to_dict(), summary={"tables": 1})])
    _read_supplements(catalog, tmp_path, ref)
    stage = ExtractStage(settings)

    def refuse(source):
        raise AssertionError("the download was read again")

    stage.extractor = refuse
    plan = _plan(stage, ctx, catalog, [ref])
    assert len(plan.pending) == 1
    [outcome] = list(stage.execute(ctx, plan.pending))
    catalog.record([outcome])

    assert outcome.status is Status.OK
    assert outcome.fingerprint == extraction_fingerprint("pubget", download, "supp-fp")
    assert (outcome.summary["supplements"], outcome.summary["supplement_tables"],
            outcome.summary["tables"]) == (1, 1, 2)
    payload = outcome.payload
    assert [t["table_id"] for t in payload["tables"]] == ["t1", "s1-table-s1"]
    assert Path(payload["full_text_path"]).is_relative_to(settings.cache_root)
    text = Path(payload["full_text_path"]).read_text()
    assert text.startswith("## Methods\n\nTwenty-four patients.")
    assert "x = -34, y = 16, z = -6" in text
    extraction = catalog.artifact(ref.id, "extract", "pubget")
    assert current_extractions(ctx, {"pubget": extraction}, {"pubget": download}) == {"pubget": extraction}
    assert (_plan(stage, ctx, catalog, [ref]).fresh) == 1


def test_passages_read_the_appended_supplements(tmp_path):
    body = "## Methods\n\nWe scanned patients.\n\n## Figure legends\n\nFig. 1. Maps."
    supplement = "\n\n## Supplementary material\n\n### Supplementary file 1 (s1.docx)\n\n"
    prose = "Activation peaked in the left insula (x = -34, y = 16, z = -6; t = 5.2)."
    text = body + supplement + prose + "\n"
    path = tmp_path / "article+supplements.txt"
    path.write_text(text)
    start = len(body + supplement)

    how, found, _, _ = find_passages({
        "text_path": str(path), "figure_captions": [{"ids": ["f1"], "span": [len(body) - 13, len(body)]}],
        "supplements": [[start, start + len(prose)]]})

    assert found and all(start <= p.span[0] for p in found)
    assert not found[0].from_legend


def test_the_parse_reads_the_appended_material_as_one_supplement_section():
    text = ("## Methods\n\nWe scanned.\n\n## Results\n\nPeaks.\n\n## Supplementary material\n\n"
            "### Supplementary file 1 (s1.docx)\n\nMethods\n\nMore methods.\n")
    start = text.index("Methods\n\nMore")

    found = sections(text, [{"span": [start, len(text)]}])

    assert [s.kind for s in found] == ["methods", "results", "supplement"]
    assert found[-1].start_char == text.index("## Supplementary material")
    assert sections(text)[-1].kind != "supplement"


def test_supplementary_pdfs_are_read_together_each_into_its_own_directory(env, tmp_path, monkeypatch):
    settings, _, _ = env
    seen = {}

    class Reader:
        def __init__(self, settings):
            seen["root"] = settings.data_root

        def extract(self, downloads):
            seen["dirs"] = [d.identifier.slug for d in downloads]
            out = []
            for d in downloads:
                text = tmp_path / f"{d.identifier.slug}.txt"
                text.write_text("Supplementary results.")
                out.append(ExtractedContent(slug=d.identifier.slug, source=DownloadSource.PDF,
                                            full_text_path=text, text_source={"method": "docling"}))
            return out

    monkeypatch.setattr("ingestion_workflow.extractors.pdf_extractor.PdfExtractor", Reader)
    pdfs = []
    for article_id, index in (("a1", 1), ("a1", 2), ("a2", 1)):
        path = tmp_path / f"{article_id}-{index}.pdf"
        path.write_bytes(b"%PDF-1.4")
        pdfs.append((article_id, index, sp.Supplement("pmc", "u", path.name, path=str(path))))

    assert SupplementsStage(settings)._read_pdfs(tmp_path / "root", pdfs) == {}

    assert seen["root"] == tmp_path / "root"
    assert len(set(seen["dirs"])) == 3
    assert [(s.read, s.read_by) for _, _, s in pdfs] == [(True, "docling")] * 3
