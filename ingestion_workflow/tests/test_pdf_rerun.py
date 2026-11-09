"""scripts/pdf_rerun.py on a real catalog: export what extract would read, import what was read.

`run` is not exercised here (it needs a PDF reader); its rows are written as it writes them.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "pdf_rerun.py"
spec = importlib.util.spec_from_file_location("pdf_rerun", SCRIPT)
pdf_rerun = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = pdf_rerun
spec.loader.exec_module(pdf_rerun)


@pytest.fixture()
def env(tmp_path, evidence):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", download_sources=["wiley", "pdf"],
                        log_to_file=False, show_progress=False)
    with Catalog.open(settings.catalog_root) as catalog:
        refs = {doi: catalog.register(Identifier(doi=doi), evidence)
                for doi in ("10.1/a", "10.1/b")}
        for doi, ref in refs.items():
            _download(catalog, tmp_path, ref, "pdf", "dl-1")
            _docling(catalog, ref)
    yield settings, refs, tmp_path


def _download(catalog, tmp_path, ref, source, fp):
    pdf = tmp_path / "pdfs" / ref.id / f"{fp}.pdf"
    pdf.parent.mkdir(parents=True, exist_ok=True)
    pdf.write_bytes(b"%PDF-1.4")
    download = DownloadResult(
        identifier=ref.identifier, source=DownloadSource(source), success=True,
        files=[DownloadedFile(file_path=pdf, file_type=FileType.PDF,
                              content_type="application/pdf", source=DownloadSource(source))])
    catalog.record([Outcome(article_id=ref.id, stage="download", source=source,
                            fingerprint=fp, payload=download.to_dict())])


def _docling(catalog, ref):
    """The extraction production holds: Docling's."""
    download = catalog.artifact(ref.id, "download", "pdf")
    catalog.record([Outcome(article_id=ref.id, stage="extract", source="pdf",
                            fingerprint=extraction_fingerprint("pdf", download),
                            payload={"slug": "docling"}, summary={"tables": 0})])


def _read(tmp_path, ref, table_ids=("table-1", "table-1-2")):
    """What MinerU made of the article on another machine, in its out dir."""
    out = tmp_path / "out" / "extractions" / ref.id
    out.mkdir(parents=True, exist_ok=True)
    (out / "article.txt").write_text("# Faces\n\nThe amygdala responded.\n")
    tables = []
    for table_id in table_ids:
        (out / f"{table_id}.html").write_text("<table><tr><td>-22</td></tr></table>")
        tables.append({"table_id": table_id, "raw_content_path": str(out / f"{table_id}.html"),
                       "caption": "Table 1.", "footer": "", "coordinates": []})
    return {"slug": "mineru", "source": "pdf", "identifier": ref.identifier.to_dict(),
            "full_text_path": str(out / "article.txt"), "tables": tables, "figure_captions": [],
            "text_source": {"method": "mineru", "reason": "first of pdf_engines"},
            "extracted_at": "2026-11-09T00:00:00"}


def _export(settings, refs, tmp_path, lines):
    ids = tmp_path / "ids.tsv"
    ids.write_text("".join(f"{line}\n" for line in lines))
    inputs = tmp_path / "inputs.jsonl"
    pdf_rerun.main(["export", str(settings.catalog_root), str(ids), str(inputs)])
    return {row["article_id"]: row for row in pdf_rerun._read_jsonl(inputs)}, inputs


def _results(tmp_path, rows):
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)
    (out / "results.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    return out


def _import(settings, out, *extra):
    pdf_rerun.main(["import", str(out), str(settings.catalog_root), *extra])
    refused = (out / "import-refused.tsv").read_text().splitlines()
    return dict(line.split("\t") for line in refused)


def _extraction(settings, ref):
    """The article's pdf extraction, and its payload's slug: whose read it is."""
    with Catalog.open(settings.catalog_root) as catalog:
        extraction = catalog.artifact(ref.id, "extract", "pdf")
        return extraction, catalog.payload(extraction)["slug"]


def test_export_names_the_download_extract_reads_and_its_fingerprint(env):
    settings, refs, tmp_path = env
    a = refs["10.1/a"]
    exported, _ = _export(settings, refs, tmp_path, ["article_id\tdecision", f"{a.id}\tswitch"])

    assert list(exported) == [a.id]
    assert exported[a.id]["download_source"] == "pdf"
    assert exported[a.id]["download_fingerprint"] == "dl-1"
    assert exported[a.id]["download"]["files"][0]["file_path"].endswith("dl-1.pdf")


def test_import_records_the_read_through_the_extract_stage(env):
    settings, refs, tmp_path = env
    a = refs["10.1/a"]
    exported, _ = _export(settings, refs, tmp_path, [a.id])
    out = _results(tmp_path, [{**exported[a.id], "read_by": "ns-pond@test",
                               "content": _read(tmp_path, a)}])

    assert _import(settings, out) == {}
    extraction, slug = _extraction(settings, a)
    with Catalog.open(settings.catalog_root) as catalog:
        download = catalog.artifact(a.id, "download", "pdf")
    assert slug == "mineru"
    assert extraction.fingerprint == extraction_fingerprint("pdf", download)
    assert extraction.summary["tables"] == 2
    assert extraction.summary["text_sha256"]


def test_a_download_changed_since_export_is_refused_and_production_kept(env):
    settings, refs, tmp_path = env
    a = refs["10.1/a"]
    exported, _ = _export(settings, refs, tmp_path, [a.id])
    with Catalog.open(settings.catalog_root) as catalog:
        _download(catalog, tmp_path, a, "pdf", "dl-2")
    out = _results(tmp_path, [{**exported[a.id], "content": _read(tmp_path, a)}])

    assert _import(settings, out) == {a.id: pdf_rerun.MOVED}
    assert _extraction(settings, a)[1] == "docling"


def test_an_older_run_is_checked_against_the_export_it_read(env):
    """Rows of a run before `download_fingerprint`: the export's payload decides."""
    settings, refs, tmp_path = env
    a, b = refs["10.1/a"], refs["10.1/b"]
    exported, inputs = _export(settings, refs, tmp_path, [a.id, b.id])
    with Catalog.open(settings.catalog_root) as catalog:
        _download(catalog, tmp_path, b, "pdf", "dl-2")
    rows = [{"article_id": ref.id, "download_source": "pdf", "content": _read(tmp_path, ref)}
            for ref in (a, b)]
    for row in exported.values():
        del row["download_fingerprint"]
    inputs.write_text("".join(json.dumps(row) + "\n" for row in exported.values()))
    out = _results(tmp_path, rows)

    assert _import(settings, out) == {a.id: pdf_rerun.UNPINNED, b.id: pdf_rerun.UNPINNED}
    assert _import(settings, out, "--inputs", str(inputs)) == {b.id: pdf_rerun.MOVED}
    assert _extraction(settings, a)[1] == "mineru"


@pytest.mark.parametrize("broken, why", [
    ("repeated", pdf_rerun.REPEATED_IDS),
    ("text", pdf_rerun.FILES_MISSING),
    ("table", pdf_rerun.FILES_MISSING),
])
def test_a_read_that_cannot_stand_is_refused(env, broken, why):
    settings, refs, tmp_path = env
    a = refs["10.1/a"]
    exported, _ = _export(settings, refs, tmp_path, [a.id])
    content = _read(tmp_path, a, ("table-1", "table-1") if broken == "repeated" else
                    ("table-1", "table-1-2"))
    if broken == "text":
        Path(content["full_text_path"]).unlink()
    if broken == "table":
        Path(content["tables"][1]["raw_content_path"]).unlink()
    out = _results(tmp_path, [{**exported[a.id], "content": content}])

    assert _import(settings, out) == {a.id: why}
    assert _extraction(settings, a)[1] == "docling"


def test_a_merged_or_unknown_article_is_refused(env, evidence):
    settings, refs, tmp_path = env
    a = refs["10.1/a"]
    exported, _ = _export(settings, refs, tmp_path, [a.id])
    with Catalog.open(settings.catalog_root) as catalog:
        later = catalog.register(Identifier(neurostore="base1"), evidence)
        assert catalog.merge([a.id, later.id]) == a.id
    rows = [{**exported[a.id], "article_id": later.id, "content": _read(tmp_path, a)},
            {**exported[a.id], "article_id": "nosucharticle", "content": {}}]
    out = _results(tmp_path, rows)

    assert _import(settings, out) == {later.id: pdf_rerun.MERGED,
                                      "nosucharticle": pdf_rerun.UNKNOWN}


def test_the_status_is_the_stage_s_own(env):
    """A read that set its tables aside is recorded as extract records one."""
    settings, refs, tmp_path = env
    a = refs["10.1/a"]
    exported, _ = _export(settings, refs, tmp_path, [a.id])
    content = {**_read(tmp_path, a, ()), "full_text_path": None,
               "symbols_as_digits": {"misread": 3, "typed": 0, "tables_set_aside": 2}}
    out = _results(tmp_path, [{**exported[a.id], "content": content}])

    assert _import(settings, out) == {}
    extraction, _ = _extraction(settings, a)
    assert extraction.status is Status.OK
    assert extraction.summary["symbols_as_digits"]["tables_set_aside"] == 2
    assert extraction.summary["has_text"] is False
