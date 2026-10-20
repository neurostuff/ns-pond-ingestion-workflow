"""`ingest refresh-text` over a catalog and an ns-pond corpus."""

from __future__ import annotations

import json

from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.cli.main import app
from ingestion_workflow.extractors.pubget_extractor import article_text
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.services import text_refresh
from ingestion_workflow.tests.services.test_citations import JATS
from lxml import etree
from typer.testing import CliRunner

OLD = "an older rendering"


def _config(tmp_path):
    path = tmp_path / "settings.yaml"
    path.write_text(json.dumps({
        "data_root": str(tmp_path / "data"), "cache_root": str(tmp_path / "cache"),
        "catalog_root": str(tmp_path / "catalog"), "ns_pond_root": str(tmp_path / "pond"),
        "log_to_file": False, "show_progress": False, "max_workers": 1,
    }), encoding="utf-8")
    (tmp_path / "pond").mkdir()
    return path


def _extractions(tmp_path, n):
    """`n` pubget extractions whose text is older than what their download now builds."""
    with Catalog.open(tmp_path / "catalog") as catalog:
        for i in range(n):
            xml = tmp_path / "downloads" / f"pmcid_{i}" / "article.xml"
            xml.parent.mkdir(parents=True)
            xml.write_text(JATS)
            text = tmp_path / "texts" / f"{i}.txt"
            text.parent.mkdir(exist_ok=True)
            text.write_text(OLD, encoding="utf-8")
            ref = catalog.register(Identifier(pmid=str(100 + i))).id
            catalog.record([
                Outcome(article_id=ref, stage="download", source="pubget",
                        payload={"files": [{"file_path": str(xml), "file_type": "xml"}]}),
                Outcome(article_id=ref, stage="extract", source="pubget", fingerprint="ex-1",
                        payload={"full_text_path": str(text)}),
            ])
    return xml


def test_each_batch_of_texts_is_recorded_as_the_run_goes(tmp_path, monkeypatch):
    """The run does not keep its results to record them at the end: a batch is recorded as
    soon as it is complete."""
    config = _config(tmp_path)
    xml = _extractions(tmp_path, 5)
    new_sha = text_refresh.sha256(article_text(etree.parse(str(xml)), xml.parent))
    monkeypatch.setattr(text_refresh, "BATCH", 2)

    def recorded():
        with Catalog.open(tmp_path / "catalog") as catalog:
            ids = list(catalog.all_article_ids())
            return sum(a.summary.get("text_sha256") == new_sha
                       for by_source in catalog.artifacts(ids, "extract").values()
                       for a in by_source.values())

    seen = []
    run = text_refresh.run

    def watched(jobs, **kwargs):
        for result in run(jobs, **kwargs):
            seen.append(recorded())
            yield result

    monkeypatch.setattr(text_refresh, "run", watched)
    result = CliRunner().invoke(app, ["refresh-text", "--source", "pubget", "--no-corpus",
                                      "--config", str(config)])

    assert result.exit_code == 0, result.output
    assert seen == [0, 0, 2, 2, 4]
    assert recorded() == 5


def test_a_corpus_text_that_is_not_utf8_is_named_and_the_others_are_still_replaced(tmp_path):
    config = _config(tmp_path)
    _extractions(tmp_path, 1)
    bad = tmp_path / "pond" / "rec0" / "processed" / "pubget" / "text.txt"
    copy = tmp_path / "pond" / "rec1" / "processed" / "pubget" / "text.txt"
    for path, content in ((bad, b"Results\xc1\xb1 x"), (copy, OLD.encode("utf-8"))):
        path.parent.mkdir(parents=True)
        path.write_bytes(content)
    report = tmp_path / "report.jsonl"

    result = CliRunner().invoke(app, ["refresh-text", "--source", "pubget", "--dry-run",
                                      "--report", str(report), "--config", str(config)])

    assert result.exit_code == 0, result.output
    rows = {r["corpus_text"]: r["status"] for r in map(json.loads, report.read_text().splitlines())
            if "corpus_text" in r}
    assert rows[str(copy)] == "would_replace"
    assert rows[str(bad)].startswith(
        "failed: UnicodeDecodeError: 'utf-8' codec can't decode byte 0xc1")
    assert str(bad) in result.output
    assert "would replace 1 text.txt files" in result.output
    assert "1 could not be read" in result.output
    assert bad.read_bytes() == b"Results\xc1\xb1 x"
