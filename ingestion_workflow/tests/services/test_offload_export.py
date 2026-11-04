"""`ingest offload export`: the bundle's requests are the ones the ns extraction sent.

The paper is a real one: article 26klegjq7mhi, as ns-pond holds it on beast (its parse,
parsed paper and ACE text), and the body its 2026-10-31 run sent for it (shard s114).
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.pipeline import Context
from ingestion_workflow.services.offload.contract import BundleDir, load_spec
from ingestion_workflow.services.offload.export import NO_ANALYSES, OVER, TEXT_MOVED, export, shard
from ingestion_workflow.services.offload.importer import import_run
from pondie_astra import prompt

DATA = Path(__file__).resolve().parents[1] / "data" / "offload"
SPEC = load_spec(Path(__file__).resolve().parents[3] / "configs" / "offload" / "ns-27b-e.json")
SENT = json.loads((DATA / "26klegjq7mhi.body.json").read_text())
RECORD = json.loads((DATA / "26klegjq7mhi.extraction.json").read_text())
ARTICLE = "26klegjq7mhi"
NOW = datetime(2026, 11, 4, 15, 0, tzinfo=timezone.utc)


@pytest.fixture()
def fake_tokens(monkeypatch):
    """A stand-in token count (characters / 4), so the tests need no tokenizer."""
    monkeypatch.setattr(prompt, "ntok", lambda s: len(s) // 4)


def _paper(root: Path, name: str) -> Path:
    paper = root / name
    shutil.copytree(DATA / "paper-26klegjq7mhi", paper)
    return paper


@pytest.fixture()
def ctx(tmp_path):
    settings = Settings(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
                        catalog_root=tmp_path / "cat", ns_pond_root=tmp_path / "pond")
    with Catalog.open(settings.catalog_root) as catalog:
        yield Context(settings, catalog, retry_after=timedelta(hours=24))


def _synced(ctx, article_id: str, paper: Path) -> None:
    ctx.catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')",
                              (article_id,))
    ctx.catalog.record([Outcome(article_id, "sync", "", Status.OK, "sync-fp",
                                summary={"path": str(paper), "parse_id": "p1", "analyses": 1,
                                         "base_study_id": "tS8tSUXeybTo"})])


def _rows(bundle: Path):
    held = BundleDir(bundle)
    return [json.loads(line) for s in held.bundle().shards
            for line in held.inputs(s.name).read_text().splitlines()]


def test_the_request_is_the_one_the_ns_extraction_sent(ctx, tmp_path, fake_tokens):
    _synced(ctx, ARTICLE, _paper(tmp_path / "pond", ARTICLE))
    report = export(ctx, SPEC, "vista", workers=1, now=NOW)
    (row,) = _rows(report.bundle)
    assert row["content"] == SENT["prompt"] + prompt.TEMPLATE_SUFFIX + SENT["template"]
    assert row["chat_template_kwargs"] == {"enable_thinking": False}
    assert row["text_sha256"] == json.loads(
        (DATA / "paper-26klegjq7mhi/parse/coordinate_parse.json").read_text())["text_sha256"]
    assert row["parse_keys"] == ["analysis#840c7bec806dd3ad"]
    bundle = BundleDir(report.bundle).bundle()
    assert bundle.bundle_id == "ns-27b-e-20261104-1500" == bundle.run["run_id"]
    assert bundle.run["adapter"] == SPEC.adapter and bundle.run["machine"] == "vista"


def test_the_token_count_is_the_one_the_shards_were_cut_by(ctx, tmp_path):
    try:
        prompt.tokenizer()
    except Exception:  # noqa: BLE001 - no NuExtract3 tokenizer in this environment
        pytest.skip("NuExtract3's tokenizer is not cached here")
    _synced(ctx, ARTICLE, _paper(tmp_path / "pond", ARTICLE))
    (row,) = _rows(export(ctx, SPEC, "vista", workers=1, now=NOW).bundle)
    assert row["n_input_tokens"] == SENT["n_input_tokens"] == 15089
    assert row["max_tokens"] == min(SPEC.serve.max_tokens,
                                    SPEC.serve.max_len - 15089 - SPEC.serve.reserve)


def test_a_paper_whose_text_moved_is_left_out(ctx, tmp_path, fake_tokens):
    paper = _paper(tmp_path / "pond", ARTICLE)
    (paper / "processed/ace/text.txt").write_text("another text")
    _synced(ctx, ARTICLE, paper)
    report = export(ctx, SPEC, "vista", workers=1, now=NOW)
    assert report.bundle is None and report.left_out == {TEXT_MOVED: 1}


def test_a_parse_without_analyses_is_left_out(ctx, tmp_path, fake_tokens):
    paper = _paper(tmp_path / "pond", ARTICLE)
    parse = json.loads((paper / "parse/coordinate_parse.json").read_text())
    (paper / "parse/coordinate_parse.json").write_text(json.dumps(dict(parse, analyses=[])))
    _synced(ctx, ARTICLE, paper)
    assert export(ctx, SPEC, "vista", workers=1, now=NOW).left_out == {NO_ANALYSES: 1}


def test_a_request_over_the_limit_is_skipped_not_sent(ctx, tmp_path, fake_tokens):
    _synced(ctx, ARTICLE, _paper(tmp_path / "pond", ARTICLE))
    report = export(ctx, SPEC.model_copy(update={"max_input_tokens": 100}), "vista",
                    workers=1, now=NOW)
    assert report.shards == [] and report.skipped == 1
    (row,) = [json.loads(line) for line in BundleDir(report.bundle).skipped.read_text()
              .splitlines()]
    assert row["status"] == "skipped" and row["reason"] == OVER.format(100)


def test_an_article_fresh_for_the_source_is_not_exported_again(ctx, tmp_path, fake_tokens):
    from ingestion_workflow.catalog import Run, RunKind

    _synced(ctx, ARTICLE, _paper(tmp_path / "pond", ARTICLE))
    sent = json.loads((DATA / "paper-26klegjq7mhi/parse/coordinate_parse.json").read_text())
    import_run(ctx.catalog, Run("ns-27b-e-20261031", RunKind.MODEL, "record", "ns-27b",
                                model=SPEC.model, adapter=SPEC.adapter,
                                created_at="2026-10-31"),
               [{"article_id": ARTICLE, "status": "ok", "text_sha256": sent["text_sha256"],
                 "record": RECORD}])
    report = export(ctx, SPEC, "vista", workers=1, now=NOW)
    assert report.fresh == 1 and report.bundle is None
    # another source still wants it
    other = export(ctx, SPEC.model_copy(update={"source": "ns-27b-f"}), "vista", workers=1,
                   now=NOW)
    assert other.fresh == 0 and len(other.shards) == 1


def test_shards_are_balanced_longest_first_within_their_size():
    rows = [{"article_id": str(i), "n_input_tokens": n} for i, n in enumerate([9, 1, 8, 2, 7])]
    out = shard(rows, size=3)
    assert [len(s) for s in out] == [3, 2]
    assert sorted(sum(r["n_input_tokens"] for r in s) for s in out) == [12, 15]


EV1_SPEC = load_spec(Path(__file__).resolve().parents[3] / "configs" / "offload" / "ns-ev1.json")
EV1_SENT = json.loads((DATA / "26klegjq7mhi.ev1.body.json").read_text())


def _recorded(ctx, status="ok"):
    from ingestion_workflow.catalog import Run, RunKind

    sent = json.loads((DATA / "paper-26klegjq7mhi/parse/coordinate_parse.json").read_text())
    import_run(ctx.catalog, Run("ns-27b-e-20261031", RunKind.MODEL, "record", "ns-27b",
                                model=SPEC.model, adapter=SPEC.adapter, created_at="2026-10-31"),
               [{"article_id": ARTICLE, "status": status, "text_sha256": sent["text_sha256"],
                 "record": RECORD if status == "ok" else None, "reason": "too long"}])


def test_the_evidence_request_is_the_one_the_ev1_run_sent(ctx, tmp_path, fake_tokens):
    from ingestion_workflow.pipeline.stages.offloaded import record_digest

    _synced(ctx, ARTICLE, _paper(tmp_path / "pond", ARTICLE))
    _recorded(ctx)
    report = export(ctx, EV1_SPEC, "ls6", workers=1, now=NOW)
    (row,) = _rows(report.bundle)
    assert row["content"] == EV1_SENT["prompt"]
    assert row["chat_template_kwargs"] == {"enable_thinking": False,
                                           "template": EV1_SENT["template"]}
    held = ctx.catalog.payload(ctx.catalog.artifact(ARTICLE, "record", "ns-27b"))
    assert row["record_digest"] == record_digest(held) == record_digest(RECORD)
    (kept,) = BundleDir(report.bundle).records("s000").read_text().splitlines()
    assert json.loads(kept) == {"article_id": ARTICLE, "record": held}


def test_evidence_waits_for_a_record_and_a_skipped_record_skips_it(ctx, tmp_path, fake_tokens):
    from ingestion_workflow.services.offload.export import NO_CURRENT_RECORD, RECORD_SKIPPED

    _synced(ctx, ARTICLE, _paper(tmp_path / "pond", ARTICLE))
    report = export(ctx, EV1_SPEC, "ls6", workers=1, now=NOW)
    assert report.bundle is None and report.left_out == {NO_CURRENT_RECORD: 1}
    _recorded(ctx, status="skipped")
    report = export(ctx, EV1_SPEC, "ls6", workers=1, now=NOW)
    (row,) = [json.loads(line) for line in BundleDir(report.bundle).skipped.read_text()
              .splitlines()]
    assert report.shards == [] and row["status"] == "skipped" and row["reason"] == RECORD_SKIPPED
