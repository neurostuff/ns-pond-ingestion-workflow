"""build_sets names why an article was left out, counts them so the numbers add up, exits non-zero
when nothing could be read, and checks the extraction text it reads for citing sentences."""

import collections
import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROLE_CLASSIFIER = Path(__file__).resolve().parents[3] / "experiments" / "role_classifier"
sys.path.insert(0, str(ROLE_CLASSIFIER))

import build_sets  # noqa: E402
from ingestion_workflow.catalog.blobs import BlobStore  # noqa: E402
from ingestion_workflow.services.offsets import sha256  # noqa: E402

TEXT = "Intro. Table 1 shows peaks. End."
ANALYSIS = {"name": "A", "points": [{"x": -3, "y": -54, "z": 66}]}


def _passage(span):
    return {
        "span": span,
        "before": None,
        "after": None,
        "heading": None,
        "space": None,
        "from_legend": False,
        "hits": [],
    }


def _catalog(tmp_path, prose, passages, text=TEXT):
    root = tmp_path / "catalog"
    root.mkdir()
    blobs = BlobStore(root / "blobs")
    path = tmp_path / "text.txt"
    path.write_text(text, encoding="utf-8")
    conn = sqlite3.connect(root / "catalog.sqlite")
    conn.execute("create table artifacts (article_id, stage, source, status, blob, summary)")
    conn.execute("insert into artifacts values ('a1','prose','','ok',?,'{}')", (blobs.put(prose),))
    stored = {"full_text_path": str(path), "text_sha256": sha256(TEXT), "passages": passages}
    conn.execute(
        "insert into artifacts values ('a1','passages','','ok',?,'{}')", (blobs.put(stored),)
    )
    conn.commit()
    conn.close()
    return root


def _wild(monkeypatch, root):
    monkeypatch.setattr(build_sets, "CATALOG", root)
    failures, tally = [], collections.Counter()
    units = list(build_sets.wild_passages(1, collections.Counter(), failures, tally))
    return units, failures, tally


def _prose(*spans):
    return {"passages": [{"span": s, "analyses": [ANALYSIS]} for s in spans]}


def test_a_passage_count_the_two_stages_disagree_on_is_named(tmp_path, monkeypatch):
    root = _catalog(tmp_path, _prose([0, 6], [7, 20]), [_passage([0, 6])])
    units, failures, _ = _wild(monkeypatch, root)
    assert units == []
    assert failures[0]["kind"] == "passage_count"


def test_a_prose_payload_without_spans_is_not_called_a_span_mismatch(tmp_path, monkeypatch):
    old_prose = {"passages": [{"text": "Intro.", "analyses": [ANALYSIS]}]}
    units, failures, _ = _wild(monkeypatch, _catalog(tmp_path, old_prose, [_passage([0, 6])]))
    assert units == []
    assert failures[0]["kind"] == "prose_predates_spans"
    assert "prose stage must be re-run" in failures[0]["reason"]


def test_an_article_with_no_passages_is_read_not_left_out(tmp_path, monkeypatch):
    units, failures, tally = _wild(monkeypatch, _catalog(tmp_path, {"passages": []}, []))
    assert (units, failures) == ([], [])
    assert (tally["scanned"], tally["read"], tally["used"]) == (1, 1, 0)


def test_counts_add_up_for_a_used_article(tmp_path, monkeypatch):
    units, failures, tally = _wild(
        monkeypatch, _catalog(tmp_path, _prose([0, 6]), [_passage([0, 6])])
    )
    assert len(units) == 1
    assert tally["scanned"] == len(failures) + tally["read"]
    assert tally["used"] == 1


def test_a_run_that_leaves_out_every_article_exits_non_zero(tmp_path, monkeypatch):
    root = _catalog(tmp_path, _prose([0, 6]), [{"text": "Intro."}])
    monkeypatch.setattr(build_sets, "CATALOG", root)
    monkeypatch.setattr(build_sets, "table_units", lambda failures=None, tally=None: iter(()))
    monkeypatch.setattr(sys, "argv", ["build_sets", str(tmp_path / "out"), "--wild-passages", "1"])
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for split in ("train", "val", "test"):
        (dataset / f"{split}.jsonl").write_text("")
    monkeypatch.setattr(build_sets, "PROSE_V3", dataset)
    with pytest.raises(SystemExit) as stop:
        build_sets.main()
    assert "built no unit" in str(stop.value)
    report = json.loads((tmp_path / "out" / "build_report.json").read_text())
    wild = report["wild_passages"]
    assert wild["scanned"] == wild["left_out"] + wild["read"] == 1
    assert wild["left_out_by_reason"] == {"passages_predate_spans": 1}


def _extraction(tmp_path, recorded_sha):
    root = tmp_path / "catalog"
    root.mkdir()
    blobs = BlobStore(root / "blobs")
    path = tmp_path / "text.txt"
    path.write_text(TEXT, encoding="utf-8")
    payload = {"full_text_path": str(path), "tables": [{"table_id": "t1", "table_number": "1"}]}
    conn = sqlite3.connect(root / "catalog.sqlite")
    conn.execute("create table articles (id)")
    conn.execute("create table aliases (kind, value, article_id)")
    conn.execute("create table artifacts (article_id, stage, source, status, blob, summary)")
    conn.execute("insert into articles values ('a1')")
    summary = json.dumps({"text_sha256": recorded_sha} if recorded_sha else {})
    conn.execute(
        "insert into artifacts values ('a1','extract','s','ok',?,?)", (blobs.put(payload), summary)
    )
    conn.commit()
    conn.close()
    return root


def test_extraction_text_is_read_when_its_hash_holds(tmp_path, monkeypatch):
    monkeypatch.setattr(build_sets, "CATALOG", _extraction(tmp_path, sha256(TEXT)))
    text, label = build_sets.Corpus().lookup("a1", "s", "t1")
    assert text == TEXT and label == "Table 1"


def test_extraction_text_that_changed_fails_that_article(tmp_path, monkeypatch):
    monkeypatch.setattr(build_sets, "CATALOG", _extraction(tmp_path, sha256("other")))
    with pytest.raises(build_sets.SpanError) as err:
        build_sets.Corpus().lookup("a1", "s", "t1")
    assert err.value.kind == "text_changed"


def test_extraction_text_with_no_recorded_hash_is_counted(tmp_path, monkeypatch):
    monkeypatch.setattr(build_sets, "CATALOG", _extraction(tmp_path, None))
    corpus = build_sets.Corpus()
    assert corpus.lookup("a1", "s", "t1")[0] == TEXT
    assert corpus.unverified == 1


def test_an_old_payload_with_no_passages_has_nothing_to_read(tmp_path):
    assert build_sets.read_passages({"passages": []}, {"passages": []}, tmp_path) == []
