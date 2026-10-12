"""Corpus passages are built from the article's text by the spans the passages stage stored: the
words a unit holds are the text's at those offsets, and an article whose spans cannot be read is
left out with its reason, never written with empty or stale context."""

import sqlite3
import sys
from pathlib import Path

import pytest

ROLE_CLASSIFIER = Path(__file__).resolve().parents[3] / "experiments" / "role_classifier"
sys.path.insert(0, str(ROLE_CLASSIFIER))

import build_sets  # noqa: E402
from ingestion_workflow.catalog.blobs import BlobStore  # noqa: E402
from ingestion_workflow.services.offsets import sha256  # noqa: E402

BEFORE = "Earlier sentence. "
PASSAGE = "Activation at (x = -3, y = -54, z = 66) was found."
AFTER = " Later sentence."
TEXT = f"Intro words. {BEFORE}{PASSAGE}{AFTER} End."


def _span(piece):
    start = TEXT.index(piece)
    return [start, start + len(piece)]


def _catalog(tmp_path, text=TEXT, indexed_sha=None, span=None, prose_span=None, passages=True):
    root = tmp_path / "catalog"
    root.mkdir()
    blobs = BlobStore(root / "blobs")
    path = tmp_path / "text.txt"
    path.write_text(text, encoding="utf-8")
    span = span or _span(PASSAGE)
    stored = {
        "full_text_path": str(path),
        "text_sha256": indexed_sha or sha256(TEXT),
        "passages": [
            {
                "span": span,
                "before": _span(BEFORE),
                "after": _span(AFTER),
                "heading": None,
                "space": None,
                "from_legend": False,
                "hits": [],
            }
        ],
    }
    # The prose payload's own copy of the words is deliberately not what the text holds.
    prose = {
        "passages": [
            {
                "span": prose_span or span,
                "text": "STALE COPY",
                "heading": "stale",
                "analyses": [{"name": "A", "points": [{"x": -3, "y": -54, "z": 66}]}],
            }
        ]
    }
    conn = sqlite3.connect(root / "catalog.sqlite")
    conn.execute("create table artifacts (article_id, stage, source, status, blob)")
    conn.execute("insert into artifacts values ('a1', 'prose', '', 'ok', ?)", (blobs.put(prose),))
    if passages:
        conn.execute(
            "insert into artifacts values ('a1', 'passages', '', 'ok', ?)", (blobs.put(stored),)
        )
    conn.commit()
    conn.close()
    return root


def _wild(monkeypatch, root):
    monkeypatch.setattr(build_sets, "CATALOG", root)
    failures = []
    found = build_sets.collections.Counter()
    units = list(build_sets.wild_passages(1, found, failures))
    return units, failures


def test_unit_text_and_context_are_the_texts_at_the_stored_spans(tmp_path, monkeypatch):
    units, failures = _wild(monkeypatch, _catalog(tmp_path))
    assert failures == []
    (unit,) = units
    assert unit["text"] == PASSAGE
    assert unit["before"] == BEFORE.strip()
    assert unit["after"] == AFTER.strip()
    assert unit["heading"] is None


@pytest.mark.parametrize(
    "kwargs, reason",
    [
        ({"indexed_sha": "0" * 64}, "gone or has changed"),
        ({"text": TEXT.replace("Intro", "Changed intro")}, "gone or has changed"),
        ({"passages": False}, "no passages artifact"),
        ({"prose_span": [0, 5]}, "prose span"),
    ],
)
def test_unreadable_spans_leave_the_article_out_with_its_reason(
    tmp_path, monkeypatch, kwargs, reason
):
    units, failures = _wild(monkeypatch, _catalog(tmp_path, **kwargs))
    assert units == []
    assert [f["article_id"] for f in failures] == ["a1"]
    assert reason in failures[0]["reason"]


def test_a_payload_that_stores_text_instead_of_spans_is_named_as_such(tmp_path, monkeypatch):
    root = _catalog(tmp_path)
    blobs = BlobStore(root / "blobs")
    old = blobs.put({"source": "x", "passages": [{"text": PASSAGE, "before": "b", "after": "a"}]})
    conn = sqlite3.connect(root / "catalog.sqlite")
    conn.execute("update artifacts set blob = ? where stage = 'passages'", (old,))
    conn.commit()
    conn.close()
    units, failures = _wild(monkeypatch, root)
    assert units == []
    assert "predates spans" in failures[0]["reason"]


def test_a_span_with_no_text_is_a_failure_not_an_empty_context(tmp_path, monkeypatch):
    end = len(TEXT)
    units, failures = _wild(monkeypatch, _catalog(tmp_path, span=[end, end]))
    assert units == []
    assert "holds no text" in failures[0]["reason"]
