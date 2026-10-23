"""Reading a repair plan: a plan in which an operation undoes an earlier one is refused."""

from __future__ import annotations

import json

import pytest
from ingestion_workflow.catalog.identity_plan import load

A, B = "o227zhwfazur", "bqb4eprbjxw7"
PMID, DOI = "41282770", "10.1016/j.pnpbp.2026.111696"


def _plan(tmp_path, lines):
    path = tmp_path / "plan.jsonl"
    path.write_text("".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8")
    return path


def _reassign(value, kind, source, target):
    move = {"kind": kind, "value": value, "from": source, "to": target}
    return {"op": "reassign", "moves": [move]}


@pytest.mark.parametrize("lines, back_to", [
    ([_reassign(PMID, "pmid", A, B), _reassign(PMID, "pmid", B, A)], f"pmid of {A}"),
    ([{"op": "remove", "article": A, "kind": "pmid", "value": PMID},
      {"op": "add", "article": A, "kind": "pmid", "value": PMID}], f"pmid of {A}"),
    ([{"op": "add", "article": A, "kind": "doi", "value": DOI},
      {"op": "remove", "article": A, "kind": "doi", "value": DOI}], "no article"),
    ([{"op": "rekind", "article": A, "kind": "doi", "value": DOI},
      _reassign(DOI, "alternate_doi", A, B), _reassign(DOI, "doi", B, A)], f"doi of {A}"),
    ([{"op": "rekind", "article": A, "kind": "doi", "value": DOI},
      _reassign(DOI, "alternate_doi", A, B), _reassign(DOI, "alternate_doi", B, A)],
     f"alternate_doi of {A}"),
    ([{"op": "split", "article": A, "aliases": [["pmid", PMID]]},
      _reassign(PMID, "pmid", "xe43l42ckkyk", A)], f"pmid of {A}"),
], ids=["reassign-and-back", "remove-then-add", "add-then-remove", "rekind-then-back",
        "reassign-of-a-rekind-and-back", "split-then-back"])
def test_an_identifier_coming_back_where_it_was_is_refused(tmp_path, lines, back_to):
    with pytest.raises(ValueError, match=f"line {len(lines)}: .* comes back to {back_to} it left"):
        load(_plan(tmp_path, lines))


@pytest.mark.parametrize("lines", [
    [_reassign(PMID, "pmid", A, B), _reassign(PMID, "pmid", B, "c77xpkhmfk46")],
    [{"op": "rekind", "article": A, "kind": "pmid", "value": PMID},
     _reassign(PMID, "preprint_pmid", A, B)],
    [{"op": "remove", "article": A, "kind": "doi", "value": DOI},
     {"op": "add", "article": B, "kind": "doi", "value": DOI}],
    [{"op": "add", "article": B, "kind": "pmid", "value": "42903537"},
     {"op": "add", "article": B, "kind": "pmid", "value": "42903537"}],
], ids=["onward", "rekind-then-move", "moved-by-remove-and-add", "the-same-op-twice"])
def test_an_identifier_moving_on_is_read(tmp_path, lines):
    assert [n for n, _ in load(_plan(tmp_path, lines))] == list(range(1, len(lines) + 1))
