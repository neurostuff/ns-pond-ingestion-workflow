"""Triage decides which tables earn a model call, and records why."""

from __future__ import annotations

import pytest

from ingestion_workflow.pipeline.stages import STAGE_ORDER, STAGE_TYPES
from ingestion_workflow.pipeline.stages.analyses import _worth_parsing
from ingestion_workflow.pipeline.stages.triage import TriageStage, _most_tables


class _Table:
    def __init__(self, table_id, path="", caption="", footer="",
                 coordinates=(), contains_coordinates=False):
        self.table_id = table_id
        self.raw_content_path = path
        self.caption = caption
        self.footer = footer
        self.coordinates = list(coordinates)
        self.contains_coordinates = contains_coordinates


def test_triage_runs_after_metadata_and_before_analyses():
    """It needs the tables extract keeps, and it decides what analyses spends
    a call on, so it can only sit between them."""
    order = list(STAGE_ORDER)
    assert order.index("extract") < order.index("metadata") < order.index("triage")
    assert order.index("triage") < order.index("analyses")
    assert STAGE_TYPES["triage"] is TriageStage
    assert TriageStage.requires == "metadata"


def test_a_table_that_does_not_serialise_is_refused_not_guessed_at():
    stage = TriageStage(settings=object())
    got = stage.judge(_Table("t1", path="/nonexistent/table.html"))
    assert got == {"table_id": "t1", "passes": False, "points": 0,
                   "route": "unreadable", "score": 0.0,
                   "reason": "the table did not serialise"}


def test_the_source_with_the_most_tables_is_triaged(monkeypatch):
    """analyses picks the source with the most tables *with coordinates*, which
    is the old filter wearing a different hat. Triage has judged nothing yet."""
    from ingestion_workflow.catalog import Status

    class _Art:
        def __init__(self, tables, coords):
            self.status = Status.OK
            self.summary = {"tables": tables, "tables_with_coordinates": coords}

    eager = _Art(tables=2, coords=2)
    complete = _Art(tables=40, coords=0)
    assert _most_tables({"ace": eager, "pubget": complete}) is complete
    assert _most_tables({}) is None


def test_analyses_sends_exactly_what_triage_passed():
    passed = {"t2"}
    tables = [_Table("t1", contains_coordinates=True), _Table("t2"), _Table("t3")]
    kept = [t.table_id for t in tables if _worth_parsing(t, passed)]
    assert kept == ["t2"]


def test_an_article_triage_has_not_reached_falls_back_rather_than_dropping():
    """None and an empty set mean different things: nothing decided yet, and
    nothing worth a call. Treating the first as the second would silently drop
    every table of every article triage has not reached."""
    holds = _Table("t1", contains_coordinates=True)
    assert _worth_parsing(holds, None) is True
    assert _worth_parsing(holds, set()) is False


# -- the chain: extract -> triage -> analyses -----------------------------

def test_analyses_hangs_off_triage_so_a_refitted_gate_makes_it_stale():
    """If analyses fingerprinted from extract, changing the gate would leave
    every existing analysis looking fresh while the set of tables it was built
    from had changed underneath it."""
    from ingestion_workflow.pipeline.stages.analyses import AnalysesStage

    assert AnalysesStage.requires == "triage"

    class _Art:
        def __init__(self, fp):
            self.fingerprint = fp

    class _S:
        llm_model = "m"

    stage = AnalysesStage(settings=_S())
    before = stage.fingerprint_for(_Art("triage-v1"))
    after = stage.fingerprint_for(_Art("triage-v2"))
    assert before != after


def test_triage_names_the_extraction_it_judged():
    """Table ids are unique only within one extraction, so analyses has to read
    the same one or the ids name different tables."""
    import inspect

    from ingestion_workflow.pipeline.stages import triage as t

    src = inspect.getsource(t.TriageStage.execute)
    assert '"source": work.upstream.source' in src


def test_analyses_reads_the_extraction_triage_named_not_the_richest_one():
    from ingestion_workflow.catalog import Status
    from ingestion_workflow.pipeline.stages.analyses import AnalysesStage

    class _Art:
        def __init__(self, name):
            self.status = Status.OK
            self.name = name

    class _Cat:
        def artifacts(self, ids, stage):
            return {"a1": {"ace": _Art("ace"), "pubget": _Art("pubget")}}

    class _Ctx:
        catalog = _Cat()

    got = AnalysesStage._extraction_for(_Ctx(), "a1", "pubget")
    assert got.name == "pubget"
    assert AnalysesStage._extraction_for(_Ctx(), "a1", "elsevier") is None


def test_triage_requires_metadata_but_still_goes_stale_on_re_extraction():
    """`metadata`'s fingerprint is the provider list and a version -- it does
    not run through `extract`. Fingerprinting from it alone would leave triage
    looking fresh after a re-extraction, holding verdicts about tables that no
    longer exist, so the extraction's fingerprint goes in as a part."""
    class _Art:
        def __init__(self, fp):
            self.fingerprint = fp

    assert TriageStage.requires == "metadata"
    stage = TriageStage(settings=object())
    base = stage.fingerprint_for(_Art("meta-1"), _Art("extract-1"))
    assert stage.fingerprint_for(_Art("meta-2"), _Art("extract-1")) != base
    assert stage.fingerprint_for(_Art("meta-1"), _Art("extract-2")) != base


def test_only_the_abstract_is_taken_from_metadata():
    """`visible_space` reads a sentence naming the coordinate space, which a
    title never carries."""
    import inspect

    from ingestion_workflow.pipeline.stages import triage as t

    src = inspect.getsource(t.TriageStage._abstract)
    assert 'payload.get("abstract")' in src and "title" not in src.split('"""')[2]


def test_an_article_with_no_metadata_to_find_is_still_triaged():
    """Attempted is the bar, not succeeded. Plenty of articles have no
    metadata, and blocking those would strand them here forever -- the
    abstract is a help when it exists, not a condition."""
    import inspect

    from ingestion_workflow.pipeline.stages import triage as t

    src = inspect.getsource(t.TriageStage.plan)
    assert "meta is None or extraction is None" in src
    assert "meta.status" not in src
