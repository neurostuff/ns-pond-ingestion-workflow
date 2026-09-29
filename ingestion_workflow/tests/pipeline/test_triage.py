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
    """No gate is consulted: there is nothing to judge, and refusing costs
    one wasted table where guessing costs a wrong answer on every one."""
    from ingestion_workflow.pipeline.stages.triage import judge_table

    got = judge_table(None, {"table_id": "t1",
                             "raw_content_path": "/nonexistent/table.html"})
    assert got == {"table_id": "t1", "passes": False, "points": 0,
                   "route": "unreadable", "score": 0.0,
                   "reason": "the table did not serialise"}


def test_judging_is_done_in_a_pool_with_one_gate_per_worker():
    """The work is parsing and a forest, so it is processor-bound and threads
    would queue behind the interpreter lock. A fitted forest is over a
    megabyte, so it loads once per worker instead of travelling with each
    task."""
    import inspect

    from ingestion_workflow.pipeline.stages import triage as t

    src = inspect.getsource(t.TriageStage._judge_all)
    assert "ProcessPoolExecutor" in src
    assert "initializer=_load_gate" in src
    assert "len(jobs) < 4" in src          # a tiny batch does not earn a pool


def test_one_bad_table_does_not_lose_the_batch():
    """A pool that raises loses every article in flight, not just the one."""
    import inspect

    from ingestion_workflow.pipeline.stages import triage as t

    src = inspect.getsource(t._judge_article)
    assert "except Exception" in src and '"route": "error"' in src


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


def test_the_abstract_and_the_title_are_both_taken_from_metadata():
    """The abstract is for the reader -- a paper often names its coordinate
    space there and nowhere in the table. The title is for deciding whether
    the article is a meta-analysis."""
    import inspect

    from ingestion_workflow.pipeline.stages import triage as t

    src = inspect.getsource(t.TriageStage._context)
    assert 'payload.get("abstract")' in src and 'payload.get("title")' in src


# -- articles that collect other papers' coordinates ----------------------

def test_a_meta_analysis_is_marked_and_its_tables_still_judged():
    """Its coordinates are real; what they are *for* is a later stage's
    question. Dropping the article here would throw away tables nothing else
    has looked at."""
    import inspect

    from ingestion_workflow.pipeline.stages import triage as t

    src = inspect.getsource(t.TriageStage.execute)
    assert '"is_meta_analysis": meta' in src
    assert "Status.SKIPPED" not in src


def test_a_meta_analysis_is_recognised_by_its_mesh_type():
    """PubMed indexes it, and it is on 99.9% of articles with a PubMed record,
    so it catches papers whose title never says so."""
    from ingestion_workflow.pipeline.stages.triage import (
        is_a_meta_analysis, publication_types)

    md = {"raw_metadata": {"pubmed": {"MedlineCitation": {"Article": {
        "PublicationTypeList": {"PublicationType": [
            {"#text": "Journal Article", "@UI": "D016428"},
            {"#text": "Meta-Analysis", "@UI": "D017418"}]}}}}}}
    assert publication_types(md) == ["Journal Article", "Meta-Analysis"]
    assert is_a_meta_analysis("Amygdala responses to faces", publication_types(md))


def test_one_publication_type_arrives_as_a_dict_not_a_list():
    """xmltodict gives a single element bare, and unwrapping only lists would
    miss every article that carries exactly one type."""
    from ingestion_workflow.pipeline.stages.triage import publication_types

    md = {"raw_metadata": {"pubmed": {"MedlineCitation": {"Article": {
        "PublicationTypeList": {"PublicationType": {
            "#text": "Systematic Review", "@UI": "D000078182"}}}}}}}
    assert publication_types(md) == ["Systematic Review"]


def test_the_title_catches_what_mesh_does_not():
    """Not every article has a PubMed record, and not every meta-analysis is
    indexed as one."""
    from ingestion_workflow.pipeline.stages.triage import is_a_meta_analysis

    assert is_a_meta_analysis("An ALE meta-analysis of working memory", [])
    assert is_a_meta_analysis("A metaanalysis of reward processing", [])
    assert is_a_meta_analysis("Activation likelihood estimation of pain", [])
    assert not is_a_meta_analysis("Amygdala responses to faces", ["Journal Article"])


def test_an_article_with_no_metadata_is_not_taken_for_a_meta_analysis():
    """Attempted is the bar, so triage runs on articles with no metadata at
    all. Those must not be dropped for lacking a title."""
    from ingestion_workflow.pipeline.stages.triage import is_a_meta_analysis, publication_types

    assert publication_types({}) == []
    assert not is_a_meta_analysis("", [])


def test_an_article_with_no_metadata_to_find_is_still_triaged():
    """Attempted is the bar, not succeeded. Plenty of articles have no
    metadata, and blocking those would strand them here forever -- the
    abstract is a help when it exists, not a condition."""
    import inspect

    from ingestion_workflow.pipeline.stages import triage as t

    src = inspect.getsource(t.TriageStage.plan)
    assert "meta is None or extraction is None" in src
    assert "meta.status" not in src
