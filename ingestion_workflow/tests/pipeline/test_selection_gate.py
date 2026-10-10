"""A stage that cannot work on an article should never be planned for it."""

from __future__ import annotations

import json

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.selection import Select, everything, narrow


@pytest.fixture()
def catalog(tmp_path):
    cat = Catalog.open(tmp_path / "cat")
    refs = {}
    for n, passed in (("a", 2), ("b", 0), ("c", 1), ("d", 0)):
        ref = cat.register(Identifier(pmid=f"100{n}"))
        refs[n] = ref.id
        cat.record([Outcome(
            article_id=ref.id, stage="triage", source="", status=Status.OK,
            fingerprint=f"fp-{n}",
            summary={"source": "ace", "tables": 3, "passed": passed})])
    return cat, refs


def test_only_articles_triage_passed_a_table_for_are_selected(catalog):
    """`analyses` declares `requires_flag = "passed"`. Triage already recorded
    the answer, so asking the catalog once beats planning every article to
    reach it -- 477,625 of them in the first corpus run wrote an artifact
    saying there was nothing to say."""
    cat, refs = catalog
    got = {r.id for r in narrow(cat, everything(cat), Select.PENDING, "analyses").refs}
    assert got == {refs["a"], refs["c"]}


def test_the_query_is_one_index_lookup_not_a_walk(catalog):
    cat, refs = catalog
    assert cat.article_ids_where_summary("triage", "passed") == {refs["a"], refs["c"]}


def test_a_stage_with_no_declared_flag_is_not_gated(catalog):
    """Only `analyses` declares one; nothing else may be narrowed by accident.
    `ALL` is used so the freshness rules cannot be mistaken for the gate."""
    cat, _ = catalog
    assert len(narrow(cat, everything(cat), Select.ALL, "triage").refs) == 4


def test_running_several_stages_together_does_not_gate(catalog):
    """`--stage triage --stage analyses`: the articles triage has not reached
    are exactly the ones to keep, so the gate cannot be applied."""
    cat, _ = catalog
    assert len(narrow(cat, everything(cat), Select.ALL, None).refs) == 4


def test_the_gate_is_not_a_freshness_filter(catalog):
    """`--select all` asks for everything regardless of what is done, but an
    article `analyses` cannot work on is still not work."""
    cat, refs = catalog
    got = {r.id for r in narrow(cat, everything(cat), Select.ALL, "analyses").refs}
    assert got == {refs["a"], refs["c"]}


def test_the_gate_holds_for_a_manifest_too(catalog, tmp_path):
    """It is applied in `narrow`, not in the stage's `plan`, so it does not
    matter how the selection was built."""
    from ingestion_workflow.pipeline.selection import from_manifest

    path = tmp_path / "m.jsonl"
    path.write_text("\n".join(
        json.dumps({"pmid": f"100{n}"}) for n in ("a", "b", "c", "d")) + "\n")
    cat, refs = catalog
    sel = narrow(cat, from_manifest(cat, path), Select.PENDING, "analyses")
    assert {r.id for r in sel.refs} == {refs["a"], refs["c"]}


def test_there_is_no_second_gate_in_the_stage(catalog):
    """One method, applied in one place. A duplicate check in `plan` would be
    a second thing to keep in step."""
    import inspect

    from ingestion_workflow.pipeline.stages.analyses import AnalysesStage

    assert "requires_flag" not in inspect.getsource(AnalysesStage.plan)


@pytest.mark.parametrize("stage, upstream", [("space", "roles"), ("upload", "space")])
def test_space_and_upload_are_gated_on_analyses_having_found_something(tmp_path, stage, upstream):
    """An article the model read and found nothing in is a legitimate `ok`, so
    the status check does not exclude it. 30,741 of them would be planned to
    upload nothing. `space` carries the count through to `upload`."""
    from ingestion_workflow.pipeline.stages import STAGE_TYPES

    assert STAGE_TYPES[stage].requires == upstream
    assert STAGE_TYPES[stage].requires_flag == "tables"

    cat = Catalog.open(tmp_path / "up")
    ids = {}
    for name, tables in (("has", 2), ("none", 0)):
        ref = cat.register(Identifier(pmid=f"200{name}"))
        ids[name] = ref.id
        cat.record([Outcome(
            article_id=ref.id, stage=upstream, source="", status=Status.OK,
            fingerprint=f"fp-{name}",
            summary={"tables": tables, "coordinates": tables * 5})])
    got = {r.id for r in narrow(cat, everything(cat), Select.ALL, stage).refs}
    assert got == {ids["has"]}


@pytest.mark.parametrize("prose", [None, "nu-prose"])
def test_every_stage_can_be_selected_alone(catalog, tmp_path, prose):
    """Selection asks a stage for the upstream it reads; triage's `gate` is its
    coordinate gate, and calling that with settings crashed `-s triage`."""
    from ingestion_workflow.config import Settings
    from ingestion_workflow.pipeline.stages import STAGE_ORDER

    cat, _ = catalog
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", prose_model=prose)
    for stage in STAGE_ORDER:
        narrow(cat, everything(cat), Select.PENDING, stage, settings=settings)


def test_every_stage_that_gates_uses_the_same_declaration():
    """One mechanism. A stage either declares the upstream field it needs or
    it is not gated; there is no second way to express it."""
    from ingestion_workflow.pipeline.stages import STAGE_TYPES

    declared = {n: getattr(t, "requires_flag", None) for n, t in STAGE_TYPES.items()}
    assert {n: f for n, f in declared.items() if f} == {
        "analyses": "passed", "roles": "tables", "space": "tables", "upload": "tables"}
    for name, flag in declared.items():
        if flag:
            assert getattr(STAGE_TYPES[name], "requires", None), \
                f"{name} gates on a flag but names no upstream stage"
