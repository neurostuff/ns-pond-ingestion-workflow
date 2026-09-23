"""`--refresh` and the default `--select pending` must not contradict.

Work named for redoing is exactly the work that looks finished, so without
this the selection drops it and the refresh silently does nothing.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Select, everything, narrow


@pytest.fixture()
def catalog(tmp_path):
    with Catalog.open(tmp_path / "cat") as cat:
        done = cat.register(Identifier(pmid="1"))
        cat.record(
            [
                Outcome(article_id=done.id, stage="download", source="ace"),
                Outcome(article_id=done.id, stage="extract", source="ace"),
            ]
        )
        undone = cat.register(Identifier(pmid="2"))
        cat.record([Outcome(article_id=undone.id, stage="download", source="ace")])
        yield cat, done, undone


def pending(cat, refresh=()):
    return {
        ref.id for ref in narrow(cat, everything(cat), Select.PENDING, "extract", refresh).refs
    }


def test_a_finished_article_is_not_pending(catalog):
    cat, done, undone = catalog
    assert pending(cat) == {undone.id}


def test_naming_its_source_makes_it_pending(catalog):
    cat, done, undone = catalog
    assert pending(cat, ["extract:ace"]) == {done.id, undone.id}


def test_naming_the_stage_makes_it_pending(catalog):
    cat, done, undone = catalog
    assert pending(cat, ["extract"]) == {done.id, undone.id}


def test_all_makes_it_pending(catalog):
    cat, done, undone = catalog
    assert pending(cat, ["all"]) == {done.id, undone.id}


def test_naming_a_different_source_does_not(catalog):
    cat, done, undone = catalog
    assert pending(cat, ["extract:pubget"]) == {undone.id}


def test_naming_a_different_stage_does_not(catalog):
    cat, done, undone = catalog
    assert pending(cat, ["analyses:ace"]) == {undone.id}


def test_select_all_is_unaffected(catalog):
    cat, done, undone = catalog
    refs = narrow(cat, everything(cat), Select.ALL, "extract", ["extract:ace"]).refs
    assert {r.id for r in refs} == {done.id, undone.id}
