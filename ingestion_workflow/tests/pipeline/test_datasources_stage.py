"""`datasources` searches the text of each paper sync wrote for the pulled dictionary's data
sources; `ingest data-sources export` writes what it found for neurostore.

The paper is Yang et al. 2022 (PMID 36590062), whose Methods name ADNI
(`test_data_source_matching`).
"""

from __future__ import annotations

import csv
import shutil
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.config import Settings
from ingestion_workflow.data_sources.export import export
from ingestion_workflow.data_sources.merge import Record, curated_records
from ingestion_workflow.data_sources.pull import pull
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import STAGE_ORDER, STAGE_TYPES
from ingestion_workflow.pipeline.stages.datasources import (
    NO_DICTIONARY,
    NOT_SYNCED,
    DataSourcesStage,
)
from test_links_stages import _record

PAPER = Path(__file__).parents[1] / "data" / "data_sources" / "36590062.txt"
CURATED_ONLY = {"curated": curated_records}


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k")
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, Context(settings, catalog)


@pytest.fixture()
def paper(env, tmp_path, evidence):
    """The paper, extracted from pubget and synced."""
    _, catalog, _ = env
    text = tmp_path / "pubget.txt"
    shutil.copy(PAPER, text)
    ref = catalog.register(Identifier(pmid="36590062"), evidence)
    _record(catalog, ref, text, text, "pubget")
    catalog.record([Outcome(article_id=ref.id, stage="sync", fingerprint="sync-1",
                            summary={"base_study_id": None})])
    return ref


def _plan(ctx, catalog, ref):
    stage = DataSourcesStage(ctx.settings)
    return stage, stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                             catalog.artifacts([ref.id], stage.requires))


def _run(ctx, catalog, ref):
    stage, plan = _plan(ctx, catalog, ref)
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return outcomes


def test_datasources_runs_last_on_what_sync_wrote():
    assert STAGE_ORDER[-1] == "datasources"
    assert STAGE_TYPES["datasources"].requires == "sync"


def test_a_synced_paper_waits_for_a_pull(env, paper):
    _, catalog, ctx = env
    _, plan = _plan(ctx, catalog, paper)
    assert (plan.pending, plan.reasons, plan.waiting) == ([], {NO_DICTIONARY: 1}, {paper.id})


def test_a_paper_sync_retracted_is_not_read(env, paper):
    _, catalog, ctx = env
    pull(catalog, CURATED_ONLY, [])
    catalog.record([Outcome(article_id=paper.id, stage="sync", fingerprint="sync-2",
                            summary={"retracted": True})])
    _, plan = _plan(ctx, catalog, paper)
    assert (plan.pending, plan.reasons) == ([], {NOT_SYNCED: 1})


def test_the_paper_names_adni_and_is_read_again_only_when_the_dictionary_changes(env, paper):
    _, catalog, ctx = env
    pull(catalog, CURATED_ONLY, [])
    (outcome,) = _run(ctx, catalog, paper)
    assert (outcome.source, outcome.summary) == ("pubget",
                                                 {"sectioned": True, "matches": {"adni": "used"}})
    assert _plan(ctx, catalog, paper)[1].fresh == 1

    pull(catalog, CURATED_ONLY, [])  # the same dictionary again
    assert _plan(ctx, catalog, paper)[1].fresh == 1

    extra = Record(provider="jpnd", record_id="jpnd_ni", name="Data Collection Study",
                   aliases=[("Data Collection Study", "name")])
    pull(catalog, {**CURATED_ONLY, "jpnd": lambda: [extra]}, ["jpnd"])
    assert len(_plan(ctx, catalog, paper)[1].pending) == 1
    # a later pull of another catalogue keeps the records of this one
    pull(catalog, {**CURATED_ONLY, "re3data": list}, ["re3data"])
    assert "jpnd_ni" in {s["id"] for s in catalog.data_sources()}


def test_export_writes_what_neurostore_loads_and_links(env, paper, tmp_path):
    _, catalog, ctx = env
    pull(catalog, CURATED_ONLY, [])
    _run(ctx, catalog, paper)
    written = export(ctx, tmp_path / "out")
    assert written["paper_entities.tsv"] == 1
    assert written["data_sources.tsv"] == len(curated_records())
    with open(tmp_path / "out" / "paper_entities.tsv", newline="") as f:
        (row,) = csv.DictReader(f, delimiter="\t")
    assert (row["paper"], row["pmid"], row["entity"], row["status"]) == (
        paper.id, "36590062", "adni", "used")
    with open(tmp_path / "out" / "data_sources.tsv", newline="") as f:
        adni = next(r for r in csv.DictReader(f, delimiter="\t") if r["id"] == "adni")
    assert (adni["hosted_by"], adni["rrid"], adni["url_source"]) == ("loni_ida", "SCR_003007",
                                                                     "rrid")

    catalog.record([Outcome(article_id=paper.id, stage="sync", fingerprint="sync-2",
                            summary={"retracted": True})])
    assert export(ctx, tmp_path / "again")["paper_entities.tsv"] == 0
