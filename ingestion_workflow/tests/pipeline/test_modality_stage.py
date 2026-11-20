"""`modality` reads each paper's modalities for its Neurostore base study: its normalized
record's, else its methods text's; `ingest modality export` writes one row per base study.

The records are real ones trimmed to what the readings use
(`tests/normalization/data/modality_records.json`); the methods text quotes a paper the
2026-11-19 hand-check read.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import OPT_IN_STAGES, STAGE_ORDER, STAGE_TYPES
from ingestion_workflow.pipeline.stages.modality import (
    NOT_IN_NEUROSTORE,
    NOTHING_TO_READ,
    ModalityStage,
)
from ingestion_workflow.services.modality_export import export
from test_links_stages import _record

RECORDS = json.loads((Path(__file__).parents[1] / "normalization" / "data"
                      / "modality_records.json").read_text())
METHODS = ("Title\n## Introduction\nPrior PET work on pain is reviewed.\n"
           "## Methods\nAll subjects underwent both DTI and fMRI scanning on a 3.0 T GE Signa "
           "system. For DTI, each volume was acquired with 40 mT/m gradients. Functional MRI "
           "(fMRI) data were collected in a second session.\n## Results\nPET agreed.\n")


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k")
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, Context(settings, catalog)


def _article(catalog, evidence, tmp_path, neurostore, text=METHODS, types=("Journal Article",)):
    ref = catalog.register(Identifier(neurostore=neurostore), evidence)
    path = tmp_path / f"{neurostore}.txt"
    path.write_text(text, encoding="utf-8")
    _record(catalog, ref, path, path, "pubget")
    catalog.record([Outcome(article_id=ref.id, stage="triage", fingerprint="triage-1",
                            payload={"publication_types": list(types), "is_meta_analysis": False},
                            summary={})])
    return ref


def _normalized(catalog, ref, record, fp="norm-1"):
    catalog.record([Outcome(article_id=ref.id, stage="normalize", source="ns-27b",
                            fingerprint=fp, payload={"study": record}, summary={})])


def _plan(ctx, catalog, refs):
    stage = ModalityStage(ctx.settings)
    ids = [r.id for r in refs]
    return stage, stage.plan(ctx, refs, catalog.artifacts(ids, stage.name),
                             catalog.artifacts(ids, stage.requires))


def _run(ctx, catalog, refs):
    stage, plan = _plan(ctx, catalog, refs)
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return {o.article_id: o for o in outcomes}


def test_modality_is_opt_in_and_runs_last_after_triage():
    assert STAGE_ORDER[-1] == "modality" and "modality" in OPT_IN_STAGES
    assert STAGE_TYPES["modality"].requires == "triage"


def test_the_record_states_the_modality(env, evidence, tmp_path):
    _, catalog, ctx = env
    ref = _article(catalog, evidence, tmp_path, "BSrecord")
    _normalized(catalog, ref, RECORDS["cbf"]["record"])
    outcome = _run(ctx, catalog, [ref])[ref.id]
    assert outcome.summary == {"base_study_id": "BSrecord", "modalities": ["fmri", "smri"],
                               "source": "record"}
    assert _plan(ctx, catalog, [ref])[1].fresh == 1
    _normalized(catalog, ref, RECORDS["mrs"]["record"], fp="norm-2")   # a new record
    assert len(_plan(ctx, catalog, [ref])[1].pending) == 1


def test_without_a_record_the_methods_text_is_read(env, evidence, tmp_path):
    _, catalog, ctx = env
    ref = _article(catalog, evidence, tmp_path, "BStext")
    outcome = _run(ctx, catalog, [ref])[ref.id]
    assert outcome.summary == {"base_study_id": "BStext", "modalities": ["dmri", "fmri"],
                               "source": "methods"}
    assert outcome.payload["text_source"] == "pubget"


def test_a_record_that_states_none_falls_to_the_text(env, evidence, tmp_path):
    _, catalog, ctx = env
    ref = _article(catalog, evidence, tmp_path, "BSnone")
    _normalized(catalog, ref, RECORDS["notacq"]["record"])     # a BrainMap "acquisition"
    assert _run(ctx, catalog, [ref])[ref.id].summary["source"] == "methods"


def test_a_review_has_no_text_reading(env, evidence, tmp_path):
    _, catalog, ctx = env
    ref = _article(catalog, evidence, tmp_path, "BSreview", types=("Journal Article", "Review"))
    assert _run(ctx, catalog, [ref])[ref.id].payload == {
        "base_study_id": "BSreview", "modalities": [], "source": "none", "reason": "review",
        "text_source": "pubget"}


def test_what_neurostore_does_not_hold_and_what_has_nothing_to_read_wait(env, evidence, tmp_path):
    _, catalog, ctx = env
    unknown = catalog.register(Identifier(pmid="1"), evidence)
    empty = catalog.register(Identifier(neurostore="BSempty"), evidence)
    plan = _plan(ctx, catalog, [unknown, empty])[1]
    assert (plan.pending, plan.reasons) == ([], {NOT_IN_NEUROSTORE: 1, NOTHING_TO_READ: 1})


def test_a_synced_paper_is_read_for_the_base_study_sync_wrote(env, evidence, tmp_path):
    _, catalog, ctx = env
    ref = _article(catalog, evidence, tmp_path, "BSheld")
    catalog.record([Outcome(article_id=ref.id, stage="sync", fingerprint="sync-1",
                            summary={"base_study_id": "BSwritten"})])
    assert _run(ctx, catalog, [ref])[ref.id].summary["base_study_id"] == "BSwritten"


def test_export_takes_the_record_over_the_text_for_a_shared_base_study(env, evidence, tmp_path):
    _, catalog, ctx = env
    by_text = _article(catalog, evidence, tmp_path, "BSshared")
    by_record = _article(catalog, evidence, tmp_path, "BSother")
    _normalized(catalog, by_record, RECORDS["ecog"]["record"])
    catalog.record([Outcome(article_id=by_record.id, stage="sync", fingerprint="sync-1",
                            summary={"base_study_id": "BSshared"})])
    _run(ctx, catalog, [by_text, by_record])
    assert export(catalog, tmp_path / "out" / "modalities.tsv") == 1
    with open(tmp_path / "out" / "modalities.tsv", newline="") as f:
        (row,) = csv.DictReader(f, delimiter="\t")
    assert row == {"base_study_id": "BSshared", "modalities": "ieeg", "source": "record",
                   "papers": by_record.id}
