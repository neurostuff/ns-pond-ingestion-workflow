"""`--refresh` must be able to name one source of a stage.

Redoing one extractor's work must not mean redoing all of them.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Artifact, Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import DownloadStage, ExtractStage
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint


def artifact(stage="extract", source="ace", fingerprint="fp"):
    return Artifact(article_id="a", stage=stage, source=source, fingerprint=fingerprint)


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        download_sources=["pubget", "elsevier", "ace", "pdf"],
    )


def ctx(settings, refresh=()):
    return Context(settings, Catalog.open(settings.catalog_root), refresh=refresh)


def test_naming_the_source_reaches_a_fresh_artifact(settings):
    fresh = artifact(fingerprint="x")
    assert ctx(settings).is_fresh(fresh, "x")
    assert not ctx(settings, ["extract:ace"]).is_fresh(fresh, "x")


def test_naming_one_source_leaves_the_others_alone(settings):
    c = ctx(settings, ["extract:ace"])
    assert not c.is_fresh(artifact(source="ace", fingerprint="x"), "x")
    assert c.is_fresh(artifact(source="pubget", fingerprint="x"), "x")
    assert c.is_fresh(artifact(source="elsevier", fingerprint="x"), "x")


def test_the_whole_stage_still_works(settings):
    c = ctx(settings, ["extract"])
    for source in ("ace", "pubget", "elsevier"):
        assert not c.is_fresh(artifact(source=source), "x")


def test_all_still_works(settings):
    c = ctx(settings, ["all"])
    assert not c.is_fresh(artifact(stage="download", source="ace"), "x")


def test_a_source_of_another_stage_is_untouched(settings):
    c = ctx(settings, ["extract:ace"])
    assert c.is_fresh(artifact(stage="download", source="ace"), "fp")


def test_it_requeues_only_articles_with_nothing_higher(settings, evidence):
    """The whole point: an ACE article that pubget already handled stays
    fresh, because a success from any source makes the article fresh."""
    with Catalog.open(settings.catalog_root) as catalog:
        ace_only = catalog.register(Identifier(pmid="1"), evidence)
        also_pubget = catalog.register(Identifier(pmcid="PMC2", pmid="2"), evidence)

        for ref, sources in ((ace_only, ["ace"]), (also_pubget, ["ace", "pubget"])):
            for source in sources:
                catalog.record(
                    [
                        Outcome(
                            article_id=ref.id, stage="download", source=source,
                            fingerprint=f"dl-{source}", payload={"files": []},
                        ),
                        Outcome(
                            article_id=ref.id, stage="extract", source=source,
                            fingerprint=extraction_fingerprint(source, Artifact(
                                article_id=ref.id, stage="download", source=source,
                                fingerprint=f"dl-{source}")),
                            payload={"tables": []},
                        ),
                    ]
                )

        refs = [ace_only, also_pubget]
        ids = [r.id for r in refs]
        plan = ExtractStage(settings).plan(
            Context(settings, catalog, refresh=["extract:ace"]),
            refs,
            catalog.artifacts(ids, "extract"),
            catalog.artifacts(ids, "download"),
        )

    assert [(w.ref.id, w.source) for w in plan.pending] == [(ace_only.id, "ace")]
    assert plan.fresh == 1


def _failed_ace(catalog, stage, evidence):
    """An article whose ACE attempt failed moments ago, so backoff holds it."""
    ref = catalog.register(Identifier(pmid="1"), evidence)
    if stage == "extract":
        catalog.record([Outcome(article_id=ref.id, stage="download", source="ace",
                                fingerprint="dl-ace", payload={"files": []})])
    catalog.record([Outcome(article_id=ref.id, stage=stage, source="ace", status=Status.FAILED,
                            fingerprint="fp", error="readabilipy cannot run")])
    return ref


def test_naming_the_source_retries_a_failed_extraction(settings, evidence):
    """It was ignored: the planner asked about the stage, not the source, so a
    failed ACE extraction inside its backoff stayed out of the plan."""
    with Catalog.open(settings.catalog_root) as catalog:
        ref = _failed_ace(catalog, "extract", evidence=evidence)
        ids = [ref.id]
        args = ([ref], catalog.artifacts(ids, "extract"), catalog.artifacts(ids, "download"))
        stage = ExtractStage(settings)
        held = stage.plan(Context(settings, catalog), *args)
        named = stage.plan(Context(settings, catalog, refresh=["extract:ace"]), *args)
    assert not held.pending
    assert [w.source for w in named.pending] == ["ace"]


def test_naming_the_source_retries_a_failed_download(settings, evidence):
    with Catalog.open(settings.catalog_root) as catalog:
        ref = _failed_ace(catalog, "download", evidence=evidence)
        settings.download_sources = ["ace"]
        args = ([ref], catalog.artifacts([ref.id], "download"), {})
        stage = DownloadStage(settings)
        held = stage.plan(Context(settings, catalog), *args)
        named = stage.plan(Context(settings, catalog, refresh=["download:ace"]), *args)
    assert not held.pending
    assert [w.source for w in named.pending] == ["ace"]
