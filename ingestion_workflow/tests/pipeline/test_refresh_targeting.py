"""`--refresh` must be able to name one source of a stage.

A migrated artifact carries no fingerprint, so a version bump cannot reach it.
An explicit instruction is the only way to redo that work, and it has to be
targetable or redoing one extractor means redoing all of them.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Artifact, Catalog, Outcome
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import ExtractStage


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


def test_a_migrated_artifact_is_fresh_without_an_instruction(settings):
    """Migration writes no fingerprint on purpose, so a bump cannot reach it."""
    migrated = artifact(fingerprint="")
    assert ctx(settings).is_fresh(migrated, "a-different-fingerprint")


def test_naming_the_source_reaches_it(settings):
    migrated = artifact(fingerprint="")
    assert not ctx(settings, ["extract:ace"]).is_fresh(migrated, "whatever")


def test_naming_one_source_leaves_the_others_alone(settings):
    c = ctx(settings, ["extract:ace"])
    assert not c.is_fresh(artifact(source="ace", fingerprint=""), "x")
    assert c.is_fresh(artifact(source="pubget", fingerprint=""), "x")
    assert c.is_fresh(artifact(source="elsevier", fingerprint=""), "x")


def test_the_whole_stage_still_works(settings):
    c = ctx(settings, ["extract"])
    for source in ("ace", "pubget", "elsevier"):
        assert not c.is_fresh(artifact(source=source), "x")


def test_all_still_works(settings):
    c = ctx(settings, ["all"])
    assert not c.is_fresh(artifact(stage="download", source="ace"), "x")


def test_a_source_of_another_stage_is_untouched(settings):
    c = ctx(settings, ["extract:ace"])
    assert c.is_fresh(artifact(stage="download", source="ace"), "")


def test_it_requeues_only_articles_with_nothing_higher(settings):
    """The whole point: an ACE article that pubget already handled stays
    fresh, because a success from any source makes the article fresh."""
    with Catalog.open(settings.catalog_root) as catalog:
        ace_only = catalog.register(Identifier(pmid="1"))
        also_pubget = catalog.register(Identifier(pmcid="PMC2", pmid="2"))

        for ref, sources in ((ace_only, ["ace"]), (also_pubget, ["ace", "pubget"])):
            for source in sources:
                catalog.record(
                    [
                        Outcome(
                            article_id=ref.id, stage="download", source=source,
                            fingerprint=f"dl-{source}", payload={"files": []},
                        ),
                        # migrated: no fingerprint
                        Outcome(
                            article_id=ref.id, stage="extract", source=source,
                            fingerprint="", payload={"tables": []},
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
