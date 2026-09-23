"""An article downloaded from several sources is extracted once."""

from __future__ import annotations

from datetime import timedelta

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import ExtractStage


@pytest.fixture()
def env(tmp_path):
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        download_sources=["pubget", "elsevier", "ace", "pdf"],
    )
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog


def downloaded(catalog, ref, *sources):
    catalog.record(
        [
            Outcome(
                article_id=ref.id,
                stage="download",
                source=source,
                fingerprint=f"dl-{source}",
                payload={"files": []},
                summary={"files": 1},
            )
            for source in sources
        ]
    )


def plan_for(settings, catalog, refs, **ctx_kwargs):
    stage = ExtractStage(settings)
    ctx = Context(settings, catalog, **ctx_kwargs)
    ids = [r.id for r in refs]
    return stage.plan(
        ctx,
        refs,
        catalog.artifacts(ids, "extract"),
        catalog.artifacts(ids, "download"),
    )


def test_one_source_one_unit_of_work(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC1"))
    downloaded(catalog, ref, "pubget")
    plan = plan_for(settings, catalog, [ref])
    assert [w.source for w in plan.pending] == ["pubget"]


def test_three_sources_still_one_unit_of_work(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC2", pmid="2", doi="10.1/b"))
    downloaded(catalog, ref, "ace", "elsevier", "pubget")

    plan = plan_for(settings, catalog, [ref])

    assert len(plan.pending) == 1
    assert plan.pending[0].source == "pubget", "configured order decides"


def test_the_configured_order_decides(env):
    settings, catalog = env
    settings = settings.merge_overrides({"download_sources": ["ace", "pubget"]})
    ref = catalog.register(Identifier(pmcid="PMC3", pmid="3"))
    downloaded(catalog, ref, "pubget", "ace")

    plan = plan_for(settings, catalog, [ref])

    assert [w.source for w in plan.pending] == ["ace"]


def test_it_falls_through_when_the_preferred_source_fails(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC4", pmid="4"))
    downloaded(catalog, ref, "pubget", "ace")
    catalog.record([Outcome.failure(ref.id, "extract", "pubget", "bad xml")])

    plan = plan_for(settings, catalog, [ref], max_attempts=1, retry_after=timedelta(0))

    assert [w.source for w in plan.pending] == ["ace"]


def test_a_success_from_any_source_is_fresh(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC5", pmid="5"))
    downloaded(catalog, ref, "pubget", "ace")
    stage = ExtractStage(settings)
    download = catalog.artifact(ref.id, "download", "ace")
    catalog.record(
        [
            Outcome(
                article_id=ref.id,
                stage="extract",
                source="ace",
                fingerprint=stage.fingerprint_for("ace", download),
                payload={"tables": []},
            )
        ]
    )

    plan = plan_for(settings, catalog, [ref])

    assert plan.pending == []
    assert plan.fresh == 1


def test_a_source_dropped_from_the_config_is_still_reachable(env):
    settings, catalog = env
    settings = settings.merge_overrides({"download_sources": ["pubget"]})
    ref = catalog.register(Identifier(pmid="6"))
    downloaded(catalog, ref, "ace")

    plan = plan_for(settings, catalog, [ref])

    assert [w.source for w in plan.pending] == ["ace"]


def test_every_source_exhausted_is_not_queued_again(env):
    settings, catalog = env
    ref = catalog.register(Identifier(pmcid="PMC7", pmid="7"))
    downloaded(catalog, ref, "pubget", "ace")
    for source in ("pubget", "ace"):
        catalog.record([Outcome.failure(ref.id, "extract", source, "nope", permanent=True)])

    plan = plan_for(settings, catalog, [ref])

    assert plan.pending == []
    assert plan.permanent == 1


def test_work_is_one_per_article_across_a_batch(env):
    settings, catalog = env
    refs = catalog.register_many(
        [Identifier(pmcid=f"PMC1{n}", pmid=f"1{n}") for n in range(20)]
    )
    for ref in refs:
        downloaded(catalog, ref, "pubget", "elsevier", "ace")

    plan = plan_for(settings, catalog, refs)

    assert len(plan.pending) == len(refs)
    assert {w.source for w in plan.pending} == {"pubget"}
