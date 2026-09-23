"""Bumping one extractor must not invalidate the others."""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Artifact, Catalog, Outcome, fingerprint
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import ExtractStage
from ingestion_workflow.pipeline.stages.extract import (
    DEFAULT_EXTRACTOR_VERSION,
    EXTRACTOR_VERSIONS,
)

UPSTREAM = Artifact(article_id="a", stage="download", source="x", fingerprint="DL")


@pytest.fixture()
def stage(tmp_path):
    return ExtractStage(Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c"))


@pytest.mark.parametrize("source", ["pubget", "elsevier", "pdf"])
def test_sources_at_version_one_keep_their_original_fingerprint(stage, source):
    """These were written under a single global EXTRACT_VERSION = 1. If their
    fingerprint moves, every stored extraction for them is silently discarded
    and recomputed -- around 197,000 of them on the live corpus."""
    assert EXTRACTOR_VERSIONS[source] == 1
    original = fingerprint("extract", source, 1, upstream="DL")
    assert stage.fingerprint_for(source, UPSTREAM) == original


def test_ace_moved_because_its_extractor_changed(stage):
    assert EXTRACTOR_VERSIONS["ace"] > 1
    original = fingerprint("extract", "ace", 1, upstream="DL")
    assert stage.fingerprint_for("ace", UPSTREAM) != original


def test_an_unlisted_source_falls_back_rather_than_crashing(stage):
    expected = fingerprint("extract", "mystery", DEFAULT_EXTRACTOR_VERSION, upstream="DL")
    assert stage.fingerprint_for("mystery", UPSTREAM) == expected


def test_a_bump_only_requeues_that_source(tmp_path):
    """An article extracted by pubget stays fresh; one extracted only by ace
    comes back as work."""
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        download_sources=["pubget", "elsevier", "ace", "pdf"],
    )
    with Catalog.open(settings.catalog_root) as catalog:
        by_pubget = catalog.register(Identifier(pmcid="PMC1"))
        by_ace = catalog.register(Identifier(pmid="2"))
        for ref, source in ((by_pubget, "pubget"), (by_ace, "ace")):
            catalog.record(
                [
                    Outcome(
                        article_id=ref.id,
                        stage="download",
                        source=source,
                        fingerprint=f"dl-{source}",
                        payload={"files": []},
                    )
                ]
            )
            # An extraction recorded under the old, global version 1.
            download = catalog.artifact(ref.id, "download", source)
            catalog.record(
                [
                    Outcome(
                        article_id=ref.id,
                        stage="extract",
                        source=source,
                        fingerprint=fingerprint(
                            "extract", source, 1, upstream=download.fingerprint
                        ),
                        payload={"tables": []},
                    )
                ]
            )

        refs = [by_pubget, by_ace]
        ids = [r.id for r in refs]
        plan = ExtractStage(settings).plan(
            Context(settings, catalog),
            refs,
            catalog.artifacts(ids, "extract"),
            catalog.artifacts(ids, "download"),
        )

    assert plan.fresh == 1, "the pubget article must not be recomputed"
    assert [(w.ref.id, w.source) for w in plan.pending] == [(by_ace.id, "ace")]


def test_an_ace_article_with_a_better_extraction_stays_fresh(tmp_path):
    """The whole point: only ACE articles with nothing higher come back."""
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        download_sources=["pubget", "elsevier", "ace", "pdf"],
    )
    with Catalog.open(settings.catalog_root) as catalog:
        ref = catalog.register(Identifier(pmcid="PMC3", pmid="3"))
        for source in ("pubget", "ace"):
            catalog.record(
                [
                    Outcome(
                        article_id=ref.id,
                        stage="download",
                        source=source,
                        fingerprint=f"dl-{source}",
                        payload={"files": []},
                    )
                ]
            )
        stage = ExtractStage(settings)
        download = catalog.artifact(ref.id, "download", "pubget")
        catalog.record(
            [
                Outcome(
                    article_id=ref.id,
                    stage="extract",
                    source="pubget",
                    fingerprint=stage.fingerprint_for("pubget", download),
                    payload={"tables": []},
                )
            ]
        )

        plan = stage.plan(
            Context(settings, catalog),
            [ref],
            catalog.artifacts([ref.id], "extract"),
            catalog.artifacts([ref.id], "download"),
        )

    assert plan.pending == []
    assert plan.fresh == 1
