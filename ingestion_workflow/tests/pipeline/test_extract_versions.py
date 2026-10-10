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


def test_a_source_fingerprint_is_its_own_version(stage, monkeypatch):
    """Bumping one source moves only that source's fingerprint."""
    before = {s: stage.fingerprint_for(s, UPSTREAM) for s in EXTRACTOR_VERSIONS}
    monkeypatch.setitem(EXTRACTOR_VERSIONS, "ace", EXTRACTOR_VERSIONS["ace"] + 1)
    after = {s: stage.fingerprint_for(s, UPSTREAM) for s in EXTRACTOR_VERSIONS}
    assert [s for s in before if before[s] != after[s]] == ["ace"]


@pytest.mark.parametrize("source", ["pubget", "elsevier", "ace", "pdf"])
def test_sources_whose_extractor_changed_moved(stage, source):
    assert EXTRACTOR_VERSIONS[source] > 1
    original = fingerprint("extract", source, 1, upstream="DL")
    assert stage.fingerprint_for(source, UPSTREAM) != original


def test_an_unlisted_source_falls_back_rather_than_crashing(stage):
    expected = fingerprint("extract", "mystery", DEFAULT_EXTRACTOR_VERSION, upstream="DL")
    assert stage.fingerprint_for("mystery", UPSTREAM) == expected


def test_a_bump_only_requeues_that_source(tmp_path, monkeypatch):
    """An article extracted by pdf stays fresh; one extracted only by ace
    comes back as work."""
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        download_sources=["pubget", "elsevier", "ace", "pdf"],
    )
    with Catalog.open(settings.catalog_root) as catalog:
        by_pdf = catalog.register(Identifier(doi="10.1/pdf"))
        by_ace = catalog.register(Identifier(pmid="2"))
        for ref, source in ((by_pdf, "pdf"), (by_ace, "ace")):
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
            # An extraction recorded under the current version.
            download = catalog.artifact(ref.id, "download", source)
            catalog.record(
                [
                    Outcome(
                        article_id=ref.id,
                        stage="extract",
                        source=source,
                        fingerprint=ExtractStage(settings).fingerprint_for(
                            source, download
                        ),
                        payload={"tables": []},
                    )
                ]
            )

        monkeypatch.setitem(EXTRACTOR_VERSIONS, "ace", EXTRACTOR_VERSIONS["ace"] + 1)
        refs = [by_pdf, by_ace]
        ids = [r.id for r in refs]
        plan = ExtractStage(settings).plan(
            Context(settings, catalog),
            refs,
            catalog.artifacts(ids, "extract"),
            catalog.artifacts(ids, "download"),
        )

    assert plan.fresh == 1, "the pdf article must not be recomputed"
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


def test_every_download_source_is_registered():
    """A source missing here silently runs at the default version, so a bump
    meant for it never reaches it."""
    from ingestion_workflow.models.download import DownloadSource

    sources = {source.value for source in DownloadSource}
    assert sources <= set(EXTRACTOR_VERSIONS)


@pytest.mark.parametrize("source", ["pmc", "europepmc"])
def test_registering_the_pmc_sources_re_extracts_nothing(stage, source):
    assert stage.fingerprint_for(source, UPSTREAM) == fingerprint(
        "extract", source, DEFAULT_EXTRACTOR_VERSION, upstream="DL")

