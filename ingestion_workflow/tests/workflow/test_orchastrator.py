from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Dict

import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    ArticleMetadata,
    CoordinateSpace,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    ExtractedTable,
    Identifier,
    Identifiers,
)
from ingestion_workflow.workflow import orchastrator


def _bundle(identifier: Identifier) -> ArticleExtractionBundle:
    table = ExtractedTable(
        table_id="Table 1",
        raw_content_path=Path(__file__),
    )
    content = ExtractedContent(
        hash_id="hash-1",
        source=DownloadSource.ELSEVIER,
        identifier=identifier,
        tables=[table],
    )
    metadata = ArticleMetadata(title="Example Article")
    return ArticleExtractionBundle(article_data=content, article_metadata=metadata)


def _install_stub_module(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    **attrs,
) -> None:
    module = types.ModuleType(module_name)
    for key, value in attrs.items():
        setattr(module, key, value)
    monkeypatch.setitem(sys.modules, module_name, module)
    parent_name, _, child = module_name.rpartition(".")
    if parent_name:
        parent = sys.modules.get(parent_name)
        if parent is not None:
            setattr(parent, child, module)


def test_run_pipeline_full_sequence(monkeypatch, tmp_path):
    calls: list[str] = []
    identifier = Identifier(pmid="123")
    identifiers = Identifiers([identifier])
    download_results = [
        DownloadResult(
            identifier=identifier,
            source=DownloadSource.ELSEVIER,
            success=True,
        )
    ]
    bundles = [_bundle(identifier)]
    analyses = {
        bundles[0].article_data.hash_id: {
            "Table 1": AnalysisCollection(
                hash_id="hash-1::table-1",
                coordinate_space=CoordinateSpace.MNI,
                identifier=identifier,
            )
        }
    }

    _install_stub_module(
        monkeypatch,
        "ingestion_workflow.workflow.gather",
        gather_identifiers=lambda settings, manifest=None: calls.append("gather") or identifiers,
    )
    _install_stub_module(
        monkeypatch,
        "ingestion_workflow.workflow.download",
        run_downloads=lambda ids, settings=None, metrics=None: calls.append(
            "download"
        )
        or download_results,
    )
    _install_stub_module(
        monkeypatch,
        "ingestion_workflow.workflow.extract",
        run_extraction=lambda results, settings=None, metrics=None, progress_hook=None: calls.append(
            "extract"
        )
        or bundles,
    )
    _install_stub_module(
        monkeypatch,
        "ingestion_workflow.workflow.create_analyses",
        run_create_analyses=lambda bundle_seq, settings=None, extractor_name=None, metrics=None: calls.append(
            "create_analyses"
        )
        or analyses,
    )

    settings = Settings(
        data_root=tmp_path,
        stages=["gather", "download", "extract", "create_analyses"],
    )
    state = orchastrator.run_pipeline(settings=settings)

    assert calls == ["gather", "download", "extract", "create_analyses"]
    assert state.identifiers is identifiers
    assert state.downloads is download_results
    assert state.bundles is bundles
    assert state.analyses is analyses


def test_pipeline_uses_manifest_when_gather_skipped(monkeypatch, tmp_path):
    identifier = Identifier(pmid="456")
    identifiers = Identifiers([identifier])
    manifest = tmp_path / "manifest.jsonl"
    identifiers.save(manifest)

    seen = {}

    _install_stub_module(
        monkeypatch,
        "ingestion_workflow.workflow.download",
        run_downloads=lambda ids, settings=None, metrics=None: seen.setdefault(
            "count", len(ids.identifiers)
        )
        or [],
    )

    settings = Settings(
        data_root=tmp_path,
        stages=["download"],
        manifest_path=manifest,
    )
    orchastrator.run_pipeline(settings=settings)

    assert seen["count"] == 1


def test_pipeline_hydrates_from_cache_when_only_create(monkeypatch, tmp_path):
    identifier = Identifier(pmid="789")
    manifest = tmp_path / "manifest.jsonl"
    Identifiers([identifier]).save(manifest)

    downloads = [
        DownloadResult(
            identifier=identifier,
            source=DownloadSource.ELSEVIER,
            success=True,
        )
    ]
    bundles = [_bundle(identifier)]
    analyses = {
        bundles[0].article_data.hash_id: {
            "Table 1": AnalysisCollection(
                hash_id="hash-1::table-1",
                coordinate_space=CoordinateSpace.MNI,
                identifier=identifier,
            )
        }
    }

    monkeypatch.setattr(
        orchastrator,
        "_hydrate_downloads_from_cache",
        lambda settings, ids: downloads,
    )
    monkeypatch.setattr(
        orchastrator,
        "_hydrate_bundles_from_cache",
        lambda settings, dls: bundles,
    )
    _install_stub_module(
        monkeypatch,
        "ingestion_workflow.workflow.create_analyses",
        run_create_analyses=lambda bundle_seq, settings=None, extractor_name=None, metrics=None: analyses,
    )

    settings = Settings(
        data_root=tmp_path,
        stages=["create_analyses"],
        manifest_path=manifest,
        use_cached_inputs=True,
    )
    state = orchastrator.run_pipeline(settings=settings)

    assert state.downloads == downloads
    assert state.bundles == bundles
    assert state.analyses == analyses


def test_pipeline_errors_without_cached_inputs(tmp_path):
    identifier = Identifier(pmid="123")
    manifest = tmp_path / "manifest.jsonl"
    Identifiers([identifier]).save(manifest)

    settings = Settings(
        data_root=tmp_path,
        stages=["extract"],
        manifest_path=manifest,
        use_cached_inputs=False,
    )
    with pytest.raises(ValueError):
        orchastrator.run_pipeline(settings=settings)


def test_pipeline_requires_manifest_when_gather_missing(tmp_path):
    settings = Settings(
        data_root=tmp_path,
        stages=["download"],
        manifest_path=None,
    )
    with pytest.raises(ValueError):
        orchastrator.run_pipeline(settings=settings)


def test_gather_stage_combines_manifest(monkeypatch, tmp_path):
    manifest_identifier = Identifier(pmid="321")
    manifest = tmp_path / "manifest.jsonl"
    Identifiers([manifest_identifier]).save(manifest)

    returned_identifiers = Identifiers(
        [Identifier(pmid="654"), manifest_identifier]
    )
    seen: Dict[str, object] = {}

    def _fake_gather_identifiers(*, settings=None, manifest=None, **_kwargs):
        seen["manifest"] = manifest
        return returned_identifiers

    _install_stub_module(
        monkeypatch,
        "ingestion_workflow.workflow.gather",
        gather_identifiers=_fake_gather_identifiers,
    )

    settings = Settings(
        data_root=tmp_path,
        stages=["gather"],
        manifest_path=manifest,
    )

    state = orchastrator.run_pipeline(settings=settings)

    assert seen["manifest"] == manifest
    assert state.identifiers is returned_identifiers
