from __future__ import annotations

from dataclasses import dataclass

import pytest

from ingestion_workflow.models import (
    ArticleExtractionBundle,
    ArticleMetadata,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    Identifier,
)
from ingestion_workflow.workflow import extract as extract_module


@dataclass
class _FakeExtractor:
    payload: list[ExtractedContent]

    def extract(self, download_results, progress_hook=None):  # pragma: no cover
        assert len(download_results) == len(self.payload)
        return list(self.payload)


class _MetadataRecorder:
    def __init__(self, response):
        self.response = response
        self.seen = None

    def enrich_metadata(self, contents):
        self.seen = list(contents)
        return dict(self.response)


@pytest.fixture(autouse=True)
def stub_validation(monkeypatch):
    monkeypatch.setattr(
        extract_module,
        "_ensure_successful_download",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        extract_module.cache,
        "cache_extraction_results",
        lambda *_args, **_kwargs: None,
    )


@pytest.fixture
def download_result():
    return DownloadResult(
        identifier=Identifier(pmid="12345"),
        source=DownloadSource.ELSEVIER,
        success=True,
        files=[],
    )


def test_run_extraction_returns_bundles(monkeypatch, download_result):
    extraction = ExtractedContent(
        slug="hash-1",
        source=DownloadSource.ELSEVIER,
        identifier=download_result.identifier,
        has_coordinates=True,
    )
    fake_extractor = _FakeExtractor([extraction])
    metadata_response = {extraction.slug: ArticleMetadata(title="Test")}
    recorder = _MetadataRecorder(metadata_response)

    monkeypatch.setattr(
        extract_module,
        "_resolve_extractor_for_source",
        lambda *_args, **_kwargs: fake_extractor,
    )
    monkeypatch.setattr(
        extract_module.cache,
        "partition_cached_extractions",
        lambda *_args, **_kwargs: ([None], [download_result]),
    )
    monkeypatch.setattr(
        extract_module,
        "MetadataService",
        lambda *_args, **_kwargs: recorder,
    )

    bundles = extract_module.run_extraction([download_result])

    assert isinstance(bundles, list)
    assert len(bundles) == 1
    bundle = bundles[0]
    assert isinstance(bundle, ArticleExtractionBundle)
    assert bundle.article_data is extraction
    assert bundle.article_metadata is metadata_response[extraction.slug]
    assert recorder.seen == [extraction]


def test_run_extraction_placeholder_metadata(monkeypatch, download_result):
    extraction = ExtractedContent(
        slug="hash-2",
        source=DownloadSource.ELSEVIER,
        identifier=download_result.identifier,
        has_coordinates=False,
    )
    fake_extractor = _FakeExtractor([extraction])
    recorder = _MetadataRecorder({})

    monkeypatch.setattr(
        extract_module,
        "_resolve_extractor_for_source",
        lambda *_args, **_kwargs: fake_extractor,
    )
    monkeypatch.setattr(
        extract_module.cache,
        "partition_cached_extractions",
        lambda *_args, **_kwargs: ([None], [download_result]),
    )
    monkeypatch.setattr(
        extract_module,
        "MetadataService",
        lambda *_args, **_kwargs: recorder,
    )

    bundles = extract_module.run_extraction([download_result])

    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle.article_data is extraction
    assert bundle.article_metadata.title in {"12345", "hash-2"}
    assert recorder.seen == [extraction]
