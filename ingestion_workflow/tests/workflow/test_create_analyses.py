from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleExtractionBundle,
    ArticleMetadata,
    Coordinate,
    CreateAnalysesResult,
    ExtractedContent,
    ExtractedTable,
    Identifier,
)
from ingestion_workflow.models.coordinate_parsing import (
    ParseAnalysesOutput,
    ParsedAnalysis,
    CoordinatePoint,
)
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.workflow import create_analyses as workflow_module
from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient


@pytest.fixture(autouse=True)
def stub_llm_init(monkeypatch):
    monkeypatch.setattr(
        CoordinateParsingClient,
        "__init__",
        lambda self, settings, **kwargs: None,
    )


def _bundle(tmp_path: Path, *, has_coordinates: bool = True) -> ArticleExtractionBundle:
    table_path = tmp_path / "table.html"
    table_path.write_text("<table><tr><td>1</td></tr></table>", encoding="utf-8")
    coordinates = [Coordinate(x=0.0, y=0.0, z=0.0)] if has_coordinates else []
    table = ExtractedTable(
        table_id="Table 1",
        raw_content_path=table_path,
        caption="Cap",
        footer="Foot",
        coordinates=coordinates,
    )
    content = ExtractedContent(
        slug="article-1",
        source=DownloadSource.ELSEVIER,
        identifier=Identifier(pmid="12345"),
        tables=[table],
    )
    metadata = ArticleMetadata(title="Integration Article", abstract="Abstract")
    return ArticleExtractionBundle(article_data=content, article_metadata=metadata)


@pytest.mark.vcr(
    match_on=["method", "scheme", "host", "port", "path"],
    record_mode="once",
)
def test_run_create_analyses_end_to_end_with_vcr(monkeypatch, tmp_path):
    bundle = _bundle(tmp_path)

    def _httpbacked_parse(_self, prompt, model=None):
        payload = {
            "parsed": {
                "analyses": [
                    {
                        "name": "Integration Analysis",
                        "description": "LLM-derived",
                        "points": [
                            {
                                "coordinates": [5.0, 10.0, 15.0],
                                "space": "MNI",
                            }
                        ],
                    }
                ]
            }
        }
        response = requests.post(
            "https://postman-echo.com/post",
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        parsed_payload = response.json()["json"]["parsed"]
        return ParseAnalysesOutput(**parsed_payload)

    monkeypatch.setattr(
        CoordinateParsingClient,
        "parse_analyses",
        _httpbacked_parse,
    )

    results = workflow_module.run_create_analyses(
        [bundle],
        settings=Settings(llm_api_key="test-key"),
    )

    assert "article-1" in results
    per_table = results["article-1"]
    assert "Table 1" in per_table
    analysis = per_table["Table 1"].analyses[0]
    assert analysis.name == "Integration Analysis"
    assert analysis.coordinates[0].x == 5.0


def test_run_create_analyses_uses_cached_entries(monkeypatch, tmp_path):
    bundle = _bundle(tmp_path)

    cached_collection = AnalysisCollection(
        slug="article-1::table-1",
        analyses=[Analysis(name="cached")],
    )
    cached_entry = CreateAnalysesResult(
        slug="article-1::table-1",
        article_slug="article-1",
        table_id="Table 1",
        sanitized_table_id="table-1",
        analysis_collection=cached_collection,
    )

    monkeypatch.setattr(
        workflow_module.cache,
        "get_cached_create_analyses_result",
        lambda *_args, **_kwargs: cached_entry,
    )
    monkeypatch.setattr(
        workflow_module.cache,
        "cache_create_analyses_results",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Should not write cache")),
    )

    class _ServiceStub:
        def __init__(self, *args, **kwargs):
            self.run_called = False

        def run(self, bundle, progress_hook=None):
            raise AssertionError("Service should not be invoked for cached tables")

    monkeypatch.setattr(
        workflow_module,
        "CreateAnalysesService",
        lambda *args, **kwargs: _ServiceStub(),
    )

    results = workflow_module.run_create_analyses(
        [bundle],
        settings=Settings(llm_api_key="test"),
    )

    assert results["article-1"]["Table 1"] is cached_collection


def test_run_create_analyses_skips_tables_without_coordinates(monkeypatch, tmp_path):
    bundle = _bundle(tmp_path, has_coordinates=False)

    run_calls: list[ArticleExtractionBundle] = []

    class _ServiceStub:
        def __init__(self, *args, **kwargs):
            pass

        def run(self, bundle, progress_hook=None):
            run_calls.append(bundle)
            return {}

    monkeypatch.setattr(
        workflow_module,
        "CreateAnalysesService",
        lambda *args, **kwargs: _ServiceStub(),
    )

    results = workflow_module.run_create_analyses(
        [bundle],
        settings=Settings(llm_api_key="test"),
    )

    assert results["article-1"] == {}
    assert not run_calls
