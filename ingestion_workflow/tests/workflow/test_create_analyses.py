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


def test_run_create_analyses_ignores_cache_when_configured(monkeypatch, tmp_path):
    bundle = _bundle(tmp_path)

    def _fail_cache(*_args, **_kwargs):
        raise AssertionError("Cache should be ignored when configured")

    monkeypatch.setattr(
        workflow_module.cache,
        "get_cached_create_analyses_result",
        _fail_cache,
    )

    class _ServiceStub:
        def __init__(self, *args, **kwargs):
            self.called = False

        def run(self, bundle, progress_hook=None):
            self.called = True
            analysis = Analysis(name="fresh")
            collection = AnalysisCollection(
                slug="article-1::table-1",
                analyses=[analysis],
            )
            return {bundle.article_data.tables[0].table_id: collection}

    service_stub = _ServiceStub()
    monkeypatch.setattr(
        workflow_module,
        "CreateAnalysesService",
        lambda *args, **kwargs: service_stub,
    )
    monkeypatch.setattr(
        workflow_module.cache,
        "cache_create_analyses_results",
        lambda *_args, **_kwargs: None,
    )

    results = workflow_module.run_create_analyses(
        [bundle],
        settings=Settings(
            llm_api_key="test",
            data_root=tmp_path,
            cache_root=tmp_path / "cache",
            ignore_cache_stages=["create_analyses"],
        ),
    )

    assert service_stub.called
    assert results["article-1"]["Table 1"].analyses[0].name == "fresh"


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


def _stamped_result(metadata):
    from ingestion_workflow.models import AnalysisCollection, CreateAnalysesResult

    return CreateAnalysesResult(
        slug="article::t1",
        article_slug="article",
        table_id="t1",
        sanitized_table_id="t1",
        analysis_collection=AnalysisCollection(slug="article::t1"),
        metadata=metadata,
    )


def test_cached_result_is_reused_when_prompt_and_model_match():
    """A cache hit requires the same prompt version and model that produced it."""
    from ingestion_workflow.config import Settings
    from ingestion_workflow.models import CreateAnalysesResult
    from ingestion_workflow.prompts.coordinate_parsing import (
        COORDINATE_PARSING_PROMPT_VERSION,
    )
    from ingestion_workflow.workflow.create_analyses import _stamp_matches

    settings = Settings(llm_model="gpt-5-mini")
    cached = _stamped_result(
        {
            "prompt_version": COORDINATE_PARSING_PROMPT_VERSION,
            "llm_model": "gpt-5-mini",
        }
    )

    assert _stamp_matches(cached, settings) is True


def test_cached_result_is_rejected_after_a_prompt_or_model_change():
    from ingestion_workflow.config import Settings
    from ingestion_workflow.models import CreateAnalysesResult
    from ingestion_workflow.prompts.coordinate_parsing import (
        COORDINATE_PARSING_PROMPT_VERSION,
    )
    from ingestion_workflow.workflow.create_analyses import _stamp_matches

    settings = Settings(llm_model="gpt-5-mini")

    stale_prompt = _stamped_result(
        {"prompt_version": "older-prompt", "llm_model": "gpt-5-mini"}
    )
    stale_model = _stamped_result(
        {
            "prompt_version": COORDINATE_PARSING_PROMPT_VERSION,
            "llm_model": "some-other-model",
        }
    )

    assert _stamp_matches(stale_prompt, settings) is False
    assert _stamp_matches(stale_model, settings) is False


def test_unstamped_cache_entries_are_grandfathered():
    """Pre-stamping entries must not trigger a corpus-wide LLM re-run."""
    from ingestion_workflow.config import Settings
    from ingestion_workflow.workflow.create_analyses import _stamp_matches

    assert _stamp_matches(_stamped_result({}), Settings(llm_model="gpt-5-mini")) is True
