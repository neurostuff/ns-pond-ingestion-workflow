from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    ArticleMetadata,
    Coordinate,
    CoordinateSpace,
    CoordinatePoint,
    ExtractedContent,
    ExtractedTable,
    Identifier,
    ParseAnalysesOutput,
    ParsedAnalysis,
    PointsValue,
)
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.services.create_analyses import CreateAnalysesService
from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient


@pytest.fixture(autouse=True)
def stub_llm_init(monkeypatch):
    monkeypatch.setattr(
        CoordinateParsingClient,
        "__init__",
        lambda self, settings, **kwargs: None,
    )


def _make_bundle(table_path: Path) -> ArticleExtractionBundle:
    table = ExtractedTable(
        table_id="Table 1",
        raw_content_path=table_path,
        caption="Sample caption",
        footer="Sample footer",
        coordinates=[Coordinate(x=0.0, y=0.0, z=0.0, space=CoordinateSpace.MNI)],
    )
    content = ExtractedContent(
        slug="article-1",
        source=DownloadSource.ELSEVIER,
        identifier=Identifier(pmid="12345"),
        tables=[table],
    )
    metadata = ArticleMetadata(title="Example Article", abstract="Details")
    return ArticleExtractionBundle(article_data=content, article_metadata=metadata)


def test_create_analyses_service_builds_collection(monkeypatch, tmp_path):
    table_path = tmp_path / "table.html"
    table_path.write_text("<table><tr><td>X</td></tr></table>", encoding="utf-8")
    bundle = _make_bundle(table_path)

    parse_output = ParseAnalysesOutput(
        analyses=[
            ParsedAnalysis(
                name="Analysis A",
                description="desc",
                points=[
                    CoordinatePoint(
                        coordinates=[10.0, 12.0, 15.0],
                        space="MNI",
                        values=[PointsValue(value=2.5, kind="T")],
                    )
                ],
            )
        ]
    )
    monkeypatch.setattr(
        CoordinateParsingClient,
        "parse_analyses",
        lambda self, prompt, model=None: parse_output,
    )
    service = CreateAnalysesService(Settings(llm_api_key="test"))
    results = service.run(bundle)

    assert "Table 1" in results
    collection = results["Table 1"]
    assert isinstance(collection, AnalysisCollection)
    assert len(collection.analyses) == 1
    analysis = collection.analyses[0]
    assert analysis.table_caption == "Sample caption"
    assert pytest.approx(analysis.coordinates[0].x) == 10.0
