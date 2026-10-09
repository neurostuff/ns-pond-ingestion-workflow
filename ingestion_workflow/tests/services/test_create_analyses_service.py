from __future__ import annotations

from pathlib import Path

import pytest
from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    ArticleMetadata,
    Coordinate,
    CoordinatePoint,
    CoordinateSpace,
    ExtractedContent,
    ExtractedTable,
    Identifier,
    ParseAnalysesOutput,
    ParsedAnalysis,
    PointsValue,
)
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.services.create_analyses import CreateAnalysesService


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


def _run_with(monkeypatch, tmp_path, analyses):
    table_path = tmp_path / "table.html"
    table_path.write_text("<table><tr><td>X</td></tr></table>", encoding="utf-8")
    monkeypatch.setattr(
        CoordinateParsingClient,
        "parse_analyses",
        lambda self, prompt, model=None: ParseAnalysesOutput(analyses=analyses),
    )
    readings: dict = {}
    service = CreateAnalysesService(Settings(llm_api_key="test"))
    results = service.run(_make_bundle(table_path), readings=readings)
    return results, readings


def _point(value=2.5):
    return CoordinatePoint(
        coordinates=[10.0, 12.0, 15.0], values=[PointsValue(value=value, kind="T")]
    )


def test_a_table_with_no_coordinates_is_a_reading_not_an_analysis(monkeypatch, tmp_path):
    """The prompt used to answer an `UNKNOWN` analysis with no points for such a
    table, which looks exactly like a contrast reported `n.s.`, so pondie read
    each one as a null result."""
    results, readings = _run_with(
        monkeypatch, tmp_path, [ParsedAnalysis(name="UNKNOWN", points=[])]
    )
    assert results == {}
    assert readings == {"Table 1": "no_coordinates"}

    results, readings = _run_with(monkeypatch, tmp_path, [])
    assert results == {}
    assert readings == {"Table 1": "no_coordinates"}


def test_a_named_contrast_with_no_points_survives(monkeypatch, tmp_path):
    """An `n.s.` row is a contrast the paper ran and found nothing for."""
    results, readings = _run_with(
        monkeypatch, tmp_path,
        [ParsedAnalysis(name="Patients > controls", points=[]),
         ParsedAnalysis(name=" unknown ", points=[])],
    )
    assert [a.name for a in results["Table 1"].analyses] == ["Patients > controls"]
    assert results["Table 1"].analyses[0].coordinates == []
    assert readings == {"Table 1": "contrasts_without_coordinates"}


def test_unlabelled_points_are_kept_under_their_placeholder(monkeypatch, tmp_path):
    results, readings = _run_with(
        monkeypatch, tmp_path, [ParsedAnalysis(name="UNKNOWN", points=[_point()])]
    )
    assert [len(a.coordinates) for a in results["Table 1"].analyses] == [1]
    assert readings == {"Table 1": "coordinates"}


def test_the_prompt_no_longer_asks_for_a_placeholder():
    import inspect

    src = inspect.getsource(CreateAnalysesService._build_prompt)
    assert 'analysis with name "UNKNOWN" and' not in src
    assert 'return "analyses": []' in src


def test_the_prompt_rules_version_reaches_only_the_prompted_fingerprint(monkeypatch):
    """Bumping it must re-parse what the prompt read, and leave the fine-tune's
    corpus, which never saw the prompt, fresh."""
    from types import SimpleNamespace

    from ingestion_workflow.pipeline.stages import analyses as mod

    class _Art:
        fingerprint = "triage-1"

    def fp(native):
        settings = SimpleNamespace(llm_model="m", llm_native_schema=native)
        return mod.AnalysesStage(settings=settings).fingerprint_for(_Art())

    before = {native: fp(native) for native in (True, False)}
    monkeypatch.setattr(mod, "PROMPT_RULES_VERSION", "bumped")
    assert fp(True) == before[True]
    assert fp(False) != before[False]
