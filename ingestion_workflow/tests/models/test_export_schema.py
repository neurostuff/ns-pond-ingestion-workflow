from __future__ import annotations

from pathlib import Path

from ingestion_workflow.models.analysis import (
    Analysis,
    AnalysisCollection,
    Coordinate,
    CoordinateSpace,
    CreateAnalysesResult,
)
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.models.export_schema import ArticleExport, build_article_export
from ingestion_workflow.models.extract import ArticleExtractionBundle, ExtractedContent, ExtractedTable
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.models.ids import Identifier


def _bundle(tmp_path: Path) -> ArticleExtractionBundle:
    table_file = tmp_path / "table.html"
    table_file.write_text("<table>1</table>", encoding="utf-8")
    full_text = tmp_path / "article.txt"
    full_text.write_text("full text", encoding="utf-8")

    identifier = Identifier(pmid="12345")
    table = ExtractedTable(
        table_id="Table A",
        raw_content_path=table_file,
        caption="Cap",
        footer="Foot",
    )
    content = ExtractedContent(
        slug="bundle-1",
        source=DownloadSource.ELSEVIER,
        identifier=identifier,
        full_text_path=full_text,
        tables=[table],
    )
    metadata = ArticleMetadata(title="Sample", abstract="Abstract")
    return ArticleExtractionBundle(article_data=content, article_metadata=metadata)


def _analysis(bundle: ArticleExtractionBundle) -> CreateAnalysesResult:
    collection = AnalysisCollection(
        slug="bundle-1::table-a",
        analyses=[Analysis(name="example", coordinates=[Coordinate(x=1, y=2, z=3, space=CoordinateSpace.MNI)])],
        coordinate_space=CoordinateSpace.MNI,
        identifier=bundle.article_data.identifier,
    )
    return CreateAnalysesResult(
        slug=collection.slug,
        article_slug=bundle.article_data.slug,
        table_id="Table A",
        sanitized_table_id="table-a",
        analysis_collection=collection,
    )


def test_build_article_export_round_trip(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    analysis = _analysis(bundle)

    slug, export_bundle = build_article_export(bundle, [analysis])
    assert slug == bundle.article_data.identifier.slug
    assert isinstance(export_bundle, ArticleExport)

    target = tmp_path / "export" / slug
    export_bundle.write(target, overwrite=True)

    identifiers = target / "identifiers.json"
    assert identifiers.exists()

    processed = target / "processed" / bundle.article_data.source.value
    assert (processed / "article_data.json").exists()
    assert (processed / "tables" / "table-a.html").exists()
    analyses_dir = processed / "analyses"
    assert list(analyses_dir.glob("*.jsonl"))

    source_dir = target / "source" / bundle.article_data.source.value
    assert (source_dir / "article.txt").exists()


def test_build_article_export_without_identifier_raises(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    bundle.article_data.identifier = None  # type: ignore[assignment]
    try:
        build_article_export(bundle, [])
    except ValueError:
        return
    raise AssertionError("Expected ValueError when identifier missing")
