from __future__ import annotations

from pathlib import Path

from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleDirectory,
    ArticleExtractionBundle,
    ArticleMetadata,
    Coordinate,
    CoordinateSpace,
    CreateAnalysesResult,
    ExtractedContent,
    ExtractedTable,
    Identifier,
)
from ingestion_workflow.models.download import DownloadSource


def _bundle(tmp_path: Path, with_identifier: bool = True) -> ArticleExtractionBundle:
    table_file = tmp_path / "Table A.xml"
    table_file.write_text("<table>A</table>", encoding="utf-8")
    full_text = tmp_path / "article.txt"
    full_text.write_text("full text body", encoding="utf-8")

    identifier = Identifier(pmid="12345") if with_identifier else None
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
    metadata = ArticleMetadata(title="Sample Title", abstract="Sample Abstract")
    return ArticleExtractionBundle(article_data=content, article_metadata=metadata)


def _analysis_result(bundle: ArticleExtractionBundle) -> CreateAnalysesResult:
    collection = AnalysisCollection(
        slug=f"{bundle.article_data.slug}::table-a",
        analyses=[Analysis(name="a1", coordinates=[Coordinate(x=1, y=2, z=3, space=CoordinateSpace.MNI)])],
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


def test_article_directory_round_trip(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    analysis = _analysis_result(bundle)

    article_dir = ArticleDirectory.from_bundle(bundle, [analysis])
    export_root = tmp_path / "export"
    article_dir.save(export_root)

    reloaded = ArticleDirectory.load(export_root, bundle.article_data.identifier.slug)
    assert reloaded.identifier.to_dict() == bundle.article_data.identifier.to_dict()

    processed = reloaded.processed[bundle.article_data.source.value]
    assert len(processed.tables_index) == 1
    assert processed.table_files["table-a.xml"].content == "<table>A</table>"
    assert processed.analyses

    source_tree = reloaded.sources[bundle.article_data.source.value]
    assert "article.txt" in source_tree.files


def test_article_directory_from_bundle_requires_identifier(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, with_identifier=False)
    try:
        ArticleDirectory.from_bundle(bundle)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError when identifier missing")
