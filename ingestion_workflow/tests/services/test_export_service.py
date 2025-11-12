from __future__ import annotations

import json
from pathlib import Path

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
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
from ingestion_workflow.services.export import ExportService


def _bundle(tmp_path: Path, with_identifier: bool = True) -> ArticleExtractionBundle:
    table_file = tmp_path / "table.html"
    table_file.write_text("<table>example</table>", encoding="utf-8")
    table = ExtractedTable(
        table_id="Table A",
        raw_content_path=table_file,
        caption="Cap",
        footer="Foot",
    )
    identifier = Identifier(pmid="12345") if with_identifier else None
    content = ExtractedContent(
        hash_id="hash-1",
        source=DownloadSource.ELSEVIER,
        identifier=identifier,
        tables=[table],
    )
    metadata = ArticleMetadata(title="Sample", abstract="Abstract")
    return ArticleExtractionBundle(
        article_data=content,
        article_metadata=metadata,
    )


def test_export_skips_when_identifier_missing(tmp_path):
    bundle = _bundle(tmp_path, with_identifier=False)
    settings = Settings(data_root=tmp_path)
    exporter = ExportService(settings)
    exporter.export(bundle)
    assert not (tmp_path / "export").exists()


def test_export_writes_structure(tmp_path):
    bundle = _bundle(tmp_path)
    bundle.article_data.full_text_path = tmp_path / "fulltext.txt"
    bundle.article_data.full_text_path.write_text("full text", encoding="utf-8")
    settings = Settings(data_root=tmp_path)
    exporter = ExportService(settings)
    exporter.export(bundle)

    root = tmp_path / "export" / bundle.article_data.identifier.hash_id
    assert (root / "identifiers.json").exists()

    source_dir = root / "source" / bundle.article_data.source.value
    assert any(source_dir.iterdir())

    processed_dir = root / "processed" / bundle.article_data.source.value
    tables_json = json.loads((processed_dir / "tables.json").read_text())
    assert len(tables_json) == 1
    copied_table = processed_dir / "tables" / "table-a.html"
    assert copied_table.exists()


def test_export_writes_analyses_jsonl(tmp_path):
    bundle = _bundle(tmp_path)
    settings = Settings(data_root=tmp_path)
    exporter = ExportService(settings, overwrite=True)

    collection = AnalysisCollection(
        hash_id="hash-1::table-a",
        analyses=[
            Analysis(
                name="analysis",
                coordinates=[
                    Coordinate(
                        x=1,
                        y=2,
                        z=3,
                        space=CoordinateSpace.MNI,
                    )
                ],
            )
        ],
        coordinate_space=CoordinateSpace.MNI,
        identifier=bundle.article_data.identifier,
    )
    result = CreateAnalysesResult(
        hash_id="hash-1::table-a",
        article_hash=bundle.article_data.hash_id,
        table_id="Table A",
        sanitized_table_id="table-a",
        analysis_collection=collection,
    )

    exporter.export(bundle, [result])
    analyses_dir = (
        settings.data_root
        / "export"
        / bundle.article_data.identifier.hash_id
        / "processed"
        / bundle.article_data.source.value
        / "analyses"
    )
    files = list(analyses_dir.glob("*.jsonl"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text())
    assert payload["analyses"][0]["name"] == "analysis"
