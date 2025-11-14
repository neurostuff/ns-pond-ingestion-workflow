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
from ingestion_workflow.services import cache as cache_service
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
        slug="hash-1",
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
    settings = Settings(
        data_root=tmp_path,
        cache_root=tmp_path / ".cache",
        ns_pond_root=tmp_path / "ns",
    )
    settings.ensure_directories()
    exporter = ExportService(settings)
    exporter.export(bundle)

    root = tmp_path / "export" / bundle.article_data.identifier.slug
    assert (root / "identifiers.json").exists()

    processed_manifest = root / "processed" / bundle.article_data.source.value / "processed.json"
    assert processed_manifest.exists()
    processed_content = json.loads(processed_manifest.read_text(encoding="utf-8"))
    assert processed_content["article_data"]["tables"][0]["table_id"] == "Table A"

    source_manifest = root / "source" / bundle.article_data.source.value / "source.json"
    assert source_manifest.exists()
    source_content = json.loads(source_manifest.read_text(encoding="utf-8"))
    assert source_content["tables"][0]["table_id"] == "Table A"
    assert source_content["raw_downloads"] == []


def test_export_writes_analyses_jsonl(tmp_path):
    bundle = _bundle(tmp_path)
    settings = Settings(
        data_root=tmp_path,
        cache_root=tmp_path / ".cache",
        ns_pond_root=tmp_path / "ns",
    )
    settings.ensure_directories()
    exporter = ExportService(settings, overwrite=True)

    collection = AnalysisCollection(
        slug="hash-1::table-a",
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
        slug="hash-1::table-a",
        article_slug=bundle.article_data.slug,
        table_id="Table A",
        sanitized_table_id="table-a",
        analysis_collection=collection,
    )
    cache_service.cache_create_analyses_results(
        settings,
        bundle.article_data.source.value,
        [result],
    )

    exporter.export(bundle, [result])
    processed_manifest = (
        settings.data_root
        / "export"
        / bundle.article_data.identifier.slug
        / "processed"
        / bundle.article_data.source.value
        / "processed.json"
    )
    manifest_payload = json.loads(processed_manifest.read_text(encoding="utf-8"))
    analysis_entry = manifest_payload["article_data"]["analyses"][0]
    assert analysis_entry["jsonl_path"] is not None
    analysis_path = Path(analysis_entry["jsonl_path"])
    assert analysis_path.exists()
    payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    assert payload["analyses"][0]["name"] == "analysis"


def test_export_overwrites_existing_tree(tmp_path):
    bundle = _bundle(tmp_path)
    bundle.article_data.full_text_path = tmp_path / "fulltext.txt"
    bundle.article_data.full_text_path.write_text("full text", encoding="utf-8")
    settings = Settings(
        data_root=tmp_path,
        cache_root=tmp_path / ".cache",
        ns_pond_root=tmp_path / "ns",
    )
    settings.ensure_directories()
    exporter = ExportService(settings, overwrite=True)

    exporter.export(bundle)
    export_root = settings.data_root / "export" / bundle.article_data.identifier.slug
    stale = export_root / "processed" / bundle.article_data.source.value / "stale.txt"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("old", encoding="utf-8")

    exporter.export(bundle)
    assert not stale.exists()
    assert (export_root / "identifiers.json").exists()
