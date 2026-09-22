"""Pyarty-powered export schema with prefix-based directory layouts."""

from __future__ import annotations

from typing import Any, Sequence

from pyarty import Dir, File, at, bundle

from .analysis import CreateAnalysesResult
from .download import DownloadResult
from .extract import ArticleExtractionBundle


@bundle
class ProcessedManifest:
    source: str
    manifest: File[dict[str, Any]] = at("processed.json")


@bundle
class SourceManifest:
    source: str
    manifest: File[dict[str, Any]] = at("source.json")


@bundle
class ArticleExport:
    identifiers: File[dict[str, Any]]
    processed: Dir[list[ProcessedManifest]] = at("processed/{source}")
    source: Dir[list[SourceManifest]] = at("source/{source}")


def build_article_export(
    bundle: ArticleExtractionBundle,
    analyses: Sequence[CreateAnalysesResult] | None = None,
    *,
    downloads: Sequence[DownloadResult] | None = None,
) -> tuple[str, ArticleExport]:
    """Build the export bundle for a single article.

    `downloads` are passed in by the caller, which already holds them; the
    export no longer re-reads a cache to find out what was downloaded.
    """
    identifier = bundle.article_data.identifier
    if identifier is None:
        raise ValueError("Cannot build export without an identifier.")

    processed_manifest = _build_processed_manifest(bundle, analyses or ())
    source_manifest = _build_source_manifest(bundle, downloads)

    export_bundle = ArticleExport(
        identifiers=identifier.to_dict(),
        processed=[processed_manifest],
        source=[source_manifest],
    )
    return identifier.slug, export_bundle


def _build_processed_manifest(
    bundle: ArticleExtractionBundle,
    analyses: Sequence[CreateAnalysesResult],
) -> ProcessedManifest:
    source_name = bundle.article_data.source.value
    article_data = bundle.article_data.to_dict()
    relevant = [result for result in analyses if result.article_slug == bundle.article_data.slug]
    article_data["analyses"] = [_analysis_entry(result) for result in relevant]

    manifest_payload = {
        "article_data": article_data,
        "article_metadata": bundle.article_metadata.to_dict(),
    }
    return ProcessedManifest(source=source_name, manifest=manifest_payload)


def _analysis_entry(result: CreateAnalysesResult) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "table_id": result.table_id,
        "sanitized_table_id": result.sanitized_table_id,
        "slug": result.slug,
        "metadata": dict(result.metadata),
        "error_message": result.error_message,
    }
    if result.analysis_paths:
        entry["jsonl_path"] = str(result.analysis_paths[0])
    else:
        entry["analysis_collection"] = result.analysis_collection.to_dict()
        entry["jsonl_path"] = None
    return entry


def _build_source_manifest(
    bundle: ArticleExtractionBundle,
    downloads: Sequence[DownloadResult] | None,
) -> SourceManifest:
    source_name = bundle.article_data.source.value
    manifest_payload = {
        "source": source_name,
        "raw_downloads": _download_entries(downloads),
        "tables": _table_entries(bundle),
    }
    return SourceManifest(source=source_name, manifest=manifest_payload)


def _download_entries(
    download_results: Sequence[DownloadResult] | None,
) -> list[dict[str, Any]]:
    return [
        {
            "filename": file.file_path.name,
            "path": str(file.file_path),
            "file_type": file.file_type.value,
            "content_type": file.content_type,
            "downloaded_at": file.downloaded_at.isoformat(),
            "md5_hash": file.md5_hash,
        }
        for result in (download_results or ())
        for file in result.files
    ]


def _table_entries(bundle: ArticleExtractionBundle) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    for table in bundle.article_data.tables:
        tables.append(
            {
                "table_id": table.table_id,
                "table_number": table.table_number,
                "caption": table.caption,
                "footer": table.footer,
                "contains_coordinates": table.contains_coordinates,
                "raw_content_path": str(table.raw_content_path),
                "coordinates_path": table.metadata.get("coordinates_path"),
                "metadata": dict(table.metadata),
            }
        )
    return tables


__all__ = [
    "ArticleExport",
    "build_article_export",
]
