"""Pyarty-powered export schema with prefix-based directory layouts."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Sequence

from pyarty import Dir, File, bundle, twig

from .analysis import CreateAnalysesResult
from .extract import ArticleExtractionBundle, ExtractedTable


# --------------------------------------------------------------------------- #
# Bundle definitions
# --------------------------------------------------------------------------- #
@bundle
class TableAsset:
    stem: str
    content: File[Path] = twig(name="{stem}", copyfile=True)


@bundle
class AnalysisAsset:
    filename: str
    payload: File[bytes] = twig(name="{filename}")


@bundle
class SourceFile:
    stem: str
    payload: File[Path] = twig(name="{stem}", copyfile=True)


@bundle
class ProcessedSource:
    source: str
    article_data: File[dict[str, Any]]
    article_metadata: File[dict[str, Any]]
    tables_manifest: File[list[dict[str, Any]]] = twig(name="tables", extension="json")
    tables: Dir[list[TableAsset]]
    analyses: Dir[list[AnalysisAsset]]


@bundle
class SourceDump:
    source: str
    files: Dir[list[SourceFile]] = twig(name=".")


@bundle
class ArticleExport:
    identifiers: File[dict[str, Any]]
    processed: Dir[list[ProcessedSource]] = twig(prefix="processed", name=("{source}", "field"))
    source: Dir[list[SourceDump]] = twig(prefix="source", name=("{source}", "field"))


# --------------------------------------------------------------------------- #
# Conversion helpers
# --------------------------------------------------------------------------- #
def build_article_export(
    bundle: ArticleExtractionBundle,
    analyses: Sequence[CreateAnalysesResult] | None = None,
) -> tuple[str, ArticleExport]:
    identifier = bundle.article_data.identifier
    if identifier is None:
        raise ValueError("Cannot build export without an identifier.")

    processed_sources = [_build_processed_source(bundle, analyses or ())]
    source_dumps = _build_source_dumps(bundle)

    export_bundle = ArticleExport(
        identifiers=identifier.to_dict(),
        processed=processed_sources,
        source=source_dumps,
    )
    return identifier.slug, export_bundle


def _build_processed_source(
    bundle: ArticleExtractionBundle,
    analyses: Sequence[CreateAnalysesResult],
) -> ProcessedSource:
    source_name = bundle.article_data.source.value
    tables_manifest = [_table_manifest_entry(table) for table in bundle.article_data.tables]
    tables = [_table_asset_for(table, idx) for idx, table in enumerate(bundle.article_data.tables)]
    tables = [asset for asset in tables if asset is not None]
    analysis_assets = _analysis_assets(bundle, analyses)

    return ProcessedSource(
        source=source_name,
        article_data=bundle.article_data.to_dict(),
        article_metadata=bundle.article_metadata.to_dict(),
        tables_manifest=tables_manifest,
        tables=tables,
        analyses=analysis_assets,
    )


def _table_manifest_entry(table: ExtractedTable) -> dict[str, Any]:
    payload = table.to_dict()
    payload["raw_content_path"] = str(table.raw_content_path)
    return payload


def _table_asset_for(table: ExtractedTable, index: int) -> TableAsset | None:
    raw_path = table.raw_content_path
    if not raw_path or not Path(raw_path).exists():
        return None
    sanitized = _sanitize_table_id(table.table_id, index)
    return TableAsset(stem=sanitized, content=Path(raw_path))


def _analysis_assets(
    bundle: ArticleExtractionBundle,
    analyses: Sequence[CreateAnalysesResult],
) -> list[AnalysisAsset]:
    results = [
        result for result in analyses if result.article_slug == bundle.article_data.slug
    ]
    used_names: set[str] = set()
    assets: list[AnalysisAsset] = []
    for index, result in enumerate(results):
        base = result.sanitized_table_id or result.table_id
        sanitized = _sanitize_table_id(base, index)
        stem = _unique_stem(sanitized, used_names, index)
        filename = f"{stem}.jsonl"
        payload = json.dumps(result.analysis_collection.to_dict(), indent=2).encode("utf-8")
        assets.append(AnalysisAsset(filename=filename, payload=payload))
    return assets


def _build_source_dumps(bundle: ArticleExtractionBundle) -> list[SourceDump]:
    files: list[SourceFile] = []
    full_text_path = bundle.article_data.full_text_path
    if full_text_path and full_text_path.exists():
        files.append(
            SourceFile(
                stem=_stem_for(full_text_path),
                payload=full_text_path,
            )
        )
    if not files:
        return []
    return [SourceDump(source=bundle.article_data.source.value, files=files)]


# --------------------------------------------------------------------------- #
# Naming helpers
# --------------------------------------------------------------------------- #
def _sanitize_table_id(table_id: str | None, index: int) -> str:
    if table_id:
        normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", table_id).strip("-")
        if normalized:
            return normalized.lower()
    return f"table-{index + 1}"


def _unique_stem(base: str, used: set[str], index: int) -> str:
    stem = base
    if stem in used:
        stem = f"{stem}-{index}"
    while stem in used:
        stem = f"{stem}-{index + 1}"
    used.add(stem)
    return stem


def _stem_for(path: Path) -> str:
    suffix = path.suffix
    if suffix:
        return path.name[: -len(suffix)]
    return path.name


__all__ = [
    "ArticleExport",
    "build_article_export",
]
