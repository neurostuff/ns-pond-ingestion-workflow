"""Writes the ns-pond on-disk layout that pondie reads.

The shapes here — `stage1/analyses.json`, `pmids.tsv`, `processed/<source>/…` —
are a contract with pondie, not an internal choice. Lifted unchanged from the
pre-refactor sync stage.
"""

from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Dict, List, Mapping, MutableMapping, Sequence, Tuple

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    DownloadResult,
    Identifier,
)
from ingestion_workflow.services.create_analyses import sanitize_table_id
from ingestion_workflow.services.logging import get_logger

logger = get_logger(__name__)


def _sync_article(
    base_study_id: str,
    bundle: ArticleExtractionBundle,
    per_table_analyses: Mapping[str, AnalysisCollection],
    downloads: Sequence[DownloadResult],
    settings: Settings,
) -> None:
    root = Path(settings.ns_pond_root) / base_study_id
    root.mkdir(parents=True, exist_ok=True)
    identifier = bundle.article_data.identifier
    if identifier is None and downloads:
        identifier = downloads[0].identifier

    if identifier:
        _write_json(
            root / "identifiers.json",
            _identifier_payload(identifier),
            overwrite=settings.sync_overwrite,
        )

    _write_processed(
        root,
        bundle,
        per_table_analyses,
        overwrite=settings.sync_overwrite,
    )
    _write_sources(
        root,
        bundle,
        downloads,
        overwrite=settings.sync_overwrite,
    )
    _write_stage1(
        root / "stage1" / "analyses.json",
        per_table_analyses,
        overwrite=settings.sync_overwrite,
    )


def _write_processed(
    root: Path,
    bundle: ArticleExtractionBundle,
    per_table_analyses: Mapping[str, AnalysisCollection],
    *,
    overwrite: bool,
) -> None:
    source_name = bundle.article_data.source.value
    processed_root = root / "processed" / source_name
    processed_root.mkdir(parents=True, exist_ok=True)

    _write_metadata(processed_root / "metadata.json", bundle, per_table_analyses, overwrite)
    _write_text(processed_root, bundle.article_data.full_text_path, overwrite)
    _write_tables_jsonl(processed_root / "tables.jsonl", bundle, overwrite)
    _write_analyses_jsonl(processed_root / "analyses.jsonl", per_table_analyses, overwrite)
    _write_coordinates_csv(processed_root / "coordinates.csv", bundle, overwrite)


def _write_sources(
    root: Path,
    bundle: ArticleExtractionBundle,
    downloads: Sequence[DownloadResult],
    *,
    overwrite: bool,
) -> None:
    source_name = bundle.article_data.source.value
    source_root = root / "source" / source_name
    source_root.mkdir(parents=True, exist_ok=True)

    for table_index, table in enumerate(bundle.article_data.tables):
        raw_path = table.raw_content_path
        if not raw_path or not raw_path.exists():
            continue
        suffix = raw_path.suffix or ".html"
        sanitized = sanitize_table_id(table.table_id, table_index)
        destination = source_root / "tables" / f"{sanitized}{suffix}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        _copy_file(raw_path, destination, overwrite)

    for download in downloads:
        if download.identifier is None:
            continue
        for file in download.files:
            target = source_root / file.file_path.name
            _copy_file(file.file_path, target, overwrite)


def _identifier_payload(identifier: Identifier) -> Dict[str, object]:
    payload = dict(identifier.to_dict())
    cleaned = {key: value for key, value in payload.items() if value not in (None, {}, "null")}
    if "other_ids" not in cleaned and payload.get("other_ids"):
        cleaned["other_ids"] = payload["other_ids"]
    if "other_ids" not in cleaned:
        cleaned["other_ids"] = {}
    return cleaned


def _write_json(path: Path, payload: Mapping[str, object], overwrite: bool) -> None:
    if path.exists() and not overwrite:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(processed_root: Path, text_path: Path | None, overwrite: bool) -> None:
    if not text_path or not text_path.exists():
        return
    suffix = text_path.suffix or ".txt"
    destination = processed_root / f"text{suffix}"
    _copy_file(text_path, destination, overwrite)


def _write_metadata(
    metadata_path: Path,
    bundle: ArticleExtractionBundle,
    per_table_analyses: Mapping[str, AnalysisCollection],
    overwrite: bool,
) -> None:
    collection = next(iter(per_table_analyses.values()), None)
    coordinate_space = collection.coordinate_space.value if collection else None
    metadata = bundle.article_metadata
    authors = "; ".join(author.name for author in metadata.authors) if metadata.authors else None
    text_path = bundle.article_data.full_text_path
    has_text = bool(text_path and Path(text_path).exists())
    payload: MutableMapping[str, object] = {
        "title": metadata.title,
        "authors": authors,
        "journal": metadata.journal,
        "keywords": metadata.keywords or None,
        "abstract": metadata.abstract,
        "publication_year": metadata.publication_year,
        "coordinate_space": coordinate_space,
        "license": metadata.license,
        "text": has_text,
    }
    _write_json(metadata_path, payload, overwrite)


def _write_tables_jsonl(path: Path, bundle: ArticleExtractionBundle, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [table.to_dict() for table in bundle.article_data.tables]
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def _write_analyses_jsonl(
    path: Path,
    per_table_analyses: Mapping[str, AnalysisCollection],
    overwrite: bool,
) -> None:
    if path.exists() and not overwrite:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for table_id, collection in per_table_analyses.items():
            for analysis in collection.analyses:
                record = {
                    **analysis.to_dict(),
                    "table_id": analysis.table_id or table_id,
                    "coordinate_space": collection.coordinate_space.value,
                }
                handle.write(json.dumps(record))
                handle.write("\n")


def _write_corpus_manifest(
    path: Path,
    synced: Sequence[Tuple[str, ArticleExtractionBundle]],
) -> None:
    """List the synced studies in the form pondie's CLI parses.

    `pmid<TAB>study_id<TAB>source`. pondie rejects a file of bare ids outright, so the
    three columns are the contract rather than a convenience.
    """
    if not synced:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for base_study_id, bundle in synced:
        identifier = bundle.article_data.identifier
        pmid = (identifier.pmid if identifier else None) or ""
        source = bundle.article_data.source.value
        lines.append(f"{pmid}\t{base_study_id}\t{source}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_stage1(
    path: Path,
    per_table_analyses: Mapping[str, AnalysisCollection],
    overwrite: bool,
) -> None:
    """Write the coordinate-table parse in the shape pondie reads it.

    pondie treats `stage1/analyses.json` as an input it never writes, and reads each
    point's xyz from a nested `coordinates` key; this repo stores x/y/z on the point
    itself, so the nesting is added here rather than teaching pondie a third shape.
    """
    if path.exists() and not overwrite:
        return
    path.parent.mkdir(parents=True, exist_ok=True)

    analyses: list[dict[str, object]] = []
    for table_id, collection in per_table_analyses.items():
        for analysis in collection.analyses:
            analyses.append(
                {
                    "name": analysis.name,
                    "description": analysis.description,
                    "table_id": analysis.table_id or table_id,
                    "table_number": analysis.table_number,
                    "table_caption": analysis.table_caption,
                    "table_footer": analysis.table_footer,
                    "coordinate_space": collection.coordinate_space.value,
                    "points": [
                        _stage1_point(coordinate, collection)
                        for coordinate in analysis.coordinates
                    ],
                }
            )

    payload = {"analyses": analyses}
    path.write_text(json.dumps(payload, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")


def _stage1_point(coordinate, collection: AnalysisCollection) -> dict[str, object]:
    space = coordinate.space or collection.coordinate_space
    point: dict[str, object] = {
        "coordinates": [coordinate.x, coordinate.y, coordinate.z],
        "space": space.value if space else None,
    }
    if coordinate.statistic_value is not None:
        point["values"] = [
            {
                "value": coordinate.statistic_value,
                "kind": coordinate.statistic_type,
            }
        ]
    if coordinate.cluster_size is not None:
        point["cluster_size"] = coordinate.cluster_size
        point["cluster_measure"] = coordinate.cluster_measure
    return point


def _write_coordinates_csv(
    path: Path,
    bundle: ArticleExtractionBundle,
    overwrite: bool,
) -> None:
    if path.exists() and not overwrite:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    standard_headers = [
        "table_id",
        "table_label",
        "table_caption",
        "table_number",
        "x",
        "y",
        "z",
        "p_value",
        "region",
        "size",
        "statistic",
        "groups",
    ]

    rows: List[Dict[str, object]] = []
    headers: List[str] = list(standard_headers)
    for table in bundle.article_data.tables:
        coord_path_raw = (table.metadata or {}).get("coordinates_path")
        coord_path: Path | None = None
        if coord_path_raw:
            candidate = Path(str(coord_path_raw))
            if not candidate.is_absolute() and table.raw_content_path:
                candidate = table.raw_content_path.parent / candidate
            coord_path = candidate

        if coord_path and coord_path.exists():
            with coord_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames:
                    for field in reader.fieldnames:
                        if field not in headers:
                            headers.append(field)
                for row in reader:
                    rows.append(row)
            continue

        for coord in table.coordinates:
            rows.append(
                {
                    "table_id": table.table_id,
                    "table_label": getattr(table, "table_id", ""),
                    "table_caption": table.caption,
                    "table_number": table.table_number,
                    "x": coord.x,
                    "y": coord.y,
                    "z": coord.z,
                    "p_value": coord.statistic_value if getattr(coord, "statistic_type", None) == "P" else "",
                    "region": "",
                    "size": coord.cluster_size if hasattr(coord, "cluster_size") else "",
                    "statistic": coord.statistic_value
                    if getattr(coord, "statistic_type", None) != "P"
                    else "",
                    "groups": "",
                }
            )

    for field in standard_headers:
        if field not in headers:
            headers.append(field)

    with path.open("w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def _copy_file(source: Path, destination: Path, overwrite: bool) -> None:
    if not source.exists():
        logger.debug("Source file missing for sync copy: %s", source)
        return
    if destination.exists() and not overwrite:
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(source, destination)
    except Exception:
        shutil.copy(source, destination)


def write_article(
    root: Path,
    base_study_id: str,
    bundle: ArticleExtractionBundle,
    analyses: Mapping[str, AnalysisCollection],
    downloads: Sequence[DownloadResult],
    *,
    overwrite: bool = True,
) -> Path:
    """Materialise one article under `root/<base_study_id>/`."""
    _sync_article(
        base_study_id,
        bundle,
        analyses,
        downloads,
        _Target(Path(root), overwrite),
    )
    return Path(root) / base_study_id


class _Target:
    """The two settings `_sync_article` reads, without requiring a full Settings."""

    def __init__(self, ns_pond_root: Path, overwrite: bool) -> None:
        self.ns_pond_root = ns_pond_root
        self.sync_overwrite = overwrite


def write_corpus_manifest(
    path: Path, synced: Sequence[Tuple[str, ArticleExtractionBundle]]
) -> None:
    _write_corpus_manifest(Path(path), synced)


__all__ = ["write_article", "write_corpus_manifest"]
