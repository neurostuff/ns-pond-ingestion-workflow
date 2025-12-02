"""
Step 5. Sync all data to remote storage.
This module will sync the staging and uploaded data to ns-pond.
New base-study ids will be created as folders and
the respective data will be copied over.
Some data may be overwritten if the base-study id already exists.
Have that be a config option, but by default we will overwrite
on a per-file basis. (DO NOT DELETE EVERYTHING AND THEN RUN AGAIN)

The structure will be as follows:
- /ns-pond/data/{base_study_id}
    - processed
        - {source_name} # such as 'elsevier', 'pubget', 'ace'.
            - text.txt
            - metadata.json
            - coordinates.csv
            - analyses.jsonl # NIMADS style analyses, each line is one analysis.
            - tables.jsonl # extracted tables, each line is one table.
    - source
        - {source_name} # such as 'elsevier', 'pubget', 'ace'.
            - article.[xml|bin|html]
            - tables/
                - {table_label}.[html|xml]

    - identifiers.json # {"pmid": "11182099", "doi": "10.1016/s0896-6273(01)00198-2", "pmcid": null, "other_ids": {}}

## metadata.json example
```
{
  "title": "Older adults preserve audiovisual integration through enhanced cortical activations, not by recruiting new regions",
    "authors": "Jones, Samuel A.; Noppeney, Uta; Jones, Samuel A.; Noppeney, Uta",
    "journal": "PLoS Biol",
    "keywords": null,
    "abstract": " \nEffective interactions with the environment rely on the integration of multisensory signals: Our brains must efficiently combine signals that share a common source, and segregate those that do not. Healthy ageing can change or impair this process. This functional magnetic resonance imaging study assessed the neural mechanisms underlying age differences in the integration of auditory and visual spatial cues. Participants were presented with synchronous audiovisual signals at various degrees of spatial disparity and indicated their perceived sound location. Behaviourally, older adults were able to maintain localisation accuracy. At the neural level, they integrated auditory and visual cues into spatial representations along dorsal auditory and visual processing pathways similarly to their younger counterparts but showed greater activations in a widespread system of frontal, temporal, and parietal areas. According to multivariate Bayesian decoding, these areas encoded critical stimulus information beyond that which was encoded in the brain areas commonly activated by both groups. Surprisingly, however, the boost in information provided by these areas with age-related activation increases was comparable across the 2 age groups. This dissociation\u2014between comparable information encoded in brain activation patterns across the 2 age groups, but age-related increases in regional blood-oxygen-level-dependent responses\u2014contradicts the widespread notion that older adults recruit new regions as a compensatory mechanism to encode task-relevant information. Instead, our findings suggest that activation increases in older adults reflect nonspecific or modulatory mechanisms related to less efficient or slower processing, or greater demands on attentional resources. \n  \nMultisensory perception changes as we get older. This study uses multivariate fMRI to explore the neural mechanisms underlying these changes, revealing that older adults recruit similar brain regions, but increase activations in some of these reasons to preserve their performance. \n ",
    "publication_year": 2024,
    "coordinate_space": "MNI",
    "license": "https://creativecommons.org/licenses/by/4.0/",
    "text": true
}
```

## coordinates.csv example
```
table_id,table_label,table_caption,table_number,x,y,z,p_value,region,size,statistic,groups
pbio.3002494.t003,Table 3,,,22.0,-54.0,-24.0,,,,,
pbio.3002494.t003,Table 3,,,6.0,-62.0,-16.0,,,,,
pbio.3002494.t003,Table 3,,,8.0,-72.0,-16.0,,,,,
pbio.3002494.t003,Table 3,,,-36.0,-20.0,64.0,,,,,
pbio.3002494.t003,Table 3,,,-32.0,-4.0,58.0,,,,,
pbio.3002494.t003,Table 3,,,-46.0,-34.0,42.0,,,,,
pbio.3002494.t003,Table 3,,,-4.0,0.0,56.0,,,,,
pbio.3002494.t003,Table 3,,,24.0,-2.0,50.0,,,,,
pbio.3002494.t003,Table 3,,,-14.0,-18.0,6.0,,,,,
pbio.3002494.t003,Table 3,,,-18.0,-68.0,54.0,,,,,
pbio.3002494.t003,Table 3,,,52.0,4.0,42.0,,,,,
pbio.3002494.t003,Table 3,,,-40.0,-36.0,10.0,,,,,
``` 
"""

from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Set

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    DownloadResult,
    Identifier,
    Identifiers,
    UploadOutcome,
)
from ingestion_workflow.services import cache
from ingestion_workflow.services.create_analyses import sanitize_table_id
from ingestion_workflow.services.logging import console_kwargs, get_logger
from ingestion_workflow.workflow.common import resolve_settings
from ingestion_workflow.workflow.upload import _hydrate_bundles_for_upload, _load_identifiers_from_manifest

try:  # pragma: no cover - imported lazily in runtime
    from ingestion_workflow.workflow.orchastrator import PipelineState
except Exception:  # pragma: no cover - type-checking only
    PipelineState = object  # type: ignore

logger = get_logger(__name__)


def run_sync(
    state: "PipelineState",
    *,
    settings: Settings | None = None,
) -> None:
    """Materialize uploaded articles into ns-pond layout."""
    resolved_settings = resolve_settings(settings)
    outcomes = _resolve_upload_outcomes(state, resolved_settings)
    successful = [outcome for outcome in outcomes if outcome.success and outcome.base_study_id]
    if not successful:
        logger.info("Sync skipped: no successful upload outcomes available.", extra=console_kwargs())
        return

    target_slugs = {outcome.slug for outcome in successful}
    identifier_set = _resolve_identifiers(state, resolved_settings)
    downloads = _resolve_downloads(state, resolved_settings, identifier_set, target_slugs)
    bundles = _resolve_bundles(state, resolved_settings, identifier_set, target_slugs)
    analyses = _resolve_analyses(state, resolved_settings, target_slugs)

    for outcome in successful:
        slug = outcome.slug
        base_id = outcome.base_study_id
        bundle = bundles.get(slug)
        per_table = analyses.get(slug, {})
        downloads_for_slug = downloads.get(slug, [])
        if bundle is None:
            logger.warning(
                "Sync skipped for %s (no bundle available)", slug, extra=console_kwargs()
            )
            continue
        _sync_article(
            base_id,
            bundle,
            per_table,
            downloads_for_slug,
            resolved_settings,
        )


def _resolve_upload_outcomes(state: "PipelineState", settings: Settings) -> List[UploadOutcome]:
    if getattr(state, "upload_outcomes", None):
        return list(state.upload_outcomes)
    index = cache.load_upload_index(settings)
    return [entry.payload for entry in index.iter_entries()]


def _resolve_identifiers(state: "PipelineState", settings: Settings) -> Identifiers | None:
    if getattr(state, "identifiers", None):
        return state.identifiers
    try:
        manifest_identifiers = _load_identifiers_from_manifest(settings)
    except Exception as exc:  # pragma: no cover - defensive for sync-only runs
        logger.warning("Unable to load manifest identifiers for sync: %s", exc, extra=console_kwargs())
        return None
    state.identifiers = manifest_identifiers
    return manifest_identifiers


def _resolve_downloads(
    state: "PipelineState",
    settings: Settings,
    identifiers: Identifiers | None,
    target_slugs: Set[str],
) -> Dict[str, List[DownloadResult]]:
    download_map: Dict[str, List[DownloadResult]] = {}
    for download in getattr(state, "downloads", []) or []:
        slug = getattr(download.identifier, "slug", None)
        if slug is None or slug not in target_slugs:
            continue
        download_map.setdefault(slug, []).append(download)

    missing = target_slugs.difference(download_map.keys())
    if not missing and download_map:
        return download_map

    cached = _hydrate_downloads_from_cache(settings, identifiers, missing or target_slugs)
    for download in cached:
        slug = getattr(download.identifier, "slug", None)
        if slug is None or slug not in target_slugs:
            continue
        download_map.setdefault(slug, []).append(download)
    return download_map


def _resolve_bundles(
    state: "PipelineState",
    settings: Settings,
    identifiers: Identifiers | None,
    target_slugs: Set[str],
) -> Dict[str, ArticleExtractionBundle]:
    bundle_map: Dict[str, ArticleExtractionBundle] = {}
    for bundle in getattr(state, "bundles", []) or []:
        slug = getattr(bundle.article_data.identifier, "slug", None) or bundle.article_data.slug
        if slug in target_slugs:
            bundle_map[slug] = bundle

    missing = target_slugs.difference(bundle_map.keys())
    if not missing:
        return bundle_map

    hydrated = _hydrate_bundles_for_upload(settings, identifiers)
    for bundle in hydrated:
        slug = getattr(bundle.article_data.identifier, "slug", None) or bundle.article_data.slug
        if slug in target_slugs and slug not in bundle_map:
            bundle_map[slug] = bundle
    return bundle_map


def _resolve_analyses(
    state: "PipelineState",
    settings: Settings,
    target_slugs: Set[str],
) -> Dict[str, Dict[str, AnalysisCollection]]:
    analyses: Dict[str, Dict[str, AnalysisCollection]] = {}
    for slug, per_table in (getattr(state, "analyses", {}) or {}).items():
        if slug in target_slugs:
            analyses[slug] = dict(per_table)

    missing = target_slugs.difference(analyses.keys())
    if not missing:
        return analyses

    cached = cache.load_cached_analysis_collections(settings)
    for slug, per_table in cached.items():
        if slug in target_slugs and slug not in analyses:
            analyses[slug] = dict(per_table)
    return analyses


def _hydrate_downloads_from_cache(
    settings: Settings,
    identifiers: Identifiers | None,
    target_slugs: Iterable[str],
) -> List[DownloadResult]:
    if identifiers is None:
        return []
    requested = set(target_slugs)
    hydrated: Dict[str, DownloadResult] = {}
    for source_name in settings.download_sources:
        index = cache.load_download_index(settings, source_name)
        for identifier in identifiers.identifiers:
            slug = getattr(identifier, "slug", None)
            if slug is None or (requested and slug not in requested):
                continue
            if slug in hydrated:
                continue
            entry = index.get_download_by_identifier(identifier)
            if entry is None:
                continue
            payload = entry.clone_payload()
            payload.identifier = identifier
            hydrated[slug] = payload
    return list(hydrated.values())


def _sync_article(
    base_study_id: str,
    bundle: ArticleExtractionBundle,
    per_table_analyses: Mapping[str, AnalysisCollection],
    downloads: Sequence[DownloadResult],
    settings: Settings,
) -> None:
    root = settings.ns_pond_root / base_study_id
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


__all__ = ["run_sync"]
