"""Gather identifiers for the ingestion workflow."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.models import Identifiers
from ingestion_workflow.services import logging
from ingestion_workflow.services.id_lookup import (
    OpenAlexIDLookupService,
    PubMedIDLookupService,
    SemanticScholarIDLookupService,
)
from ingestion_workflow.services.search import PubMedSearchService


DEFAULT_SEARCH_START_YEAR = 1990
OUTPUT_SUBDIR = "manifests"


@dataclass(frozen=True)
class SearchQuery:
    """Description of a PubMed search to perform."""

    query: str
    start_year: Optional[int] = None


ID_LOOKUP_SERVICE_FACTORIES = {
    "semantic_scholar": SemanticScholarIDLookupService,
    "pubmed": PubMedIDLookupService,
    "openalex": OpenAlexIDLookupService,
}


logger = logging.get_logger("workflow.gather")


def gather_identifiers(
    *,
    settings: Settings | None = None,
    manifest: Identifiers | str | Path | None = None,
    queries: Sequence[SearchQuery | str] | None = None,
    label: str | None = None,
) -> Identifiers:
    """
    Collect identifiers from the manifest and/or PubMed searches,
    fill in missing ids/add other ids,
    and persist the combined manifest for downstream workflow stages.
    """

    resolved_settings = _resolve_settings(settings)
    combined = Identifiers()
    combined.set_index("pmid", "doi", "pmcid")

    initial_manifest_count = 0
    cache_hits = 0

    if manifest is not None:
        manifest_identifiers = _load_manifest(manifest, resolved_settings)
        initial_manifest_count = len(manifest_identifiers.identifiers)
        _extend_identifiers(combined, manifest_identifiers)
        cache_hits += initial_manifest_count

    for search_query in _normalize_queries(queries):
        search_service = PubMedSearchService(
            search_query.query,
            resolved_settings,
            start_year=search_query.start_year or DEFAULT_SEARCH_START_YEAR,
        )
        search_results = search_service.search()
        if search_results.identifiers:
            logger.info(
                "PubMed query '%s' returned %d identifiers",
                search_query.query,
                len(search_results.identifiers),
            )
        before_extend = len(combined.identifiers)
        _extend_identifiers(combined, search_results)
        cache_hits += before_extend

    combined.deduplicate()

    expansion_stats: dict[str, int] = {}
    for provider in resolved_settings.metadata_providers:
        service_class = ID_LOOKUP_SERVICE_FACTORIES.get(provider)
        if service_class is None:
            logger.warning(
                "Unsupported metadata provider configured: %s",
                provider,
            )
            continue
        service = service_class(resolved_settings)
        before_expand = len(combined.identifiers)
        service.find_identifiers(combined)
        after_expand = len(combined.identifiers)
        expansion_stats[provider] = after_expand - before_expand

    combined.deduplicate()
    combined.set_index("pmid", "doi", "pmcid")

    output_path = _output_path(resolved_settings, label)
    combined.save(output_path)
    logger.info(
        "Gather summary: %d total identifiers "
        "(manifest=%d, search=%d, expansions=%s) -> saved to %s",
        len(combined.identifiers),
        initial_manifest_count,
        len(combined.identifiers) - initial_manifest_count,
        ", ".join(f"{provider}:{delta}" for provider, delta in expansion_stats.items()),
        output_path,
    )

    return combined


def _resolve_settings(settings: Settings | None) -> Settings:
    """
    Load default settings if necessary and ensure required
    directories exist before gathering identifiers.
    """
    if settings is None:
        settings = load_settings()
    settings.ensure_directories()
    return settings


def _load_manifest(
    manifest: Identifiers | str | Path,
    settings: Settings,
) -> Identifiers:
    """
    Load a manifest of identifiers from disk (or pass through an Identifiers object)
    so the gather stage can seed the workflow without running searches.
    """
    if isinstance(manifest, Identifiers):
        return manifest

    path = Path(manifest)
    if not path.is_absolute():
        path = settings.data_root / path

    if path.suffix.lower() != ".jsonl":
        raise ValueError("Manifests must be provided as JSONL files")

    identifiers = Identifiers.load(path)
    logger.info(
        "Loaded %d identifiers from manifest %s",
        len(identifiers.identifiers),
        path,
    )
    return identifiers


def _extend_identifiers(target: Identifiers, source: Identifiers) -> None:
    """
    Append all identifiers from a source list into the combined manifest,
    preserving existing index settings.
    """
    for identifier in source.identifiers:
        target.append(identifier)


def _normalize_queries(
    queries: Sequence[SearchQuery | str] | None,
) -> list[SearchQuery]:
    """
    Convert raw search inputs (SearchQuery objects or strings) into a
    normalized list of SearchQuery instances for the gather stage.
    """
    if not queries:
        return []

    normalized: list[SearchQuery] = []
    for item in queries:
        if isinstance(item, SearchQuery):
            normalized.append(item)
            continue
        normalized.append(SearchQuery(query=str(item)))
    return normalized


def _output_path(settings: Settings, label: str | None) -> Path:
    """
    Determine the output path for the manifest produced by the gather stage,
    creating the manifest directory if needed.
    """
    output_dir = settings.data_root / OUTPUT_SUBDIR
    output_dir.mkdir(parents=True, exist_ok=True)

    if label:
        slug = _slugify(label)
        filename = f"{slug}.jsonl"
    else:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{timestamp}.jsonl"

    return output_dir / filename


def _slugify(value: str) -> str:
    """
    Create a filesystem-safe slug from user-provided
    labels when naming manifest files.
    """
    mapped = re.sub(r"[^a-z0-9]+", "-", value.lower())
    slug = mapped.strip("-")
    return slug or "manifest"


__all__ = ["SearchQuery", "gather_identifiers"]
