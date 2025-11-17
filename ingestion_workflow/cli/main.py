"""
Command line interface for the ingestion workflow.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List, Optional

import typer

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.models import (
    ArticleExtractionBundle,
    DownloadResult,
    DownloadSource,
    Identifiers,
)
from ingestion_workflow.services import cache
from ingestion_workflow.workflow import (
    SearchQuery,
    gather_identifiers,
)
from ingestion_workflow.workflow import create_analyses as create_analyses_workflow
from ingestion_workflow.workflow.download import run_downloads
from ingestion_workflow.workflow.extract import run_extraction
from ingestion_workflow.workflow.orchastrator import run_pipeline
from ingestion_workflow.workflow.stats import StageMetrics

app = typer.Typer(
    name="Article Ingestion Workflow",
    help="Neuroimaging article ingestion workflow for Neurostore",
)


@app.command()
def run(
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        exists=False,
        help="Optional YAML settings override.",
    ),
    stages: Optional[List[str]] = typer.Option(
        None,
        "--stages",
        "-s",
        help="Subset of pipeline stages to execute in canonical order.",
    ),
    manifest_path: Optional[Path] = typer.Option(
        None,
        "--manifest",
        "-m",
        exists=False,
        help="Identifiers manifest to use when skipping gather stage.",
    ),
    use_cached_inputs: Optional[bool] = typer.Option(
        None,
        "--use-cached-inputs/--no-use-cached-inputs",
        help="Toggle hydration from cached outputs when stages are skipped.",
    ),
) -> None:
    """Run the orchestrated ingestion workflow pipeline."""

    overrides: dict[str, object] = {}
    if stages:
        overrides["stages"] = [stage.lower() for stage in stages]
    if manifest_path is not None:
        overrides["manifest_path"] = manifest_path
    if use_cached_inputs is not None:
        overrides["use_cached_inputs"] = use_cached_inputs
    settings = load_settings(config_path, overrides=overrides or None)
    run_pipeline(settings=settings)


@app.command()
def search(
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        exists=False,
        help="Optional YAML settings override.",
    ),
    queries: Optional[List[str]] = typer.Option(
        None,
        "--query",
        "-q",
        help="PubMed query to run (repeatable).",
    ),
    start_year: Optional[int] = typer.Option(
        None,
        "--start-year",
        help="Earliest publication year for all queries.",
    ),
    manifest_path: Optional[Path] = typer.Option(
        None,
        "--manifest",
        "-m",
        exists=False,
        help="Optional identifiers manifest to seed the search.",
    ),
    label: Optional[str] = typer.Option(
        None,
        "--label",
        "-l",
        help="Label to use when saving the manifest.",
    ),
) -> None:
    """Gather identifiers via PubMed queries and persist a manifest."""

    settings = load_settings(config_path)
    manifest_seed = manifest_path or settings.manifest_path
    if not queries and manifest_seed is None:
        raise typer.BadParameter("Provide at least one --query or --manifest to search.")

    search_queries = (
        [SearchQuery(query=query, start_year=start_year) for query in queries]
        if queries
        else None
    )
    result = gather_identifiers(
        settings=settings,
        manifest=manifest_seed,
        queries=search_queries,
        label=label,
    )
    typer.echo(
        "Gather stage produced "
        f"{len(result.identifiers)} identifiers; manifest saved to "
        f"{settings.data_root / 'manifests'} (check logs for exact filename)."
    )


@app.command()
def download(
    manifest_path: Optional[Path] = typer.Option(
        None,
        "--manifest",
        "-m",
        exists=False,
        help="Identifiers manifest to download content for.",
    ),
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        exists=False,
        help="Optional YAML settings override.",
    ),
) -> None:
    """Run the download stage for a manifest."""

    settings = load_settings(config_path)
    identifiers = _load_identifiers_from_manifest(settings, manifest_path)
    if not identifiers.identifiers:
        typer.echo("Manifest contains no identifiers; nothing to download.")
        return

    metrics = StageMetrics()
    results = run_downloads(identifiers, settings=settings, metrics=metrics)
    success_slugs = {
        result.identifier.slug
        for result in results
        if result.success and result.identifier
    }
    total = len(identifiers.identifiers)
    typer.echo(
        "Download stage complete: "
        f"{len(success_slugs)}/{total} identifiers succeeded (cache hits: {metrics.cache_hits})."
    )


@app.command()
def extract(
    manifest_path: Optional[Path] = typer.Option(
        None,
        "--manifest",
        "-m",
        exists=False,
        help="Identifiers manifest to operate on.",
    ),
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        exists=False,
        help="Optional YAML settings override.",
    ),
) -> None:
    """Run the extract stage using cached downloads."""

    settings = load_settings(config_path)
    identifiers = _load_identifiers_from_manifest(settings, manifest_path)
    if not identifiers.identifiers:
        typer.echo("Manifest contains no identifiers; nothing to extract.")
        return

    downloads = _hydrate_downloads_from_cache(settings, identifiers)
    if not downloads:
        typer.echo("No cached downloads found for the provided manifest.")
        raise typer.Exit(code=1)

    metrics = StageMetrics()
    bundles = run_extraction(downloads, settings=settings, metrics=metrics)
    typer.echo(
        "Extract stage complete: "
        f"{len(bundles)} bundles from {len(downloads)} downloads (cache hits: {metrics.cache_hits})."
    )


@app.command("index-legacy-downloads")
def index_legacy_downloads_cli(
    extractor_name: DownloadSource = typer.Argument(
        ...,
        case_sensitive=False,
        help="Download source name whose legacy files should be indexed (ace|pubget).",
    ),
    legacy_directory: Path = typer.Argument(
        ...,
        exists=False,
        file_okay=False,
        dir_okay=True,
        help="Path to the legacy download directory to index.",
    ),
    update: bool = typer.Option(
        False,
        "--update/--no-update",
        help="Refresh existing cache entries with newly detected files instead of adding new entries.",
    ),
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        exists=False,
        help="Optional YAML settings override.",
    ),
    namespace: Optional[str] = typer.Option(
        None,
        "--namespace",
        "-n",
        help="Cache namespace override (defaults to downloads namespace).",
    ),
) -> None:
    """Index legacy download payloads for a given extractor into the cache."""

    settings = load_settings(config_path)
    resolved_dir = _resolve_working_path(settings, legacy_directory)

    if not resolved_dir.exists():
        raise typer.BadParameter(f"Legacy directory not found: {resolved_dir}")
    if not resolved_dir.is_dir():
        raise typer.BadParameter(f"Legacy path must be a directory: {resolved_dir}")

    cache_namespace = namespace or cache.DOWNLOAD_CACHE_NAMESPACE
    before_index = cache.load_download_index(
        settings,
        extractor_name.value,
        namespace=cache_namespace,
    )
    before = before_index.count()

    result = cache.index_legacy_downloads(
        settings,
        extractor_name.value,
        resolved_dir,
        update_existing=update,
        namespace=cache_namespace,
    )
    after = result.index.count()
    added = result.added if not update else 0
    updated = result.updated

    if update:
        typer.echo(
            "Legacy indexing update complete: "
            f"{updated} entries refreshed (total {after} cached for {extractor_name.value})."
        )
    else:
        # fallback to before/after diff when add tracking was bypassed (e.g. no batches)
        added = added or max(0, after - before)
        typer.echo(
            "Legacy indexing complete: "
            f"{added} new entries added (total {after} cached for {extractor_name.value})."
        )


@app.command()
def create_analyses(
    bundles_path: Path = typer.Argument(
        ...,
        exists=True,
        readable=True,
        help="Path to JSON containing ArticleExtractionBundle payloads.",
    ),
    output_path: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Optional output path for the analyses JSON (defaults to stdout).",
    ),
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        exists=False,
        help="Optional YAML settings override.",
    ),
    extractor_name: Optional[str] = typer.Option(
        None,
        "--extractor",
        "-e",
        help="Extractor namespace to use for cache lookups.",
    ),
    export: Optional[bool] = typer.Option(
        None,
        "--export/--no-export",
        help="Enable exporting bundle artifacts to the data-root export folder.",
    ),
    export_overwrite: Optional[bool] = typer.Option(
        None,
        "--export-overwrite/--no-export-overwrite",
        help="Overwrite exported files when they already exist.",
    ),
    n_llm_workers: Optional[int] = typer.Option(
        None,
        "--n-llm-workers",
        help="Number of parallel workers for LLM analysis creation.",
    ),
) -> None:
    """Execute the create-analyses workflow step for serialized bundles."""

    settings = load_settings(config_path)
    overrides: dict[str, object] = {}
    if export is not None:
        overrides["export"] = export
    if export_overwrite is not None:
        overrides["export_overwrite"] = export_overwrite
    if n_llm_workers is not None:
        overrides["n_llm_workers"] = max(1, n_llm_workers)
    if overrides:
        settings = settings.merge_overrides(overrides)
    payload = json.loads(bundles_path.read_text(encoding="utf-8"))
    bundles = _load_bundles(payload)
    results = create_analyses_workflow.run_create_analyses(
        bundles,
        settings=settings,
        extractor_name=extractor_name,
    )
    serializable = {
        article_slug: {
            table_id: collection.to_dict() for table_id, collection in table_map.items()
        }
        for article_slug, table_map in results.items()
    }
    output = json.dumps(serializable, indent=2, sort_keys=True)
    if output_path is None:
        typer.echo(output)
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output, encoding="utf-8")
        typer.echo(f"Wrote analyses to {output_path}")


@app.command()
def upload():
    pass


@app.command()
def sync():
    pass


def main() -> None:
    """Main entry point for CLI."""
    app()


if __name__ == "__main__":
    main()


def _load_bundles(payload: Any) -> Iterable[ArticleExtractionBundle]:
    """Deserialize bundles from JSON-compatible payloads."""
    if isinstance(payload, dict):
        if "bundles" in payload:
            payload = payload["bundles"]
        elif all(isinstance(value, dict) for value in payload.values()):
            payload = payload.values()
    if not isinstance(payload, (list, tuple)):
        raise typer.BadParameter("Expected a list or mapping of ArticleExtractionBundle payloads.")
    return [
        ArticleExtractionBundle.from_dict(item)  # type: ignore[arg-type]
        for item in payload
    ]


def _resolve_manifest_path(
    settings: Settings,
    manifest_path: Optional[Path],
) -> Path:
    candidate = manifest_path or settings.manifest_path
    if candidate is None:
        raise typer.BadParameter("Manifest path must be provided via --manifest or configuration.")

    resolved = Path(candidate)
    if not resolved.is_absolute():
        resolved = settings.data_root / resolved

    if not resolved.exists():
        raise FileNotFoundError(f"Manifest file not found: {resolved}")

    return resolved


def _load_identifiers_from_manifest(
    settings: Settings,
    manifest_path: Optional[Path],
) -> Identifiers:
    path = _resolve_manifest_path(settings, manifest_path)
    return Identifiers.load(path)


def _hydrate_downloads_from_cache(
    settings: Settings,
    identifiers: Identifiers,
) -> List[DownloadResult]:
    if not identifiers.identifiers:
        return []

    hydrated: dict[str, DownloadResult] = {}
    for source_name in settings.download_sources:
        index = cache.load_download_index(settings, source_name)
        for identifier in identifiers.identifiers:
            slug = identifier.slug
            if slug in hydrated:
                continue
            entry = index.get_download(slug)
            if entry is None:
                continue
            hydrated[slug] = entry.result

    return list(hydrated.values())


def _resolve_working_path(settings: Settings, path: Path) -> Path:
    """Resolve user-provided directories relative to data_root when applicable."""
    resolved = path if path.is_absolute() else settings.data_root / path
    return resolved
