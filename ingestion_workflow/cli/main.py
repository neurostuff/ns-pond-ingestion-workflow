"""
Command line interface for the ingestion workflow.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List, Optional

import typer

from ingestion_workflow.config import load_settings
from ingestion_workflow.models import ArticleExtractionBundle
from ingestion_workflow.workflow import create_analyses as create_analyses_workflow
from ingestion_workflow.workflow.orchastrator import run_pipeline

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
def search():
    pass


@app.command()
def download():
    pass


@app.command()
def extract():
    pass


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
        article_hash: {
            table_id: collection.to_dict() for table_id, collection in table_map.items()
        }
        for article_hash, table_map in results.items()
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
