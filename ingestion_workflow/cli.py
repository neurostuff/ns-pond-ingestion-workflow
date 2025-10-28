"""Command-line interface for the ingestion workflow."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from ingestion_workflow.config import get_settings
from ingestion_workflow.logging_utils import configure_logging, get_logger
from ingestion_workflow.pipeline import IngestionPipeline, PipelineConfig

app = typer.Typer(help="Neurostore ingestion workflow CLI.")
logger = get_logger(__name__)


def _resolve_sources(sources: Optional[str]) -> Optional[list[str]]:
    if sources is None:
        return None
    return [item.strip() for item in sources.split(",") if item.strip()]


@app.callback()
def main(
    ctx: typer.Context,
    env_file: Optional[Path] = typer.Option(
        None,
        "--env-file",
        help="Path to .env file with credentials and settings.",
    ),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level."),
) -> None:
    """CLI entrypoint hook."""
    configure_logging(log_level)
    ctx.obj = {"env_file": str(env_file) if env_file else None}


@app.command("run")
def run_pipeline(
    ctx: typer.Context,
    sources: Optional[str] = typer.Option(
        None,
        "--sources",
        help="Comma-separated list of extractor names to run.",
    ),
    resume: bool = typer.Option(False, "--resume", help="Resume from existing manifest."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit number of identifiers."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Execute without side effects."),
    query: Optional[str] = typer.Option(
        None,
        "--query",
        help="PubMed Entrez search query used to gather PMIDs.",
    ),
) -> None:
    """Execute the full ingestion pipeline."""
    env_file = ctx.obj.get("env_file") if ctx.obj else None
    pipeline = IngestionPipeline(
        PipelineConfig(
            env_file=env_file,
            sources=_resolve_sources(sources),
            resume=resume,
            limit=limit,
            dry_run=dry_run,
            search_query=query,
        )
    )
    pipeline.run()


@app.command("settings")
def show_settings(ctx: typer.Context) -> None:
    """Print resolved settings for debugging."""
    env_file = ctx.obj.get("env_file") if ctx.obj else None
    settings = get_settings(env_file)
    for key, value in settings.model_dump().items():  # type: ignore[attr-defined]
        typer.echo(f"{key}: {value}")


def main_cli() -> None:
    """Allow `python -m ingestion_workflow` execution."""
    app()


if __name__ == "__main__":
    main_cli()
