"""The `ingest` command line.

Five verbs over one mental model: a catalog of articles, each advancing through
stages. `add` puts articles in, `run` advances them, `status`/`show` report.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path
from typing import List, Optional

import typer

from ingestion_workflow.catalog import Catalog, Status
from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.models.ids import Identifier, Identifiers
from ingestion_workflow.pipeline import (
    Context,
    Select,
    Selection,
    build,
    everything,
    from_manifest,
    narrow,
    run_stages,
)
from ingestion_workflow.pipeline.stages import STAGE_ORDER, STAGE_TYPES
from ingestion_workflow.services.logging import configure_logging

app = typer.Typer(
    name="ingest",
    help="Ingest neuroimaging articles into Neurostore.",
    no_args_is_help=True,
    add_completion=False,
)

ConfigOption = typer.Option(None, "--config", "-c", help="YAML settings file.")


# -- shared plumbing ---------------------------------------------------------


def _settings(config: Optional[Path], **overrides) -> Settings:
    overrides = {key: value for key, value in overrides.items() if value is not None}
    settings = load_settings(config, overrides=overrides)
    _configure_logging(settings)
    return settings


def _configure_logging(settings: Settings) -> None:
    log_file = settings.log_file or (settings.data_root / "logs" / "pipeline.log")
    if not Path(log_file).is_absolute():
        log_file = settings.data_root / log_file
    configure_logging(
        log_to_file=settings.log_to_file,
        log_file=Path(log_file) if settings.log_to_file else None,
        log_to_console=settings.log_to_console,
        level=logging.DEBUG if settings.verbose else logging.INFO,
    )


def _catalog(settings: Settings) -> Catalog:
    return Catalog.open(settings.catalog_root)


def _context(settings: Settings, catalog: Catalog, refresh: List[str]) -> Context:
    return Context(
        settings,
        catalog,
        refresh=refresh or (),
        max_attempts=settings.max_attempts,
        retry_after=timedelta(hours=settings.retry_after_hours),
    )


def _parse_identifier(token: str) -> Identifier:
    """Recognise a bare pmid, a PMC id or a DOI without being told which."""
    token = token.strip()
    if not token:
        raise typer.BadParameter("empty identifier")
    lowered = token.lower()
    if lowered.startswith("pmc") or lowered.startswith("https://www.ncbi.nlm.nih.gov/pmc"):
        return Identifier(pmcid=token)
    if token.isdigit():
        return Identifier(pmid=token)
    if lowered.startswith("10.") or "doi.org/" in lowered or lowered.startswith("doi:"):
        return Identifier(doi=token)
    if "pubmed.ncbi.nlm.nih.gov" in lowered:
        return Identifier(pmid=token)
    raise typer.BadParameter(f"could not tell what kind of identifier {token!r} is")


# -- add ---------------------------------------------------------------------


@app.command()
def add(
    identifiers: Optional[List[str]] = typer.Argument(
        None, help="PMIDs, PMC ids or DOIs, mixed freely."
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", exists=True, help="Text file with one identifier per line."
    ),
    manifest: Optional[Path] = typer.Option(
        None, "--manifest", "-m", exists=True, help="JSONL identifiers manifest."
    ),
    query: Optional[List[str]] = typer.Option(
        None, "--query", "-q", help="PubMed query to search (repeatable)."
    ),
    start_year: Optional[int] = typer.Option(
        None, "--start-year", help="Earliest year for queries."
    ),
    neurostore: Optional[List[str]] = typer.Option(
        None,
        "--neurostore",
        help="Neurostore base_study_id (repeatable). Opaque, so it needs naming.",
    ),
    from_neurostore: bool = typer.Option(
        False,
        "--from-neurostore",
        help="Pull in group-level base studies that are in a studyset and have no llm study yet.",
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-n", help="Cap how many --from-neurostore results to take."
    ),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Register articles in the catalog. Downloads nothing."""
    settings = _settings(config)
    found: List[Identifier] = [_parse_identifier(token) for token in (identifiers or [])]
    found += [Identifier(neurostore=token) for token in (neurostore or []) if token.strip()]

    if file:
        found += [
            _parse_identifier(line)
            for line in file.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        ]
    if manifest:
        found += list(Identifiers.load(manifest).identifiers)
    if from_neurostore:
        found += _discover_from_neurostore(settings, limit)

    if query:
        from ingestion_workflow.services.search import PubMedSearchService

        for text in query:
            service = PubMedSearchService(text, settings, start_year=start_year or 1990)
            results = service.search()
            typer.echo(f"query {text!r}: {len(results.identifiers):,} results")
            found += list(results.identifiers)

    if not found:
        raise typer.BadParameter(
            "give identifiers, --file, --manifest, --query or --neurostore"
        )

    with _catalog(settings) as catalog:
        before = catalog.count_articles()
        refs = catalog.register_many(found)
        after = catalog.count_articles()

    typer.echo(
        f"{len(refs):,} identifiers resolved to {after - before:,} new articles "
        f"({len(refs) - (after - before):,} already known); catalog now holds {after:,}."
    )


def _discover_from_neurostore(settings: Settings, limit: Optional[int]) -> List[Identifier]:
    """Read the database the upload stage writes to, and take nothing else."""
    from ingestion_workflow.services.db import SessionFactory, SSHTunnel
    from ingestion_workflow.services.discover import unprocessed_base_studies

    with SSHTunnel(settings) as tunnel:
        sessions = SessionFactory(settings, tunnel=tunnel)
        sessions.configure()
        with sessions.session() as session:
            found = list(unprocessed_base_studies(session, limit=limit))
    typer.echo(f"neurostore: {len(found):,} base studies with no llm study yet")
    return found


# -- run ---------------------------------------------------------------------


@app.command()
def run(
    stage: Optional[List[str]] = typer.Option(
        None, "--stage", "-s", help=f"Stages to run: {', '.join(STAGE_ORDER)} (repeatable)."
    ),
    select: Select = typer.Option(
        Select.PENDING, "--select", help="Which articles, relative to what is already done."
    ),
    manifest: Optional[Path] = typer.Option(
        None, "--manifest", "-m", exists=True, help="Restrict to a JSONL manifest."
    ),
    refresh: Optional[List[str]] = typer.Option(
        None,
        "--refresh",
        "-r",
        help="Ignore cached results for this stage (repeatable, or 'all').",
    ),
    limit: Optional[int] = typer.Option(None, "--limit", "-n", help="Stop after N articles."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the plan and change nothing."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Advance articles through the pipeline."""
    settings = _settings(config)
    stages = build(stage, settings)

    with _catalog(settings) as catalog:
        selection = _select(catalog, manifest, select, stage)
        if limit:
            selection = Selection(selection.refs[:limit], f"{limit:,} of {selection.description}")
        typer.echo(f"selection: {selection.description}\n")
        if not selection.refs:
            typer.echo("nothing to do.")
            return

        ctx = _context(settings, catalog, refresh or [])
        report = run_stages(ctx, stages, selection.refs, dry_run=dry_run)

    typer.echo(_render(report, dry_run=dry_run))


def _select(catalog: Catalog, manifest: Optional[Path], mode: Select, stage) -> Selection:
    base = from_manifest(catalog, manifest) if manifest else everything(catalog)
    only = stage[0] if stage and len(stage) == 1 else None
    return narrow(catalog, base, mode, only)


def _render(report, *, dry_run: bool) -> str:
    lines = ["  plan" if dry_run else "  result"]
    for stage_report in report.stages.values():
        lines.append("    " + stage_report.line(planned_only=dry_run))
    return "\n".join(lines)


# -- status ------------------------------------------------------------------


@app.command()
def status(config: Optional[Path] = ConfigOption) -> None:
    """Show what each stage has done, and what it could do next."""
    settings = _settings(config)
    requirements = {name: STAGE_TYPES[name].requires for name in STAGE_ORDER}
    with _catalog(settings) as catalog:
        total = catalog.count_articles()
        counts = catalog.status_counts()
        ready = catalog.ready_counts(requirements)
        stranded = catalog.stranded_artifacts()

    typer.echo(f"catalog: {total:,} articles at {settings.catalog_root}\n")
    if not counts and not any(ready.values()):
        typer.echo("no stage has run yet, and nothing is queued.")
        return

    states = [s.value for s in Status]
    header = f"{'stage':<12}" + "".join(f"{state:>12}" for state in states) + f"{'ready':>12}"
    typer.echo(header)
    for name in STAGE_ORDER:
        row = counts.get(name, {})
        waiting = ready.get(name, 0)
        if not row and not waiting:
            continue
        typer.echo(
            f"{name:<12}"
            + "".join(f"{row.get(state, 0):>12,}" for state in states)
            + f"{waiting:>12,}"
        )
    typer.echo(
        "\nready = the upstream stage succeeded and this stage has no entry yet."
    )
    if stranded:
        typer.echo(
            f"\n{stranded:,} artifacts are attached to articles a merge retired "
            "and nothing can reach. Run `ingest repair` to hand them over."
        )


# -- show --------------------------------------------------------------------


def _find(catalog: Catalog, token: str):
    """Resolve whatever the user typed: a bibliographic id, a Neurostore
    base_study_id, or the catalog's own article id."""
    try:
        found = catalog.resolve(_parse_identifier(token))
    except typer.BadParameter:
        found = None
    if found is not None:
        return found
    # Neurostore base_study_ids and article ids are both opaque strings; try the
    # alias table before assuming the token is an article id.
    found = catalog.resolve(Identifier(neurostore=token))
    if found is not None:
        return found
    if any(vars(catalog.identifier(token)).values()):
        return catalog.ref(token)
    return None


@app.command()
def show(
    identifier: str = typer.Argument(
        ..., help="A PMID, PMC id, DOI, Neurostore base_study_id or article id."
    ),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Print everything the catalog knows about one article."""
    settings = _settings(config)
    with _catalog(settings) as catalog:
        ref = _find(catalog, identifier)
        if ref is None:
            typer.echo(f"not in the catalog: {identifier}")
            raise typer.Exit(code=1)

        aliases = " · ".join(
            f"{kind} {value}"
            for kind, value in vars(ref.identifier).items()
            if value and kind != "other_ids"
        )
        typer.echo(f"article {ref.id}\n  aliases   {aliases}")
        for artifact in catalog.artifacts_for_article(ref.id):
            label = f"{artifact.stage}/{artifact.source}" if artifact.source else artifact.stage
            summary = ", ".join(f"{k}={v}" for k, v in sorted(artifact.summary.items()) if v)
            detail = artifact.error or summary or "-"
            typer.echo(
                f"  {label:<18} {artifact.status.value:<10} {artifact.updated_at[:10]}  {detail}"
            )


@app.command()
def repair(config: Optional[Path] = ConfigOption) -> None:
    """Hand over artifacts left attached to articles a merge retired."""
    settings = _settings(config)
    with _catalog(settings) as catalog:
        before = catalog.stranded_artifacts()
        moved = catalog.repair_merges()
        after = catalog.stranded_artifacts()
    if not before:
        typer.echo("nothing to repair.")
        return
    typer.echo(f"reattached artifacts from {moved:,} articles: {before:,} stranded -> {after:,}")


# -- migrate -----------------------------------------------------------------


@app.command()
def migrate(
    old_cache: Path = typer.Argument(
        ..., exists=True, file_okay=False, help="Pre-refactor .cache directory"
    ),
    stage: Optional[List[str]] = typer.Option(None, "--stage", "-s", help="Only these stages."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Count what would be imported."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Import pre-refactor sqlite caches into the catalog. Reads only."""
    from ingestion_workflow.migrate import migrate_caches

    settings = _settings(config)
    with _catalog(settings) as catalog:
        report = migrate_caches(old_cache, catalog, stages=stage, dry_run=dry_run)
    typer.echo(report.render())


def main() -> None:
    app()


if __name__ == "__main__":
    main()
