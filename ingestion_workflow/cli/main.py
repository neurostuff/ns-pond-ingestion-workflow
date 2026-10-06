"""The `ingest` command line.

Five verbs over one mental model: a catalog of articles, each advancing through
stages. `add` puts articles in, `run` advances them, `status`/`show` report.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Sequence

import typer

from ingestion_workflow.catalog import ArticleRef, Catalog, Status
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
    pdfs: Optional[Path] = typer.Option(
        None,
        "--pdfs",
        exists=True,
        file_okay=False,
        help=(
            "Folder of PDFs already downloaded. Their ids are read off each PDF, "
            "or found by title, and each PDF becomes its article's download."
        ),
    ),
    in_place: bool = typer.Option(
        False,
        "--in-place",
        help=(
            "Point the catalog at the --pdfs files where they are, instead of "
            "copying them into the PDF cache. For a folder that is their "
            "permanent home."
        ),
    ),
    prefer_over: Optional[List[str]] = typer.Option(
        None,
        "--prefer-over",
        help=(
            "Attach a --pdfs file even to an article already downloaded, when "
            "every download it has is from this source (repeatable), e.g. ace."
        ),
    ),
    neurostore: Optional[List[str]] = typer.Option(
        None,
        "--neurostore",
        help="Neurostore base_study_id (repeatable). Opaque, so it needs naming.",
    ),
    enrich: bool = typer.Option(
        True,
        "--enrich/--no-enrich",
        help=(
            "Fill in missing pmcid/doi from Semantic Scholar, PubMed and "
            "OpenAlex. A PubMed search returns PMIDs only, and pubget needs a "
            "PMCID, so without this those articles cannot be downloaded."
        ),
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
    local = _read_pdfs(settings, pdfs) if pdfs else []
    found += [pdf.identifier for pdf in local if pdf.identifier and not pdf.supplement]

    if query:
        from ingestion_workflow.services.search import PubMedSearchService

        for text in query:
            service = PubMedSearchService(text, settings, start_year=start_year or 1990)
            results = service.search()
            typer.echo(f"query {text!r}: {len(results.identifiers):,} results")
            found += list(results.identifiers)

    if not found:
        raise typer.BadParameter(
            "give identifiers, --file, --manifest, --query, --pdfs or --neurostore"
        )

    with _catalog(settings) as catalog:
        before = catalog.count_articles()
        refs = catalog.register_many(found)
        if enrich:
            refs = _enrich(settings, catalog, refs)
        after = catalog.count_articles()
        if local:
            _attach_pdfs(
                settings, catalog, pdfs, local, in_place=in_place, prefer_over=prefer_over or ()
            )

    typer.echo(
        f"{len(refs):,} identifiers resolved to {after - before:,} new articles "
        f"({len(refs) - (after - before):,} already known); catalog now holds {after:,}."
    )


def _read_pdfs(settings: Settings, folder: Path) -> list:
    import requests

    from ingestion_workflow.services.local_pdfs import (
        confirm_dois,
        doi_registered,
        find_pdfs,
        match_titles,
        read_pdf,
        title_searchers,
    )

    local = [read_pdf(path) for path in find_pdfs(folder)]
    confirm_dois(local, doi_registered(requests.Session()))
    match_titles(local, title_searchers(settings))
    found_by: dict = {}
    for pdf in local:
        found_by[pdf.found_by or "nothing"] = found_by.get(pdf.found_by or "nothing", 0) + 1
    typer.echo(
        f"pdfs {folder}: {len(local):,} files; ids from "
        + ", ".join(f"{how} {n:,}" for how, n in sorted(found_by.items()))
    )
    return local


def _attach_pdfs(
    settings: Settings,
    catalog: Catalog,
    folder: Path,
    local: list,
    *,
    in_place: bool,
    prefer_over: Sequence[str] = (),
) -> None:
    """Make each PDF its article's download, and write what became of each.

    The manifest names the articles, for `ingest run --manifest`; the report
    names the files, so the unresolved ones can be identified by hand.
    """
    import csv

    from ingestion_workflow.models import DownloadSource
    from ingestion_workflow.pipeline.stages.download import DownloadStage
    from ingestion_workflow.services.local_pdfs import attach

    store = None
    if not in_place:
        store = Path(settings.pdf_cache_root or settings.get_cache_dir("pdf")) / "local"
    fp = DownloadStage(settings).fingerprint_for(DownloadSource.PDF)
    outcome = attach(catalog, local, fp, store, prefer_over)

    out_dir = settings.data_root / "manifests"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / f"{folder.resolve().name}.jsonl"
    report_path = out_dir / f"{folder.resolve().name}.tsv"

    refs = {}
    with report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            ["status", "found_by", "article_id", "doi", "pmid", "pmcid", "title", "path"]
        )
        for pdf in local:
            ref = catalog.resolve(pdf.identifier) if pdf.identifier else None
            if ref is not None and not pdf.supplement:
                refs[ref.id] = ref
            ids = ref.identifier if ref is not None else pdf.identifier
            writer.writerow(
                [
                    outcome[pdf.path] if not pdf.error else f"unreadable: {pdf.error}",
                    pdf.found_by or "",
                    ref.id if ref is not None else "",
                    (ids.doi if ids else "") or "",
                    (ids.pmid if ids else "") or "",
                    (ids.pmcid if ids else "") or "",
                    pdf.title or "",
                    str(pdf.path),
                ]
            )
    Identifiers([ref.identifier for ref in refs.values()]).save(manifest_path)

    counts: dict = {}
    for status in outcome.values():
        key = "duplicate" if status.startswith("duplicate") else status
        counts[key] = counts.get(key, 0) + 1
    typer.echo(
        "  " + ", ".join(f"{status} {n:,}" for status, n in sorted(counts.items()))
        + f"\n  manifest {manifest_path}\n  report   {report_path}"
    )


def _enrich(settings: Settings, catalog: Catalog, refs: Sequence[ArticleRef]) -> List[ArticleRef]:
    """Fill in the identifiers a source needs to address an article.

    A PubMed search yields PMIDs; pubget addresses articles by PMCID, so
    without this every searched article is unreachable to it. Each provider
    consults the catalog first, so an article that already has all three ids
    costs nothing.
    """
    from ingestion_workflow.services.id_lookup import (
        OpenAlexIDLookupService,
        PubMedIDLookupService,
        SemanticScholarIDLookupService,
    )

    providers = {
        "semantic_scholar": SemanticScholarIDLookupService,
        "pubmed": PubMedIDLookupService,
        "openalex": OpenAlexIDLookupService,
    }
    identifiers = Identifiers([ref.identifier for ref in refs])
    identifiers.set_index("pmid", "doi", "pmcid")

    for name in settings.metadata_providers:
        factory = providers.get(name)
        if factory is None:
            typer.echo(f"  unknown metadata provider {name!r}, skipping")
            continue
        service = factory(settings, catalog)
        if not service.can_run():
            typer.echo(f"  {name}: not configured, skipping")
            continue
        before = _complete(identifiers)
        service.find_identifiers(identifiers)
        gained = _complete(identifiers) - before
        typer.echo(f"  {name}: completed {gained:,} more")

    return catalog.register_many(list(identifiers.identifiers))


def _complete(identifiers: Identifiers) -> int:
    return sum(
        1 for i in identifiers.identifiers if i.pmid and i.doi and i.pmcid
    )


def _upload_source(settings: Settings) -> str:
    """`discover` asks which base studies have no study from this extractor,
    so an unset source would silently answer about the wrong one."""
    from ingestion_workflow.services.upload import resolve_upload_source

    return resolve_upload_source(settings)


def _discover_from_neurostore(settings: Settings, limit: Optional[int]) -> List[Identifier]:
    """Read the database the upload stage writes to, and take nothing else."""
    from ingestion_workflow.services.db import SessionFactory, SSHTunnel
    from ingestion_workflow.services.discover import unprocessed_base_studies

    with SSHTunnel(settings) as tunnel:
        sessions = SessionFactory(settings, tunnel=tunnel)
        sessions.configure()
        with sessions.session() as session:
            found = list(
                unprocessed_base_studies(session, limit=limit, source=_upload_source(settings))
            )
    typer.echo(
        f"neurostore: {len(found):,} base studies with no {_upload_source(settings)} study yet"
    )
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
        help=(
            "Ignore cached results and redo the work. Takes a stage "
            "(`extract`), one source of a stage (`extract:ace`), or `all`. "
            "Repeatable."
        ),
    ),
    limit: Optional[int] = typer.Option(None, "--limit", "-n", help="Stop after N articles."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the plan and change nothing."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Advance articles through the pipeline."""
    settings = _settings(config)
    stages = build(stage, settings)

    with _catalog(settings) as catalog:
        selection = _select(catalog, manifest, select, stage, refresh or [])
        if limit:
            selection = Selection(selection.refs[:limit], f"{limit:,} of {selection.description}")
        typer.echo(f"selection: {selection.description}\n")
        if not selection.refs:
            typer.echo("nothing to do.")
            return

        ctx = _context(settings, catalog, refresh or [])
        report = run_stages(ctx, stages, selection.refs, dry_run=dry_run)

    typer.echo(_render(report, dry_run=dry_run))


def _select(
    catalog: Catalog,
    manifest: Optional[Path],
    mode: Select,
    stage,
    refresh: Sequence[str] = (),
) -> Selection:
    base = from_manifest(catalog, manifest) if manifest else everything(catalog)
    only = stage[0] if stage and len(stage) == 1 else None
    return narrow(catalog, base, mode, only, refresh)


def _render(report, *, dry_run: bool) -> str:
    lines = ["  plan" if dry_run else "  result"]
    for stage_report in report.stages.values():
        lines.append("    " + stage_report.line(planned_only=dry_run))
    return "\n".join(lines)


# -- status ------------------------------------------------------------------


@app.command()
def serve(
    config: Optional[Path] = ConfigOption,
    weights: Optional[Path] = typer.Option(
        None, "--weights", "-w", help="Merged model directory to serve."
    ),
    stop: bool = typer.Option(False, "--stop", help="Stop the server and exit."),
    check: bool = typer.Option(
        False, "--check", help="Report what a running server is serving, and exit."
    ),
    log_file: Optional[Path] = typer.Option(None, "--log-file"),
) -> None:
    """Serve the fine-tuned extractor for the native path.

    The launch is derived from the same settings the client reads -- the served
    name from `llm_model`, the address from `llm_api_base`, the window from the
    client itself, the card split from the weights and the cards present -- so
    there is nothing here to get wrong by remembering it differently.
    """
    from ingestion_workflow.services import extractor_server

    settings = _settings(config)
    if stop:
        extractor_server.stop()
        typer.echo("stopped")
        return
    if check:
        typer.echo(extractor_server.check(settings))
        return
    plan = extractor_server.start(settings, weights=weights, log_file=log_file)
    typer.echo(
        f"serving {plan.served_name} at {plan.base_url} "
        f"(tp={plan.tensor_parallel} dp={plan.data_parallel}, "
        f"window {plan.max_model_len})"
    )


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
def exclude(
    identifiers: Optional[List[str]] = typer.Argument(
        None, help="Articles to mark: PMID, PMC id, DOI, Neurostore base_study_id or article id."
    ),
    table: Optional[List[str]] = typer.Option(
        None, "--table", "-t", help="Mark only this table (repeatable). Without it, the whole article."
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", exists=True, dir_okay=False,
        help="JSON list of {article_id, table_id, reason?, note?}; rows whose `verdict` is "
             "present and not 'not coordinates' are ignored, so a review file can be given as is.",
    ),
    reason: str = typer.Option("not coordinates", "--reason", help="Why: recorded with the mark."),
    note: str = typer.Option("", "--note", help="Free text recorded with the mark."),
    remove: bool = typer.Option(False, "--remove", help="Withdraw the marks instead of adding them."),
    list_: bool = typer.Option(False, "--list", help="Print the marks on the given articles, or all."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Mark tables as holding no coordinates, so upload and sync leave them out.

    Marks are a person's verdict and stay in the catalog until withdrawn. The
    next `ingest run -s upload -s sync` acts on them: a marked table's analyses
    are removed from the study version, and an article with nothing left is
    retracted -- its version deleted unless a studyset or an annotation uses it,
    and its corpus directory moved to `<ns_pond_root>-retracted/`.
    """
    settings = _settings(config)
    with _catalog(settings) as catalog:
        rows: List[tuple] = []
        for token in identifiers or []:
            ref = _find(catalog, token)
            if ref is None:
                typer.echo(f"not in the catalog: {token}")
                raise typer.Exit(code=1)
            # A pmid can alias several articles; say which one this resolved to.
            typer.echo(f"{token} -> article {ref.id} ({ref.identifier.slug})")
            for table_id in table or [Catalog.WHOLE_ARTICLE]:
                rows.append((ref.id, str(table_id), reason, note))
        if file is not None:
            for entry in json.loads(file.read_text()):
                verdict = entry.get("verdict")
                if verdict is not None and verdict != "not coordinates":
                    continue
                if not any(vars(catalog.identifier(entry["article_id"])).values()):
                    typer.echo(f"  skipped, not an article id in this catalog: {entry['article_id']}")
                    continue
                rows.append((entry["article_id"], str(entry.get("table_id") or Catalog.WHOLE_ARTICLE),
                             entry.get("reason") or entry.get("kind") or reason,
                             entry.get("note") or note))

        if list_:
            marks = catalog.exclusions([r[0] for r in rows] if rows else None)
            for article_id, tables in sorted(marks.items()):
                for table_id, mark in sorted(tables.items()):
                    typer.echo(f"{article_id}  {table_id:<24} {mark['reason']}  {mark['note']}")
            typer.echo(f"{sum(len(t) for t in marks.values()):,} marks on {len(marks):,} articles")
            return
        if not rows:
            raise typer.BadParameter("give articles, --file, or --list")
        if remove:
            n = sum(catalog.unexclude(a, None if t == Catalog.WHOLE_ARTICLE and not table else t)
                    for a, t, _, _ in rows)
            typer.echo(f"withdrew {n:,} marks")
            return
        catalog.exclude(rows)
        typer.echo(
            f"marked {len(rows):,} tables on {len({r[0] for r in rows}):,} articles; "
            "run upload and sync to act on them"
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
