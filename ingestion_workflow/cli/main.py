"""The `ingest` command line.

Five verbs over one mental model: a catalog of articles, each advancing through
stages. `add` puts articles in, `run` advances them, `status`/`show` report.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import typer

from ingestion_workflow.catalog import WHOLE_ARTICLE, ArticleRef, Catalog, Status, identity_plan
from ingestion_workflow.cli.options import (
    BackupOption,
    CatalogOption,
    ConfigOption,
    ProseOption,
    apply_option,
    dry_run_option,
    manifest_option,
    require_new_backup,
)
from ingestion_workflow.catalog.evidence import Evidence
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.doi_registry import DoiRegistry
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.models.ids import Identifier, Identifiers
from ingestion_workflow.pipeline import (
    Context,
    Select,
    Selection,
    build,
    everything,
    narrow,
    resolve_manifest,
    run_stages,
)
from ingestion_workflow.pipeline.refingerprint import refingerprint
from ingestion_workflow.pipeline.stages import PROSE_STAGES, STAGE_ORDER, STAGE_TYPES, upstream_of
from ingestion_workflow.services.logging import configure_logging
from ingestion_workflow.services.search import (
    SEARCH_SERVICES,
    SearchReport,
    SearchRun,
    resolve_hits,
    surviving,
)

app = typer.Typer(
    name="ingest",
    help="Ingest neuroimaging articles into Neurostore.",
    no_args_is_help=True,
    add_completion=False,
)

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


def _registering_catalog(settings: Settings) -> Catalog:
    """The catalog, opened to check the identifiers it registers or repairs against
    PubMed and the DOI registries, for the commands that do."""
    if not settings.pubmed_email:
        raise typer.BadParameter("the catalog checks identifiers against PubMed: set pubmed_email",
                                 param_hint="--config")
    pubmed = PubMedClient(settings.pubmed_email, settings.pubmed_api_key,
                          tool=settings.pubmed_tool or "ingestion-workflow")
    registry = DoiRegistry(CrossrefClient(settings.crossref_email))
    return Catalog.open(settings.catalog_root, evidence=Evidence(pubmed, registry))


def _report_refusals(catalog: Catalog) -> None:
    """Print what registration refused to add this run; the `refusals` table keeps it."""
    if not catalog.refused:
        return
    typer.echo(f"{len(catalog.refused):,} identifiers not added (kept in the catalog's "
               "refusals table):")
    for kind, value, article_id, why in catalog.refused[:20]:
        typer.echo(f"  {kind} {value}" + (f" ({article_id})" if article_id else "") + f": {why}")
    if len(catalog.refused) > 20:
        typer.echo(f"  ... and {len(catalog.refused) - 20:,} more")


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
    if lowered.startswith(
        ("pmc", "https://www.ncbi.nlm.nih.gov/pmc", "https://pmc.ncbi.nlm.nih.gov/")
    ):
        try:
            return Identifier(pmcid=token)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    if token.isdigit():
        return Identifier(pmid=token)
    if lowered.startswith("10.") or "doi.org/" in lowered or lowered.startswith("doi:"):
        return Identifier(doi=token)
    if "pubmed.ncbi.nlm.nih.gov" in lowered:
        return Identifier(pmid=token)
    raise typer.BadParameter(f"could not tell what kind of identifier {token!r} is")


# -- add ---------------------------------------------------------------------

SearchProvider = Enum("SearchProvider", {name: name for name in SEARCH_SERVICES}, type=str)


@app.command()
def add(
    identifiers: Optional[List[str]] = typer.Argument(
        None, help="PMIDs, PMC ids or DOIs, mixed freely."
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", exists=True, help="Text file with one identifier per line."
    ),
    manifest: Optional[Path] = manifest_option("JSONL identifiers manifest."),
    search: Optional[str] = typer.Option(
        None,
        "--search",
        "-s",
        help=(
            "Search query, e.g. 'fMRI OR \"functional MRI\"' (syntax: docs/search.md). "
            "PubMed and OpenAlex search title and abstract of journal articles, "
            "Semantic Scholar title and abstract of every publication type, "
            "ScienceDirect the full text."
        ),
    ),
    year: Optional[str] = typer.Option(
        None,
        "--year",
        help=(
            "Publication year of --search: 2026, or a range 2025:2026; by default 1990 "
            "onward (BOLD fMRI began in 1990). Each provider "
            "dates a work its own way: PubMed's [dp] (electronic or print date), "
            "Semantic Scholar's year, OpenAlex's publication_year, ScienceDirect's date."
        ),
    ),
    provider: Optional[List[SearchProvider]] = typer.Option(
        None,
        "--provider",
        help="Where --search looks (repeatable). Default: all of them.",
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

    if (year or provider) and not search:
        raise typer.BadParameter("--year and --provider apply to --search")
    run = _search(settings, search, year, provider) if search else None
    searched = run.hits if run else {}
    found += [record for records in searched.values() for record in records]

    if not found:
        raise typer.BadParameter(
            "give identifiers, --file, --manifest, --search, --pdfs or --neurostore"
        )

    with _registering_catalog(settings) as catalog:
        before = catalog.count_articles()
        held = set(catalog.all_article_ids()) if searched else set()
        refs = catalog.register_many(found)
        if enrich:
            refs = _enrich(settings, catalog, refs)
        # Several providers find one article, and enrichment joins records of one.
        refs = list({ref.id: ref for ref in refs}.values())
        _report_refusals(catalog)
        after = catalog.count_articles()
        if searched:
            resolved = resolve_hits(catalog, searched)
            report = SearchReport.build(surviving(catalog, held), resolved)
            _report_search(report)
            search_id = catalog.record_search(run.query, run.years, run.rendered, resolved)
            typer.echo(
                f"  kept as search {search_id}: `ingest searches manifest {search_id}` "
                "selects its articles by provider"
            )
        if local:
            _attach_pdfs(
                settings, catalog, pdfs, local, in_place=in_place, prefer_over=prefer_over or ()
            )

    typer.echo(
        f"{len(refs):,} identifiers resolved to {after - before:,} new articles "
        f"({len(refs) - (after - before):,} already known); catalog now holds {after:,}."
    )


def _search(
    settings: Settings, text: str, year: Optional[str], providers: Optional[List[SearchProvider]]
) -> SearchRun:
    """The query `text` run at each provider."""
    from ingestion_workflow.services.query_dialect import QueryError, Years, parse
    from ingestion_workflow.services.search import search_services

    try:
        query = parse(text, Years.parse(year)) if year else parse(text)
        typer.echo(f"years: {query.years}" + ("" if year else " (the default; --year to change)"))
        services = search_services(
            query, [p.value for p in providers or SearchProvider], settings, typer.echo
        )
    except (QueryError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from None
    searched: Dict[str, List[Identifier]] = {}
    for name, service in services.items():
        searched[name] = list(service.search().identifiers)
        typer.echo(f"{name}: {len(searched[name]):,} records")
    return SearchRun(
        text, str(query.years), {name: service.rendered for name, service in services.items()},
        searched,
    )


def _report_search(report: SearchReport) -> None:
    """Per-provider articles, overlap, and how many the catalog already held."""
    for name, articles in report.articles.items():
        typer.echo(
            f"  {name}: {len(articles):,} articles, {report.unique(name):,} found by no other"
        )
    for (a, b), shared in report.shared().items():
        typer.echo(f"  {a} and {b} share {shared:,}")
    distinct = report.distinct
    known = len(report.known)
    typer.echo(
        f"  {len(distinct):,} distinct: {known:,} already in the catalog, "
        f"{len(distinct) - known:,} new"
    )
    for name, count in report.without_id.items():
        if count:
            # The catalog keys articles on DOI, PMID and PMCID; register_many skips the rest.
            typer.echo(
                f"  {count:,} {name} records have no DOI, PMID or PMCID and were not registered"
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
        CrossrefIDLookupService,
        OpenAlexIDLookupService,
        PubMedIDLookupService,
        SemanticScholarIDLookupService,
    )

    providers = {
        "semantic_scholar": SemanticScholarIDLookupService,
        "pubmed": PubMedIDLookupService,
        "openalex": OpenAlexIDLookupService,
        "crossref": CrossrefIDLookupService,
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
        if isinstance(service, CrossrefIDLookupService):
            typer.echo(f"  {name}: {len(service.resolved):,} collisions resolved, "
                       f"{len(service.omitted):,} ids not filed")
            for kind, value, _, why in service.omitted:
                typer.echo(f"    {kind} {value}: {why}")

    return catalog.register_many(list(identifiers.identifiers))


def _complete(identifiers: Identifiers) -> int:
    return sum(
        1 for i in identifiers.identifiers if i.pmid and i.doi and i.pmcid
    )


def _upload_source(settings: Settings) -> str:
    """`discover` asks which base studies have no study from this extractor,
    so an unset source would silently answer about the wrong one."""
    from ingestion_workflow.services.discover import resolve_upload_source

    return resolve_upload_source(settings)


def _discover_from_neurostore(settings: Settings, limit: Optional[int]) -> List[Identifier]:
    """Read the neurostore deployment's database directly, and take nothing else."""
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
        Select.PENDING,
        "--select",
        help=(
            "Which articles to run. `pending` (default): anything a stage would "
            "act on -- never run, stale (its inputs or the code changed since it "
            "was built), or failed transiently and due a retry; not permanent "
            "failures, not fresh results. `new`: never attempted. `failed`: has a "
            "retryable failure. `all`: every article."
        ),
    ),
    manifest: Optional[Path] = manifest_option("Restrict to a JSONL manifest."),
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
    dry_run: bool = dry_run_option("Print the plan and change nothing."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Advance articles through the pipeline."""
    settings = _settings(config)
    if "ace" in settings.download_sources:
        from ingestion_workflow.extractors.ace_extractor import prepare_node

        try:
            prepare_node(settings)
        except RuntimeError as exc:
            raise typer.BadParameter(str(exc)) from exc
    stages = build(stage, settings)

    # A manifest's identifiers are registered.
    with (_registering_catalog if manifest else _catalog)(settings) as catalog:
        selection = _select(catalog, manifest, select, stage, refresh or [], settings)
        if limit:
            selection = Selection(selection.refs[:limit], f"{limit:,} of {selection.description}")
        typer.echo(f"selection: {selection.description}\n")
        if not selection.refs:
            typer.echo("nothing to do.")
            return

        ctx = _context(settings, catalog, refresh or [])
        report = run_stages(ctx, stages, selection.refs, dry_run=dry_run)
        _report_refusals(catalog)

    typer.echo(_render(report, dry_run=dry_run))


def _select(
    catalog: Catalog,
    manifest: Optional[Path],
    mode: Select,
    stage,
    refresh: Sequence[str] = (),
    settings=None,
) -> Selection:
    if manifest:
        refs = resolve_manifest(catalog, manifest, register=True).refs
        base = Selection(refs, f"{len(refs):,} articles from {manifest.name}")
    else:
        base = everything(catalog)
    only = stage[0] if stage and len(stage) == 1 else None
    return narrow(catalog, base, mode, only, refresh, settings, stages=stage or ())


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
    requirements = {name: upstream_of(STAGE_TYPES[name], settings)[0] for name in STAGE_ORDER}
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
            f"{kind} {', '.join(value) if isinstance(value, list) else value}"
            for kind, value in ref.identifier.to_dict().items()
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
        None, "--table", "-t",
        help="Mark only this table (repeatable). Without it, the whole article."
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", exists=True, dir_okay=False,
        help="JSON list of {article_id, table_id, reason?, note?}; rows whose `verdict` is "
             "present and not 'not coordinates' are ignored, so a review file can be given as is.",
    ),
    reason: str = typer.Option("not coordinates", "--reason", help="Why: recorded with the mark."),
    note: str = typer.Option("", "--note", help="Free text recorded with the mark."),
    remove: bool = typer.Option(
        False, "--remove", help="Withdraw the marks instead of adding them."),
    list_: bool = typer.Option(
        False, "--list", help="Print the marks on the given articles, or all."),
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
            for table_id in table or [WHOLE_ARTICLE]:
                rows.append((ref.id, str(table_id), reason, note))
        if file is not None:
            for entry in json.loads(file.read_text()):
                verdict = entry.get("verdict")
                if verdict is not None and verdict != "not coordinates":
                    continue
                if not any(vars(catalog.identifier(entry["article_id"])).values()):
                    typer.echo("  skipped, not an article id in this catalog: "
                               f"{entry['article_id']}")
                    continue
                rows.append((entry["article_id"],
                             str(entry.get("table_id") or WHOLE_ARTICLE),
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
            n = sum(catalog.unexclude(a, None if t == WHOLE_ARTICLE and not table else t)
                    for a, t, _, _ in rows)
            typer.echo(f"withdrew {n:,} marks")
            return
        catalog.exclude(rows)
        typer.echo(
            f"marked {len(rows):,} tables on {len({r[0] for r in rows}):,} articles; "
            "run upload and sync to act on them"
        )


@app.command()
def repair(
    apply: bool = apply_option("Hand them over (default: only count)."),
    backup: Optional[Path] = BackupOption,
    config: Optional[Path] = ConfigOption,
) -> None:
    """Hand over what a merge stranded, and store the fingerprints artifacts lack.

    Exclusions are read through the merge either way; this moves the rows, artifacts
    with them, so they sit on the article they belong to. Run it again after any run
    of an older version that merged articles.

    An artifact with no fingerprint is not fresh. Every stage is planned as `ingest
    run --dry-run` plans it, under this config, and each such artifact the plan would
    reuse once it had the fingerprint expected of it is given that fingerprint; those
    made from it are carried over to the fingerprint they are expected to have now.
    Nothing is made again. Plan with the config the runs use and its models served,
    or the stages reading them cannot be asked.
    """
    require_new_backup(apply, backup)
    settings = _settings(config)
    with _catalog(settings) as catalog:
        artifacts, exclusions = catalog.stranded_artifacts(), catalog.stranded_exclusions()
        typer.echo(f"stranded: {artifacts:,} artifacts, {exclusions:,} exclusions")
        if artifacts or exclusions:
            # Planned after the hand-over only, so the table a dry run prints is the
            # one an --apply writes.
            later = "fingerprints are planned once nothing is stranded"
            if not apply:
                typer.echo(f"dry run: nothing written; --apply --backup <path> hands them "
                           f"over; {later}")
                return
            typer.echo(f"catalog copied to {catalog.backup(backup)}")
            done = catalog.repair_merges()
            typer.echo(
                f"reattached artifacts from {done.artifact_articles:,} articles; "
                f"moved {done.exclusions_moved:,} exclusions, "
                f"dropped {len(done.exclusions_dropped):,} the survivor already held"
            )
            for article_id, table_id, reason, note in done.exclusions_dropped:
                typer.echo(f"  dropped {article_id} {table_id}: {reason} {note}".rstrip())
            typer.echo(f"{later}: run `ingest repair` again")
            return
        stages = build([n for n in STAGE_ORDER if settings.prose_model or n not in PROSE_STAGES],
                       settings)
        found = refingerprint(_context(settings, catalog, []), stages,
                              everything(catalog).refs, catalog.empty_fingerprints())
        typer.echo(_render_refingerprint(found))
        if not found.rewrites:
            typer.echo("nothing to repair.")
            return
        rerun = found.rerun()
        if not apply:
            typer.echo("dry run: nothing written; --apply --backup <path> writes them"
                       + (" once nothing would run again" if rerun else ""))
            return
        if rerun:
            typer.echo("refusing to write: " + ", ".join(
                f"{n:,} {stage}" for stage, n in rerun.items()) + " would run again.")
            for stage in rerun:
                counts, reasons = found.stages[stage], found.plan.stages[stage].reasons
                if counts.empty_rerun:
                    typer.echo(f"  {stage}: {counts.empty_rerun:,} have no fingerprint and "
                               f"their blob is gone: run the stage for them "
                               f"(`ingest run -s {stage}`) first")
                for reason, n in reasons.items():
                    typer.echo(f"  {stage}: {n:,} blocked: {reason}")
                if (counts.lost or counts.unasked) and not reasons:
                    typer.echo(f"  {stage}: {counts.lost + counts.unasked:,} reused now "
                               "would not be, with no plan blocking them")
            typer.echo("Give the blocked stages what they wait on (the models the config "
                       "names, text hashes from `ingest refresh-text`), then plan again.")
            raise typer.Exit(1)
        typer.echo(f"catalog copied to {catalog.backup(backup)}")
        stored = catalog.store_fingerprints(list(found.rewrites.values()))
        typer.echo(f"stored {stored:,} fingerprints; "
                   f"{len(found.rewrites) - stored:,} artifacts were written meanwhile")


#: What an identity change leaves stale without changing a fingerprint: metadata
#: hashes no identifier, and triage and prose hash metadata's fingerprint.
IDENTITY_REFRESH = ("metadata", "triage", "prose")


@app.command("repair-identity")
def repair_identity(
    plan: Path = typer.Argument(..., exists=True, dir_okay=False,
                                help="Reviewed plan: JSON lines, one operation each."),
    apply: bool = apply_option("Write the plan (default: run it and roll it back)."),
    backup: Optional[Path] = BackupOption,
    manifest: Optional[Path] = typer.Option(
        None, "--manifest",
        help="Where --apply writes the touched articles' identifiers, for `ingest run "
             "--manifest`."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Repair article identities: move, remove, rekind or add aliases, split, merge, and
    move, swap or forget artifacts, as a reviewed plan says.

    The plan runs as one transaction. Each operation is checked against the catalog
    as the ones before it left it, and refused when the catalog is not as the plan
    says (an alias no longer where the plan puts it, an artifact gone). Then every
    article it touched, every article NCBI, Crossref or a download links to one of
    those, and every article holding two of a kind, is checked against the catalog's
    invariants (`catalog/invariants.py`), before the plan and after it. A dry run lists
    every refusal, every violation the plan makes, and those that were there before;
    --apply writes nothing while there is a refusal or a violation the plan makes. The
    first run reads the ids every download names into the catalog, once. An article
    the plan leaves with no extraction
    has its passages taken back, and the next `ingest run` takes back what was made
    from them. Stop the pipeline first: the plan holds the write lock while it runs,
    and NCBI and the DOI registries are asked about the ids.
    """
    require_new_backup(apply, backup)
    if apply and manifest is None:
        raise typer.BadParameter("--apply needs --manifest <path>", param_hint="--manifest")
    settings = _settings(config)
    try:
        operations = identity_plan.load(plan)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="PLAN") from exc
    with _registering_catalog(settings) as catalog:
        outcome = identity_plan.apply(catalog, operations, backup=backup if apply else None)
    for op, n in sorted(outcome.applied.items()):
        typer.echo(f"  {n:6,}  {op}")
    typer.echo(f"{sum(outcome.applied.values()):,} of {len(operations):,} operations "
               f"{'applied' if outcome.committed else 'would apply'}; "
               f"{len(outcome.new_articles):,} new articles; "
               f"{len(outcome.touched):,} articles touched")
    for number, operation, why in outcome.refused:
        typer.echo(f"refused line {number} ({operation['op']}): {why}")
    for violation in outcome.violations:
        typer.echo(f"violation {violation.article_id} {violation.rule}: {violation.detail}")
    for violation in outcome.accepted:
        typer.echo(f"accepted, kept apart until neurostore makes one base study a version of "
                   f"the other: {violation.article_id} {violation.rule}: {violation.detail}")
    for violation in outcome.preexisting:
        typer.echo(f"pre-existing, not caused by this plan: {violation.article_id} "
                   f"{violation.rule}: {violation.detail}")
    if outcome.refused or outcome.violations:
        typer.echo(f"{len(outcome.refused):,} refused, {len(outcome.violations):,} violations "
                   "the plan makes. Nothing was written; plan again from the catalog as it is.")
        raise typer.Exit(1)
    if not outcome.committed:
        typer.echo("dry run: nothing written; --apply --backup <path> --manifest <path> "
                   "writes it")
        return
    typer.echo(f"catalog copied to {backup}")
    Identifiers(outcome.touched).save(manifest)
    stages = " ".join(f"-s {stage} --refresh {stage}" for stage in IDENTITY_REFRESH)
    typer.echo(f"touched articles in {manifest}; refresh what identity feeds: "
               f"ingest run {stages} --manifest {manifest}")
    if outcome.taken_back:
        typer.echo(f"{len(outcome.taken_back):,} articles were left with no extraction and their "
                   f"passages taken back; `ingest run --manifest {manifest}` takes back what "
                   "was made from them, through upload")


def _render_refingerprint(found) -> str:
    lines = ["fingerprints:",
             f"  {'stage':<11}{'filled':>10}{'carried':>10}"
             f"{'empty, runs again':>19}{'empty, unplanned':>18}{'fresh, runs again':>19}"
             f"{'not asked, stale':>18}"]
    for stage in STAGE_ORDER:
        n = found.stages.get(stage)
        if n is None:
            continue
        lines.append(f"  {stage:<11}{n.filled:>10,}{n.carried:>10,}"
                     f"{n.empty_rerun:>19,}{n.empty_unplanned:>18,}{n.lost:>19,}"
                     f"{n.unasked:>18,}")
    return "\n".join(lines)


@app.command("refresh-text")
def refresh_text(
    source: Optional[List[str]] = typer.Option(
        None, "--source",
        help="Extraction sources to read (default: all; a PDF's text only has its hash recorded)."
    ),
    corpus: bool = typer.Option(
        True, "--corpus/--no-corpus", help="Also replace the ns-pond corpus's copies of the texts."
    ),
    dry_run: bool = dry_run_option("Report what would change; write nothing."),
    report: Optional[Path] = typer.Option(
        None, "--report", help="Write one JSON line per extraction here."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Rebuild extractions' text from their downloads, without re-running extract.

    The extraction keeps its tables, payload and fingerprint; its text hash and the
    passages' spans move to the new text in the same catalog write, and the references
    and the sync are marked stale so they are read and written again. For a change
    to the text alone (pubget keeping superscripts, the figure captions written under
    "Figure legends", ACE's short texts built again without readability). An ACE
    extraction's `text_source` and summary `text_from` are set to how its new text was made.
    """
    from collections import Counter

    from ingestion_workflow.services import text_refresh

    settings = _settings(config)
    sources = tuple(source or text_refresh.SOURCES)
    unknown = set(sources) - set(text_refresh.SOURCES)
    if unknown:
        raise typer.BadParameter(f"cannot rebuild the text of {', '.join(sorted(unknown))}")
    counts: Counter = Counter()
    new_of: dict = {}  # old sha256 -> {(new sha256, path)}
    out = report.open("w") if report else None
    with _catalog(settings) as catalog:
        jobs = list(text_refresh.jobs(catalog, sources))
    typer.echo(f"{len(jobs):,} extractions to check ({', '.join(sources)})")
    carried: Counter = Counter()
    pending: list = []

    def carry() -> None:
        # What the catalog stores against each text moves with it, a batch of texts per
        # write. The catalog is open only while a batch is carried: the pool forks its
        # workers between batches, and none may inherit a connection.
        with _catalog(settings) as catalog:
            carried.update(text_refresh.carry_all(catalog, pending, write=not dry_run))
        pending.clear()

    for result in text_refresh.run(jobs, write=not dry_run, workers=settings.max_workers):
        counts[result.status.split(":")[0]] += 1
        if result.status in ("rewritten", "would_rewrite"):
            new_of.setdefault(result.old_sha256, set()).add((result.new_sha256, result.text_path))
            pending.append(result)
        elif result.old_sha256 and not dry_run:
            pending.append(result)  # its text stays: to record its hash
        if len(pending) == text_refresh.BATCH:
            carry()
        if out:
            row = {k: v for k, v in result.__dict__.items() if k != "edits"}
            out.write(json.dumps(row) + "\n")
    if pending:
        carry()
    typer.echo(", ".join(f"{k} {v:,}" for k, v in sorted(counts.items())))
    typer.echo(f"passages: {'would remap' if dry_run else 'remapped'} {carried['remapped']:,}; "
               f"{carried['stale']:,} left for the passages stage to read again, "
               f"{carried['read_again']:,} remapped and read again for the coordinates their "
               f"new text holds")
    if corpus:
        # an old text two extractions shared, rebuilt two ways, names no single replacement
        replaced = {old: next(iter(news))[1] for old, news in new_of.items()
                    if len({sha for sha, _ in news}) == 1}
        ambiguous = len(new_of) - len(replaced)
        n = failed = 0
        for target, status in text_refresh.refresh_corpus(settings.ns_pond_root, replaced, sources,
                                                          write=not dry_run):
            if status.startswith("failed"):
                failed += 1
                typer.echo(f"corpus: {target} left as it is, {status}")
            else:
                n += 1
            if out:
                out.write(json.dumps({"corpus_text": str(target), "status": status}) + "\n")
        verb = "would replace" if dry_run else "replaced"
        typer.echo(f"corpus: {verb} {n:,} text.txt files under {settings.ns_pond_root}"
                   + (f"; {ambiguous:,} old texts skipped as ambiguous" if ambiguous else "")
                   + (f"; {failed:,} could not be read" if failed else ""))
    if out:
        out.close()


# -- revalidate-downloads ----------------------------------------------------


@app.command("revalidate-downloads")
def revalidate_downloads(
    apply: bool = apply_option("Record the failures (default: dry run)."),
    backup: Optional[Path] = BackupOption,
    manifest: Optional[Path] = manifest_option("Check only the articles in this JSONL manifest (default: every article)."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Fail stored downloads that are not the article: paywall, error, empty-shell,
    citation-stub and abstract-only pages, checked again without fetching.

    Each is recorded as a failed download with its reason, and the extraction made from
    it fails with "download failed validation: <reason>". A run then moves triage to
    another source's text, and `run --stage download` plans the next source. Running it
    again finds nothing new. Stop other runs first: a row they rewrite during the scan is
    left as they wrote it.
    """
    from ingestion_workflow.pipeline.stages import DownloadStage
    from ingestion_workflow.services import download_revalidate as revalidate

    require_new_backup(apply, backup)
    settings = _settings(config)
    with _catalog(settings) as catalog:
        ctx = _context(settings, catalog, [])
        if manifest:
            # a dry run writes nothing, so unknown articles are not registered
            named = resolve_manifest(catalog, manifest, register=False)
            article_ids = list(dict.fromkeys(ref.id for ref in named.refs))
            typer.echo(f"manifest {manifest.name}: {named.entries:,} entries, "
                       f"{len(article_ids):,} articles in the catalog")
            if named.unknown:
                typer.echo(f"not in the catalog, not checked: {len(named.unknown):,}")
                for identifier in named.unknown:
                    typer.echo(f"  {identifier.slug}")
        else:
            article_ids = list(catalog.all_article_ids())
        found = revalidate.scan(catalog, article_ids,
                                workers=settings.max_workers, recorded_path=ctx.recorded_path)
        effects = revalidate.effects(ctx, DownloadStage(settings), found)
        typer.echo(f"checked {found.checked:,} stored downloads holding a page; "
                   f"{len(found.missing):,} with files gone were left alone")
        typer.echo(f"bad pages: {len(found.bad):,}")
        for (reason, source), n in sorted(found.counts().items()):
            typer.echo(f"  {reason:<14} {source:<10} {n:>8,}")
        typer.echo(f"OK extractions made from a bad page: {len(found.extractions):,}")
        typer.echo("their articles, by what happens next: "
                   + ", ".join(f"{k} {v:,}" for k, v in sorted(effects["next"].items())))
        typer.echo("artifacts made from a judged extraction that fails, by stage: "
                   + ", ".join(f"{k} {v:,}" for k, v in effects["stale"].items()))
        if not apply:
            typer.echo("dry run: nothing written; --apply --backup <path> records these")
            return
        if not found.outcomes():
            typer.echo("nothing to record; no backup made")
            return
        typer.echo(f"catalog copied to {catalog.backup(backup)}")
        recorded = revalidate.apply(catalog, found)
        changed = found.changed
        left = f"; left {changed:,} rows another run rewrote since the scan" if changed else ""
        typer.echo(f"recorded {recorded:,} failures{left}")


# -- migrate -----------------------------------------------------------------


@app.command()
def migrate(
    old_cache: Path = typer.Argument(
        ..., exists=True, file_okay=False, help="Pre-refactor .cache directory"
    ),
    stage: Optional[List[str]] = typer.Option(None, "--stage", "-s", help="Only these stages."),
    dry_run: bool = dry_run_option("Count what would be imported."),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Import pre-refactor sqlite caches into the catalog. Reads only."""
    from ingestion_workflow.migrate import migrate_caches

    settings = _settings(config)
    with _registering_catalog(settings) as catalog:
        report = migrate_caches(old_cache, catalog, stages=stage, dry_run=dry_run)
    typer.echo(report.render())
    if not dry_run:
        typer.echo("migrated artifacts carry no fingerprint, so none is fresh: "
                   "run `ingest repair` to store the ones the plan expects")


# -- report ------------------------------------------------------------------

report_app = typer.Typer(help="Reports read from the catalog. They never write to it.",
                         no_args_is_help=True)
app.add_typer(report_app, name="report")


def _catalog_root(catalog: Optional[Path], config: Optional[Path]) -> Path:
    return catalog if catalog is not None else _settings(config).catalog_root


@report_app.command("funnel")
def report_funnel(
    out: Path = typer.Option(Path("funnel.html"), "--out", "-o", help="HTML file to write."),
    samples: int = typer.Option(
        20, "--samples", help="Article ids kept per bucket for the drill-down."),
    prose: Optional[bool] = ProseOption,
    catalog: Optional[Path] = CatalogOption,
    config: Optional[Path] = ConfigOption,
) -> None:
    """Write one self-contained HTML page: every article's state at every stage."""
    from ingestion_workflow.services.funnel_report import write_report

    funnel = write_report(_catalog_root(catalog, config), out, samples=samples, prose=prose)
    typer.echo(f"{funnel.total:,} articles in {funnel.seconds:.1f}s -> {out} "
               f"({out.stat().st_size / 1e6:.1f} MB)")
    _echo_flow(funnel)


def _echo_flow(funnel) -> None:
    from ingestion_workflow.services.funnel_report import page_model

    for step in page_model(funnel)["flow"]:
        left = sum(group[2] for group in step["excluded"])
        waiting = sum(group[2] for group in step["waiting"])
        top = max(step["excluded"] + step["waiting"], key=lambda group: group[2], default=None)
        check = "ok" if step["adds_up"] and step["follows"] else "DOES NOT ADD UP"
        typer.echo(f"  {step['stage']:>9}  in {step['in']:>9,}  passed {step['passed']:>9,}  "
                   f"left out {left:>9,}  waiting {waiting:>9,}  {check}"
                   + (f"  top: {top[2]:,} {top[0]}: {top[1]}" if top else ""))
    for problem in funnel.reconcile():
        typer.echo(f"  does not reconcile: {problem}")


def _pipeline_settings(config: Optional[Path]):
    """The pipeline's settings when a config is named; the trace needs them to plan."""
    return load_settings(config) if config is not None else None


@report_app.command("trace")
def report_trace(
    article_id: str = typer.Argument(..., help="Article id, as the page lists it."),
    prose: Optional[bool] = ProseOption,
    catalog: Optional[Path] = CatalogOption,
    config: Optional[Path] = ConfigOption,
) -> None:
    """Print how one article got where it is, as JSON: per stage its state, and each
    record's path, status, reason, when it was written, its fingerprint and inputs.
    With --config, each stage's own plan says whether the record is fresh."""
    from ingestion_workflow.services.funnel_report import trace_article

    found = trace_article(_catalog_root(catalog, config), article_id,
                          settings=_pipeline_settings(config), prose=prose)
    typer.echo(json.dumps(found, indent=1))


@report_app.command("serve")
def report_serve(
    port: int = typer.Option(8765, "--port", help="Port on 127.0.0.1."),
    samples: int = typer.Option(
        20, "--samples", help="Article ids kept per bucket for the drill-down."),
    prose: Optional[bool] = ProseOption,
    catalog: Optional[Path] = CatalogOption,
    config: Optional[Path] = ConfigOption,
) -> None:
    """Serve the funnel page with a full trace for each article clicked (read-only)."""
    from ingestion_workflow.services.funnel_report import serve

    server = serve(_catalog_root(catalog, config), port=port, samples=samples, prose=prose,
                   settings=_pipeline_settings(config))
    _echo_flow(server.funnel)
    typer.echo(f"serving on http://127.0.0.1:{server.server_address[1]}/ (Ctrl-C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


@report_app.command("funnel-ids")
def report_funnel_ids(
    stage: str = typer.Option(..., "--stage", help="Stage to list."),
    state: Optional[List[str]] = typer.Option(
        None, "--state", help="Only this state (passed, failed, ...); repeat for any of several. "
        "With --by-source, the row's status (ok, failed, permanent, skipped, taken_back)."),
    reason: Optional[str] = typer.Option(
        None, "--reason", help="Reason contains this text, as the page's reason box."),
    exact_reason: Optional[str] = typer.Option(
        None, "--exact-reason", help="Reason is exactly this, as a clicked reason on the page."),
    source: Optional[List[str]] = typer.Option(
        None, "--source", help="Has an ok download from this source; repeat for any of several."),
    tried: Optional[List[str]] = typer.Option(
        None, "--tried", help="Has a download recorded from this source, failed or not; "
        "repeat for any of several."),
    judged: Optional[List[str]] = typer.Option(
        None, "--judged", help="Triage judged this source's text; repeat for any of several."),
    day_from: Optional[str] = typer.Option(
        None, "--from", help="Registered on or after this day (YYYY-MM-DD)."),
    day_to: Optional[str] = typer.Option(
        None, "--to", help="Registered on or before this day (YYYY-MM-DD)."),
    prose: Optional[bool] = ProseOption,
    by_source: Optional[str] = typer.Option(
        None, "--by-source", help="List this source's own download or extraction rows at "
        "--stage (download or extract), as the page's source table does."),
    reason_group: Optional[str] = typer.Option(
        None, "--reason-group", help="Reason falls in this group, as a clicked group in the "
        "flow: its origin and primary part, e.g. 'not open access'."),
    catalog: Optional[Path] = CatalogOption,
    config: Optional[Path] = ConfigOption,
) -> None:
    """Print every article in one funnel bucket, tab-separated: id, stage, state, reason."""
    from ingestion_workflow.services.funnel_report import STATES, TAKEN_BACK, matching_ids

    allowed = ("ok", "failed", "permanent", "skipped", TAKEN_BACK) if by_source else STATES
    for one in state or ():
        if one not in allowed:
            raise typer.BadParameter(f"state must be one of {', '.join(allowed)}")
    if by_source and stage not in ("download", "extract"):
        raise typer.BadParameter("--by-source lists download or extract rows only")
    for row in matching_ids(_catalog_root(catalog, config), stage=stage, states=state or (),
                            reason=reason, exact_reason=exact_reason,
                            sources=source or (), tried=tried or (), judged=judged or (),
                            day_from=day_from, day_to=day_to, prose=prose,
                            by_source=by_source, group=reason_group):
        typer.echo("\t".join(row))


# -- searches ----------------------------------------------------------------

searches_app = typer.Typer(
    help="The searches `add --search` ran, and their articles by provider.", no_args_is_help=True
)
app.add_typer(searches_app, name="searches")


@searches_app.command("list")
def searches_list(config: Optional[Path] = ConfigOption) -> None:
    """Each search kept: id, time, query, years, and per provider the query sent and its counts."""
    with _catalog(_settings(config)) as catalog:
        runs = catalog.searches()
    for run in runs:
        typer.echo(f"search {run['id']}  {run['searched_at']}  years {run['years']}")
        typer.echo(f"  query {run['query']}")
        for name, found in run["providers"].items():
            typer.echo(
                f"  {name}: {found['articles']:,} articles from {found['records']:,} records, "
                f"{found['without_id']:,} without an id; sent {found['query']}"
            )
    if not runs:
        typer.echo("no searches kept.")


@searches_app.command("manifest")
def searches_manifest(
    search_id: int = typer.Argument(..., help="The search's id, from `searches list`."),
    found_by: Optional[List[SearchProvider]] = typer.Option(
        None, "--found-by", help="Articles any of these providers found (repeatable)."
    ),
    only_found_by: Optional[SearchProvider] = typer.Option(
        None, "--only-found-by", help="Articles this provider found and no other did."
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", "-o",
        help="JSONL manifest to write; default <data_root>/manifests/search-<id>.jsonl.",
    ),
    config: Optional[Path] = ConfigOption,
) -> None:
    """Write a manifest of a search's articles, for `ingest run --manifest`.

    With neither option, every article the search found. `--found-by` and
    `--only-found-by` are exclusive.
    """
    if found_by and only_found_by:
        raise typer.BadParameter("give --found-by or --only-found-by, not both")
    settings = _settings(config)
    with _catalog(settings) as catalog:
        try:
            ids = catalog.search_articles(
                search_id,
                found_by=[p.value for p in found_by or ()],
                only=only_found_by.value if only_found_by else None,
            )
        except KeyError as exc:
            raise typer.BadParameter(exc.args[0]) from None
        identifiers = catalog.identifiers(ids)
    path = out or settings.data_root / "manifests" / f"search-{search_id}.jsonl"
    Identifiers([identifiers[i] for i in ids]).save(path)
    typer.echo(f"{len(ids):,} articles -> {path}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
