"""Run the page check again on stored downloads, without fetching anything.

A download stored as OK before the check existed can be a paywall notice, an error
page, an empty shell or a citation stub. Each such download is recorded as a failed
download with its reason, the way the download stage records one it has just fetched;
the extraction made from it fails with "download failed validation: <reason>". From
there the stages do the rest: triage judges another source's extraction, an article
with no other download plans its next source, and an article no source can serve stays
failed rather than having its text taken back.
"""

from __future__ import annotations

import copy
import multiprocessing
import sqlite3
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import Artifact, Catalog, Outcome, Status, utcnow
from ingestion_workflow.models import FileType
from ingestion_workflow.services import download_check

#: The error an extraction made from a download that failed the check is recorded with.
FAILED_VALIDATION = "download failed validation"

#: Stages after extract whose artifacts were made, through triage or passages, from the
#: article's judged extraction.
DOWNSTREAM = ("metadata", "notices", "references", "triage", "analyses", "passages", "prose",
              "resolve", "roles", "space", "upload", "sync")

_BATCH = 900


@dataclass(frozen=True)
class BadDownload:
    article_id: str
    source: str
    reason: str
    message: str
    fingerprint: str


@dataclass
class Revalidation:
    """What a pass over the stored downloads found, and the outcomes that record it."""

    checked: int = 0
    #: OK downloads whose page fails the check now.
    bad: List[BadDownload] = field(default_factory=list)
    #: (OK extraction, reason): made from a download that failed the check, now or earlier.
    extractions: List[Tuple[Artifact, str]] = field(default_factory=list)
    #: Downloads whose files are gone from disk: left as they are, and counted.
    missing: List[Tuple[str, str]] = field(default_factory=list)

    def outcomes(self) -> List[Outcome]:
        out = [
            Outcome.failure(b.article_id, "download", b.source, b.message,
                            fingerprint=b.fingerprint, summary={"bad_page": b.reason})
            for b in self.bad
        ]
        out += [
            Outcome.failure(e.article_id, "extract", e.source, f"{FAILED_VALIDATION}: {reason}",
                            fingerprint=e.fingerprint, summary={"bad_page": reason})
            for e, reason in self.extractions
        ]
        return out

    def counts(self) -> Counter:
        """(reason, source) -> downloads that fail the check."""
        return Counter((b.reason, b.source) for b in self.bad)


def _html_files(catalog: Catalog, download: Artifact) -> Optional[List[str]]:
    payload = catalog.payload(download)
    if payload is None:
        return None
    return [str(f["file_path"]) for f in payload.get("files", [])
            if f.get("file_type") == FileType.HTML.value]


def _check(item: Tuple[str, str, List[str]]):
    article_id, source, paths = item
    for path in paths:
        if not Path(path).is_file():
            return article_id, source, None, "missing"
        bad = download_check.check_file(Path(path))
        if bad is not None:
            return article_id, source, bad.reason, bad.message
    return article_id, source, None, None


def _map(items: List[Tuple[str, str, List[str]]], workers: int) -> Iterator[tuple]:
    if workers <= 1:
        yield from map(_check, items)
        return
    # fork: the check is lxml and regexes; spawn would re-import the package per worker
    fork = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=workers, mp_context=fork) as pool:
        yield from pool.map(_check, items, chunksize=64)


def scan(
    catalog: Catalog,
    article_ids: Sequence[str],
    *,
    workers: int = 1,
) -> Revalidation:
    """Check every OK download of these articles that holds an HTML page.

    XML and PDF downloads hold no page and are not read. Idempotent: a download already
    failed is not checked again, and its OK extraction, if a run stopped before failing
    it, is failed now.
    """
    result = Revalidation()
    for start in range(0, len(article_ids), _BATCH * 20):
        chunk = list(article_ids[start:start + _BATCH * 20])
        downloads = catalog.artifacts(chunk, "download")
        extractions = catalog.artifacts(chunk, "extract")
        items = []
        for article_id, by_source in downloads.items():
            for source, download in by_source.items():
                if download.status is not Status.OK:
                    continue
                paths = _html_files(catalog, download)
                if paths is None:
                    result.missing.append((article_id, source))
                elif paths:
                    items.append((article_id, source, paths))
        failed: Dict[Tuple[str, str], str] = {
            (article_id, source): download.summary["bad_page"]
            for article_id, by_source in downloads.items()
            for source, download in by_source.items()
            if download.status is not Status.OK and download.summary.get("bad_page")
        }
        for article_id, source, reason, message in _map(items, workers):
            result.checked += 1
            if message == "missing":
                result.missing.append((article_id, source))
            elif reason is not None:
                download = downloads[article_id][source]
                result.bad.append(
                    BadDownload(article_id, source, reason, message, download.fingerprint))
                failed[(article_id, source)] = reason
        for (article_id, source), reason in sorted(failed.items()):
            extraction = extractions.get(article_id, {}).get(source)
            if extraction is not None and extraction.status is Status.OK:
                result.extractions.append((extraction, reason))
    return result


def apply(catalog: Catalog, revalidation: Revalidation) -> int:
    """Record the failures, downloads and their extractions in one transaction."""
    outcomes = revalidation.outcomes()
    catalog.record(outcomes)
    return len(outcomes)


def backup(catalog_root: Path, target: Path) -> Path:
    """A consistent copy of the catalog database, WAL included, at `target`.

    Only the database changes: a failure stores no blob, so the blob store needs no copy.
    """
    target = Path(target)
    if target.exists():
        raise FileExistsError(f"{target} exists; give a new path for the backup")
    target.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(Path(catalog_root) / "catalog.sqlite")
    dest = sqlite3.connect(target)
    try:
        source.backup(dest)
    finally:
        dest.close()
        source.close()
    return target


# -- what the stages will do with it -------------------------------------------


def effects(ctx, download_stage, revalidation: Revalidation) -> Dict[str, Counter]:
    """What each affected article's stages will do once the failures are recorded.

    Articles with an extraction that fails are sorted under `next`: `kept` (triage judges
    another source's extraction already), `switch` (triage
    judges another source's current extraction with text), `extract` (another download is
    OK and not yet extracted), `download:<source>` (no other download is OK; the download
    plan's next source), `no_text` (the other sources' extractions hold no text) or `none`
    (no source has a try left). The last two stay failed; their text is not taken back.
    A source counts with its tries left whether or not its backoff has passed. `stale`
    counts, by stage, their OK artifacts made from the extraction that fails.
    """
    from ingestion_workflow.pipeline.stages.triage import judged_extraction

    failing_downloads = {(b.article_id, b.source) for b in revalidation.bad}
    failing_extractions = {(e.article_id, e.source) for e, _ in revalidation.extractions}
    ids = sorted({a for a, _ in failing_downloads | failing_extractions})
    out = {"next": Counter(), "stale": Counter(), "judged_before": Counter()}
    ctx = copy.copy(ctx)
    ctx.retry_after = timedelta(0)
    now = utcnow()
    for start in range(0, len(ids), _BATCH):
        chunk = ids[start:start + _BATCH]
        refs = {ref_id: ctx.catalog.ref(ref_id) for ref_id in chunk}
        downloads = ctx.catalog.artifacts(chunk, "download")
        extractions = ctx.catalog.artifacts(chunk, "extract")
        attempts = {
            source: ctx.catalog.attempt_counts(chunk, "download", source)
            for source in ctx.settings.download_sources
        }
        downstream = {stage: ctx.catalog.artifacts(chunk, stage) for stage in DOWNSTREAM}
        for article_id in chunk:
            before_d = downloads.get(article_id, {})
            before_e = extractions.get(article_id, {})
            after_d = {s: _failed(a) if (article_id, s) in failing_downloads else a
                       for s, a in before_d.items()}
            after_e = {s: _failed(a) if (article_id, s) in failing_extractions else a
                       for s, a in before_e.items()}
            for source in after_d:
                if (article_id, source) in failing_downloads:
                    count, _ = attempts.setdefault(source, {}).get(article_id, (0, None))
                    attempts[source][article_id] = (count + 1, now)
            judged = judged_extraction(ctx, before_e, before_d)
            if judged is None or (article_id, judged.source) not in failing_extractions:
                if any((article_id, s) in failing_extractions for s in before_e):
                    out["next"]["kept"] += 1
                continue
            out["judged_before"][judged.source] += 1
            for stage, found in downstream.items():
                if any(a.status is Status.OK for a in found.get(article_id, {}).values()):
                    out["stale"][stage] += 1
            ref = refs[article_id]
            out["next"][_next(ctx, download_stage, ref, after_d, after_e, attempts)] += 1
    return out


def _failed(artifact: Artifact) -> Artifact:
    return replace(artifact, status=Status.FAILED)


def _next(ctx, download_stage, ref, downloads, extractions, attempts) -> str:
    from ingestion_workflow.pipeline.stages.extract import current_extractions

    current = current_extractions(ctx, extractions, downloads)
    if any(e.summary.get("has_text", True) for e in current.values()):
        return "switch"
    if any(d.status is Status.OK and s not in current for s, d in downloads.items()):
        return "extract"
    if current:
        return "no_text"
    source = download_stage.next_source(ctx, ref, downloads, attempts)
    return f"download:{source.value}" if source is not None else "none"
