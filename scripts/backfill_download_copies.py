"""Record the copy each stored download holds, in the summary of its catalog row.

The download stage records a download's copy (route, version, access, url) when it
fetches it; this does the same, offline, for the downloads stored before it did, by
the same rule (`extractors.copies.copy_of`), reading each download's payload and files:

    python scripts/backfill_download_copies.py <catalog root>                  # dry run
    python scripts/backfill_download_copies.py <catalog root> --apply --backup <path>

Run it where the pipeline runs: a download's files may be stored relative to there, and
its settings give the OpenAlex key (no email is sent) and the metadata cache. A PDF
download records, as the pdf source now does when it fetches one, the licence its URL was
listed with (`PdfCandidate.license`): OpenAlex's, from its locations of the work, which
this asks for in a dry run too (those locations also type a URL no provider typed,
`pdf_extractor.site_type`), else Semantic Scholar's `openAccessPdf`, read offline from
the metadata cache. A download's licence is not in the metadata fingerprint, so run this
before the metadata stage decides the licences.

A dry run opens the catalog read-only and prints what it would record, counted by
source, route, version and access. --apply first copies the catalog to `--backup`, a
path not yet taken, then changes only the summaries: no fingerprint, status or
`updated_at` moves, so nothing goes stale. The copy replaces the `article_version` that
the pdf, wiley and springer sources recorded. A row written meanwhile is left alone, and so
is one whose payload or files are gone; each is printed with why. A second run finds
nothing left to record.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from contextlib import nullcontext
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from lxml import etree

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog  # noqa: E402
from ingestion_workflow.catalog.blobs import BlobStore  # noqa: E402
from ingestion_workflow.clients.openalex import (  # noqa: E402
    LocationLookupFailed,
    OpenAlexClient,
)
from ingestion_workflow.config import load_settings  # noqa: E402
from ingestion_workflow.extractors.copies import copy_of  # noqa: E402
from ingestion_workflow.extractors.pdf_extractor import (  # noqa: E402
    _openalex_candidates,
    _url_key,
    site_type,
)
from ingestion_workflow.models import (  # noqa: E402
    DownloadResult,
    DownloadSource,
    Identifiers,
)
from study_schema.models.paper_parse import CopyRoute  # noqa: E402

CHUNK = 500

Row = Tuple[str, str, str, str]
#: (article, source, summary read, the copy or None, why not)
Found = Tuple[str, str, str, Optional[dict], Optional[str]]


def _copy(blobs: Path, row: Row) -> Found:
    article_id, source, blob, summary = row
    payload = BlobStore(blobs).get(blob)
    if payload is None:
        return article_id, source, summary, None, "payload blob missing"
    try:
        copy = copy_of(DownloadResult.from_dict(payload)).to_dict()
    except (OSError, ValueError, KeyError, IndexError, etree.LxmlError) as exc:
        return article_id, source, summary, None, f"{type(exc).__name__}: {exc}"
    return article_id, source, summary, copy, None


def _with_copy(summary: str, copy: dict) -> dict:
    """The summary with its copy, which replaces the version some sources recorded."""
    kept = {k: v for k, v in json.loads(summary).items() if k != "article_version"}
    return {**kept, "copy": copy}


def _chunk(args: Tuple[Path, Sequence[Row]]) -> List[Found]:
    blobs, rows = args
    return [_copy(blobs, row) for row in rows]


def copies_of(root: Path, rows: Sequence[Row], workers: int) -> List[Found]:
    """Each row's copy, read from its payload and files, in the order of `rows`."""
    chunks = [(Path(root) / "blobs", rows[i:i + CHUNK]) for i in range(0, len(rows), CHUNK)]
    with ProcessPoolExecutor(workers) as pool:
        return [found for done in pool.map(_chunk, chunks) for found in done]


def _semantic_scholar_license(cache: Path, slug: str, url: str) -> Optional[str]:
    """The licence the cached Semantic Scholar record lists for its open-access PDF at
    `url`."""
    path = cache / f"{slug}.json"
    if not path.exists():
        return None
    listed = json.loads(path.read_text(encoding="utf-8"))["raw_metadata"].get(
        "semantic_scholar", {}).get("openAccessPdf")
    if isinstance(listed, dict) and listed.get("url") and _url_key(listed["url"]) == _url_key(url):
        return listed.get("license") or None
    return None


def typed_by_site(root: Path, rows: Sequence[Row], found: List[Found],
                  openalex: OpenAlexClient, semantic_scholar: Path, out) -> List[Found]:
    """`found`, with each PDF that has a URL recording the licence its URL was listed with
    (OpenAlex's, else the cached Semantic Scholar's) and an untyped one typed by its work's
    OpenAlex locations."""
    blobs = BlobStore(Path(root) / "blobs")
    pdfs = {i: (DownloadResult.from_dict(blobs.get(rows[i][2])),
                copy["route"] == CopyRoute.unrecorded.value)
            for i, (_, source, _, copy, _) in enumerate(found)
            if source == DownloadSource.PDF.value and copy is not None and "url" in copy}
    if not pdfs:
        return found
    failed: Dict[str, str] = {}
    try:
        listed = openalex.get_locations(Identifiers([r.identifier for r, _ in pdfs.values()]))
    except LocationLookupFailed as exc:
        failed = exc.failed
        for slug, why in exc.failed.items():
            print(f"listed\t{slug}\tOpenAlex lookup failed: {why}", file=out)
        listed = exc.found
    candidates = _openalex_candidates(listed)
    typed = list(found)
    for i, (result, untyped) in pdfs.items():
        slug = result.identifier.slug
        if slug in failed:
            # Recorded from no locations the copy would lack its licence, and the next
            # run skips a recorded row; unwritten, a rerun retries the lookup.
            typed[i] = (*found[i][:3], None, "OpenAlex lookup failed")
            continue
        attempt = [a for a in result.url_attempts if a.get("outcome") == "pdf"][-1]
        if untyped:
            attempt["site_type"] = site_type(str(attempt.get("pdf_url") or attempt["url"]),
                                             candidates.get(slug, []))
        # The listing at the attempt's URL, as the fetch matched it.
        here = [c for c in candidates.get(slug, []) if _url_key(c.url) == _url_key(attempt["url"])]
        openalex_license = next((c.license for c in here if c.license), None)
        license = openalex_license or _semantic_scholar_license(
            semantic_scholar, slug, str(attempt["url"]))
        if license:
            attempt["license"] = license
            attempt["license_by"] = "openalex" if openalex_license else "semantic_scholar"
        article_id, source, summary, _, _ = found[i]
        typed[i] = (article_id, source, summary, copy_of(result).to_dict(), None)
    return typed


def run(root: Path, *, openalex: OpenAlexClient, semantic_scholar: Path, apply: bool = False,
        backup: Optional[Path] = None, workers: int = 1, out=sys.stdout) -> Counter:
    path = Path(root) / "catalog.sqlite"
    if not path.exists():
        raise SystemExit(f"no catalog at {path}")
    if apply and (backup is None or Path(backup).exists()):
        raise SystemExit("--apply needs --backup <path>, a path not yet taken")
    conn = (sqlite3.connect(path, isolation_level=None) if apply
            else sqlite3.connect(f"file:{path}?mode=ro", uri=True))
    counts: Counter = Counter()
    licensed: Counter = Counter()
    try:
        rows = conn.execute(
            "SELECT article_id, source, blob, summary FROM artifacts "
            "WHERE stage='download' AND status='ok' ORDER BY article_id, source").fetchall()
        todo = [r for r in rows if "copy" not in json.loads(r[3])]
        counts["already recorded"] = len(rows) - len(todo)
        if apply:
            # Not Catalog.open, which would migrate an older catalog first.
            catalog = Catalog(conn, Path(root))
            print(f"catalog backed up to {catalog.backup(backup)}", file=out)
        found = typed_by_site(root, todo, copies_of(root, todo, workers), openalex,
                              semantic_scholar, out)
        with catalog._write() if apply else nullcontext(conn):
            for article_id, source, summary, copy, why in found:
                if copy is None:
                    counts["left: " + why.split(":")[0]] += 1
                    print(f"left\t{article_id}\t{source}\t{why}", file=out)
                    continue
                if apply and not conn.execute(
                        "UPDATE artifacts SET summary=? WHERE article_id=? AND stage='download' "
                        "AND source=? AND status='ok' AND summary=?",
                        (json.dumps(_with_copy(summary, copy), separators=(",", ":")),
                         article_id, source, summary)).rowcount:
                    counts["left: written meanwhile"] += 1
                    print(f"left\t{article_id}\t{source}\twritten meanwhile", file=out)
                    continue
                key = (source, copy["route"], copy.get("version", "-"), copy["access"])
                counts["\t".join(key)] += 1
                if "license" in copy:
                    licensed[copy["license_by"]] += 1
    finally:
        conn.close()
    print("source\troute\tversion\taccess\tdownloads", file=out)
    for key, n in sorted(counts.items()):
        print(f"{key}\t{n}", file=out)
    for by, n in sorted(licensed.items()):
        print(f"licence listed by {by}\t{n}", file=out)
    print("applied" if apply else "dry run, nothing written", file=out)
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("catalog", type=Path, help="catalog directory (holding catalog.sqlite)")
    parser.add_argument("--apply", action="store_true", help="write; the default is a dry run")
    parser.add_argument("--backup", type=Path, help="where --apply copies the catalog first")
    parser.add_argument("--workers", type=int, default=1, help="processes reading downloads")
    args = parser.parse_args(argv)
    settings = load_settings()
    if not settings.openalex_api_key:
        raise SystemExit("set openalex_api_key: PDFs are typed and licensed by their works' "
                         "OpenAlex locations")
    cache = settings.get_cache_dir("metadata") / "semantic_scholar"
    if not cache.is_dir():
        raise SystemExit(f"no Semantic Scholar metadata cache at {cache}")
    run(args.catalog, openalex=OpenAlexClient(api_key=settings.openalex_api_key),
        semantic_scholar=cache, apply=args.apply, backup=args.backup, workers=args.workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
