"""Read stored PDFs again with `pdf_engines` (MinerU, then Docling), away from the catalog.

The extract stage reads a PDF where the catalog lives; this reads it on any GPU machine
the PDFs are copied to, and records the result in a catalog afterwards:

    python scripts/pdf_rerun.py export <catalog root> <article ids file> <inputs.jsonl>
    python scripts/pdf_rerun.py run <inputs.jsonl> <out dir> [--remap OLD=NEW] [--chunk N]
    python scripts/pdf_rerun.py import <out dir> <catalog root> [--remap OLD=NEW] [--ids FILE]

An ids file's lines start with an article id; a header line `article_id ...` is skipped.

`export` writes each article's pdf download (the one the extract stage would read,
`extraction_inputs`), its fingerprint and this checkout's commit, opening the catalog
read-only. `run` reads them with `PdfExtractor.extract`, as the extract stage does,
`--chunk` at a time, appending each article's `ExtractedContent`, the download's
fingerprint, the commit that read it and the chunk's wall-clock to
`<out dir>/results.jsonl`; a second run reads only the articles not there yet, nor in any
`--done-in` out dir (another machine's run of the same shard). `--shard i/n` takes every
n-th input from the i-th, the same articles however many are done. `--remap` rewrites a
path prefix: in `run`, where the PDFs were copied to; in `import`, where the out dir was
copied back to. `--workers-per-gpu` and `--max-workers` are `mineru_workers_per_gpu` and
`mineru_max_workers`; `--min-free-gb` starts fewer on a machine others share, and the
`--done-in` dirs are read again before each chunk.

`import` records each result (those `--ids` lists, or all) through the extract stage
(`ExtractStage.execute`, handed the stored reads), under the fingerprint of the download it
read, so the stages after extract see a new extraction; run references and links, read
from the text, after it. It refuses, and counts by reason, a result whose article is
unknown or merged, whose download is no longer the one read, whose files are missing, or
whose tables repeat an id (read before 7e3ac063); the refused are written to
`<out dir>/import-refused.tsv`, and nothing is recorded for them. A result from an older
`run` names no download fingerprint: `--inputs` names the export it was read from, whose
download payload is then compared instead. The results name files in the out dir: copy
them where the catalog's data lives first, never over another extraction's, and `--remap`
to there. Run import where the extract stage runs, which is where relative download paths
resolve.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger("pdf_rerun")

#: Results recorded together.
BATCH = 500
#: Why import leaves a result out.
UNKNOWN = "unknown article"
MERGED = "article merged into another"
NO_DOWNLOAD = "no OK pdf download now"
MOVED = "read another download than the article's current one"
UNPINNED = "names no download it read (an older run: pass its --inputs)"
DOWNLOAD_MISSING = "its download's files are missing here"
FILES_MISSING = "its text or table files are missing"
REPEATED_IDS = "repeats a table id (read before 7e3ac063)"


def _remap(value, pairs: List[Tuple[str, str]]):
    """`value` with every string's leading OLD replaced by NEW, through dicts and lists."""
    if isinstance(value, str):
        for old, new in pairs:
            if value.startswith(old):
                return new + value[len(old):]
        return value
    if isinstance(value, list):
        return [_remap(v, pairs) for v in value]
    if isinstance(value, dict):
        return {k: _remap(v, pairs) for k, v in value.items()}
    return value


def _pairs(remap: List[str]) -> List[Tuple[str, str]]:
    return [tuple(pair.split("=", 1)) for pair in remap]


def _read_jsonl(path: Path) -> Iterator[Dict]:
    if path.exists():
        with path.open() as fh:
            for line in fh:
                if line.strip():
                    yield json.loads(line)


def _ids(path) -> List[str]:
    """The article id each line starts with, a header's `article_id` left out."""
    ids = [line.split()[0] for line in Path(path).read_text().splitlines() if line.strip()]
    return [i for i in ids if i != "article_id"]


def _files(download) -> Tuple[str, ...]:
    """What tells one download's files from another's."""
    return tuple(str(f.file_path) for f in download.files)


def export(args) -> None:
    from ingestion_workflow.catalog import Catalog, Status
    from ingestion_workflow.pipeline.stages.extract import extraction_inputs
    from ingestion_workflow.services.offload.export import ns_pond_commit

    root = Path(args.catalog).resolve()
    conn = sqlite3.connect((root / "catalog.sqlite").as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    catalog = Catalog(conn, root)
    commit = ns_pond_commit()
    asked = _ids(args.ids)
    ids = list(dict.fromkeys(catalog.current_id(i) for i in asked))
    downloads = catalog.artifacts(ids, "download")
    written = 0
    with Path(args.inputs).open("w") as out:
        for article_id in ids:
            ok = {s: a for s, a in downloads.get(article_id, {}).items() if a.status is Status.OK}
            download = extraction_inputs(ok).get("pdf")
            if download is None:
                logger.warning("%s: no OK pdf download", article_id)
                continue
            out.write(json.dumps({"article_id": article_id, "download_source": download.source,
                                  "download_fingerprint": download.fingerprint,
                                  "exported_by": commit,
                                  "download": catalog.payload(download)}) + "\n")
            written += 1
    print(f"wrote {written} of {len(asked)} articles to {args.inputs}")


def run(args) -> None:
    from ingestion_workflow.config import load_settings
    from ingestion_workflow.extractors.pdf_extractor import PdfExtractor
    from ingestion_workflow.models import DownloadResult
    from ingestion_workflow.services.offload.export import ns_pond_commit

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    results = out_dir / "results.jsonl"
    done = {row["article_id"] for path in [out_dir, *map(Path, args.done_in)]
            for row in _read_jsonl(path / "results.jsonl")}
    pairs = _pairs(args.remap)
    todo = list(_read_jsonl(Path(args.inputs)))
    if args.shard:
        index, count = map(int, args.shard.split("/"))
        todo = todo[index::count]
    todo = [row for row in todo if row["article_id"] not in done]
    print(f"{len(done)} done, {len(todo)} to read", flush=True)

    settings = load_settings(args.settings, {"data_root": out_dir,
                                             "mineru_workers_per_gpu": args.workers_per_gpu})
    commit = ns_pond_commit()

    with results.open("a") as fh:
        for start in range(0, len(todo), args.chunk):
            workers = _workers(args)
            done = {row["article_id"] for path in map(Path, args.done_in)
                    for row in _read_jsonl(path / "results.jsonl")}
            chunk = [row for row in todo[start:start + args.chunk]
                     if row["article_id"] not in done]
            if not chunk:
                continue
            extractor = PdfExtractor(settings.model_copy(
                update={"mineru_max_workers": workers or None}))
            downloads = [DownloadResult.from_dict(_remap(row["download"], pairs)) for row in chunk]
            began = time.monotonic()
            contents = extractor.extract(downloads)
            seconds = round(time.monotonic() - began, 1)
            for row, content in zip(chunk, contents):
                fh.write(json.dumps({"article_id": row["article_id"],
                                     "download_source": row["download_source"],
                                     "download_fingerprint": row.get("download_fingerprint"),
                                     "read_by": commit,
                                     "chunk_started": began, "chunk_seconds": seconds,
                                     "chunk_size": len(chunk),
                                     "content": content.to_dict()}) + "\n")
            fh.flush()
            print(f"{start + args.chunk}/{len(todo)}: {len(chunk)} in {seconds}s"
                  f" on {workers or 'all'} workers", flush=True)


def _workers(args) -> int:
    """MinerU workers to start now (0: as many as the GPUs fit), waiting while memory
    allows none.

    With `--min-free-gb`, each worker is counted as `--worker-gb` of the memory available
    above that floor; MinerU's workers exit between chunks, so this is read without them.
    """
    if not args.min_free_gb:
        return args.max_workers
    while True:
        with open("/proc/meminfo") as fh:
            available = next(int(line.split()[1]) for line in fh
                             if line.startswith("MemAvailable")) / 2**20
        fits = int((available - args.min_free_gb) // args.worker_gb)
        if fits > 0:
            return min(fits, args.max_workers or fits)
        print(f"{available:.0f} GB available: waiting for room", flush=True)
        time.sleep(60)


class Replay:
    """An extractor that hands back what another machine read from each download."""

    def __init__(self, contents: Dict[Tuple[str, ...], object]) -> None:
        self.contents = contents

    def extract(self, downloads) -> List[object]:
        return [self.contents.get(_files(download)) for download in downloads]


def _refusal(row: Dict, content, download, payload: Dict, read,
             exported: Optional[Dict]) -> Optional[str]:
    """Why a result read with `content` must not stand for the article's current
    `download` (its `payload`, `read` as a DownloadResult), or None."""
    pinned = row.get("download_fingerprint") or (exported or {}).get("download_fingerprint")
    if row["download_source"] != download.source:
        return MOVED
    if pinned:
        if pinned != download.fingerprint:
            return MOVED
    elif exported is None:
        return UNPINNED
    elif exported["download"] != payload:
        return MOVED
    if not read.files or any(not f.file_path.exists() for f in read.files):
        return DOWNLOAD_MISSING
    ids = [table.table_id for table in content.tables or []]
    if len(set(ids)) != len(ids):
        return REPEATED_IDS
    paths = [content.full_text_path] + [t.raw_content_path for t in content.tables or []]
    if any(path and not Path(path).exists() for path in paths):
        return FILES_MISSING
    return None


def import_(args) -> None:
    from ingestion_workflow.catalog import Catalog, Status
    from ingestion_workflow.config import load_settings
    from ingestion_workflow.models import DownloadResult, DownloadSource, ExtractedContent
    from ingestion_workflow.pipeline import Context
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages.extract import (
        ExtractStage,
        extraction_fingerprint,
        extraction_inputs,
    )

    out_dir = Path(args.out)
    pairs = _pairs(args.remap)
    rows = list(_read_jsonl(out_dir / "results.jsonl"))
    if args.ids:
        wanted = set(_ids(args.ids))
        rows = [row for row in rows if row["article_id"] in wanted]
    exported = {row["article_id"]: row for path in args.inputs for row in _read_jsonl(Path(path))}
    settings = load_settings(args.settings)
    refused: List[Tuple[str, str]] = []
    recorded: Counter = Counter()
    read_by = Counter(row.get("read_by", "unknown (before read_by)") for row in rows)
    with Catalog.open(args.catalog) as catalog:
        ctx = Context(settings, catalog)
        for start in range(0, len(rows), BATCH):
            batch = rows[start:start + BATCH]
            ids = [row["article_id"] for row in batch]
            downloads = catalog.artifacts(ids, "download")
            works, contents = [], {}
            for row in batch:
                article_id = row["article_id"]
                if not catalog.live(article_id):
                    merged = catalog.current_id(article_id) != article_id
                    refused.append((article_id, MERGED if merged else UNKNOWN))
                    continue
                ok = {s: a for s, a in downloads.get(article_id, {}).items()
                      if a.status is Status.OK}
                download = extraction_inputs(ok).get("pdf")
                if download is None:
                    refused.append((article_id, NO_DOWNLOAD))
                    continue
                payload = ctx.payload(download)
                read = DownloadResult.from_dict(payload)
                content = ExtractedContent.from_dict(_remap(row["content"], pairs))
                why = _refusal(row, content, download, payload, read, exported.get(article_id))
                if why is not None:
                    refused.append((article_id, why))
                    continue
                works.append(Work(ref=catalog.ref(article_id), source="pdf",
                                  fingerprint=extraction_fingerprint(download.source, download),
                                  upstream=download))
                contents[_files(read)] = content
            stage = ExtractStage(settings, {DownloadSource.PDF: Replay(contents)})
            outcomes = list(stage.execute(ctx, works))
            for outcome in outcomes:
                recorded["set aside" if outcome.summary.get("symbols_as_digits")
                         else outcome.status.value] += 1
            catalog.record(outcomes)
    with (out_dir / "import-refused.tsv").open("w") as fh:
        fh.writelines(f"{article_id}\t{why}\n" for article_id, why in refused)
    print(f"{len(rows):,} results in {out_dir}; recorded in {args.catalog}: "
          + ", ".join(f"{n:,} {s}" for s, n in sorted(recorded.items())))
    for why, n in Counter(why for _, why in refused).most_common():
        print(f"  refused {n:,}: {why}")
    for commit, n in read_by.most_common():
        print(f"  read by {commit}: {n:,}")


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("export")
    p.add_argument("catalog")
    p.add_argument("ids")
    p.add_argument("inputs")
    p.set_defaults(func=export)
    p = sub.add_parser("run")
    p.add_argument("inputs")
    p.add_argument("out")
    p.add_argument("--settings")
    p.add_argument("--remap", action="append", default=[])
    p.add_argument("--chunk", type=int, default=200)
    p.add_argument("--shard", help="i/n: every n-th article from the i-th")
    p.add_argument("--workers-per-gpu", type=int, default=1)
    p.add_argument("--max-workers", type=int, default=0, help="0: as many as the GPUs fit")
    p.add_argument("--min-free-gb", type=float, default=0,
                   help="memory to leave available; fewer workers, or a wait, below it")
    p.add_argument("--worker-gb", type=float, default=11)
    p.add_argument("--done-in", action="append", default=[],
                   help="another run's out dir, whose articles are not read again")
    p.set_defaults(func=run)
    p = sub.add_parser("import")
    p.add_argument("out")
    p.add_argument("catalog")
    p.add_argument("--settings")
    p.add_argument("--ids", help="file whose lines start with the article ids to import")
    p.add_argument("--inputs", action="append", default=[],
                   help="the export an older run read, which names no download fingerprint")
    p.add_argument("--remap", action="append", default=[])
    p.set_defaults(func=import_)
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
