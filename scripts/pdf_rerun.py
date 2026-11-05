"""Read stored PDFs again with `pdf_engines` (MinerU, then Docling), away from the catalog.

The extract stage reads a PDF where the catalog lives; this reads it on any GPU machine
the PDFs are copied to, and records the result in a catalog afterwards:

    python scripts/pdf_rerun.py export <catalog root> <article ids file> <inputs.jsonl>
    python scripts/pdf_rerun.py run <inputs.jsonl> <out dir> [--remap OLD=NEW] [--chunk N]
    python scripts/pdf_rerun.py import <out dir> <catalog root> [--remap OLD=NEW]

`export` writes each article's pdf download payload (the one the extract stage would
read, `extraction_inputs`), opening the catalog read-only. `run` reads them with
`PdfExtractor.extract`, as the extract stage does, `--chunk` at a time, appending each
article's `ExtractedContent` and the chunk's wall-clock to `<out dir>/results.jsonl`; a
second run reads only the articles not there yet, nor in any `--done-in` out dir (another
machine's run of the same shard). `--shard i/n` takes every n-th input from the i-th,
the same articles however many are done. `--remap` rewrites a path prefix: in
`run`, where the PDFs were copied to; in `import`, where the out dir was copied back to.
`--workers-per-gpu` runs more than one MinerU worker on a card with room for them.

`import` records each result as the extract stage would (`ExtractStage._outcome`), under
the fingerprint of the download it read, so the stages after extract see a new
extraction. Point it at a copy of the catalog, never the production one.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

logger = logging.getLogger("pdf_rerun")


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


def export(args) -> None:
    from ingestion_workflow.catalog import BlobStore
    from ingestion_workflow.pipeline.stages.extract import PDF_SOURCES

    root = Path(args.catalog)
    conn = sqlite3.connect(f"file:{root / 'catalog.sqlite'}?mode=ro", uri=True)
    blobs = BlobStore(root / "blobs")
    ids = [line.strip() for line in Path(args.ids).read_text().split() if line.strip()]
    written = 0
    with Path(args.inputs).open("w") as out:
        for article_id in ids:
            rows = {source: (status, blob) for source, status, blob in conn.execute(
                "SELECT source, status, blob FROM artifacts WHERE article_id=? AND "
                "stage='download' AND source IN (%s)" % ",".join("?" * len(PDF_SOURCES)),
                (article_id, *PDF_SOURCES))}
            # extraction_inputs: the first PDF source whose download is OK.
            source = next((s for s in PDF_SOURCES if rows.get(s, ("",))[0] == "ok"), None)
            if source is None:
                logger.warning("%s: no OK pdf download", article_id)
                continue
            out.write(json.dumps({"article_id": article_id, "download_source": source,
                                  "download": blobs.get(rows[source][1])}) + "\n")
            written += 1
    print(f"wrote {written} of {len(ids)} articles to {args.inputs}")


def run(args) -> None:
    from ingestion_workflow.config import load_settings
    from ingestion_workflow.extractors import mineru_convert
    from ingestion_workflow.extractors.pdf_extractor import PdfExtractor
    from ingestion_workflow.models import DownloadResult

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

    settings = load_settings(args.settings, {"data_root": out_dir})
    if args.workers_per_gpu > 1:
        found = mineru_convert.mineru_gpus
        mineru_convert.mineru_gpus = lambda: found() * args.workers_per_gpu
    extractor = PdfExtractor(settings)

    with results.open("a") as fh:
        for start in range(0, len(todo), args.chunk):
            chunk = todo[start:start + args.chunk]
            downloads = [DownloadResult.from_dict(_remap(row["download"], pairs)) for row in chunk]
            began = time.monotonic()
            contents = extractor.extract(downloads)
            seconds = round(time.monotonic() - began, 1)
            for row, content in zip(chunk, contents):
                fh.write(json.dumps({"article_id": row["article_id"],
                                     "download_source": row["download_source"],
                                     "chunk_started": began, "chunk_seconds": seconds,
                                     "chunk_size": len(chunk),
                                     "content": content.to_dict()}) + "\n")
            fh.flush()
            print(f"{start + len(chunk)}/{len(todo)}: {len(chunk)} in {seconds}s", flush=True)


def import_(args) -> None:
    from ingestion_workflow.catalog import Catalog
    from ingestion_workflow.config import load_settings
    from ingestion_workflow.models import ExtractedContent
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages.extract import ExtractStage, extraction_fingerprint

    pairs = _pairs(args.remap)
    catalog = Catalog.open(args.catalog)
    stage = ExtractStage(load_settings(args.settings))
    rows = list(_read_jsonl(Path(args.out) / "results.jsonl"))
    outcomes = []
    for row in rows:
        article_id = catalog.current_id(row["article_id"])
        download = catalog.artifact(article_id, "download", row["download_source"])
        work = Work(ref=catalog.ref(article_id), source="pdf",
                    fingerprint=extraction_fingerprint(download.source, download),
                    upstream=download)
        content = ExtractedContent.from_dict(_remap(row["content"], pairs))
        outcome = stage._outcome(work, "pdf", content)
        if download.source != "pdf":
            outcome.summary["download_source"] = download.source
        outcomes.append(outcome)
        if len(outcomes) >= 500:
            catalog.record(outcomes)
            outcomes = []
    catalog.record(outcomes)
    print(f"recorded {len(rows)} pdf extractions in {args.catalog}")


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
    p.add_argument("--done-in", action="append", default=[],
                   help="another run's out dir, whose articles are not read again")
    p.set_defaults(func=run)
    p = sub.add_parser("import")
    p.add_argument("out")
    p.add_argument("catalog")
    p.add_argument("--settings")
    p.add_argument("--remap", action="append", default=[])
    p.set_defaults(func=import_)
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
