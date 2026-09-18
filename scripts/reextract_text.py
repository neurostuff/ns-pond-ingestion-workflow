"""Re-extract article text from existing ns-pond source XML, keeping cross-references
and tables inline.

Run this after updating pubget and elsevier-coordinate-extraction to versions that
support preserve_cross_references/keep_tables. It overwrites processed/{source}/text.txt
in-place for every article that has a source XML file.

Resumable: each successful (article, source) pair is recorded, keyed by a hash of its
source XML, in a progress file next to the data directory. Re-running the script skips
anything already done under the current MIGRATION_VERSION, unless --force is given (e.g.
because the extraction logic changed and everything needs redoing, as when tables were
added to what was previously a cross-references-only pass).

Usage:
    .venv/bin/python scripts/reextract_text.py [--workers N] [--dry-run] [--force]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

NS_POND_DATA = Path("/data/alejandro/projects/ns-pond/data")

# Bump this whenever the extraction logic changes in a way that requires
# reprocessing articles already recorded in the progress file.
MIGRATION_VERSION = "keep-refs-and-tables-v1"


def _reextract_pubget(article_dir: Path) -> tuple[bool, str]:
    source_xml = article_dir / "source" / "pubget" / "article.xml"
    article_input_dir = source_xml.parent
    text_path = article_dir / "processed" / "pubget" / "text.txt"

    if not source_xml.exists():
        return False, "no source XML"

    try:
        from lxml import etree
        from pubget._text import _insert_tables
        from pubget._utils import load_stylesheet

        article_tree = etree.parse(str(source_xml))
        stylesheet = load_stylesheet("text_extraction.xsl")
        transformed = stylesheet(
            article_tree,
            **{
                "preserve-crossrefs": etree.XSLT.strparam("true"),
                "keep-tables": etree.XSLT.strparam("true"),
            },
        )

        text_parts = []
        for field in ("title", "keywords", "abstract", "body"):
            elem = transformed.find(field)
            if elem is not None and elem.text:
                part = elem.text.strip()
                if field == "body" and part:
                    part = _insert_tables(part, article_input_dir)
                if part:
                    text_parts.append(part)
        full_text = "\n\n".join(text_parts)

        text_path.parent.mkdir(parents=True, exist_ok=True)
        text_path.write_text(full_text, encoding="utf-8")
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _reextract_elsevier(article_dir: Path) -> tuple[bool, str]:
    source_xml = article_dir / "source" / "elsevier" / "content.xml"
    metadata_json = article_dir / "source" / "elsevier" / "metadata.json"
    text_path = article_dir / "processed" / "elsevier" / "text.txt"

    if not source_xml.exists():
        return False, "no source XML"

    try:
        from elsevier_coordinate_extraction.extract.text import save_article_text
        from elsevier_coordinate_extraction.types import ArticleContent

        payload = source_xml.read_bytes()
        metadata: dict = {}
        if metadata_json.exists():
            try:
                metadata = json.loads(metadata_json.read_text(encoding="utf-8"))
            except Exception:
                pass

        article_content = ArticleContent(
            doi=metadata.get("doi") or "",
            payload=payload,
            content_type="application/xml",
            format="xml",
            retrieved_at=metadata.get("retrieved_at") or "",
            metadata=metadata,
        )

        text_path.parent.mkdir(parents=True, exist_ok=True)
        save_article_text(
            article_content,
            text_path.parent,
            stem="text",
            preserve_cross_references=True,
            keep_tables=True,
        )
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _source_hash(source_xml: Path) -> str:
    return hashlib.sha256(source_xml.read_bytes()).hexdigest()


def _load_progress(progress_path: Path) -> dict[tuple[str, str], tuple[str, str]]:
    """Map (article_dir_name, source) -> (source_hash, migration_version)."""
    done: dict[tuple[str, str], tuple[str, str]] = {}
    if not progress_path.exists():
        return done
    with progress_path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                key = (record["article_dir"], record["source"])
                done[key] = (record["source_hash"], record["version"])
            except (json.JSONDecodeError, KeyError):
                continue
    return done


def _process_article(args: tuple[str, Path, str]) -> tuple[str, Path, str, bool, str]:
    source, article_dir, source_hash = args
    if source == "pubget":
        ok, msg = _reextract_pubget(article_dir)
    else:
        ok, msg = _reextract_elsevier(article_dir)
    return source, article_dir, source_hash, ok, msg


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess every article, ignoring the progress file.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=NS_POND_DATA,
        help="Root ns-pond data directory",
    )
    parser.add_argument(
        "--progress-file",
        type=Path,
        default=None,
        help="Where to record completed (article, source) pairs. "
        "Defaults to <data-dir>/.reextract_progress.jsonl",
    )
    opts = parser.parse_args()

    data_dir: Path = opts.data_dir
    progress_path: Path = opts.progress_file or (data_dir / ".reextract_progress.jsonl")
    done = {} if opts.force else _load_progress(progress_path)

    article_dirs = sorted(d for d in data_dir.iterdir() if d.is_dir())
    logger.info("Scanning %d article directories under %s", len(article_dirs), data_dir)

    candidates: list[tuple[str, Path]] = []
    for d in article_dirs:
        if (d / "source" / "pubget" / "article.xml").exists():
            candidates.append(("pubget", d))
        if (d / "source" / "elsevier" / "content.xml").exists():
            candidates.append(("elsevier", d))

    tasks: list[tuple[str, Path, str]] = []
    skipped = 0
    for source, article_dir in candidates:
        source_xml = (
            article_dir / "source" / "pubget" / "article.xml"
            if source == "pubget"
            else article_dir / "source" / "elsevier" / "content.xml"
        )
        source_hash = _source_hash(source_xml)
        prior = done.get((article_dir.name, source))
        if prior is not None and prior == (source_hash, MIGRATION_VERSION):
            skipped += 1
            continue
        tasks.append((source, article_dir, source_hash))

    logger.info(
        "Found %d candidates (%d already done under %s, %d to process)",
        len(candidates),
        skipped,
        MIGRATION_VERSION,
        len(tasks),
    )

    if opts.dry_run:
        logger.info("Dry-run: exiting without writing.")
        return

    total = len(tasks)
    ok_count = 0
    fail_count = 0

    with progress_path.open("a", encoding="utf-8") as progress_file, ProcessPoolExecutor(
        max_workers=opts.workers
    ) as pool:
        futures = {pool.submit(_process_article, t): t for t in tasks}
        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            try:
                source, article_dir, source_hash, ok, msg = future.result()
            except Exception as exc:
                source, article_dir, source_hash = futures[future]
                ok, msg = False, str(exc)

            if ok:
                ok_count += 1
                progress_file.write(
                    json.dumps(
                        {
                            "article_dir": article_dir.name,
                            "source": source,
                            "source_hash": source_hash,
                            "version": MIGRATION_VERSION,
                        }
                    )
                    + "\n"
                )
                progress_file.flush()
            else:
                fail_count += 1
                logger.warning("[%s] %s: %s", source, article_dir.name, msg)

            if done_count % 1000 == 0 or done_count == total:
                logger.info(
                    "Progress: %d/%d  ok=%d  failed=%d", done_count, total, ok_count, fail_count
                )

    logger.info("Done. ok=%d  failed=%d  skipped=%d", ok_count, fail_count, skipped)
    if fail_count:
        sys.exit(1)


if __name__ == "__main__":
    main()
